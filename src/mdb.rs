/**********************************************
  > File Name		: mdb.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 15 Dec 2021 04:08:42 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::sync::{Arc, Weak, atomic::{AtomicU8, Ordering}, Mutex, MutexGuard, RwLock};
use std::fs::{File, OpenOptions};
use memmap::{self, MmapMut};
use std::collections::{VecDeque};
use std::thread;
use std::mem::{self, size_of};
use crate::errors::Errors;
//use std::io;
use std::os::unix::prelude::FileExt;
use std::ptr::{self, NonNull};
use std::alloc::{alloc, dealloc, Layout};
use std::process;
use std::fmt;
use std::cell::{RefCell, Ref};

use crate::consts;
use crate::txn::{Txn, ReadTxnInfo, Reader, unit};
use crate::{info, debug, error, jump_head, jump_head_mut};

pub type Pageno = usize;
type Ptr = [u8];
type Cmpfunc = fn(v1: Val, v2: Val) -> i32;


struct Val {
    size: usize,
    data: Ptr
}

/**
 * Structure of a memory page.
 * Includes header, pointer array, empty space and heap area.
 * 
 * As the order of PageHead fields does matter, I have to use #[repr(C)]
 * so the compiler won't disorder PageHead fields. But alignment is still
 * optimized.
 */
/// page bounds
#[derive(Debug)]
struct PageBounds {
    upper_bound: usize,
    lower_bound: usize,
}

#[derive(Debug)]
struct PageHead {
    pageno: Pageno,
    page_flags: u32,
    page_bounds: PageBounds,
    /// size of overflow pages
    overflow_pages: usize,
}

#[derive(Debug)]
struct DBHead {
    version: u32,
    magic: u32,
    page_size: usize, // os memory page size, in C, got by sysconf(_SC_PAGE_SIZE)
    flags: u32,
    /// size of map region
    mapsize: usize,
}


/**
 * DBStat store information mainly about the database B+ tree.
 * Stored in the file header.
 */
#[derive(Debug, Clone, Copy)]
pub struct DBStat {
    page_size: usize,
    depth: usize,
    branch_pages: usize,
    leaf_pages: usize,
    overflow_pages: usize,
    entries: usize
}

#[derive(Debug, Clone, Copy)]
pub struct DBMetaData {
    db_stat: DBStat,
    root: Pageno,
    ///last used page in file
    last_page: Pageno, 
    ///last commited transaction id.
    last_txn_id: u32, 
}

struct DB {
    md_root: Pageno,
    cmp_func: Cmpfunc,
    db_head: DBHead
}

#[derive(Debug)]
pub struct Env<'a> {
    env_flags: u32,
    fd: Option<File>,
    mmap: Option<MmapMut>,
    w_txn: RefCell<Option<Weak<Txn<'a>>>>, //current write transaction
    env_head: Option<DBHead>,
    env_meta: Mutex<Option<DBMetaData>>,
    read_txn_info: Mutex<ReadTxnInfo>,
    write_mutex: Mutex<i32>,
    txn_id: Mutex<u32>, //increase when begin a new write transaction.
    w_txn_first_page: Option<RwLock<Pageno>>
}


impl DBStat {
    fn new() -> Self {
        Self {
            page_size: 0,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0
        }
    }
}


impl DB {
    
}

impl DBHead {
    fn new() -> Self {
        Self {
            version: 0,
            magic: 0,
            page_size: 0,
            flags: 0,
            mapsize: 0
        }
    }
}


impl Env<'_> {
    /**
     * create a new environment
     */
    pub fn new() -> Self {
        Self {
            env_flags: 0,
            fd: None,
            mmap: None,
            w_txn: RefCell::new(None),
            env_head: Some(DBHead::new()),
            env_meta: Mutex::new(None),//later to read in with env_open().
            read_txn_info: Mutex::new(ReadTxnInfo::new()),
            write_mutex: Mutex::new(0),
            txn_id: Mutex::new(0),
            w_txn_first_page: Some(RwLock::new(consts::P_INVALID))
        }
    }

    pub fn get_root_pageno(&self) -> Pageno {
        self.env_meta.lock().unwrap().unwrap().root
    }

    pub fn get_last_page(&self) -> Pageno {
        self.env_meta.lock().unwrap().unwrap().last_page
    }

    pub fn get_meta(&self) -> Option<DBMetaData> {
        *self.env_meta.lock().unwrap()
    }

    //pub fn get_txn_info<'a>(&'a self) -> Option<Ref<'a, ReadTxnInfo>> {
        //Some(self.txn_info.as_ref().unwrap().borrow())
    //}

    pub fn add_txn_id(&self) {
        (*self.txn_id.lock().unwrap()) += 1;
    }

    pub fn get_txn_id(&self) -> u32 {
        *self.txn_id.lock().unwrap()
    }

    pub fn lock_w_mutex<'a>(&'a self) -> MutexGuard<'a, i32> {
        self.write_mutex.lock().unwrap()
    }

    pub fn add_reader(&self, pid: u32, tid: thread::ThreadId) -> Result<Reader, Errors> {
        let mut mg = self.read_txn_info.lock().unwrap();

        let reader = {
            let readers = &mut mg.readers;
            let mut i: usize = 0;
            while i < consts::MAX_READERS {
                if readers[i].pid == 0 {
                    readers[i].pid = pid;
                    readers[i].tid = tid;
                    break;
                }
            }

            if i == consts::MAX_READERS {
                return Err(Errors::ReadersMaxedOut);
            } else {
                readers[i]
            }
        };

        mg.num_readers += 1;
        Ok(reader)
    }

    /**
     * Open a database file.
     * Create mode: create database if not exist.
     */
    pub fn env_open(&mut self, path: &str, flags: u32, mode: u32) -> Result<(), Errors> {
        if mode & consts::READ_ONLY != 0 && mode & consts::READ_WRITE != 0 {
            return Err(Errors::InvalidFlag(mode));
        }

        let read = true;
        let write = mode & consts::READ_WRITE != 0;
        let create = mode & consts::CREATE != 0;

        let fd = match OpenOptions::new()
            .read(read)
            .write(write)
            .create(create)
            .open(path) {
                Err(e) => {
                    return Err(Errors::StdFileError(e));
                },
                Ok(v) => v
        };
        
        self.fd = Some(fd);
        
        self.env_flags = flags;

        let mut new_env = false;
        match self.env_read_header() {
            Ok(_) => {},
            Err(Errors::EmptyFile) => {
                new_env = true;
            },
            Err(e) => {
                return Err(e);
            }
        }

        // as memmap doesn't allow 0-length file mapped
        // so init a file first if creating a new env
        if new_env {
            debug!("Creating new database file: {}", path);
            self.env_write_header()?;
            self.env_read_header()?;

            self.env_init_meta()?;
        }

        self.mmap = Some(unsafe {
            match memmap::MmapMut::map_mut(self.fd.as_ref().unwrap()) {
                Err(e) => {
                    return Err(Errors::MmapError(e));
                },
                Ok(v) => v
            }
        });

        debug!("mapped length: {}", self.mmap.as_ref().unwrap().len());

        self.env_read_meta()?;

        Ok(())
    }

    /**
     * Read database file header.
     */
    pub fn env_read_header(&mut self) -> Result<(), Errors> {
        if let None = self.fd {
            return Err(Errors::UnexpectedNoneValue(String::from("environment file handle is None")));
        }

        let mut buf = [0 as u8; consts::PAGE_SIZE];
        match self.fd.as_ref().unwrap().read_at(&mut buf, 0) {
            Err(e) => {
                return Err(Errors::StdReadError(e));
            },
            Ok(read_size) => {
                if read_size == 0 {
                    return Err(Errors::EmptyFile);
                } else if read_size < consts::PAGE_SIZE {
                    return Err(Errors::ShortRead(read_size));
                } 
            }
        }

        let page_ptr: *const u8 = buf.as_ptr();

        let head_page: &PageHead = unsafe {&*(page_ptr as *const PageHead)};
        assert!(head_page.page_flags & consts::P_HEAD != 0);

        //let header: &DBHead = unsafe { // header of database
            //&*(page_ptr.offset(size_of::<Page>() as isize) as *const DBHead)
        //};
        let header: &DBHead = jump_head!(page_ptr, DBHead);

        if header.version > consts::VERSION {
            return Err(Errors::InvalidVersion(header.version));
        } else if header.magic != consts::MAGIC {
            return Err(Errors::InvalidMagic(header.magic));
        }

        unsafe {ptr::copy(page_ptr.offset(size_of::<PageHead>() as isize) as *const DBHead, self.env_head.as_mut().unwrap() as *mut DBHead, 1)};

        assert_eq!(self.env_head.as_ref().unwrap().magic, consts::MAGIC);
        Ok(())
    }

    /**
     * When creating a new env, need to write a header to file first before mapping.
     */
    fn env_write_header(&mut self) -> Result<(), Errors> {
        let mut head_buf = [0 as u8; consts::PAGE_SIZE];
        //init page head
        unsafe {
            let page = &mut *(head_buf.as_mut_ptr() as *mut PageHead);
            page.pageno = 0;
            page.page_flags = consts::P_HEAD;
            page.page_bounds = PageBounds {
                upper_bound: 0,
                lower_bound: 0
            };
            page.overflow_pages = 0;
        }

        unsafe {
            let head = &mut *(head_buf.as_mut_ptr().offset(size_of::<PageHead>() as isize) as *mut DBHead);
            head.version = consts::VERSION;
            head.magic = consts::MAGIC;
            head.flags = 0;
            head.page_size = consts::PAGE_SIZE;
            head.mapsize = 0;
        }

        match self.fd.as_ref().unwrap().write_at(head_buf.as_ref(), 0) {
            Err(e) => {
                return Err(Errors::StdWriteError(e));
            },
            Ok(write_size) => {
                if write_size < head_buf.len() {
                    return Err(Errors::ShortWrite(write_size));
                }
            }
        }
        Ok(())
    }

    /**
     * Initialize metadata page, with two toggle pages.
     */
    fn env_init_meta(&mut self) -> Result<(), Errors> {
        let page_size: usize = self.env_head.as_ref().unwrap().page_size;
        let layout = match Layout::from_size_align(page_size*2, size_of::<u8>()) {
            Ok(v) => v,
            Err(e) => {
                return Err(Errors::LayoutError(e));
            }
        };
        let page_ptr1 = unsafe {alloc(layout)};
        assert!(!page_ptr1.is_null());
        let page_ptr2 = unsafe {page_ptr1.offset(page_size as isize)};

        let page1: &mut PageHead = unsafe {
            &mut *(page_ptr1 as *mut PageHead)
        };

        let page2: &mut PageHead = unsafe {
            &mut *(page_ptr2 as *mut PageHead)
        };

        page1.pageno = 1;
        page1.page_flags = consts::P_META;

        page2.pageno = 2;
        page2.page_flags = consts::P_META;

        let meta1: &mut DBMetaData = jump_head_mut!(page_ptr1, DBMetaData);
        let meta2: &mut DBMetaData = jump_head_mut!(page_ptr2, DBMetaData);

        meta1.db_stat = DBStat {
            page_size: self.env_head.as_ref().unwrap().page_size,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0
        };
        meta1.root = consts::P_INVALID;
        meta1.last_page = 2;
        meta1.last_txn_id = 0;

        meta2.db_stat = DBStat {
            page_size: self.env_head.as_ref().unwrap().page_size,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0
        };
        meta2.root = consts::P_INVALID;
        meta2.last_page = 2;
        meta2.last_txn_id = 0;

        let buf = unsafe {
            std::slice::from_raw_parts(page_ptr1, page_size*2)
        };

        match self.fd.as_ref().unwrap().write_at(&buf, page_size as u64) {
            Err(e) => {
                return Err(Errors::StdWriteError(e));
            },
            Ok(write_size) => {
                if write_size < page_size*2 {
                    return Err(Errors::ShortWrite(write_size));
                }
            }
        }

        unsafe {dealloc(page_ptr1, layout)};//always remember to dealloc memory allocated by alloc
        Ok(())
    }

    pub fn env_read_meta(&self) -> Result<(), Errors> {
        let page_ptr1 = self.get_page(1)?;
        let page_ptr2 = self.get_page(2)?;
        debug!("page_ptr1: {:?}", page_ptr1);
        debug!("page_ptr2: {:?}", page_ptr2);
        
        let page1: &PageHead = unsafe {
            &*(page_ptr1 as *const PageHead)
        };
        assert!(page1.page_flags & consts::P_META != 0);
        
        let page2: &PageHead = unsafe {
            &*(page_ptr2 as *const PageHead)
        };
        assert!(page2.page_flags & consts::P_META != 0);

        let meta1: &DBMetaData = jump_head!(page_ptr1, DBMetaData);
        let meta2: &DBMetaData = jump_head!(page_ptr2, DBMetaData);

        debug!("meta1: {:?}", meta1);
        debug!("meta2: {:?}", meta2);

        *self.env_meta.lock().unwrap() = Some(
            if meta2.last_txn_id > meta1.last_txn_id {
                debug!("Using meta page 2");
                *meta2
            } else {
                debug!("Using meta page 1");
                *meta1
            }
        );
        Ok(())
    }

    pub fn get_page(&self, pageno: Pageno) -> Result<*mut u8, Errors> {
        let w_txn_first_page = *self.w_txn_first_page.as_ref().unwrap().read().unwrap();
        if w_txn_first_page != consts::P_INVALID && pageno >= w_txn_first_page {
            Ok(ptr::null_mut())
        } else {
            //let map_ptr: *mut u8 = self.mmap.as_mut().unwrap().as_mut().as_mut_ptr();
            let map_ptr: *mut u8 = self.mmap.as_ref().unwrap().as_ptr() as *mut u8;
            let page_size = self.env_head.as_ref().unwrap().page_size;
            Ok(unsafe {map_ptr.offset((pageno * page_size) as isize)})
        }
    }
}

impl<'a> Env<'a> {
    pub fn set_w_txn(&'a self, w_txn: Option<Weak<Txn<'a>>>) {
        // can't set write transaction when there's already a write transaction.
        // can't set empty write transaction when it's already empty
        assert_ne!(self.w_txn.borrow().is_none(), w_txn.is_none());

        self.w_txn.replace(w_txn);
    }
}
