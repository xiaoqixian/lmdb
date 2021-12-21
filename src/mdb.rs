/**********************************************
  > File Name		: mdb.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 15 Dec 2021 04:08:42 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::sync::{Arc, Weak, atomic::{AtomicU8, Ordering}, Mutex, MutexGuard};
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
use lock_api;

use crate::{info, debug, error, jump_head, jump_head_mut};

type Pageno = usize;
type Ptr = [u8];
type Cmpfunc = fn(v1: Val, v2: Val) -> i32;

macro_rules! offset_of {
    ($ty:ty, $field:ident) => {
        unsafe {&(*(0 as *const $ty)).$field as *const _ as isize}
    }
}

mod consts {
    ///meta data consts
    pub const VERSION: u32 = 1;
    pub const MAGIC: u32 = 0xBEEFC0DE;
    pub const MAX_KEY_SIZE: usize = 255;

    ///page flags and consts
    pub const P_INVALID: super::Pageno = std::usize::MAX;//invalid page number
    pub const P_HEAD: u32 = 0x01;
    pub const P_META: u32 = 0x02;
    pub const P_BRANCH: u32 = 0x04;
    pub const P_LEAF: u32 = 0x08;
    pub const P_DIRTY: u32 = 0x10;
    pub const P_OVERFLOW: u32 = 0x20;
    pub const PAGE_SIZE: usize = 4096;//4096 bytes page size on my machine.

    ///file flags
    pub const READ_ONLY: u32 = 0x1;
    pub const READ_WRITE: u32 = 0x2;
    pub const CREATE: u32 = 0x4;

    pub const MAX_READERS: usize = 126;
}

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
struct PageBounds {
    upper_bound: usize,
    lower_bound: usize,
}

struct PageHead {
    pageno: Pageno,
    page_flags: u32,
    page_bounds: PageBounds,
    /// size of overflow pages
    overflow_pages: usize,
}

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
struct DBStat {
    pub page_size: usize,
    pub depth: usize,
    pub branch_pages: usize,
    pub leaf_pages: usize,
    pub overflow_pages: usize,
    pub entries: usize
}

#[derive(Debug, Clone, Copy)]
struct DBMetaData {
    pub db_stat: DBStat,
    pub root: Pageno,
    ///last used page in file
    pub last_page: Pageno, 
    ///last commited transaction id.
    pub last_txn_id: u32, 
}

struct DB {
    md_root: Pageno,
    cmp_func: Cmpfunc,
    db_head: DBHead
}

struct Env<'a> {
    env_flags: u32,
    fd: Option<File>,
    mmap: Option<MmapMut>,
    w_txn: Option<Weak<Txn<'a>>>, //current write transaction
    env_head: Option<DBHead>,
    env_meta: Option<DBMetaData>,
    txn_info: Option<TxnInfo>,
}

/**
 * Information for managing transactions.
 */
struct TxnInfo {
    txn_id: u32, 
    ///write transaction atomic value, only when the value is 0, the write transaction
    ///can be begined.
    write_mutex: Mutex<i32>,
    read_mutex: Mutex<i32>,
    readers: [Reader; consts::MAX_READERS]
}

union unit {
    dirty_queue: mem::ManuallyDrop<VecDeque<NonNull<*mut u8>>>,
    reader: Reader //Reader record read thread information
}
struct Txn<'a> {
    txn_id: u32,
    txn_root: Mutex<Pageno>,
    txn_next_pgno: Mutex<Pageno>,
    txn_first_pgno: Mutex<Pageno>,
    env: Arc<Env<'a>>,
    write_lock: Option<MutexGuard<'a, i32>>,
    u: unit, //if a write transaction, it's dirty_queue; if a read transaction, it's Reader
    flags: Mutex<u32>
}

#[derive(Copy, Clone)]
struct Reader {
    tid: thread::ThreadId,
    pid: u32,
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

impl Env<'_> {
    /**
     * create a new environment
     */
    fn new() -> Self {
        Self {
            env_flags: 0,
            fd: None,
            mmap: None,
            w_txn: None,
            env_head: Some(DBHead {
                version: 0,
                magic: 0,
                page_size: 0,
                flags: 0,
                mapsize: 0,
            }),
            env_meta: None,//later to read in with env_open().
            txn_info: Some(TxnInfo {
                txn_id: 0,
                write_mutex: Mutex::new(0),
                read_mutex: Mutex::new(0),
                readers: [Reader {tid: thread::current().id(), pid: 0}; consts::MAX_READERS] //if reader is null identified by pid.
            })
        }
    }

    /**
     * Open a database file.
     * Create mode: create database if not exist.
     */
    fn env_open(&mut self, path: &str, flags: u32, mode: u32) -> Result<(), Errors> {
        if mode & consts::READ_ONLY != 0 && mode & consts::READ_WRITE != 0 {
            return Err(Errors::InvalidFlag(mode));
        }

        let read = true;
        let write = mode & consts::READ_WRITE != 0;
        let create = mode & consts::CREATE != 0;

        let mut fd = match OpenOptions::new()
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
/*        match self.env_meta {*/
            /*Some(_) => {*/
                /*panic!("new environment should not have any metadata");*/
            /*},*/
            /*None => {*/
                /*self.env_meta = Some(DBMetaData {*/
                    /*db_stat: DBStat::new(),*/
                    /*root: consts::P_INVALID,*/
                    /*last_page: 2, //first 2 pages are for sure: P_HEAD & P_META*/
                    /*txn_id: 0*/
                /*});*/
            /*}*/
        /*}*/

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

        /// as memmap doesn't allow 0-length file mapped
        /// so init a file first if creating a new env
        if new_env {
            debug!("Creating new database file: {}", path);
            self.env_write_header()?;
            self.env_read_header()?;

            self.env_init_meta()?;
        }

        self.env_read_meta()?;

        self.mmap = Some(unsafe {
            match memmap::MmapMut::map_mut(self.fd.as_ref().unwrap()) {
                Err(e) => {
                    return Err(Errors::MmapError(e));
                },
                Ok(v) => v
            }
        });

        Ok(())
    }

    /**
     * Read database file header.
     */
    fn env_read_header(&mut self) -> Result<(), Errors> {
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
        assert!(head_page.page_flags & consts::P_HEAD == 0);

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
        let head = DBHead {
            version: consts::VERSION,
            magic: consts::MAGIC,
            flags: 0,
            page_size: consts::PAGE_SIZE,
            mapsize: 0
        };
        
        let head_buf = unsafe {
            std::slice::from_raw_parts(&head as *const _ as *const u8, size_of::<DBHead>())
        };

        match self.fd.as_ref().unwrap().write_at(head_buf.as_ref(), 0) {
            Err(e) => {
                return Err(Errors::StdWriteError(e));
            },
            Ok(write_size) => {
                if write_size < size_of::<DBHead>() {
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

    fn env_read_meta(&mut self) -> Result<(), Errors> {
        let page_ptr1 = self.get_page(1)?;
        let page_ptr2 = self.get_page(2)?;
        
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

        self.env_meta = Some(
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

    pub fn get_page(&mut self, pageno: Pageno) -> Result<*mut u8, Errors> {
        if self.w_txn.is_some() {
            let dirty_page = match self.w_txn.as_ref().unwrap().upgrade() {
                None => {
                    panic!("env write transaction is some but dropped");
                },
                Some(v) => {
                    pageno >= *v.txn_first_pgno.lock().unwrap()
                }
            };
            Ok(ptr::null_mut()) //temporary
        } else {
            let map_ptr: *mut u8 = self.mmap.as_mut().unwrap().as_mut().as_mut_ptr();
            let page_size = self.env_head.as_ref().unwrap().page_size;
            Ok(unsafe {map_ptr.offset((pageno * page_size) as isize)})
        }
    }
}


impl<'a> Txn<'a> {
    pub fn new(env: &'a Arc<Env<'a>>, read_only: bool) -> Result<Arc<Self>, Errors> {
        //let env_mut_ref: &mut Env = unsafe {
            //&mut *(Arc::into_raw(env.clone()) as *mut Env)
        //};
        if !read_only {
            debug!("try to lock write_mutex");
            //while env_mut_ref.txn_info.as_mut().unwrap().write_mutex.compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                
            //}
            let mutex_guard = env.txn_info.as_ref().unwrap().write_mutex.lock().unwrap();
            debug!("write_mutex unlocked");

            unsafe {
                let env_ptr = Arc::as_ptr(env) as *mut Env;
                (*env_ptr).txn_info.as_mut().unwrap().txn_id += 1;
                (*env_ptr).env_read_meta()?;
            }

            assert!(env.w_txn.is_none());
            
            //always read metadata before begin a new transaction
            //read metadata also won't affect read transactions because of toggle meta pages. 

            let txn: Arc<Self> = Arc::new(Self {
                txn_id: env.txn_info.as_ref().unwrap().txn_id,
                txn_root: Mutex::new(env.env_meta.as_ref().unwrap().root),
                txn_next_pgno: Mutex::new(env.env_meta.as_ref().unwrap().last_page+1),
                txn_first_pgno: Mutex::new(env.env_meta.as_ref().unwrap().last_page+1),
                env: env.clone(),
                write_lock: Some(mutex_guard),
                u: unit {
                    dirty_queue: mem::ManuallyDrop::new(VecDeque::new())
                },
                flags: Mutex::new(0)
            });

            //env_mut_ref.w_txn = Some(Arc::downgrade(&txn));
            unsafe {
                (*(Arc::as_ptr(env) as *mut Env)).w_txn = Some(Arc::downgrade(&txn));
            }

            debug!("begin a write transaction {} on root {}", &txn.txn_id, *txn.txn_root.lock().unwrap());
            Ok(txn)
        } else {
            //I don't find pthread_get_specific like function in rust,
            //so we have to iterate all readers to make sure that this is a new thread.
            let reader = {
                let readers = unsafe {&mut (*(Arc::as_ptr(env) as *mut Env)).txn_info.as_mut().unwrap().readers};
                let mut i: usize = 0;
                //let read_guard = env_mut_ref.txn_info.as_mut().unwrap().read_mutex.lock().unwrap();
                for i in 0..consts::MAX_READERS {
                    if readers[i].pid == 0 {
                        readers[i].pid = process::id();
                        readers[i].tid = thread::current().id();
                    }
                }
                readers[i]
            };

            unsafe {
                (*(Arc::as_ptr(env) as *mut Env)).env_read_meta()?;
            }

            let txn_info = env.txn_info.as_ref().unwrap();
            let env_meta = env.env_meta.as_ref().unwrap();

            let txn = Arc::new(Self {
                txn_id: txn_info.txn_id,
                txn_root: Mutex::new(env_meta.root),
                txn_next_pgno: Mutex::new(env_meta.last_page),
                txn_first_pgno: Mutex::new(env_meta.last_page),
                env: env.clone(),
                write_lock: None,
                u: unit {
                    reader: reader
                },
                flags: Mutex::new(consts::READ_ONLY),
            });

            debug!("begin a read only transaction {} on root {}", &txn.txn_id, *txn.txn_root.lock().unwrap());
            Ok(txn)
        }
    }           
}
