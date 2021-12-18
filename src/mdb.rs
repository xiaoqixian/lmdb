/**********************************************
  > File Name		: mdb.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 15 Dec 2021 04:08:42 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::sync::{Arc, Mutex};
use std::fs::{File, OpenOptions};
use memmap::{self, MmapMut};
use std::collections::{VecDeque};
use std::thread;
use std::mem::{self, size_of};
use crate::errors::Errors;
use std::io;
use std::os::unix::prelude::FileExt;
use std::ptr::{self,};

use crate::{info, debug, error, jump_head};

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
}

struct Val {
    size: usize,
    data: Ptr
}

/**
 * Structure of a memory page.
 * Includes header, pointer array, empty space and heap area.
 * 
 * As the order of Page fields does matter, I have to use #[repr(C)]
 * so the compiler won't disorder Page fields. But alignment is still
 * optimized.
 */
/// page bounds
struct PageBounds {
    upper_bound: usize,
    lower_bound: usize
}

#[repr(C)]
struct Page {
    pageno: Pageno,
    page_flags: u32,
    page_bounds: PageBounds,
    /// size of overflow pages
    overflow_pages: usize,
    //ptrs: *mut u8 // ptrs are for accessing left space of a page, so pointer type doesn't really matter, and that's why I have to keep Page fields in order.
}

struct DBHead {
    version: u32,
    magic: u32,
    page_size: usize, // os memory page size, in C, got by sysconf(_SC_PAGE_SIZE)
    flags: u32,
    /// size of map region
    mapsize: usize
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
    pub txn_id: u32, 
}

struct DB {
    md_root: Pageno,
    cmp_func: Cmpfunc,
    db_head: DBHead
}

struct Env {
    env_flags: u32,
    fd: Option<File>,
    mmap: Option<MmapMut>,
    w_txn: Option<Txn>, //current write transaction
    env_head: Option<DBHead>,
    env_meta: Option<DBMetaData>,
    txn_info: Option<TxnInfo>,
}

/**
 * Information for managing transactions.
 */
struct TxnInfo {
    ///write transaction mutex, only one write transaction allowed at a time.
    write_mutex: Arc<Mutex<i32>>,
    readers: Vec<Reader>
}

union unit {
    dirty_queue: mem::ManuallyDrop<VecDeque<Pageno>>,
    reader: Reader //Reader record read thread information
}
struct Txn {
    txn_id: u32,
    txn_root: Pageno,
    txn_next_pgno: Pageno,
    env: &'static Env,//as when begin a transaction, you have to create a environment, so static reference is fine.
    u: unit, //if a write transaction, it's dirty_queue; if a read transaction, it's Reader
    flags: u32
}

#[derive(Copy, Clone)]
struct Reader {
    tid: thread::ThreadId
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

impl Env {
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
                write_mutex: Arc::new(Mutex::new(0)),//the number doesn't matter
                readers: Vec::new()
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
                    return Err(Errors::StdFileError(String::from(format!("{:?}", e))));
                },
                Ok(v) => v
        };
        
        self.fd = Some(fd);
        
        self.env_flags = flags;
        match self.env_meta {
            Some(_) => {
                panic!("new environment should not have any metadata");
            },
            None => {
                self.env_meta = Some(DBMetaData {
                    db_stat: DBStat::new(),
                    root: consts::P_INVALID,
                    last_page: 2, //first 2 pages are for sure: P_HEAD & P_META
                    txn_id: 0
                });
            }
        }

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
            self.env_write_header();
        }

        self.mmap = Some(unsafe {
            match memmap::MmapMut::map_mut(self.fd.as_ref().unwrap()) {
                Err(e) => {
                    return Err(Errors::MmapError(String::from(format!("memmap error: {:?}", e))));
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
            return Err(Errors::Seldom(String::from("environment file handle is None")));
        }

        let mut buf = [0 as u8; consts::PAGE_SIZE];
        match self.fd.as_ref().unwrap().read_at(&mut buf, 0) {
            Err(e) => {
                return Err(Errors::StdReadError(String::from(format!("{:?}", e))));
            },
            Ok(read_size) => {
                if read_size == 0 {
                    return Err(Errors::EmptyFile);
                } else if read_size < consts::PAGE_SIZE {
                    return Err(Errors::ShortRead(String::from(format!("read_size: {}", read_size))));
                } 
            }
        }

        let page_ptr: *const u8 = buf.as_ptr();

        let head_page: &Page = unsafe {&*(page_ptr as *const Page)};
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

        unsafe {ptr::copy(page_ptr.offset(size_of::<Page>() as isize) as *const DBHead, self.env_head.as_mut().unwrap() as *mut DBHead, 1)};

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
                return Err(Errors::StdWriteError(format!("{:?}", e)));
            },
            Ok(write_size) => {
                if write_size < size_of::<DBHead>() {
                    return Err(Errors::ShortWrite(format!("Header short write: {}", write_size)));
                }
            }
        }
        Ok(())
    }
}
