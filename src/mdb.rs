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
use crate::cursor::Cursor;
use crate::txn::{Txn, ReadTxnInfo, Reader, unit, Val};
use crate::{info, debug, error, jump_head, jump_head_mut, ptr_ref, ptr_mut_ref};

pub type Pageno = usize;
pub type Indext = u16; //index of nodes in a node.
type CmpFunc = dyn Fn(&Val, &Val) -> i32;

struct Array<T> where T: Sized + Copy {
    inner: *mut T
}

impl<T> Array<T> where T: Sized + Copy {
    fn new(ptr: *mut u8) -> Self {
        assert!(!ptr.is_null());
        Self {
            inner: ptr as *mut T
        }
    }
}

impl<T> std::ops::Index<usize> for Array<T> where T: Sized + Copy {
    type Output = T;
    fn index(&self, index: usize) -> &Self::Output {
        unsafe {&*self.inner.offset(index as isize)}
    }
}

impl<T> std::ops::IndexMut<usize> for Array<T> where T: Sized + Copy {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        unsafe {&mut *self.inner.offset(index as isize)}
    }
}

/**
 * return 1 if key1 greater than key2
 * return -1 if key1 less than key2
 * return 0 if key1 equals to key2
 */
fn default_compfunc(key1: &Val, key2: &Val) -> i32 {
    let len = if key1.size < key2.size {key1.size} else {key2.size};
    let val1 = unsafe {std::slice::from_raw_parts(key1.data, len)};
    let val2 = unsafe {std::slice::from_raw_parts(key2.data, len)};

    for i in 0..len {
        if val1[i] < val2[i] {
            return -1;
        } else if val1[i] > val2[i] {
            return 1;
        }
    }

    if key1.size < key2.size {
        return -1;
    } else if key1.size > key2.size {
        return 1;
    } else {
        return 0;
    }
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
#[derive(Copy, Clone, Debug)]
pub struct PageBounds {
    upper_bound: usize,
    lower_bound: usize,
}

#[derive(Copy, Clone, Debug)]
pub struct PageHead {
    pageno: Pageno,
    page_flags: u32,
    page_bounds: PageBounds,
    /// size of overflow pages
    overflow_pages: usize,
}

/**
 * A pair of page pointers.
 * With a parent page and one of it's child page.
 * And an index of the child page in the parent page.
 */
pub struct PageParent {
    pub page: *mut u8,
    pub parent: *mut u8,
    pub index: usize
}

/**
 * To present a dirty page or a group of dirty pages.
 */
pub struct DirtyPageHead {
    parent: *mut u8,
    index: usize, //index that this page in it's parent page.
    num: usize, //number of allocated pages, this head not included.
    layout: Layout //used for dirty page deallocation
}

/**
 * Struct of nodes in the B+ tree.
 */
#[derive(Clone, Copy)]
pub union PagenoDatasize {
    pageno: Pageno,
    datasize: usize
}
#[derive(Clone, Copy)]
pub struct Node {
    u: PagenoDatasize,
    node_flags: u32,
    key_size: usize,
    key_data: *mut u8,
}


#[derive(Debug)]
pub struct DBHead {
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

//struct DB {
    //md_root: Pageno,
    //cmp_func: CmpFunc,
    //db_head: DBHead
//}

//#[derive(Debug)]
pub struct Env<'a> {
    env_flags: u32,
    fd: Option<File>,
    cmp_func: &'a CmpFunc,
    mmap: Option<MmapMut>,
    w_txn: RefCell<Option<Weak<Txn<'a>>>>, //current write transaction
    env_head: Option<DBHead>,
    env_meta: Mutex<Option<DBMetaData>>,
    read_txn_info: Mutex<ReadTxnInfo>,
    write_mutex: Mutex<i32>,
    txn_id: Mutex<u32>, //increase when begin a new write transaction.
    w_txn_first_page: Option<RwLock<Pageno>>
}


impl PageHead {
    pub fn new(pageno: Pageno) -> Self {
        Self {
            pageno,
            page_flags: 0,
            page_bounds: PageBounds {
                lower_bound: size_of::<PageHead>(),
                upper_bound: consts::PAGE_SIZE
            },
            overflow_pages: 0
        }
    }

    pub fn get_page_head(page_ptr: *mut u8) -> Self {
        assert!(!page_ptr.is_null());
        unsafe {
            *(page_ptr as *const Self)
        }
    }

    pub fn get_pageno(page_ptr: *mut u8) -> Pageno {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).pageno
        }
    }

    pub fn get_lower_bound(page_ptr: *mut u8) -> usize {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_bounds.lower_bound
        }
    }

    pub fn get_upper_bound(page_ptr: *mut u8) -> usize {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_bounds.upper_bound
        }
    }

    pub fn set_lower_bound(page_ptr: *mut u8, lower_bound: usize) {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *mut Self)).page_bounds.lower_bound = lower_bound;
        }
    }

    pub fn set_upper_bound(page_ptr: *mut u8, upper_bound: usize) {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *mut Self)).page_bounds.upper_bound = upper_bound;
        }
    }
    
    pub fn is_set(page_ptr: *mut u8, flag: u32) -> bool {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_flags & flag != 0
        }
    }

    pub fn get_flags(page_ptr: *mut u8) -> u32 {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_flags
        }
    }

    pub fn get_info(page_ptr: *mut u8) -> String {
        assert!(!page_ptr.is_null());
        let page = unsafe {&*(page_ptr as *const PageHead)};
        String::from(format!("Page {{ pageno: {}, flags: {:#X}, lower_bound: {}, upper_bound: {}, overflow_pages: {}  }}", page.pageno, page.page_flags, page.page_bounds.lower_bound, page.page_bounds.upper_bound, page.overflow_pages))
    }

    pub fn num_keys(page_ptr: *mut u8) -> usize {
        let lower_bound = unsafe {*(page_ptr as *const PageHead)}.page_bounds.lower_bound;
        (lower_bound - size_of::<PageHead>()) >> 1 //because ptr index length is 2 bytes.
    }

    /**
     * update a child page number for a branch page.
     */
    pub fn update_child(parent: *mut u8, pageno: Pageno, index: usize) -> Result<(), Errors> {
        if !Self::is_set(parent, consts::P_BRANCH) {
            return Err(Errors::InvalidPageType(PageHead::get_info(parent)));
        }

        //let node_offset = unsafe {*(parent.offset((size_of::<PageHead>() + index*size_of::<Indext>()) as isize) as *const Indext) as isize};
        let node_offset = Array::<Indext>::new(parent)[index] as isize;
        debug!("child node offset {}", node_offset);
        let node = unsafe {&mut *(parent.offset(node_offset) as *mut Node)};
        node.u.pageno = pageno;
        Ok(())
    }

    pub fn get_node(page_ptr: *mut u8, index: usize) -> Result<Node, Errors> {
        let num_keys = Self::num_keys(page_ptr);
        if index >= num_keys {
            error!("Node index overflow, num_keys: {}, index: {}", num_keys, index);
            return Err(Errors::IndexOverflow(index));
        }
        let ptrs = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<PageHead>() as isize)});
        Ok(unsafe {*(page_ptr.offset(ptrs[index] as isize) as *const Node)})
    }

    pub fn left_space(page_ptr: *mut u8) -> usize {
        let page_head = unsafe {&*(page_ptr as *const PageHead)};
        page_head.page_bounds.upper_bound - page_head.page_bounds.lower_bound
    }

    /**
     * search a node in a page by a key.
     * as all nodes are sorted, we use binary search.
     *
     * @return (index, exact): index is the index of the smallest node greater than key, 
     * exact means if this key is a exact compare, if is, then
     * there is already a exactly same key exists in this page already.
     * If the key is greater than all child nodes in this page, then return Ok(None).
     */
    pub fn search_node(page_ptr: *mut u8, key: &Val, cmp_func: &CmpFunc) -> Result<Option<(usize, bool)>, Errors> {
        let mut low: i32 = if PageHead::is_set(page_ptr, consts::P_LEAF) {0} else {1};
        let mut high: i32 = (PageHead::num_keys(page_ptr) - 1) as i32;
        let mut mid: i32 = -1;
        let mut cmp_res: i32 = 0;
        let node_offsets = Array::<Indext>::new(page_ptr);

        let mut index: usize = PageHead::num_keys(page_ptr) - 1;
        let mut exact: bool = false;

        while low <= high {
            mid = (low+high) >> 1;
            assert!(mid >= 0);
            let mid_node = unsafe {&*(page_ptr.offset(node_offsets[mid as usize] as isize) as *const Node)};
            let mid_key = Val::new(mid_node.key_size, mid_node.key_data);
            
            cmp_res = cmp_func(&key, &mid_key);

            if cmp_res < 0 {
                high = mid - 1;
            } else if cmp_res > 0 {
                low = mid + 1;
            } else {
                break;
            }
        }

        if cmp_res > 0 {
            mid += 1;
            if mid >= PageHead::num_keys(page_ptr) as i32 {
                return Ok(None);
            }
        }

        Ok(Some((mid as usize, cmp_res == 0)))
    }

    /**
     * append a node to a page.
     * If no room in this page, we need to split the page.
     *
     * If this is a branch page, then data is None.
     * If this is a leaf page, then pageno is None.
     */
    pub fn add_node(page_ptr: *mut u8, key: &Val, data: Option<&Val>, pageno: Option<Pageno>, index: usize, flags: u32, txn: &Txn) -> Result<(), Errors> {
        assert_ne!(data.is_none(), pageno.is_none());

        let page_size = txn.env.env_head.unwrap().page_size;
        //evaluate node size needed
        let mut node_size = size_of::<Node>() + key.size;
        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            assert!(data.is_some());
            
            if flags & consts::V_BIGDATA != 0 {
                //big data put on overflow pages
                node_size += size_of::<Pageno>();
            } else if data.unwrap().size >= page_size/consts::MINKEYS {
                let mut over_pages = data.unwrap().size + page_size - 1;
                over_pages /= page_size;

                let ofp = txn.env.new_page(txn, consts::P_OVERFLOW, over_pages)?;

            }
        }

        match Self::search_node(page_ptr, key, &default_compfunc)? {
            None => 
        }
        Ok(())
    }

    /**
     * delete a node from a page.
     * all nodes are aranged tightly.
     */
    pub fn del_node(page_ptr: *mut u8, index: usize) -> Result<(), Errors> {
        let num_keys = Self::num_keys(page_ptr);
        assert!(index < num_keys);

        let mut node_offsets = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<PageHead>() as isize)});
        let node = unsafe {*(page_ptr.offset(node_offsets[index] as isize) as *const Node)};
        let node_offset = node_offsets[index];
        let mut node_size = size_of::<Node>() + node.key_size;
        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            if node.node_flags & consts::V_BIGDATA != 0 {
                node_size += size_of::<Pageno>();
            } else {
                node_size += node.u.datasize;
            }
        }

        //update all node addresses if necessary
        let mut i: usize = 0;
        let mut k: usize = 0;
        while i < num_keys {
            if i != index {
                node_offsets[k] = node_offsets[i];
                if node_offsets[k] < node_offset {
                    node_offsets[k] += node_size as Indext;
                }
                k += 1;
            }
            i += 1;
        }
        
        Self::set_lower_bound(page_ptr, Self::get_lower_bound(page_ptr) - size_of::<Indext>());

        //move all nodes address smaller than this node up node size 
        let upper_bound = Self::get_upper_bound(page_ptr);
        unsafe {
            ptr::copy(page_ptr.offset(upper_bound as isize), page_ptr.offset((upper_bound - node_size) as isize), node_offset as usize - node_size - upper_bound);
        }

        Self::set_upper_bound(page_ptr, upper_bound + node_size);

        Ok(())
    }
}

impl fmt::Debug for PageParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageParent")
            .field("page", &PageHead::get_pageno(self.page))
            .field("parent", &PageHead::get_pageno(self.parent))
            .field("index", &self.index)
            .finish()
    }
}

impl PageParent {
    pub fn new() -> Self {
        Self {
            page: ptr::null_mut(),
            parent: ptr::null_mut(),
            index: std::usize::MAX
        }
    }
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


impl fmt::Debug for Env<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Env")
            .field("env_flags", &self.env_flags)
            .field("fd", &self.fd)
            .field("cmp_func", &"default_compfunc")
            .field("mmap", &self.mmap)
            .field("w_txn", &self.w_txn)
            .field("env_head", &self.env_head)
            .field("env_meta", &self.env_meta)
            .field("read_txn_info", &self.read_txn_info)
            .field("write_mutex", &self.write_mutex)
            .field("txn_id", &self.txn_id)
            .field("w_txn_first_page", &self.w_txn_first_page)
            .finish()
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
            cmp_func: &default_compfunc,
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
        let header: &DBHead = jump_head!(page_ptr, PageHead, DBHead);

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

        let meta1: &mut DBMetaData = jump_head_mut!(page_ptr1, PageHead, DBMetaData);
        let meta2: &mut DBMetaData = jump_head_mut!(page_ptr2, PageHead, DBMetaData);

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

        let meta1: &DBMetaData = jump_head!(page_ptr1, PageHead, DBMetaData);
        let meta2: &DBMetaData = jump_head!(page_ptr2, PageHead, DBMetaData);

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

    /**
     * allocate @num number of new pages.
     * ONLY way pages can be allocated.
     * pages deallocated when the write transaction is committed.
     *
     * notice the difference between allocate_page and new_page.
     */
    fn allocate_page(&self, num: usize, parent: *mut u8, index: usize, txn: &Txn) -> Result<*mut u8, Errors> {
        let layout = match Layout::from_size_align(num * self.env_head.as_ref().unwrap().page_size + size_of::<DirtyPageHead>(), 1) {
            Err(e) => {
                error!("Layout error {:?}", e);
                return Err(Errors::LayoutError(e));
            },
            Ok(v) => v
        };

        let ptr: *mut u8 = unsafe {alloc(layout)};

        let dpage_head: &mut DirtyPageHead = ptr_mut_ref!(ptr, DirtyPageHead);
        
        dpage_head.parent = parent;
        dpage_head.num = num;
        dpage_head.layout = layout;
        dpage_head.index = index;

        let page: &mut PageHead = jump_head_mut!(ptr, DirtyPageHead, PageHead);
        page.pageno = txn.get_next_pageno();
        txn.add_next_pageno(num);

        txn.add_dirty_page(ptr)?;
        Ok(ptr)
    }

    /**
     * allocate new page, only when a page need to split.
     * page reallocation not included.
     * @param txn: as only provided only for the write transaction, a txn ref parameter 
     * needed.
     * @param flag: branch page or leaf page, used to update database stat information.
     *
     * @return Ok: a ptr includes DirtyPageHead and pages allocated returned in the form
     * of *mut u8.
     */
    pub fn new_page(&self, txn: &Txn, flag: u32, num: usize) -> Result<*mut u8, Errors> {
        if txn.get_txn_flags() & consts::READ_ONLY_TXN != 0 {
            return Err(Errors::ReadOnlyTxnNotAllowed);
        }

        let ptr = self.allocate_page(num, ptr::null_mut(), 0, txn)?;
        Ok(ptr) //temporary
    }

    /**
     * touch a page and make it dirty.
     * though the parent page is not reallocated, it's child page number is updated.
     * page_parent is a mutable reference and it got updated.
     */
    pub fn touch(&self, page_parent: &mut PageParent, txn: &Txn) -> Result<(), Errors> {
        assert!(!page_parent.page.is_null());
        
        if !PageHead::is_set(page_parent.page, consts::P_DIRTY) {
            debug!("touching page {} -> {}", PageHead::get_pageno(page_parent.page), txn.get_next_pageno());
            let dpage_ptr = self.allocate_page(1, page_parent.parent, page_parent.index, txn)?;
            
            let new_pageno = jump_head!(dpage_ptr, DirtyPageHead, PageHead).pageno;
            
            unsafe {
                ptr::copy::<u8>(page_parent.page, dpage_ptr.offset(size_of::<DirtyPageHead>() as isize), self.env_head.as_ref().unwrap().page_size);
            }

            let new_page = jump_head_mut!(dpage_ptr, DirtyPageHead, PageHead);
            new_page.pageno = new_pageno;
            new_page.page_flags |= consts::P_DIRTY;

            //update new page in it's parent
            if !page_parent.parent.is_null() {
                PageHead::update_child(page_parent.parent, new_pageno, page_parent.index)?;
            }

            page_parent.page = new_page as *mut _ as *mut u8;
        }
        Ok(())
    }

    /**
     * search_page: search a page buy a key.
     * @param modify: if we set modify and the page we searched is not dirty yet, then 
     * we should touch a new page to replace it. 
     */
    pub fn search_page(&self, key: &Val, txn: Option<&Txn>, cursor: Option<&Cursor>, modify: bool) -> Result<PageParent, Errors> {
        let root: Pageno = {
            if txn.is_none() {
                if let Err(e) = self.env_read_meta() {
                    return Err(e);
                }
                self.get_root_pageno()
            } else if txn.unwrap().get_txn_flags() >= consts::TXN_BROKEN {
                return Err(Errors::BrokenTxn(String::from(format!("{:?}", txn))));
            } else {
                txn.unwrap().get_txn_root()
            }
        };

        let mut page_parent = PageParent::new();

        if root == consts::P_INVALID {
            return Err(Errors::EmptyTree);
        }

        page_parent.page = self.get_page(root)?;
        debug!("root page with flags {:#X}", PageHead::get_flags(page_parent.page));
        
        // if this is the first time the current write transaction modifies.
        // touch a new root page 
        if modify && !PageHead::is_set(page_parent.page, consts::P_DIRTY) {
            self.touch(&mut page_parent, txn.unwrap())?;
            txn.unwrap().update_root(PageHead::get_pageno(page_parent.page))?;
        }

        self.search_page_root(Some(key), txn, cursor, &mut page_parent, modify)?;
        Ok(page_parent)
    }

    /**
     * search a page from root page
     * if key is None, it initilizes the cursor at the left most leaf node.
     */
    fn search_page_root(&self, key: Option<&Val>, txn: Option<&Txn>, cursor: Option<&Cursor>, page_parent: &mut PageParent, modify: bool) -> Result<(), Errors> {
        //TODO: cursor needs to push a page here
        
        if txn.is_none() && modify {
            return Err(Errors::Seldom(String::from("if to modify, a write txn ref is required")));
        }

        let mut page_ptr = page_parent.page;
        let mut i: usize = 0;
        
        while PageHead::is_set(page_ptr, consts::P_BRANCH) {
            if key.is_none() {
                i = 0;
            } else {
                match PageHead::search_node(page_ptr, &key.unwrap(), self.cmp_func)? {
                    None => {i = PageHead::num_keys(page_ptr) - 1},
                    Some((index, exact)) => {
                        if exact {
                            i = index;
                        } else {
                            i = index-1;
                        }
                    }
                }
            }

            if key.is_some() {
                debug!("following index {} for key {:?}", i, &key.unwrap().get_readable_data());
            }

            page_parent.parent = page_ptr;
            let node = PageHead::get_node(page_ptr, i)?;
            page_parent.page = self.get_page(unsafe {node.u.pageno})?;
            page_parent.index = i;

            if modify {
                self.touch(page_parent, txn.unwrap())?;
            }

            page_ptr = page_parent.page;
        }

        assert!(PageHead::is_set(page_parent.page, consts::P_LEAF));

        debug!("found leaf page {} at index {}", PageHead::get_info(page_parent.page), page_parent.index);

        Ok(())
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
