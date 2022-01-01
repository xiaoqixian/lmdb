/**********************************************
  > File Name		: mdb.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 15 Dec 2021 04:08:42 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::sync::{Arc, Weak, Mutex, MutexGuard, RwLock};
use std::fs::{File, OpenOptions};
use memmap::{self, MmapMut};
use std::collections::VecDeque;
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
use crate::{info, debug, error, jump_head, jump_head_mut, ptr_ref, ptr_mut_ref, back_head_mut};
use crate::page::{PageHead, PageParent, PageBounds, DirtyPageHead, Node};
use crate::flags::{EnvFlags, PageFlags, NodeFlags};

pub type Pageno = usize;
pub type Indext = u16; //index of nodes in a node.
pub type CmpFunc = dyn Fn(&Val, &Val) -> i32;

pub struct Array<T> where T: Sized + Copy {
    inner: *mut T
}

impl<T> Array<T> where T: Sized + Copy {
    pub fn new(ptr: *mut u8) -> Self {
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
    env_flags: EnvFlags,
    fd: Option<File>,
    pub cmp_func: &'a CmpFunc,
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
            env_flags: EnvFlags::new(0),
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

    pub fn get_page_size(&self) -> usize {
        self.env_head.as_ref().unwrap().page_size
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
    pub fn env_open(&mut self, path: &str, env_flags: EnvFlags, mode: u32) -> Result<(), Errors> {
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
        
        self.env_flags = env_flags;

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
        assert!(head_page.page_flags.is_set(consts::P_HEAD));

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
        assert!(page1.page_flags.is_set(consts::P_META));
        
        let page2: &PageHead = unsafe {
            &*(page_ptr2 as *const PageHead)
        };
        assert!(page2.page_flags.is_set(consts::P_META));

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
     *
     * allocated page flags are clean, only pageno is set.
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
     *  needed.
     * @param flag: branch page or leaf page or overflow page, used to update database 
     *  stat information.
     *
     * @return Ok: a ptr includes DirtyPageHead and pages allocated returned in the form
     * of *mut u8.
     */
    pub fn new_page(&self, txn: &Txn, page_flags: PageFlags, num: usize) -> Result<*mut u8, Errors> {
        if txn.get_txn_flags().is_set(consts::READ_ONLY_TXN) {
            return Err(Errors::ReadOnlyTxnNotAllowed);
        }

        let ptr = self.allocate_page(num, ptr::null_mut(), std::usize::MAX, txn)?;
        let page_head = jump_head_mut!(ptr, DirtyPageHead, PageHead);
        page_head.page_flags = page_flags | consts::P_DIRTY;
        page_head.page_bounds.lower_bound = size_of::<PageHead>();
        page_head.page_bounds.upper_bound = self.env_head.as_ref().unwrap().page_size;
        
        //update env stat
        let mut mg = self.env_meta.lock().unwrap();
        let mut env_meta: &mut DBMetaData = mg.as_mut().unwrap();
        if page_flags.is_set(consts::P_LEAF) {
            env_meta.db_stat.leaf_pages += 1;
        } else if page_flags.is_set(consts::P_BRANCH) {
            env_meta.db_stat.branch_pages += 1;
        } else if page_flags.is_set(consts::P_OVERFLOW) {
            env_meta.db_stat.overflow_pages += 1;
        }
        
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
            } else if txn.unwrap().get_txn_flags().is_broken() {
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

    /**
     * split a page, but only when more than 1/4 of the page space is used.
     * supports inserting a new node during splitting.
     *
     * splitting a page needs to insert a new node into the parent page, may cause it's
     * parent page splitted.
     *
     * splitting a page also needs a new key/val pair to be inserted into either this page
     * or it's right sigling page. Inserted page pointer and the key's index returned.
     */
    pub fn split(&self, page: *mut u8, key: &Val, val: Option<&Val>, pageno: Option<Pageno>, index: usize, node_flags: NodeFlags, txn: &Txn) -> Result<(*mut u8, usize), Errors> {
        assert_ne!(val.is_none(), pageno.is_none());
        debug!("splitting page {}", PageHead::get_pageno(page));

        let mut dpage = back_head_mut!(page, DirtyPageHead);
        let mut ret_ptr = ptr::null_mut();
        let mut ret_index = std::usize::MAX;

        //create a parent page if it's a root page.
        if dpage.parent.is_null() {
            let parent_ptr = self.new_page(txn, consts::P_BRANCH, 1)?;
            
            let mut mg = self.env_meta.lock().unwrap();
            let mut env_meta: &mut DBMetaData = mg.as_mut().unwrap();
            env_meta.db_stat.depth += 1;
            debug!("B+ tree depth increases 1");
            
            dpage.parent = unsafe {parent_ptr.offset(size_of::<DirtyPageHead>() as isize)};
            debug!("root split! new root = {}", PageHead::get_pageno(dpage.parent));
            PageHead::add_node(dpage.parent, None, None, Some(PageHead::get_pageno(page)), 0, NodeFlags::new(0), txn)?;
        }

        let page_size = self.env_head.as_ref().unwrap().page_size;

        let sib_dpage_ptr = self.new_page(txn, PageHead::get_flags(page), 1)?;
        let mut sib_dpage = ptr_mut_ref!(sib_dpage_ptr, DirtyPageHead);
        sib_dpage.parent = dpage.parent;
        sib_dpage.index = dpage.index + 1;
        
        //alloc a temp page and copy all data on spiltting page to it.
        let layout = match Layout::from_size_align(page_size, 1) {
            Ok(v) => v,
            Err(e) => {
                return Err(Errors::LayoutError(e));
            }
        };
        let copy = unsafe {
            let copy = alloc(layout);
            ptr::copy_nonoverlapping(page, copy, page_size);
            ptr::write_bytes::<u8>(page.offset(size_of::<PageHead>() as isize), 0, page_size - size_of::<PageHead>());
            copy
        };
        PageHead::set_lower_bound(page, size_of::<PageHead>());
        PageHead::set_upper_bound(page, page_size);

        let num_keys = PageHead::num_keys(copy);
        let split_index = num_keys/2 + 1;

        //create a seperator key and insert it into the parent page.
        let sep_key = if split_index == index {
            *key
        } else {
            let mid_node = PageHead::get_node(copy, split_index)?;
            Val {size: mid_node.key_size, data: mid_node.key_data}
        };

        if PageHead::left_space(dpage.parent) < size_of::<Indext>() + size_of::<Node>() + sep_key.size {
            //no enough space in the parent node, split the parent page.
            //assume parent page is dirty, as normally only when we need to put a pair,
            //then we may need to split a page, so all it's ancestor pages are dirty.
            assert!(PageHead::is_set(dpage.parent, consts::P_DIRTY));

            self.split(dpage.parent, &sep_key, None, Some(jump_head_mut!(sib_dpage_ptr, DirtyPageHead, PageHead).pageno), split_index, consts::NODE_NONE, txn)?;

            if dpage.parent != sib_dpage.parent && dpage.index >= PageHead::num_keys(dpage.parent) {
                dpage.parent = sib_dpage.parent;
                dpage.index = sib_dpage.index - 1;
            }

        } else {
            PageHead::add_node(dpage.parent, Some(&sep_key), None, Some(jump_head_mut!(sib_dpage_ptr, DirtyPageHead, PageHead).pageno), split_index, consts::NODE_NONE, txn)?;
        }

        let mut i: usize = 0;//index in copy
        let mut k: usize = 0;//index in this page and sibling page.
        let mut ins_new = false;//is the new key is inserted.
        let is_leaf = PageHead::is_set(page, consts::P_LEAF);

        while i < num_keys {
            let ins_page_ptr = if i < split_index {
                page
            } else {
                if i == split_index {
                    k = if i == index && ins_new {1} else {0};
                }
                unsafe {sib_dpage_ptr.offset(size_of::<DirtyPageHead>() as isize)}
            };
    
            //get node
            if i == index && !ins_new {
                PageHead::add_node(ins_page_ptr, Some(key), val, pageno, k, consts::NODE_NONE, txn)?;
                ins_new = true;
                ret_index = k;
                ret_ptr = ins_page_ptr;
            } else if i == num_keys {
                break;
            } else {
                let node = PageHead::get_node(copy, i)?;
                let temp_key = Val {data: node.key_data, size: node.key_size};
                
                if is_leaf {
                    let temp_val = Val {data: node.val_data, size: unsafe {node.u.datasize}};
                    PageHead::add_node(ins_page_ptr, Some(&temp_key), Some(&temp_val), None, k, node.node_flags, txn)?;
                } else {
                    PageHead::add_node(ins_page_ptr, Some(&temp_key), None, Some(unsafe {node.u.pageno}), k, node.node_flags, txn)?;
                }
                i += 1;
            }

            k += 1;
        }

        unsafe {dealloc(copy, layout);}
        Ok((ret_ptr, ret_index))
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
