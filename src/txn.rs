/**********************************************
  > File Name		: txn.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 21 Dec 2021 04:21:37 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, Arc, Weak};
use std::mem::{self, size_of};
use std::ptr::NonNull;
use std::fmt;
use std::thread;
use std::process;
use std::cell::RefCell;
use std::os::unix::prelude::FileExt;
use std::alloc::dealloc;

use crate::consts;
use crate::errors::Errors;
use crate::mdb::{Env, Pageno};
use crate::page::{PageHead, DirtyPageHead};
use crate::{debug, jump_head_ptr, jump_head, error, info, jump_head_mut, ptr_ref};
use crate::flags::{self, TxnFlag, NodeFlag, OperationFlag};

#[derive(Copy, Clone)]
pub struct Val {
    pub size: usize,
    pub data: *mut u8
}
/**
 * Information for managing transactions.
 */
//#[derive(Debug)]
pub struct ReadTxnInfo {
    pub num_readers: usize,
    pub readers: [Reader; consts::MAX_READERS]
}

pub union unit {
    pub dirty_queue: mem::ManuallyDrop<VecDeque<NonNull<u8>>>,
    pub reader: Reader //Reader record read thread information
}
//#[derive(Debug)]
pub struct Txn<'a> {
    txn_id: u32,
    txn_root: RefCell<Pageno>,
    txn_next_pgno: RefCell<Pageno>,
    txn_first_pgno: Pageno, //this field is immutable is whole transaction lifetime, so don't need lock
    pub env: Arc<Env<'a>>,
    write_lock: Option<MutexGuard<'a, i32>>,
    u: RefCell<unit>, //if a write transaction, it's dirty_queue; if a read transaction, it's Reader
    txn_flags: TxnFlag
}

#[derive(Copy, Clone, Debug)]
pub struct Reader {
    pub tid: thread::ThreadId,
    pub pid: u32,
}

impl Reader {
    pub fn new() -> Self {
        Self {
            tid: thread::current().id(),
            pid: 0
        }
    }
}

impl Val {
    pub fn new(size: usize, data: *mut u8) -> Self {
        Self {
            size,
            data,
        }
    }
    /**
     * 
     */
    pub fn get_readable_data(&self) -> String {
        if self.size == 0 {
            return String::from("");
        }
        let mut res = vec![0 as u8; self.size];
        let data_ref = unsafe {std::slice::from_raw_parts(self.data as *const _, self.size)};
        for i in 0..self.size {
            res[i] = data_ref[i];
        }
        String::from_utf8(res).unwrap()
    }
}

impl std::fmt::Debug for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Val")
            .field("size", &self.size)
            .field("data", &self.get_readable_data())
            .finish()
    }
}

impl std::fmt::Debug for Txn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Txn")
            .field("txn_id", &self.txn_id)
            .field("txn_root", &self.txn_root)
            .field("txn_next_pgno", &self.txn_next_pgno)
            .field("txn_first_pgno", &self.txn_first_pgno)
            .field("env", &self.env)
            .field("write_lock", &self.write_lock)
            .field("u", &unsafe {
                if self.txn_flags.is_set(flags::READ_ONLY_TXN) {
                    format!("{:?}", self.u.borrow().reader)
                } else {
                    String::from("dirty_queue")
                }
            })
            .field("txn_flags", &self.txn_flags)
            .finish()
    }
}

impl fmt::Debug for ReadTxnInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadTxnInfo")
            .field("num_readers", &self.num_readers)
            .finish()
    }
}

impl ReadTxnInfo {
    pub fn new() -> Self {
        Self {
            num_readers: 0,
            readers: [Reader::new(); consts::MAX_READERS]
        }
    }
}

impl<'a> Txn<'a> {
    pub fn new(env: &'a Arc<Env<'a>>, read_only: bool) -> Result<Self, Errors> {
        //let env_mut_ref: &mut Env = unsafe {
            //&mut *(Arc::into_raw(env.clone()) as *mut Env)
        //};
        if !read_only {
            debug!("try to lock write_mutex");
            //while env_mut_ref.txn_info.as_mut().unwrap().write_mutex.compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                
            //}
            //let mutex_guard = env.txn_info.as_ref().unwrap().write_mutex.lock().unwrap();
            let mutex_guard = env.lock_w_mutex();
            debug!("write_mutex locked");

            unsafe {
                let env_ptr = Arc::as_ptr(env) as *mut Env;
                (*env_ptr).add_txn_id();
                (*env_ptr).env_read_meta()?;
            }

            //assert!(env.w_txn.is_none());
            
            //always read metadata before begin a new transaction
            //read metadata also won't affect read transactions because of toggle meta pages. 

            let txn = Self {
                txn_id: env.get_txn_id(),
                txn_root: RefCell::new(env.get_root_pageno()),
                txn_next_pgno: RefCell::new(env.get_last_page()+1),
                txn_first_pgno: env.get_last_page()+1,
                env: env.clone(),
                write_lock: Some(mutex_guard),
                u: RefCell::new(unit {
                    dirty_queue: mem::ManuallyDrop::new(VecDeque::new())
                }),
                txn_flags: TxnFlag::new(0)
            };

            env.set_w_txn_first_page(txn.txn_first_pgno);

            //TODO: Not sure if I need to put a txn ref in env.
            //env.set_w_txn(Some(Arc::downgrade(&txn)));

            debug!("begin a write transaction {} on root {}", txn.txn_id, *txn.txn_root.borrow());
            Ok(txn)
        } else {
            //I don't find pthread_get_specific like function in rust,
            //so we have to iterate all readers to make sure that this is a new thread.
            let reader = env.add_reader(process::id(), thread::current().id())?;
            debug!("get reader: {:?}", reader);

            env.env_read_meta()?;
            debug!("read meta data: {:?}", env.get_meta());

            let txn = Self {
                txn_id: env.get_txn_id(),
                txn_root: RefCell::new(env.get_root_pageno()),
                txn_next_pgno: RefCell::new(env.get_last_page()+1),
                txn_first_pgno: env.get_last_page()+1,
                env: env.clone(),
                write_lock: None,
                u: RefCell::new(unit {
                    reader,
                }),
                txn_flags: flags::READ_ONLY_TXN,
            };

            debug!("begin a read only transaction {} on root {}", txn.txn_id, *txn.txn_root.borrow());
            Ok(txn)
        }
    }

    /**
     * add a dirty page ptr to dirty_queue
     */
    pub fn add_dirty_page(&self, dpage_ptr: *mut u8) -> Result<(), Errors> {
        unsafe {self.u.borrow_mut().dirty_queue.push_back(NonNull::new(dpage_ptr).unwrap())};
        Ok(())
    }

    pub fn find_dirty_page(&self, pageno: Pageno) -> Result<*mut u8, Errors> {
        unsafe {
            for ptr in self.u.borrow().dirty_queue.iter() {
                let ph = jump_head!((*ptr).as_ptr(), DirtyPageHead, PageHead);
                if ph.pageno == pageno {
                    return Ok((*ptr).as_ptr().offset(size_of::<DirtyPageHead>() as isize));
                }
            }
        }
        Err(Errors::PageNotFound(pageno))
    }

    #[inline]
    pub fn get_txn_flags(&self) -> TxnFlag {
        self.txn_flags
    }

    #[inline]
    pub fn get_next_pageno(&self) -> Pageno {
        *self.txn_next_pgno.borrow()
    }

    #[inline]
    pub fn add_next_pageno(&self, num: usize) {
        *self.txn_next_pgno.borrow_mut() += num;
    }

    #[inline]
    pub fn update_root(&self, pageno: Pageno) -> Result<(), Errors> {
        *self.txn_root.borrow_mut() = pageno;
        Ok(())
    }

    #[inline]
    pub fn get_txn_root(&self) -> Pageno {
        *self.txn_root.borrow()
    }

    #[inline]
    pub fn get_last_page(&self) -> Pageno {
        *self.txn_next_pgno.borrow() - 1
    }

    #[inline]
    pub fn get_txn_id(&self) -> u32 {
        self.txn_id
    }

    /**
     * put a key/value pair into the database.
     *
     * flags could be 
     *      K_OVERRITE: allow key overrite if key exists, if key exists and this flag not
     *                  setted, return Err(KeyExist)
     */
    pub fn txn_put(&mut self, key: Val, val: Val, flags: OperationFlag) -> Result<(), Errors> {
        info!("put key {:?}", key);
        if self.txn_flags.is_set(flags::READ_ONLY_TXN) {
            return Err(Errors::TryToPutInReadOnlyTxn);
        }

        if key.size == 0 || key.size >= consts::MAX_KEY_SIZE {
            return Err(Errors::InvalidKey(String::from(format!("{:?}", key))));
        }

        if key.data.is_null() {
            return Err(Errors::KeyNull);
        }
        if val.data.is_null() {
            return Err(Errors::ValNull);
        }

        match self.env.search_page(&key, Some(&self), None, true) {
            Ok(page_parent) => {
                info!("searched page_parent {:?}", page_parent);
                let insert_index = match PageHead::search_node(page_parent.page, &key, self.env.cmp_func)? {
                    None => {
                        PageHead::num_keys(page_parent.page)
                    },
                    Some((index, exact)) => {
                        if exact {
                            if !flags.is_set(flags::K_OVERRITE) {
                                error!("KeyExist: {:?}", &key);
                                return Err(Errors::KeyExist(format!("{:?}", &key)));
                            }
                            PageHead::del_node(page_parent.page, index)?;
                        }
                        index
                    }
                };

                match PageHead::add_node(page_parent.page, Some(&key), Some(&val), None, insert_index, flags::NODE_NONE, &self) {
                    Ok(_) => {},
                    Err(Errors::NoSpace(s)) => {
                        debug!("add node no space");
                        //need to split the page.
                        //page spliting also support inserting a new node.
                        self.env.split(page_parent.page, &key, Some(&val), None, insert_index, flags::NODE_NONE, &self)?;
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Err(Errors::EmptyTree) => {
                debug!("allocating a new root page");
                let root_ptr = self.env.new_page(&self, flags::P_LEAF, 1)?;
                PageHead::add_node(jump_head_ptr!(root_ptr, DirtyPageHead), Some(&key), Some(&val), None, 0, flags::NODE_NONE, &self)?;
                self.env.add_depth();
                self.txn_root.replace(jump_head!(root_ptr, DirtyPageHead, PageHead).pageno);
                debug!("new txn_root: {}", *self.txn_root.borrow());
            },
            Err(e) => {
                return Err(e)
            }
        }

        self.env.add_entry();

        Ok(())
    }

    /**
     * commit a transaction
     * 1. write all dirty pages to file
     * 2. flush mmap
     */
    pub fn txn_commit(&mut self) -> Result<(), Errors> {
        if self.txn_flags.is_set(flags::READ_ONLY_TXN) {
            return Err(Errors::ReadOnlyTxnNotAllowed);
        }

        if self.txn_flags.is_broken() {
            debug!("trying to commit a broken transaction, aborted");
            self.txn_abort()?;
        }

        {
            let dq: &mut mem::ManuallyDrop<VecDeque<NonNull<u8>>> = unsafe {
                &mut self.u.borrow_mut().dirty_queue
            };
            while !dq.is_empty() {
                let ptr = dq.pop_front().unwrap().as_ptr();
                let dpage = ptr_ref!(ptr, DirtyPageHead);
                let ph: &mut PageHead = jump_head_mut!(ptr, DirtyPageHead, PageHead);

                assert!(ph.page_flags.is_set(flags::P_DIRTY));
                ph.page_flags ^= flags::P_DIRTY;
                assert!(!ph.page_flags.is_set(flags::P_DIRTY));
                
                let buf = unsafe {std::slice::from_raw_parts(ptr.offset(size_of::<DirtyPageHead>() as isize), self.env.get_page_size())};
                let ofs = ph.pageno as u64 * self.env.get_page_size() as u64;

                match self.env.fd.as_ref().unwrap().write_all_at(buf, ofs) {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(Errors::StdIOError(e));
                    }
                }
                info!("write pageno {} back to file", ph.pageno);

                unsafe {dealloc(ptr, dpage.layout);}
            }
        }

        //sync written data to file
        if let Err(e) = self.env.fd.as_ref().unwrap().sync_all() {
            return Err(Errors::StdIOError(e));
        }
        //update metadata, important
        if let Err(e) = self.env.env_write_meta(&self) {
            return Err(e);
        }
        //sync written meta data to file 
        if let Err(e) = self.env.fd.as_ref().unwrap().sync_all() {
            return Err(Errors::StdIOError(e));
        }
        self.txn_abort()?;
        Ok(())
    }

    pub fn txn_abort(&mut self) -> Result<(), Errors> {
        debug!("abort transaction on root {}", self.txn_root.borrow());

        if self.txn_flags.is_set(flags::READ_ONLY_TXN) {
            self.env.del_reader(unsafe {self.u.borrow().reader})?;
        } else {
            let dq = unsafe {
                &mut self.u.borrow_mut().dirty_queue
            };
            while !dq.is_empty() {
                let ptr = dq.pop_front().unwrap().as_ptr();
                let dpage = ptr_ref!(ptr, DirtyPageHead);
                unsafe {dealloc(ptr, dpage.layout);}
            }

            unsafe {mem::ManuallyDrop::drop(dq);}
            self.write_lock = None;
        }
        assert!(self.env.try_lock_w_mutex().is_ok());
        Ok(())
    }
}
