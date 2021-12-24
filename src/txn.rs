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
use std::mem;
use std::ptr::{NonNull};
use std::fmt;
use std::thread;
use std::process;
use std::cell::RefCell;

use crate::consts;
use crate::errors::Errors;
use crate::mdb::{Env, Pageno, PageHead};
use crate::{debug};

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
    txn_flags: u32
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
    pub fn get_readable_data(&self) -> [char; 10] {
        let mut res = [32 as char; 10];
        let len = if self.size < 10 {self.size} else {10};
        let data_ref = unsafe {std::slice::from_raw_parts(self.data as *const _, len)};
        for i in 0..len {
            res[i] = data_ref[i] as char;
        }
        res
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
                if self.txn_flags & consts::READ_ONLY_TXN == 0 {
                    String::from("dirty_queue")
                } else {
                    format!("{:?}", self.u.borrow().reader)
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
    pub fn new(env: &'a Arc<Env<'a>>, read_only: bool) -> Result<Arc<Self>, Errors> {
        //let env_mut_ref: &mut Env = unsafe {
            //&mut *(Arc::into_raw(env.clone()) as *mut Env)
        //};
        if !read_only {
            debug!("try to lock write_mutex");
            //while env_mut_ref.txn_info.as_mut().unwrap().write_mutex.compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                
            //}
            //let mutex_guard = env.txn_info.as_ref().unwrap().write_mutex.lock().unwrap();
            let mutex_guard = env.lock_w_mutex();
            debug!("write_mutex unlocked");

            unsafe {
                let env_ptr = Arc::as_ptr(env) as *mut Env;
                (*env_ptr).add_txn_id();
                (*env_ptr).env_read_meta()?;
            }

            //assert!(env.w_txn.is_none());
            
            //always read metadata before begin a new transaction
            //read metadata also won't affect read transactions because of toggle meta pages. 

            let txn: Arc<Self> = Arc::new(Self {
                txn_id: env.get_txn_id(),
                txn_root: RefCell::new(env.get_root_pageno()),
                txn_next_pgno: RefCell::new(env.get_last_page()+1),
                txn_first_pgno: env.get_last_page()+1,
                env: env.clone(),
                write_lock: Some(mutex_guard),
                u: RefCell::new(unit {
                    dirty_queue: mem::ManuallyDrop::new(VecDeque::new())
                }),
                txn_flags: 0
            });

            env.set_w_txn(Some(Arc::downgrade(&txn)));

            debug!("begin a write transaction {} on root {}", txn.txn_id, *txn.txn_root.borrow());
            Ok(txn)
        } else {
            //I don't find pthread_get_specific like function in rust,
            //so we have to iterate all readers to make sure that this is a new thread.
            let reader = env.add_reader(process::id(), thread::current().id())?;
            debug!("get reader: {:?}", reader);

            env.env_read_meta()?;
            debug!("read meta data: {:?}", env.get_meta());

            let txn = Arc::new(Self {
                txn_id: env.get_txn_id(),
                txn_root: RefCell::new(env.get_root_pageno()),
                txn_next_pgno: RefCell::new(env.get_last_page()+1),
                txn_first_pgno: env.get_last_page()+1,
                env: env.clone(),
                write_lock: None,
                u: RefCell::new(unit {
                    reader,
                }),
                txn_flags: consts::READ_ONLY_TXN,
            });

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

    pub fn get_txn_flags(&self) -> u32 {
        self.txn_flags
    }

    pub fn get_next_pageno(&self) -> Pageno {
        *self.txn_next_pgno.borrow()
    }

    pub fn add_next_pageno(&self, num: usize) {
        *self.txn_next_pgno.borrow_mut() += num;
    }

    pub fn update_root(&self, pageno: Pageno) -> Result<(), Errors> {
        *self.txn_root.borrow_mut() = pageno;
        Ok(())
    }

    pub fn get_txn_root(&self) -> Pageno {
        *self.txn_root.borrow()
    }

    /**
     * put a key/value pair into the database.
     *
     * flags could be 
     *      K_OVERRITE: allow key overrite if key exists, if key exists and this flag not
     *                  setted, return Err(KeyExist)
     */
    pub fn txn_put(&mut self, key: Val, val: Val, flags: u32) -> Result<(), Errors> {
        if self.txn_flags & consts::READ_ONLY_TXN != 0 {
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
                let insert_index = match PageHead::search_node(page_parent.page, &key, self.env.cmp_func)? {
                    None => {
                        PageHead::num_keys(page_parent.page) - 1
                    },
                    Some((index, exact)) => {
                        if exact {
                            if flags & consts::K_OVERRITE == 0 {
                                return Err(Errors::KeyExist(format!("{:?}", &key)));
                            }
                            PageHead::del_node(page_parent.page, index)?;
                        }
                        index
                    }
                }

            },
            Err(Errors::EmptyTree) => {
                debug!("allocating a new root page");
                
            },
            Err(e) => {
                return Err(e)
            }
        }

        Ok(())
    }
}
