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
use crate::mdb::{Env, Pageno};
use crate::{debug};

/**
 * Information for managing transactions.
 */
//#[derive(Debug)]
pub struct ReadTxnInfo {
    pub num_readers: usize,
    pub readers: [Reader; consts::MAX_READERS]
}

pub union unit {
    pub dirty_queue: mem::ManuallyDrop<VecDeque<NonNull<*mut u8>>>,
    pub reader: Reader //Reader record read thread information
}
//#[derive(Debug)]
pub struct Txn<'a> {
    pub txn_id: u32,
    pub txn_root: RefCell<Pageno>,
    pub txn_next_pgno: RefCell<Pageno>,
    pub txn_first_pgno: Pageno, //this field is immutable is whole transaction lifetime, so don't need lock
    pub env: Arc<Env<'a>>,
    pub write_lock: Option<MutexGuard<'a, i32>>,
    pub u: unit, //if a write transaction, it's dirty_queue; if a read transaction, it's Reader
    pub flags: u32
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

impl std::fmt::Debug for Txn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Txn")
            .field("txn_id", &self.txn_id)
            .field("txn_root", &self.txn_root)
            .field("txn_next_pgno", &self.txn_next_pgno)
            .field("txn_first_pgno", &self.txn_first_pgno)
            .field("env", &self.env)
            .field("write_lock", &self.write_lock)
            .field("u", unsafe {if self.flags & consts::READ_ONLY_TXN == 0 {
                &self.u.dirty_queue
            } else {
                &self.u.reader
            }})
            .field("flags", &self.flags)
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
                u: unit {
                    dirty_queue: mem::ManuallyDrop::new(VecDeque::new())
                },
                flags: 0
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
                txn_root: RefCell::new(env.get_root_pageno()+1),
                txn_next_pgno: RefCell::new(env.get_last_page()+1),
                txn_first_pgno: env.get_last_page(),
                env: env.clone(),
                write_lock: None,
                u: unit {
                    reader,
                },
                flags: consts::READ_ONLY_TXN,
            });

            debug!("begin a read only transaction {} on root {}", txn.txn_id, *txn.txn_root.borrow());
            Ok(txn)
        }
    }
}
