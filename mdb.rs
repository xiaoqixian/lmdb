/**********************************************
  > File Name		: mdb.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 15 Dec 2021 04:08:42 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::sync::{Arc};
use std::fs::{File, OpenOptions};
use memmap::{*};
use std::collections::{VecDeque};
use std::thread;

type pageno_t = usize;
type ptr = [u8];
type cmp_func_t = fn(v1: Val, v2: Val) -> i32;

struct Val {
    size: usize,
    data: ptr
}

struct DBHead {
    version: usize,
    page_size: usize, ///os memory page size, in C, got by sysconf(_SC_PAGE_SIZE)
}

struct DB {
    md_root: pageno_t,
    cmp_func: fn(v1: Val, v2: Val) -> i32,
    db_head: DBHead
}

struct Env {
    fd: Arc<File>,
    mmap: MmapMut,
    w_txn: Txn, ///current write transaction
}

struct Txn {
    txn_root: pageno_t,
    txn_next_pgno: pageno_t,
    env: &'static Env,//as when begin a transaction, you have to create a environment, so static reference is fine.
    union unit {
        dirty_queue: VecDeque<pageno_t>,
        reader: Reader //Reader record read thread information
    },
    flags: u32
}

struct Reader {
    tid: thread::ThreadId;
}


impl DB {
    
}

impl Env {
    fn new() -> Self {
        Self {
            
        }
    }
}
