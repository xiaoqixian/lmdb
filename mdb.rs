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

type pageno_t = usize;
type ptr = [u8];

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
    w_txn: Txn, //current write transaction
}

struct Txn {
    
}
