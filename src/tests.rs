/**********************************************
  > File Name		: tests.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 21 Dec 2021 04:46:48 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/


use crate::txn::{Txn, Val};
use crate::mdb::Env;
use std::sync::{Arc};
use crate::errors::Errors;
use super::consts;
use crate::info;
use rand::prelude::*;

fn prepare_env<'a>(mode: u32) -> Arc<Env<'a>> {
    let mut env_raw = Env::new();
    env_raw.env_open(&"test.db", consts::ENV_NONE, mode).unwrap();
    Arc::new(env_raw)
}

//#[test]
fn test1() -> Result<(), Errors> {
    let env_raw = Env::new();

    let env = Arc::new(env_raw);
    
    let w_txn1 = Txn::new(&env, false)?;
    let r_txn1 = Txn::new(&env, true)?;
    let w_txn2 = Txn::new(&env, false)?;
    Ok(())
}

#[test]
fn test2() -> Result<(), Errors> {
    let env = prepare_env(consts::READ_WRITE | consts::CREATE);
    //let env = prepare_env(consts::READ_WRITE);
    
    let mut w_txn1 = Txn::new(&env, false)?;
    for i in 0..40960 {
    //for i in 0..64 {
        let mut ks = format!("key{}{}, size = {}", rand::random::<u32>() % 10, i, i);
        let key = Val {
            size: ks.as_bytes().len(),
            data: ks.as_mut_ptr()
        };

        let mut vs = format!("val{}, size = {}, maybe I should attach a long meaningless words to stuff my page, maybe I should attach a long meaningless words to stuff my page", i, i);
        let val = Val {
            size: vs.as_bytes().len(),
            data: vs.as_mut_ptr()
        };

        w_txn1.txn_put(key, val, consts::OP_NONE)?;
        info!("put {}", i);
    }
    w_txn1.txn_commit()?;
    println!("DBMetaData: {:?}", env.get_meta().unwrap());
    Ok(())
}

//#[test]
fn test3() {
    use crate::page::PageHead;
    use crate::mdb::DBMetaData;
    use memmap;
    use crate::jump_head;
    use std::mem::size_of;

    let fd = std::fs::OpenOptions::new().read(true).write(true).open("test.db").unwrap();
    let mmap = unsafe {memmap::MmapMut::map_mut(&fd).unwrap()};

    let mut ptr = unsafe {mmap.as_ptr().offset(4096)};
    let meta1 = crate::jump_head_mut!(ptr, PageHead, DBMetaData);

    let meta11 = crate::jump_head!(ptr, PageHead, DBMetaData);
    println!("meta1: {:?}", meta11);
}
