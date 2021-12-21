/**********************************************
  > File Name		: tests.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 21 Dec 2021 04:46:48 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/


use crate::txn::Txn;
use crate::mdb::Env;
use std::sync::{Arc};
use crate::errors::Errors;
use super::consts;
#[test]
fn test1() -> Result<(), Errors> {
    let mut env_raw = Env::new();
    env_raw.env_open(&"test.db", 0, consts::READ_WRITE | consts::CREATE)?;

    let env = Arc::new(env_raw);
    
    let w_txn1 = Txn::new(&env, false)?;
    let r_txn1 = Txn::new(&env, true)?;
    let w_txn2 = Txn::new(&env, false)?;
    Ok(())
}
