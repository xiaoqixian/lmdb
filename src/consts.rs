/**********************************************
  > File Name		: consts.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 21 Dec 2021 04:26:58 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use crate::mdb;

///meta data consts
pub const VERSION: u32 = 1;
pub const MAGIC: u32 = 0xBEEFC0DE;
pub const MAX_KEY_SIZE: usize = 255;

///page flags and consts
pub const P_INVALID: mdb::Pageno = std::usize::MAX;//invalid page number
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

///transaction flags and consts
pub const READ_ONLY_TXN: u32 = 0x1;
///flags greater than 0x80000000 means transaction have errors
pub const TXN_BROKEN: u32 = 0x80000000;
pub const MAX_READERS: usize = 126;
