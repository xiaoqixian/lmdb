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
pub const PAGE_SIZE: usize = 4096;//4096 bytes page size on my machine.

pub const MAX_READERS: usize = 126;
pub const MINKEYS: usize = 2;
