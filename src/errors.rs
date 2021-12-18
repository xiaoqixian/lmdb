/**********************************************
  > File Name		: error.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Thu 16 Dec 2021 07:47:36 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

#[derive(Debug)]
pub enum Errors {
    ///File IO errors
    StdIOError(String),
    StdFileError(String),
    StdReadError(String),
    StdWriteError(String),
    ShortRead(String),
    ShortWrite(String),
    CreateExistFile(String),

    ///File content errors
    InvalidVersion(u32),
    InvalidMagic(u32),

    ///memmap crate errors
    MmapError(String),

    ///Common errors
    InvalidFlag(u32),
    Seldom(String),

    ///harmless errors
    EmptyFile,
}
