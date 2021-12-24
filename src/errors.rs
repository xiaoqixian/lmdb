/**********************************************
  > File Name		: error.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Thu 16 Dec 2021 07:47:36 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::io;

#[derive(Debug)]
pub enum Errors {
    ///File IO errors
    StdIOError(io::Error),
    StdFileError(io::Error),
    StdReadError(io::Error),
    StdWriteError(io::Error),
    ShortRead(usize),
    ShortWrite(usize),
    //CreateExistFile(String),

    ///File content errors
    InvalidVersion(u32),
    InvalidMagic(u32),

    ///Alloc crate errors
    LayoutError(std::alloc::LayoutError),

    ///memmap crate errors
    MmapError(io::Error),

    ///Common errors
    InvalidFlag(u32),
    UnexpectedNoneValue(String),
    UnmappedEnv,
    ReadersMaxedOut,
    TryToPutInReadOnlyTxn,
    InvalidKey(String),
    KeyNull,
    ValNull,
    BrokenTxn(String),
    ReadOnlyTxnNotAllowed,
    InvalidPageType(String),
    IndexOverflow(usize),
    KeyExist(String),
    NoSpace(String),

    Seldom(String), //hard to classify

    ///harmless errors
    EmptyFile,
    EmptyTree,
}
