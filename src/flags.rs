/**********************************************
  > File Name		: flags.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 27 Dec 2021 09:57:32 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/
use std::{fmt, ops};

pub trait FlagType {}

#[derive(Clone, Copy)]
pub struct Flag<T>
where T: Default + Copy + Clone + FlagType {
    inner: u32,
    t: T
}

impl<T> Flag<T> 
where T: Default + Copy + Clone + FlagType {
    pub fn new(inner: u32) -> Self {
        Flag {
            inner,
            t: Default::default()
        }
    }

    pub fn is_set(&self, rhs: Self) -> bool {
        self.inner & rhs.get_inner() != 0
    }

    pub fn get_inner(&self) -> u32 {
        self.inner
    }

    pub fn get_mut_inner(&mut self) -> &mut u32 {
        &mut self.inner
    }
}

impl Flag<_TxnFlag> {
    pub fn is_broken(&self) -> bool {
        self.inner >= 0x80000000
    }
}

impl<T> ops::BitOr for Flag<T> 
where T: Default + Copy + Clone + FlagType {
    type Output = Flag<T>;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self::new(self.get_inner() | rhs.get_inner())
    }
}

impl<T> ops::BitOrAssign for Flag<T> 
where T: Default + Copy + Clone + FlagType {
    fn bitor_assign(&mut self, rhs: Self) {
        *self.get_mut_inner() |= rhs.get_inner();
    }
}

impl<T> ops::BitAnd for Flag<T> 
where T: Default + Copy + Clone + FlagType {
    type Output = Flag<T>;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self::new(self.get_inner() & rhs.get_inner())
    }
}

impl<T> ops::BitAndAssign for Flag<T>
where T: Default + Copy + Clone + FlagType {
    fn bitand_assign(&mut self, rhs: Self) {
        *self.get_mut_inner() &= rhs.get_inner();
    }
}

impl<T> ops::BitXor for Flag<T> 
where T: Default + Copy + Clone + FlagType {
    type Output = Flag<T>;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self::new(self.get_inner() ^ rhs.get_inner())
    }
}

impl<T> ops::BitXorAssign for Flag<T>
where T: Default + Copy + Clone + FlagType {
    fn bitxor_assign(&mut self, rhs: Self) {
        *self.get_mut_inner() ^= rhs.get_inner();
    }
}

impl<T> fmt::UpperHex for Flag<T>
where T: Default + Copy + Clone + FlagType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val = self.inner;
        std::fmt::UpperHex::fmt(&val, f)
    }
}

impl<T> fmt::Debug for Flag<T>
where T: Default + Copy + Clone + FlagType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flag")
            .field("inner", &self.inner)
            .finish()
    }
}

#[derive(Default, Clone, Copy)]
pub struct _PageFlag {}
impl FlagType for _PageFlag {}

#[derive(Default, Clone, Copy)]
pub struct _NodeFlag {}
impl FlagType for _NodeFlag {}

#[derive(Default, Clone, Copy)]
pub struct _FileFlag {}
impl FlagType for _FileFlag {}

#[derive(Default, Clone, Copy)]
pub struct _TxnFlag {}
impl FlagType for _TxnFlag {}

#[derive(Default, Clone, Copy)]
pub struct _EnvFlag {}
impl FlagType for _EnvFlag {}

#[derive(Default, Clone, Copy)]
pub struct _OperationFlag {}
impl FlagType for _OperationFlag {}

pub type PageFlag = Flag<_PageFlag>;
pub type NodeFlag = Flag<_NodeFlag>;
pub type TxnFlag = Flag<_TxnFlag>;
pub type FileFlag = Flag<_FileFlag>;
pub type EnvFlag = Flag<_EnvFlag>;
pub type OperationFlag = Flag<_OperationFlag>;

pub const P_HEAD: PageFlag = PageFlag {inner: 0x1, t: _PageFlag {}};
pub const P_META: PageFlag = PageFlag {inner: 0x02, t: _PageFlag {}};
pub const P_BRANCH: PageFlag = PageFlag {inner: 0x04, t: _PageFlag {}};
pub const P_LEAF: PageFlag = PageFlag {inner: 0x08, t: _PageFlag {}};
pub const P_DIRTY: PageFlag = PageFlag {inner: 0x10, t: _PageFlag {}};
pub const P_OVERFLOW: PageFlag = PageFlag {inner: 0x20, t: _PageFlag {}};

pub const READ_ONLY_TXN: TxnFlag = TxnFlag {inner: 0x1, t: _TxnFlag {}};
pub const TXN_BROKEN: TxnFlag = TxnFlag {inner: 0x80000000, t: _TxnFlag {}};

pub const NODE_NONE: NodeFlag = NodeFlag {inner: 0, t: _NodeFlag {}};
pub const NODE_BRANCH: NodeFlag = NodeFlag {inner: 0x1, t: _NodeFlag {}};
pub const NODE_LEAF: NodeFlag = NodeFlag {inner: 0x2, t: _NodeFlag {}};
pub const V_BIGDATA: NodeFlag = NodeFlag {inner: 0x10, t: _NodeFlag {}};

pub const OP_NONE: OperationFlag = OperationFlag {inner: 0, t: _OperationFlag {}};
pub const K_OVERRITE: OperationFlag = OperationFlag {inner: 0x1, t: _OperationFlag {}};

pub const ENV_NONE: EnvFlag = EnvFlag {inner: 0, t: _EnvFlag {}};

///file flags
pub const READ_ONLY: u32 = 0x1;
pub const READ_WRITE: u32 = 0x2;
pub const CREATE: u32 = 0x4;
/*impl fmt::Debug for PageFlag {*/
    /*fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {*/
        /*fmt::UpperHex::fmt(&self.inner, f)*/
    /*}*/
/*}*/

/*impl PageFlag {*/
    /*pub fn new(val: u32) -> Self {*/
        /*Self {inner: val}*/
    /*}*/

    /*pub fn is_set(&self, flag: Self) -> bool {*/
        /*self.inner & flag.inner != 0*/
    /*}*/
/*}*/

/*impl std::ops::BitOr for PageFlag {*/
    /*type Output = PageFlag;*/
    /*fn bitor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner | rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitOrAssign for PageFlag {*/
    /*fn bitor_assign(&mut self, rhs: Self) {*/
        /*self.inner |= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitAnd for PageFlag {*/
    /*type Output = PageFlag;*/
    /*fn bitand(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner & rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitAndAssign for PageFlag {*/
    /*fn bitand_assign(&mut self, rhs: Self) {*/
        /*self.inner &= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitXor for PageFlag {*/
    /*type Output = PageFlag;*/
    /*fn bitxor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner ^ rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitXorAssign for PageFlag {*/
    /*fn bitxor_assign(&mut self, rhs: Self) {*/
        /*self.inner ^= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::fmt::UpperHex for PageFlag {*/
    /*fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {*/
        /*let val = self.inner;*/

        /*std::fmt::UpperHex::fmt(&val, f) // delegate to i32's implementation*/
    /*}*/
/*}*/


/*#[derive(Debug, Clone, Copy)]*/
/*pub struct TxnFlags {*/
    /*pub inner: u32*/
/*}*/

/*impl TxnFlags {*/
    /*pub fn new(val: u32) -> Self {*/
        /*Self {inner: val}*/
    /*}*/

    /*pub fn is_set(&self, flag: Self) -> bool {*/
        /*self.inner & flag.inner != 0*/
    /*}*/

    /*pub fn get_inner(&self) -> u32 {*/
        /*self.inner*/
    /*}*/

    /*pub fn is_broken(&self) -> bool {*/
        /*self.inner >= crate::consts::TXN_BROKEN.get_inner()*/
    /*}*/
/*}*/

/*impl std::ops::BitOr for TxnFlags {*/
    /*type Output = TxnFlags;*/
    /*fn bitor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner | rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitOrAssign for TxnFlags {*/
    /*fn bitor_assign(&mut self, rhs: Self) {*/
        /*self.inner |= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitAnd for TxnFlags {*/
    /*type Output = TxnFlags;*/
    /*fn bitand(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner & rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitAndAssign for TxnFlags {*/
    /*fn bitand_assign(&mut self, rhs: Self) {*/
        /*self.inner &= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitXor for TxnFlags {*/
    /*type Output = TxnFlags;*/
    /*fn bitxor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner ^ rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitXorAssign for TxnFlags {*/
    /*fn bitxor_assign(&mut self, rhs: Self) {*/
        /*self.inner ^= rhs.inner;*/
    /*}*/
/*}*/

/*#[derive(Debug, Clone, Copy)]*/
/*pub struct NodeFlags {*/
    /*pub inner: u32*/
/*}*/

/*impl NodeFlags {*/
    /*pub fn new(val: u32) -> Self {*/
        /*Self {inner: val}*/
    /*}*/

    /*pub fn is_set(&self, flag: Self) -> bool {*/
        /*self.inner & flag.inner != 0*/
    /*}*/

    /*pub fn get_inner(&self) -> u32 {*/
        /*self.inner*/
    /*}*/
/*}*/

/*impl std::ops::BitOr for NodeFlags {*/
    /*type Output = NodeFlags;*/
    /*fn bitor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner | rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitOrAssign for NodeFlags {*/
    /*fn bitor_assign(&mut self, rhs: Self) {*/
        /*self.inner |= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitAnd for NodeFlags {*/
    /*type Output = NodeFlags;*/
    /*fn bitand(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner & rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitAndAssign for NodeFlags {*/
    /*fn bitand_assign(&mut self, rhs: Self) {*/
        /*self.inner &= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitXor for NodeFlags {*/
    /*type Output = NodeFlags;*/
    /*fn bitxor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner ^ rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitXorAssign for NodeFlags {*/
    /*fn bitxor_assign(&mut self, rhs: Self) {*/
        /*self.inner ^= rhs.inner;*/
    /*}*/
/*}*/


/*#[derive(Debug, Clone, Copy)]*/
/*pub struct EnvFlags {*/
    /*pub inner: u32*/
/*}*/

/*impl EnvFlags {*/
    /*pub fn new(val: u32) -> Self {*/
        /*Self {inner: val}*/
    /*}*/

    /*pub fn is_set(&self, flag: Self) -> bool {*/
        /*self.inner & flag.inner != 0*/
    /*}*/

    /*pub fn get_inner(&self) -> u32 {*/
        /*self.inner*/
    /*}*/
/*}*/

/*impl std::ops::BitOr for EnvFlags {*/
    /*type Output = EnvFlags;*/
    /*fn bitor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner | rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitOrAssign for EnvFlags {*/
    /*fn bitor_assign(&mut self, rhs: Self) {*/
        /*self.inner |= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitAnd for EnvFlags {*/
    /*type Output = EnvFlags;*/
    /*fn bitand(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner & rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitAndAssign for EnvFlags {*/
    /*fn bitand_assign(&mut self, rhs: Self) {*/
        /*self.inner &= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitXor for EnvFlags {*/
    /*type Output = EnvFlags;*/
    /*fn bitxor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner ^ rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitXorAssign for EnvFlags {*/
    /*fn bitxor_assign(&mut self, rhs: Self) {*/
        /*self.inner ^= rhs.inner;*/
    /*}*/
/*}*/

/*#[derive(Debug, Clone, Copy)]*/
/*pub struct FileFlags {*/
    /*pub inner: u32*/
/*}*/

/*impl FileFlags {*/
    /*pub fn new(val: u32) -> Self {*/
        /*Self {inner: val}*/
    /*}*/

    /*pub fn is_set(&self, flag: Self) -> bool {*/
        /*self.inner & flag.inner != 0*/
    /*}*/

    /*pub fn get_inner(&self) -> u32 {*/
        /*self.inner*/
    /*}*/
/*}*/

/*impl std::ops::BitOr for FileFlags {*/
    /*type Output = FileFlags;*/
    /*fn bitor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner | rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitOrAssign for FileFlags {*/
    /*fn bitor_assign(&mut self, rhs: Self) {*/
        /*self.inner |= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitAnd for FileFlags {*/
    /*type Output = FileFlags;*/
    /*fn bitand(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner & rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitAndAssign for FileFlags {*/
    /*fn bitand_assign(&mut self, rhs: Self) {*/
        /*self.inner &= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitXor for FileFlags {*/
    /*type Output = FileFlags;*/
    /*fn bitxor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner ^ rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitXorAssign for FileFlags {*/
    /*fn bitxor_assign(&mut self, rhs: Self) {*/
        /*self.inner ^= rhs.inner;*/
    /*}*/
/*}*/

/*#[derive(Debug, Clone, Copy)]*/
/*pub struct OperationFlags {*/
    /*pub inner: u32*/
/*}*/

/*impl OperationFlags {*/
    /*pub fn new(val: u32) -> Self {*/
        /*Self {inner: val}*/
    /*}*/

    /*pub fn is_set(&self, flag: Self) -> bool {*/
        /*self.inner & flag.inner != 0*/
    /*}*/

    /*pub fn get_inner(&self) -> u32 {*/
        /*self.inner*/
    /*}*/
/*}*/

/*impl std::ops::BitOr for OperationFlags {*/
    /*type Output = OperationFlags;*/
    /*fn bitor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner | rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitOrAssign for OperationFlags {*/
    /*fn bitor_assign(&mut self, rhs: Self) {*/
        /*self.inner |= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitAnd for OperationFlags {*/
    /*type Output = OperationFlags;*/
    /*fn bitand(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner & rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitAndAssign for OperationFlags {*/
    /*fn bitand_assign(&mut self, rhs: Self) {*/
        /*self.inner &= rhs.inner;*/
    /*}*/
/*}*/

/*impl std::ops::BitXor for OperationFlags {*/
    /*type Output = OperationFlags;*/
    /*fn bitxor(self, rhs: Self) -> Self::Output {*/
        /*Self {inner: self.inner ^ rhs.inner}*/
    /*}*/
/*}*/

/*impl std::ops::BitXorAssign for OperationFlags {*/
    /*fn bitxor_assign(&mut self, rhs: Self) {*/
        /*self.inner ^= rhs.inner;*/
    /*}*/
/*}*/
