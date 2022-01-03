/**********************************************
  > File Name		: flags.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 27 Dec 2021 09:57:32 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/**
 * Flags for Pages.
 */
#[derive(Debug, Clone, Copy)]
pub struct PageFlags {
    pub inner: u32
}

impl PageFlags {
    pub fn new(val: u32) -> Self {
        Self {inner: val}
    }

    pub fn is_set(&self, flag: Self) -> bool {
        self.inner & flag.inner != 0
    }
}

impl std::ops::BitOr for PageFlags {
    type Output = PageFlags;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner | rhs.inner}
    }
}

impl std::ops::BitOrAssign for PageFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.inner |= rhs.inner;
    }
}

impl std::ops::BitAnd for PageFlags {
    type Output = PageFlags;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner & rhs.inner}
    }
}

impl std::ops::BitAndAssign for PageFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.inner &= rhs.inner;
    }
}

impl std::ops::BitXor for PageFlags {
    type Output = PageFlags;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner ^ rhs.inner}
    }
}

impl std::ops::BitXorAssign for PageFlags {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.inner ^= rhs.inner;
    }
}

impl std::fmt::UpperHex for PageFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = self.inner;

        std::fmt::UpperHex::fmt(&val, f) // delegate to i32's implementation
    }
}


/**
 * Flags for transactions.
 */
#[derive(Debug, Clone, Copy)]
pub struct TxnFlags {
    pub inner: u32
}

impl TxnFlags {
    pub fn new(val: u32) -> Self {
        Self {inner: val}
    }

    pub fn is_set(&self, flag: Self) -> bool {
        self.inner & flag.inner != 0
    }

    pub fn get_inner(&self) -> u32 {
        self.inner
    }

    pub fn is_broken(&self) -> bool {
        self.inner >= crate::consts::TXN_BROKEN.get_inner()
    }
}

impl std::ops::BitOr for TxnFlags {
    type Output = TxnFlags;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner | rhs.inner}
    }
}

impl std::ops::BitOrAssign for TxnFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.inner |= rhs.inner;
    }
}

impl std::ops::BitAnd for TxnFlags {
    type Output = TxnFlags;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner & rhs.inner}
    }
}

impl std::ops::BitAndAssign for TxnFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.inner &= rhs.inner;
    }
}

impl std::ops::BitXor for TxnFlags {
    type Output = TxnFlags;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner ^ rhs.inner}
    }
}

impl std::ops::BitXorAssign for TxnFlags {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.inner ^= rhs.inner;
    }
}

/**
 * Flags for Nodes.
 */
#[derive(Debug, Clone, Copy)]
pub struct NodeFlags {
    pub inner: u32
}

impl NodeFlags {
    pub fn new(val: u32) -> Self {
        Self {inner: val}
    }

    pub fn is_set(&self, flag: Self) -> bool {
        self.inner & flag.inner != 0
    }

    pub fn get_inner(&self) -> u32 {
        self.inner
    }
}

impl std::ops::BitOr for NodeFlags {
    type Output = NodeFlags;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner | rhs.inner}
    }
}

impl std::ops::BitOrAssign for NodeFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.inner |= rhs.inner;
    }
}

impl std::ops::BitAnd for NodeFlags {
    type Output = NodeFlags;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner & rhs.inner}
    }
}

impl std::ops::BitAndAssign for NodeFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.inner &= rhs.inner;
    }
}

impl std::ops::BitXor for NodeFlags {
    type Output = NodeFlags;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner ^ rhs.inner}
    }
}

impl std::ops::BitXorAssign for NodeFlags {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.inner ^= rhs.inner;
    }
}


/**
 * Flags for Env.
 */
#[derive(Debug, Clone, Copy)]
pub struct EnvFlags {
    pub inner: u32
}

impl EnvFlags {
    pub fn new(val: u32) -> Self {
        Self {inner: val}
    }

    pub fn is_set(&self, flag: Self) -> bool {
        self.inner & flag.inner != 0
    }

    pub fn get_inner(&self) -> u32 {
        self.inner
    }
}

impl std::ops::BitOr for EnvFlags {
    type Output = EnvFlags;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner | rhs.inner}
    }
}

impl std::ops::BitOrAssign for EnvFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.inner |= rhs.inner;
    }
}

impl std::ops::BitAnd for EnvFlags {
    type Output = EnvFlags;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner & rhs.inner}
    }
}

impl std::ops::BitAndAssign for EnvFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.inner &= rhs.inner;
    }
}

impl std::ops::BitXor for EnvFlags {
    type Output = EnvFlags;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner ^ rhs.inner}
    }
}

impl std::ops::BitXorAssign for EnvFlags {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.inner ^= rhs.inner;
    }
}

/**
 * Flags for file.
 */
#[derive(Debug, Clone, Copy)]
pub struct FileFlags {
    pub inner: u32
}

impl FileFlags {
    pub fn new(val: u32) -> Self {
        Self {inner: val}
    }

    pub fn is_set(&self, flag: Self) -> bool {
        self.inner & flag.inner != 0
    }

    pub fn get_inner(&self) -> u32 {
        self.inner
    }
}

impl std::ops::BitOr for FileFlags {
    type Output = FileFlags;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner | rhs.inner}
    }
}

impl std::ops::BitOrAssign for FileFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.inner |= rhs.inner;
    }
}

impl std::ops::BitAnd for FileFlags {
    type Output = FileFlags;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner & rhs.inner}
    }
}

impl std::ops::BitAndAssign for FileFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.inner &= rhs.inner;
    }
}

impl std::ops::BitXor for FileFlags {
    type Output = FileFlags;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner ^ rhs.inner}
    }
}

impl std::ops::BitXorAssign for FileFlags {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.inner ^= rhs.inner;
    }
}

/**
 * Flags for operations
 */
#[derive(Debug, Clone, Copy)]
pub struct OperationFlags {
    pub inner: u32
}

impl OperationFlags {
    pub fn new(val: u32) -> Self {
        Self {inner: val}
    }

    pub fn is_set(&self, flag: Self) -> bool {
        self.inner & flag.inner != 0
    }

    pub fn get_inner(&self) -> u32 {
        self.inner
    }
}

impl std::ops::BitOr for OperationFlags {
    type Output = OperationFlags;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner | rhs.inner}
    }
}

impl std::ops::BitOrAssign for OperationFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.inner |= rhs.inner;
    }
}

impl std::ops::BitAnd for OperationFlags {
    type Output = OperationFlags;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner & rhs.inner}
    }
}

impl std::ops::BitAndAssign for OperationFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.inner &= rhs.inner;
    }
}

impl std::ops::BitXor for OperationFlags {
    type Output = OperationFlags;
    fn bitxor(self, rhs: Self) -> Self::Output {
        Self {inner: self.inner ^ rhs.inner}
    }
}

impl std::ops::BitXorAssign for OperationFlags {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.inner ^= rhs.inner;
    }
}
