/**********************************************
  > File Name		: page.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 24 Dec 2021 05:08:26 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::alloc::Layout;
use std::mem::size_of;
use std::ptr;
use std::fmt;

use crate::mdb::{Pageno, Array, Indext, CmpFunc};
use crate::txn::{Txn, Val};
use crate::consts;
use crate::errors::Errors;
use crate::{debug, error, jump_head, jump_head_mut};

/**
 * Structure of a memory page.
 * Includes header, pointer array, empty space and heap area.
 * 
 * As the order of PageHead fields does matter, I have to use #[repr(C)]
 * so the compiler won't disorder PageHead fields. But alignment is still
 * optimized.
 */
/// page bounds
#[derive(Copy, Clone, Debug)]
pub struct PageBounds {
    pub upper_bound: usize,
    pub lower_bound: usize,
}

#[derive(Copy, Clone, Debug)]
pub struct PageHead {
    pub pageno: Pageno,
    pub page_flags: u32,
    pub page_bounds: PageBounds,
    /// size of overflow pages
    pub overflow_pages: usize,
}

/**
 * A pair of page pointers.
 * With a parent page and one of it's child page.
 * And an index of the child page in the parent page.
 */
pub struct PageParent {
    pub page: *mut u8,
    pub parent: *mut u8,
    pub index: usize
}

/**
 * To present a dirty page or a group of dirty pages.
 */
pub struct DirtyPageHead {
    pub parent: *mut u8,
    pub index: usize, //index that this page in it's parent page.
    pub num: usize, //number of allocated pages, this head not included.
    pub layout: Layout //used for dirty page deallocation
}

/**
 * Struct of nodes in the B+ tree.
 */
#[derive(Clone, Copy)]
pub union PagenoDatasize {
    pub pageno: Pageno,
    pub datasize: usize
}
#[derive(Clone, Copy)]
pub struct Node {
    pub u: PagenoDatasize,
    pub node_flags: u32,
    pub key_size: usize,
    pub key_data: *mut u8,
}


impl PageHead {
    pub fn new(pageno: Pageno) -> Self {
        Self {
            pageno,
            page_flags: 0,
            page_bounds: PageBounds {
                lower_bound: size_of::<PageHead>(),
                upper_bound: consts::PAGE_SIZE
            },
            overflow_pages: 0
        }
    }

    pub fn get_page_head(page_ptr: *mut u8) -> Self {
        assert!(!page_ptr.is_null());
        unsafe {
            *(page_ptr as *const Self)
        }
    }

    pub fn get_pageno(page_ptr: *mut u8) -> Pageno {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).pageno
        }
    }

    pub fn get_lower_bound(page_ptr: *mut u8) -> usize {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_bounds.lower_bound
        }
    }

    pub fn get_upper_bound(page_ptr: *mut u8) -> usize {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_bounds.upper_bound
        }
    }

    pub fn set_lower_bound(page_ptr: *mut u8, lower_bound: usize) {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *mut Self)).page_bounds.lower_bound = lower_bound;
        }
    }

    pub fn set_upper_bound(page_ptr: *mut u8, upper_bound: usize) {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *mut Self)).page_bounds.upper_bound = upper_bound;
        }
    }
    
    pub fn is_set(page_ptr: *mut u8, flag: u32) -> bool {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_flags & flag != 0
        }
    }

    pub fn get_flags(page_ptr: *mut u8) -> u32 {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_flags
        }
    }

    pub fn get_info(page_ptr: *mut u8) -> String {
        assert!(!page_ptr.is_null());
        let page = unsafe {&*(page_ptr as *const PageHead)};
        String::from(format!("Page {{ pageno: {}, flags: {:#X}, lower_bound: {}, upper_bound: {}, overflow_pages: {}  }}", page.pageno, page.page_flags, page.page_bounds.lower_bound, page.page_bounds.upper_bound, page.overflow_pages))
    }

    pub fn num_keys(page_ptr: *mut u8) -> usize {
        let lower_bound = unsafe {*(page_ptr as *const PageHead)}.page_bounds.lower_bound;
        (lower_bound - size_of::<PageHead>()) >> 1 //because ptr index length is 2 bytes.
    }

    /**
     * update a child page number for a branch page.
     */
    pub fn update_child(parent: *mut u8, pageno: Pageno, index: usize) -> Result<(), Errors> {
        if !Self::is_set(parent, consts::P_BRANCH) {
            return Err(Errors::InvalidPageType(PageHead::get_info(parent)));
        }

        //let node_offset = unsafe {*(parent.offset((size_of::<PageHead>() + index*size_of::<Indext>()) as isize) as *const Indext) as isize};
        let node_offset = Array::<Indext>::new(parent)[index] as isize;
        debug!("child node offset {}", node_offset);
        let node = unsafe {&mut *(parent.offset(node_offset) as *mut Node)};
        node.u.pageno = pageno;
        Ok(())
    }

    pub fn get_node(page_ptr: *mut u8, index: usize) -> Result<Node, Errors> {
        let num_keys = Self::num_keys(page_ptr);
        if index >= num_keys {
            error!("Node index overflow, num_keys: {}, index: {}", num_keys, index);
            return Err(Errors::IndexOverflow(index));
        }
        let ptrs = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<PageHead>() as isize)});
        Ok(unsafe {*(page_ptr.offset(ptrs[index] as isize) as *const Node)})
    }

    pub fn left_space(page_ptr: *mut u8) -> usize {
        let page_head = unsafe {&*(page_ptr as *const PageHead)};
        page_head.page_bounds.upper_bound - page_head.page_bounds.lower_bound
    }

    /**
     * search a node in a page by a key.
     * as all nodes are sorted, we use binary search.
     *
     * @return (index, exact): index is the index of the smallest node greater than key, 
     * exact means if this key is a exact compare, if is, then
     * there is already a exactly same key exists in this page already.
     * If the key is greater than all child nodes in this page, then return Ok(None).
     */
    pub fn search_node(page_ptr: *mut u8, key: &Val, cmp_func: &CmpFunc) -> Result<Option<(usize, bool)>, Errors> {
        let mut low: i32 = if PageHead::is_set(page_ptr, consts::P_LEAF) {0} else {1};
        let mut high: i32 = (PageHead::num_keys(page_ptr) - 1) as i32;
        let mut mid: i32 = -1;
        let mut cmp_res: i32 = 0;
        let node_offsets = Array::<Indext>::new(page_ptr);

        let mut index: usize = PageHead::num_keys(page_ptr) - 1;
        let mut exact: bool = false;

        while low <= high {
            mid = (low+high) >> 1;
            assert!(mid >= 0);
            let mid_node = unsafe {&*(page_ptr.offset(node_offsets[mid as usize] as isize) as *const Node)};
            let mid_key = Val::new(mid_node.key_size, mid_node.key_data);
            
            cmp_res = cmp_func(&key, &mid_key);

            if cmp_res < 0 {
                high = mid - 1;
            } else if cmp_res > 0 {
                low = mid + 1;
            } else {
                break;
            }
        }

        if cmp_res > 0 {
            mid += 1;
            if mid >= PageHead::num_keys(page_ptr) as i32 {
                return Ok(None);
            }
        }

        Ok(Some((mid as usize, cmp_res == 0)))
    }

    /**
     * append a node to a page.
     * If no room in this page, we need to split the page, but it's the job of this function.
     * So it just returns Err(Errors::NoSpace)
     *
     * If this is a branch page, then data is None.
     * If this is a leaf page, then pageno is None.
     */
    pub fn add_node(page_ptr: *mut u8, key: &Val, val: Option<&Val>, pageno: Option<Pageno>, index: usize, f: u32, txn: &Txn) -> Result<(), Errors> {
        assert_ne!(val.is_none(), pageno.is_none());

        let mut flags = f; // as rust doesn't allow mutable paprameters
        let page_size = txn.env.get_page_size();
        //evaluate node size needed
        let mut node_size = size_of::<Node>() + key.size;
        let mut ofp: *mut u8 = ptr::null_mut();

        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            assert!(val.is_some());
            
            if flags & consts::V_BIGDATA != 0 {
                //data already on overflow pages
                //data in val is the page number.
                node_size += size_of::<Pageno>();
            } else if val.unwrap().size >= page_size/consts::MINKEYS {
                let mut over_pages = val.unwrap().size + page_size - 1;
                over_pages /= page_size;

                ofp = txn.env.new_page(txn, consts::P_OVERFLOW, over_pages)?;
                flags |= consts::V_BIGDATA;
                node_size += size_of::<Pageno>();
            } else {
                node_size += val.unwrap().size;
            }
        }

        if node_size+size_of::<Indext>() >= Self::left_space(page_ptr) {
            debug!("page no enough space for another node");
            debug!("page_bounds: {:?}, node_size: {}", unsafe {*(page_ptr as *const Self)}.page_bounds, node_size);
            return Err(Errors::NoSpace(format!("page_bounds: {:?}, node_size: {}", unsafe {*(page_ptr as *const Self)}.page_bounds, node_size)));
        }

        let mut i = Self::num_keys(page_ptr);
        let mut node_offsets = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<Self>() as isize)});
        while i > index {
            node_offsets[i] = node_offsets[i-1];
            i -= 1;
        }

        let ofs = Self::get_upper_bound(page_ptr) - node_size;
        node_offsets[index] = ofs as Indext;

        let node = unsafe {&mut *(page_ptr.offset(ofs as isize) as *mut Node)};
        node.key_size = key.size;
        node.node_flags = flags;
        node.key_data = unsafe {page_ptr.offset((ofs + size_of::<Node>()) as isize)};

        //copy key data
        unsafe {
            ptr::copy_nonoverlapping(key.data, node.key_data, key.size);
        }
        
        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            if flags & consts::V_BIGDATA != 0 {
                node.u.datasize = size_of::<Pageno>();
                if f & consts::V_BIGDATA != 0 { //data already on overflow pages.
                    unsafe { ptr::copy_nonoverlapping(val.unwrap().data, page_ptr.offset((ofs + size_of::<Node>() + key.size) as isize), val.unwrap().size); }
                } else {
                    assert!(!ofp.is_null());
                    let page = jump_head!(ofp, DirtyPageHead, PageHead);
                    unsafe {
                        ptr::copy_nonoverlapping(&page.pageno as *const _ as *const u8, page_ptr.offset((ofs + size_of::<Node>() + key.size) as isize), size_of::<Pageno>());

                        //copy val data to overflow_pages
                        ptr::copy_nonoverlapping(val.unwrap().data, ofp.offset((size_of::<DirtyPageHead>() + size_of::<PageHead>()) as isize), val.unwrap().size);
                    }
                }
            } else { // if normal size data.
                node.u.datasize = val.unwrap().size;
                //copy val data
                unsafe {
                    ptr::copy_nonoverlapping(val.unwrap().data, page_ptr.offset((ofs + size_of::<Node>() + key.size) as isize), val.unwrap().size);
                }
            }

        } else {
            node.u.pageno = pageno.unwrap();
        }

        Ok(())
    }

    /**
     * delete a node from a page.
     * all nodes are aranged tightly.
     */
    pub fn del_node(page_ptr: *mut u8, index: usize) -> Result<(), Errors> {
        let num_keys = Self::num_keys(page_ptr);
        assert!(index < num_keys);

        let mut node_offsets = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<PageHead>() as isize)});
        let node = unsafe {*(page_ptr.offset(node_offsets[index] as isize) as *const Node)};
        let node_offset = node_offsets[index];
        let mut node_size = size_of::<Node>() + node.key_size;
        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            if node.node_flags & consts::V_BIGDATA != 0 {
                node_size += size_of::<Pageno>();
            } else {
                node_size += unsafe {node.u.datasize};
            }
        }

        //update all node addresses if necessary
        let mut i: usize = 0;
        let mut k: usize = 0;
        while i < num_keys {
            if i != index {
                node_offsets[k] = node_offsets[i];
                if node_offsets[k] < node_offset {
                    node_offsets[k] += node_size as Indext;
                }
                k += 1;
            }
            i += 1;
        }
        
        Self::set_lower_bound(page_ptr, Self::get_lower_bound(page_ptr) - size_of::<Indext>());

        //move all nodes address smaller than this node up node size 
        let upper_bound = Self::get_upper_bound(page_ptr);
        unsafe {
            ptr::copy(page_ptr.offset(upper_bound as isize), page_ptr.offset((upper_bound - node_size) as isize), node_offset as usize - node_size - upper_bound);
        }

        Self::set_upper_bound(page_ptr, upper_bound + node_size);

        Ok(())
    }
}

impl fmt::Debug for PageParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageParent")
            .field("page", &PageHead::get_pageno(self.page))
            .field("parent", &PageHead::get_pageno(self.parent))
            .field("index", &self.index)
            .finish()
    }
}

impl PageParent {
    pub fn new() -> Self {
        Self {
            page: ptr::null_mut(),
            parent: ptr::null_mut(),
            index: std::usize::MAX
        }
    }
}


