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
use crate::{debug, error, jump_head, jump_head_mut, info};
use crate::flags::{PageFlags, NodeFlags};

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

#[derive(Copy, Clone)]
pub struct PageHead {
    pub pageno: Pageno,
    pub page_flags: PageFlags,
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
#[derive(Clone, Copy)]
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
    pub node_flags: NodeFlags,
    pub key_size: usize, //key_size could be zero, but only when it's a branch node and insert index is zero.
}

impl fmt::Debug for PageHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageHead")
            .field("pageno", if self.pageno == consts::P_INVALID {&"P_INVALID"} else {&self.pageno})
            .field("page_flags", &self.page_flags)
            .field("page_bounds", &self.page_bounds)
            .field("overflow_pages", &self.overflow_pages)
            .finish()
    }
}

impl fmt::Debug for DirtyPageHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parent_pageno = if self.parent.is_null() {consts::P_INVALID} else {PageHead::get_pageno(self.parent)};

        f.debug_struct("DirtyPageHead")
            .field("parent", if parent_pageno == consts::P_INVALID {
                &"null"
            } else {
                &parent_pageno
            })
            .field("index", if self.index == std::usize::MAX {&"None"} else {&self.index})
            .field("num", &self.num)
            .field("layout", &self.layout)
            .finish()
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.key_size != 0 {
            let key_data = unsafe {(self as *const Self).offset(1) as *const u8};
            let s = std::str::from_utf8(unsafe {std::slice::from_raw_parts(key_data, self.key_size)}).unwrap();
            f.debug_struct("Node")
                .field("node_flags", &self.node_flags)
                .field("key", &s)
                .finish()
        } else {
            f.debug_struct("Node")
                .field("node_flags", &self.node_flags)
                .field("key", &"None")
                .finish()
        }
    }
}

impl PageHead {
    pub fn new(pageno: Pageno) -> Self {
        Self {
            pageno,
            page_flags: PageFlags::new(0),
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
    
    pub fn is_set(page_ptr: *mut u8, flag: PageFlags) -> bool {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_flags.is_set(flag)
        }
    }

    pub fn get_flags(page_ptr: *mut u8) -> PageFlags {
        assert!(!page_ptr.is_null());
        unsafe {
            (*(page_ptr as *const Self)).page_flags
        }
    }


    pub fn get_info(page_ptr: *mut u8) -> String {
        String::from("fuck")
    }

    pub fn show_info(page_ptr: *mut u8) {
        assert!(!page_ptr.is_null());
        if Self::is_set(page_ptr, consts::P_BRANCH) {
            println!("head: {:?}", unsafe {&*(page_ptr as *const Self)});
            println!("nodes: ");
            let arr = Array::<Indext>::new_jump_head(page_ptr);
            let num_keys = Self::num_keys(page_ptr);

            for i in 0..num_keys {
                println!("ofs = {}, {:?}", arr[i], unsafe {ptr::read(page_ptr.offset(arr[i] as isize) as *const Node)});
            }
        }
    }

    pub fn num_keys(page_ptr: *mut u8) -> usize {
        let lower_bound = unsafe {*(page_ptr as *const PageHead)}.page_bounds.lower_bound;
        (lower_bound - size_of::<PageHead>()) >> 1 //because ptr index length is 2 bytes.
    }

    pub fn branch_size(page_ptr: *mut u8, index: usize) -> Result<usize, Errors> {
        assert!(!page_ptr.is_null());
        let node = Self::get_node(page_ptr, index)?;
        Ok(size_of::<Indext>() + size_of::<Node>() + node.key_size)
    }

    pub fn get_dpage_head(page_ptr: *mut u8) -> DirtyPageHead {
        unsafe {
            *(page_ptr.offset(-(size_of::<DirtyPageHead>() as isize)) as *const DirtyPageHead)
        }
    }

    /**
     * update a child page number for a branch page.
     */
    pub fn update_child(parent: *mut u8, pageno: Pageno, index: usize) -> Result<(), Errors> {
        if !Self::is_set(parent, consts::P_BRANCH) {
            return Err(Errors::InvalidPageType(format!("{:?}", parent)));
        }

        //let node_offset = unsafe {*(parent.offset((size_of::<PageHead>() + index*size_of::<Indext>()) as isize) as *const Indext) as isize};
        let node_offset = Array::<Indext>::new_jump_head(parent)[index] as isize;
        debug!("child node offset {}", node_offset);
        let node = unsafe {&mut *(parent.offset(node_offset) as *mut Node)};
        node.u.pageno = pageno;
        Ok(())
    }

    pub fn get_node_ptr(page_ptr: *mut u8, index: usize) -> Result<*const Node, Errors> {
        let num_keys = Self::num_keys(page_ptr);
        if index >= num_keys {
            error!("Node index overflow, num_keys: {}, index: {}", num_keys, index);
            return Err(Errors::IndexOverflow(index));
        }
        let ptrs = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<PageHead>() as isize)});
        Ok(unsafe {page_ptr.offset(ptrs[index] as isize) as *const Node})
    }

    /**
     * get a copy of a node.
     */
    pub fn get_node(page_ptr: *mut u8, index: usize) -> Result<Node, Errors> {
        let node_ptr = Self::get_node_ptr(page_ptr, index)?;
        Ok(unsafe {*node_ptr})
    }

    pub fn get_key(page_ptr: *mut u8, index: usize) -> Result<Val, Errors> {
        let node_ptr = Self::get_node_ptr(page_ptr, index)?;
        let key_size = unsafe {(*node_ptr).key_size};
        if key_size == 0 {
            Ok(Val {size: 0, data: ptr::null_mut()})
        } else {
            Ok(unsafe {Val {size: key_size, data: node_ptr.offset(1) as *mut u8}})
        }
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
        info!("search_node for {} on page {}, address {:?}", key.get_readable_data(), Self::get_pageno(page_ptr), page_ptr);
        let mut low: i32 = if PageHead::is_set(page_ptr, consts::P_LEAF) {0} else {1};
        let mut high: i32 = PageHead::num_keys(page_ptr) as i32 - 1;
        let mut mid: i32 = -1;
        let mut cmp_res: i32 = -2;
        let node_offsets = Array::<Indext>::new_jump_head(page_ptr);

        print!("node_offsets: "); node_offsets.show(Self::num_keys(page_ptr));

        while low <= high {
            mid = (low+high) >> 1;
            let ofs = node_offsets[mid as usize] as isize;
            debug!("low = {}, high = {}, ofs = {}", low, high, ofs);
            assert!(mid >= 0);
            let mid_node = unsafe {&*(page_ptr.offset(ofs) as *const Node)};
            info!("mid_node for {}: {:?}", mid, mid_node);
            let mid_key = Val::new(mid_node.key_size, unsafe {page_ptr.offset(ofs + size_of::<Node>() as isize)});
            
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

        if PageHead::is_set(page_ptr, consts::P_BRANCH) {
            let mid_node = unsafe {&*(page_ptr.offset(node_offsets[mid as usize] as isize) as *const Node)};
            info!("key {} go with node {:?}", key.get_readable_data(), mid_node);
        }

        debug!("searched index {}", mid);
        Ok(Some((mid as usize, cmp_res == 0)))
    }


    /**
     * append a node to a page.
     * If no room in this page, we need to split the page, but it's the job of this function.
     * So it just returns Err(Errors::NoSpace)
     *
     * If this is a branch page, then data is None.
     * If this is a leaf page, then pageno is None.
     *
     * Also the key can be None, but only for branch pages and index has to be 0.
     */
    pub fn add_node(page_ptr: *mut u8, key: Option<&Val>, val: Option<&Val>, pageno: Option<Pageno>, index: usize, f: NodeFlags, txn: &Txn) -> Result<(), Errors> {
        assert_ne!(val.is_none(), pageno.is_none());
        debug!("index: {}, PageHead::is_set(P_BRANCH): {}, key.is_none(): {}", index, PageHead::is_set(page_ptr, consts::P_BRANCH), key.is_none());
        assert_eq!(index == 0 && PageHead::is_set(page_ptr, consts::P_BRANCH), key.is_none());

        info!("add node on page {} at index {} with key {:?}", Self::get_pageno(page_ptr), index, key);
        let mut flags = f; // as rust doesn't allow mutable paprameters
        let page_size = txn.env.get_page_size();
        let key_size = match key {
            None => 0,
            Some(v) => v.size
        };
        //evaluate node size needed
        let mut node_size = size_of::<Node>() + key_size;
        let mut ofp: *mut u8 = ptr::null_mut();

        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            assert!(val.is_some());
            
            if flags.is_set(consts::V_BIGDATA) {
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

        if node_size + size_of::<Indext>() >= Self::left_space(page_ptr) {
            debug!("page no enough space for another node");
            debug!("page_bounds: {:?}, node_size: {}", unsafe {*(page_ptr as *const Self)}.page_bounds, node_size);
            return Err(Errors::NoSpace(format!("page_bounds: {:?}, node_size: {}", unsafe {*(page_ptr as *const Self)}.page_bounds, node_size)));
        }

        let mut i = Self::num_keys(page_ptr);
        let mut node_offsets = Array::<Indext>::new(unsafe {page_ptr.offset(size_of::<Self>() as isize)});

        //move all nodes after index up one slot.
        while i > index {
            node_offsets[i] = node_offsets[i-1];
            i -= 1;
        }

        let ofs = Self::get_upper_bound(page_ptr) - node_size;
        node_offsets[index] = ofs as Indext;
        debug!("set ofs {} for index {} in {}", node_offsets[index], index, Self::get_pageno(page_ptr));

        let node = unsafe {&mut *(page_ptr.offset(ofs as isize) as *mut Node)};
        node.key_size = key_size;
        node.node_flags = flags;

        //copy key
        if let Some(k) = key {
            unsafe {
                let key_data = page_ptr.offset((ofs + size_of::<Node>()) as isize);
                ptr::copy_nonoverlapping(k.data, key_data, k.size);
            }
        }
        
        //copy data or set overflow pagenumber.
        if PageHead::is_set(page_ptr, consts::P_LEAF) {
            if flags.is_set(consts::V_BIGDATA) {
                node.u.datasize = size_of::<Pageno>();
                if f.is_set(consts::V_BIGDATA) {
                    unsafe { ptr::copy_nonoverlapping(val.unwrap().data, page_ptr.offset((ofs + size_of::<Node>() + key.unwrap().size) as isize), val.unwrap().size); }
                } else {
                    assert!(!ofp.is_null());
                    let page = jump_head!(ofp, DirtyPageHead, PageHead);
                    unsafe {
                        ptr::copy_nonoverlapping(&page.pageno as *const _ as *const u8, page_ptr.offset((ofs + size_of::<Node>() + key.unwrap().size) as isize), size_of::<Pageno>());


                        //copy val data to overflow_pages
                        ptr::copy_nonoverlapping(val.unwrap().data, ofp.offset((size_of::<DirtyPageHead>() + size_of::<PageHead>()) as isize), val.unwrap().size);
                    }
                }
            } else { // if normal size data.
                node.u.datasize = val.unwrap().size;
                //copy val data
                unsafe {
                    ptr::copy_nonoverlapping(val.unwrap().data, page_ptr.offset((ofs + size_of::<Node>() + key_size) as isize), val.unwrap().size);
                }
            }

        } else {
            //for branch nodes.
            node.u.pageno = pageno.unwrap();
        }

        Self::set_lower_bound(page_ptr, Self::get_lower_bound(page_ptr) + size_of::<Indext>());
        Self::set_upper_bound(page_ptr, Self::get_upper_bound(page_ptr) - node_size);

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
            if node.node_flags.is_set(consts::V_BIGDATA) {
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

impl DirtyPageHead {
    #[inline]
    pub fn get_parent(dpage_ptr: *mut Self) -> *mut u8 {
        assert!(!dpage_ptr.is_null());
        unsafe {(*dpage_ptr).parent}
    }

    #[inline]
    pub fn get_index(dpage_ptr: *mut Self) -> *mut u8 {
        assert!(!dpage_ptr.is_null());
        unsafe {(*dpage_ptr).parent}
    }
}
