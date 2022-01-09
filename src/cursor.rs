/**********************************************
  > File Name		: cursor.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 22 Dec 2021 11:24:15 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

//use std::ptr::{NonNull};
use std::fmt;
use std::sync::{Arc};
use std::ptr;

use crate::page::{PageHead, DirtyPageHead, Node, PageParent};
use crate::mdb::Env;
use crate::txn::{Txn, Val};
use crate::errors::Errors;
use crate::consts;

/**
 * We use a cursor to search in the database,
 * As all keys are stored by order, to improve search efficiency,
 * a cursor supports multiple search modes.
 * 
 * If a cursor is not initilized, it will be set at the leftmost 
 * key of the tree.
 * And pages along the way is stored in a stack, if what to search 
 * does not hit in this page. It pops out the current leaf page, and 
 * finds it's next sibling by it's parent page which is now the top 
 * page of the stack. If all leaf pages in it's parent does not hit.
 * The parent page is popped out too, and so on. Until finds the target
 * or the stack is empty.
 */
#[derive(Clone)]
pub struct Cursor<'a> {
    env: Arc<Env<'a>>,
    path: Vec<PageParent>
}

impl fmt::Debug for Cursor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut path_string = String::from("");
        for i in 0..self.path.len() {
            path_string.push_str(format!("{}->", PageHead::get_pageno(self.path[i].page)).as_str());
        }
        f.debug_struct("Cursor")
            .field("path", &path_string)
            .finish()
    }
}

impl<'a> Cursor<'a> {
    pub fn new(env_ref: &Arc<Env<'a>>) -> Self {
        Self {
            env: env_ref.clone(),
            path: Vec::new()
        }
    }

    /**
     * Get a key/value pair by a key. 
     * Search result has to be accurate.
     * And the path will be cleared and re-filled with the new path.
     * If found, a Val type val returned.
     * If not found, KeyNotFound error returned.
     *
     * If a txn is provided and is writable, get page by it's root page.
     * Other than just get page by env's root page.
     *
     * Here's the difference between a read-only transaction and no transaction provided.
     * If it's a read-only transaction, the root page is determined once the transaction 
     * is created. And all read operations during the transaction are done by this root page.
     * And if you call get without prividing any transaction, the root page may be updated
     * each time as there may be a write transaction committed between two get operations.
     * So if @txn is None, first call env.env_read_meta then ask for the root pageno.
     */
    pub fn get(&mut self, key: &Val, txn: Option<&Txn>) -> Result<Val, Errors> {
        if !self.path.is_empty() {
            self.path.clear();
        }

        let mut page_ptr = match txn {
            None => {
                self.env.env_read_meta()?;
                self.env.get_page(self.env.get_root_pageno(), None)?
            },
            Some(v) => {
                self.env.get_page(v.get_txn_root(), Some(v))?
            }
        };

        let index = match PageHead::search_node(page_ptr, key, self.env.cmp_func)? {
            None => {
                let num_keys = PageHead::num_keys(page_ptr);
                if num_keys == 0 {
                    return Err(KeyNotFound);
                }
                num_keys - 1
            },
            Some((mut index, exact)) => {
                if !exact {index -= 1;}
                index
            }
        };
        self.path.push(PageParent {parent: ptr::null_mut(), page: page_ptr, index: std::usize::MAX});//push root.

        while PageHead::is_set(page_ptr, consts::P_BRANCH) {
            let index = match PageHead::search_node(page_ptr, key, self.env.cmp_func)? {
                None => { //key is greater than all keys in the page.
                    PageHead::num_keys(page_ptr) - 1
                },
                Some((mut index, exact)) => {
                    if !exact {index -= 1;}
                    index
                }
            };

            let node: Node = PageHead::get_node(page_ptr, index)?;
            //next get the page that nodes points to.
            let child = self.env.get_page(unsafe {node.u.pageno}, txn)?;

            self.path.push(PageParent {
                parent: page_ptr,
                page: child,
                index 
            });

            page_ptr = child;
        }

        assert!(PageHead::is_set(page_ptr, consts::P_LEAF));
        let index = match PageHead::search_node(page_ptr, key, self.env.cmp_func)? {
            None => {
                return Err(Errors::KeyNotFound(format!("key {:?} not found", key)));
            },
            Some((index, exact)) => {
                if !exact {
                    return Err(Errors::KeyNotFound(format!("key {:?} not found", key)));
                }
                index
            }
        };

        let node_ptr: *const Node = PageHead::get_node_ptr(page_ptr, index)?;
        
        Ok(Node::get_val(node_ptr))
    }

    /**
     * Get the val of the next key
     * The cursor has to be initilized, which means self.path is not empty.
     * Otherwise CursorUninitialized error is returned.
     * If there's no next key, EOF error is returned.
     */
    pub fn next(&mut self, txn: Option<&Txn>) -> Result<Val, Errors> {
        if self.path.is_empty() {
            return Err(Errors::CursorUninitialized);
        }

        let mut page_parent: PageParent;
        while !self.path.is_empty() {
            page_parent = self.path.pop().unwrap();
            page_parent.index += 1;
            
            let num_keys = PageHead::num_keys(page_parent.page);
            assert!(page_parent.index <= num_keys);
            if num_keys > page_parent.index {
                self.path.push(page_parent);
                page_parent.index = 0;
                while PageHead::is_set(page_parent.page, consts::P_BRANCH) {
                    page_parent.parent = page_parent.page;
                    page_parent.page = self.env.get_page(unsafe {PageHead::get_node(page_parent.page, page_parent.index)?.u.pageno}, txn)?;
                    self.path.push(page_parent);
                }
                break;
            }
        }

        assert!(PageHead::is_set(page_parent.page, consts::P_LEAF));

        let node_ptr = PageHead::get_node_ptr(page_parent.page, 0)?;
        Ok(Node::get_val(node_ptr))
    }
}
