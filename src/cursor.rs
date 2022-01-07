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
     * If found, a Val type val returned.
     * If not found, KeyNotFound error returned.
     *
     * If a txn is provided and is writable, get page by it's root page.
     */
    pub fn get(&mut self, key: &Val, txn: Option<&Txn>) -> Result<Val, Errors> {
        if !self.path.is_empty() {
            self.path.clear();
        }

        let mut page_parent = PageParent::new();
        page_parent.page = match txn {
            None => {
                self.env.env_read_meta()?;
                self.env.get_page(self.env.get_root_pageno(), None)?
            },
            Some(v) => {
                self.env.get_page(v.get_txn_root(), Some(v))?
            }
        };

        while PageHead::is_set(page_parent.page, consts::P_BRANCH) {
            let node: &Node = match PageHead::search_node(page_parent.page, key, self.env.cmp_func)? {
                None => { //key is greater than all keys in the page.
                    let index = PageHead::num_keys(page_parent.page) - 1;
                    unsafe {&*PageHead::get_node_ptr(page_parent.page, index)?}
                },
                Some((mut index, exact)) => {
                    if !exact {index -= 1;}
                    unsafe {&*PageHead::get_node_ptr(page_parent.page, index)?}
                }
            };

            //next get the page that nodes points to.
        }

        Ok(Val::new(0, ptr::null_mut()))
    }
}
