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
use crate::{info, debug, show_keys};

/**
 * We use a cursor to search in the database,
 * As all keys are stored by order, to improve search efficiency,
 * a cursor supports multiple search modes.
 * 
 * If a cursor is not initilized, it will be set at the leftmost 
 * key of the tree.
 * And pages along the way is stored in a stack @path, if what to search 
 * does not hit in this page. It pops out the current leaf page, and 
 * finds it's next sibling by it's parent page which is now the top 
 * page of the stack. If all leaf pages in it's parent does not hit.
 * The parent page is popped out too, and so on. Until finds the target
 * or the stack is empty.
 *
 * The type of elements in path is PageParent, but the top of a valid path is 
 * always a leaf page as a parent and a node as a "page", just to be simple.
 * And to differ a node pointer with a read page pointer, the page field of the
 * page_parent of a leaf page and a node is always null.
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
     * A cursor has to be initilized first before using.
     * A cursor can be initilized in two ways:
     * 1. call init method
     * 2. call get method
     *
     * init method put the cursor at the leftmost leaf node 
     * of the tree.
     */
    pub fn init(&mut self, txn: Option<&Txn>) -> Result<(), Errors> {
        if !self.path.is_empty() {
            return Err(Errors::CursorInitialized);
        }
        
        let root = match txn {
            None => {
                self.env.env_read_meta()?;
                self.env.get_root_pageno()
            },
            Some(txn) => {
                txn.get_txn_root()
            }
        };
        info!("Cursor initilization get root page {}", root);

        let mut page_parent = PageParent::new();
        page_parent.page = self.env.get_page(root, txn)?;

        page_parent.index = 0;
        while true {
            if PageHead::is_set(page_parent.page, consts::P_LEAF) {
                page_parent.parent = page_parent.page;
                page_parent.page = ptr::null_mut();
                page_parent.index = 0;
                self.path.push(page_parent);
                break;
            }

            page_parent.parent = page_parent.page;
            debug!("get first child {} of page {}", unsafe {PageHead::get_node(page_parent.page, 0)?.u.pageno}, PageHead::get_pageno(page_parent.page));
            page_parent.page = match self.env.get_page(unsafe {PageHead::get_node(page_parent.page, 0)?.u.pageno}, txn) {
                Ok(v) => v,
                Err(e) => {
                    self.path.clear();
                    return Err(e);
                }
            };

            self.path.push(page_parent);
        }

        info!("cursor init at key: {:?}", Node::get_key(PageHead::get_node_ptr(page_parent.parent, 0)?).unwrap());

        Ok(())
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
        info!("get key {:?}", key);
        if !self.path.is_empty() {
            self.path.clear();
        }

        let mut page_ptr = match txn {
            None => {
                self.env.env_read_meta()?;
                self.env.get_page(match self.env.get_root_pageno() {
                    consts::P_INVALID => {return Err(Errors::EmptyTree);},
                    v => v
                }, None)?
            },
            Some(v) => {
                self.env.get_page(v.get_txn_root(), Some(v))?
            }
        };

        while PageHead::is_set(page_ptr, consts::P_BRANCH) {
            //PageHead::show_keys(page_ptr)?;
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
        //PageHead::show_keys(page_ptr)?;

        let index = match PageHead::search_node(page_ptr, key, self.env.cmp_func)? {
            None => {
                info!("key doesn't hit because of greater than all keys");
                show_keys!(page_ptr);
                return Err(Errors::KeyNotFound(format!("key {:?} not found", key)));
            },
            Some((index, exact)) => {
                if !exact {
                    info!("key just doesn't hit");
                    show_keys!(page_ptr);
                    return Err(Errors::KeyNotFound(format!("key {:?} not found", key)));
                }
                index
            }
        };

        self.path.push(PageParent {
            parent: page_ptr,
            page: ptr::null_mut(),
            index
        });
        let node_ptr: *const Node = PageHead::get_node_ptr(page_ptr, index)?;
        
        Ok(Node::get_val(node_ptr))
    }

    /**
     * Get the next key/value pair beside this key.
     * The cursor must be initilized, which means self.path is not empty.
     * Otherwise CursorUninitialized error is returned.
     * If there's no next key, EOF error is returned.
     */
    pub fn next(&mut self, txn: Option<&Txn>) -> Result<(Val, Val), Errors> {
        if self.path.is_empty() {
            return Err(Errors::CursorUninitialized);
        }

        let mut page_parent = PageParent::new();
        while !self.path.is_empty() {
            page_parent = self.path.pop().unwrap();
            page_parent.index += 1;
            
            let num_keys = PageHead::num_keys(page_parent.parent);
            assert!(page_parent.index <= num_keys);
            if num_keys > page_parent.index {
                if page_parent.page.is_null() {
                    self.path.push(page_parent);
                    break;
                }

        println!("");

                let temp_pageno = PageHead::get_pageno(page_parent.page);//TODO
                page_parent.page = self.env.get_page(unsafe {PageHead::get_node(page_parent.parent, page_parent.index)?.u.pageno}, txn)?;
                debug!("get {} right sibling page {} ", temp_pageno, unsafe {PageHead::get_node(page_parent.parent, page_parent.index)?.u.pageno});
                self.path.push(page_parent);
                page_parent.index = 0;
            
                while PageHead::is_set(page_parent.page, consts::P_BRANCH) {
                    page_parent.parent = page_parent.page;
                    page_parent.page = self.env.get_page(unsafe {PageHead::get_node(page_parent.page, page_parent.index)?.u.pageno}, txn)?;
                    debug!("get {} first child {}", PageHead::get_pageno(page_parent.parent), PageHead::get_pageno(page_parent.page));
                    self.path.push(page_parent);
                }

                assert!(PageHead::is_set(page_parent.page, consts::P_LEAF));
                page_parent.parent = page_parent.page;
                page_parent.page = ptr::null_mut();
                self.path.push(page_parent);

                break;
            }
        }

        if self.path.is_empty() {
            return Err(Errors::EOF);
        }

        //info!("cursor next at {:?}", page_parent);

        let node_ptr = PageHead::get_node_ptr(page_parent.parent, page_parent.index)?;
        Ok((Node::get_key(node_ptr).unwrap(), Node::get_val(node_ptr)))
    }
}
