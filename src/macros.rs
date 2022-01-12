/**********************************************
  > File Name		: macros.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Sat 18 Dec 2021 11:34:52 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

#[macro_export]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }}
}

#[cfg(debug_assertions)]
#[macro_export]
macro_rules! info {
    ($string: expr) => {
        //println!("{}[INFO {}:{}] {}", termion::color::Fg(termion::color::Blue), file!(), line!(), $string);
        colour::blue_ln!("[INFO {}:{}] {}", file!(), line!(), $string);
    };
    ($string: expr, $($formats: tt)*) => {
        let s = format!($string, $($formats)*);
        colour::blue_ln!("[INFO {}:{}] {}", file!(), line!(), s);
    }
}

#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! info {
    ($string: expr) => {};
    ($string: expr, $($formats: expr)*) => {}
}

#[cfg(debug_assertions)]
#[macro_export]
macro_rules! debug {
    ($string: expr) => {
        colour::yellow_ln!("[DEBUG {}:{}:{}] {}", file!(), crate::function!(), line!(), $string);
    };
    ($string: expr, $($formats: tt)*) => {
        let s = format!($string, $($formats)*);
        colour::yellow_ln!("[DEBUG {}:{}:{}] {}", file!(), crate::function!(), line!(), s);
    }
}

#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! debug {
    ($string: expr) => {};
    ($string: expr, $($formats: expr)*) => {}
}

#[cfg(debug_assertions)]
#[macro_export]
macro_rules! error {
    ($string: expr) => {
        colour::red_ln!("[ERROR {}:{}:{}] {}", file!(), crate::function!(), line!(), $string);
    };
    ($string: expr, $($formats: tt)*) => {
        let s = format!($string, $($formats)*);
        colour::red_ln!("[ERROR {}:{}:{}] {}", file!(), crate::function!(), line!(), s);
    }
}

#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! error {
    ($string: expr) => {};
    ($string: expr, $($formats: expr)*) => {}
}

#[cfg(debug_assertions)]
#[macro_export]
macro_rules! show_keys {
    ($ptr: expr) => {
        crate::page::PageHead::show_keys($ptr);
    }
}

#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! show_keys {
    ($ptr: expr) => {}
}

/**
 * jump the header of a page
 */
#[macro_export]
macro_rules! jump_head {
    ($ptr: expr, $head: ty, $to: ty) => {
        unsafe {
            &*($ptr.offset(size_of::<$head>() as isize) as *const $to)
        }
    }
}

#[macro_export]
macro_rules! jump_head_mut {
    ($ptr: expr, $head: ty, $to: ty) => {
        unsafe {
            &mut *($ptr.offset(size_of::<$head>() as isize) as *mut $to)
        }
    }
}

#[macro_export]
macro_rules! back_head_mut {
    ($ptr: expr, $head: ty) => {
        unsafe {
            &mut *($ptr.offset(-(size_of::<$head>() as isize)) as *mut $head)
        }
    };
}

#[macro_export]
macro_rules! jump_head_ptr {
    ($ptr: expr, $head: ty) => {
        unsafe {
            $ptr.offset(size_of::<$head>() as isize)
        }
    };
}

macro_rules! offset_of {
    ($ty:ty, $field:ident) => {
        unsafe {&(*(0 as *const $ty)).$field as *const _ as isize}
    }
}

#[macro_export]
macro_rules! ptr_ref {
    ($ptr: expr, $type: ty) => {
        unsafe {
            &*($ptr as *const $type)
        }
    }
}

#[macro_export]
macro_rules! ptr_mut_ref {
    ($ptr: ident, $type: ty) => {
        unsafe {
            &mut *($ptr as *mut $type)
        }
    }
}

