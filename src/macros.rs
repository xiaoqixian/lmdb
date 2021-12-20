/**********************************************
  > File Name		: macros.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Sat 18 Dec 2021 11:34:52 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

#[cfg(debug_assertions)]
#[macro_export]
macro_rules! info {
    ($string: expr) => {
        //println!("{}[INFO {}:{}] {}", termion::color::Fg(termion::color::Blue), file!(), line!(), $string);
        colour::blue_ln!("[INFO {}:{}] {}", file!(), line!(), $string);
    };
    ($string: expr, $($formats: expr)*) => {
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
        colour::yellow_ln!("[DEBUG {}:{}] {}", file!(), line!(), $string);
    };
    ($string: expr, $($formats: tt)*) => {
        let s = format!($string, $($formats)*);
        colour::yellow_ln!("[DEBUG {}:{}] {}", file!(), line!(), s);
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
        colour::red!("[ERROR {}:{}] {}", file!(), line!(), $string);
    };
    ($string: expr, $($formats: expr)*) => {
        let s = format!($string, $($formats)*);
        colour::red!("[ERROR {}:{}] {}", file!(), line!(), s);
    }
}

#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! error {
    ($string: expr) => {};
    ($string: expr, $($formats: expr)*) => {}
}

/**
 * jump the header of a page
 */
#[macro_export]
macro_rules! jump_head {
    ($ptr: expr, $type: ty) => {
        unsafe {
            &*($ptr.offset(size_of::<PageHead>() as isize) as *const $type)
        }
    }
}

#[macro_export]
macro_rules! jump_head_mut {
    ($ptr: expr, $type: ty) => {
        unsafe {
            &mut *($ptr.offset(size_of::<PageHead>() as isize) as *mut $type)
        }
    }
}
