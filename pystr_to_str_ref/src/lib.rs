use std::ffi::{c_void, c_char};
use std::fmt::Write;

#[allow(non_camel_case_types)]
pub struct converter(String);

#[no_mangle]
pub unsafe extern "C" fn questdb_pystr_converter_new() -> *mut converter {
    Box::into_raw(Box::new(converter(String::with_capacity(64))))
}

#[no_mangle]
pub unsafe extern "C" fn questdb_pystr_converter_free(c: *mut converter) {
    if !c.is_null() {
        drop(Box::from_raw(c));
    }
}

fn encode_ucs1(dest: &mut String, buf: &[u8]) -> bool {
    write!(dest, "'nyi").unwrap();
    false
}

fn encode_ucs2(dest: &mut String, buf: &[u16]) -> bool {
    write!(dest, "'nyi").unwrap();
    false
}

fn encode_ucs4(dest: &mut String, buf: &[u32]) -> bool {
    write!(dest, "'nyi").unwrap();
    false
}

/// Converts a Python string to a UTF8 buffer.
/// * Width is 1 for UCS1, 2 for UCS2, 4 for UCS4.
/// * Count is the number of code points.
/// * Input is the pointer to the UCS{1,2,4} data.
/// * size_out is the resulting size in bytes of the UTF8 string.
/// * buf_out is set to point to the UTF8 string.
/// Returns true for success of false for failure.
/// In case of failure, size_out and buf_out contain the error message.
#[no_mangle]
pub unsafe extern "C" fn questdb_pystr_to_convert(
        c: *mut converter,
        width: u8, count: usize, input: *const c_void,
        size_out: *mut usize, buf_out: *mut *const c_char) -> bool {
    let sbuf: &mut String = &mut (*c).0;
    sbuf.clear();
    let ok = match width {
        1 => encode_ucs1(
            sbuf,
            std::slice::from_raw_parts(input as *const u8, count)),
        2 => encode_ucs2(
            sbuf,
            std::slice::from_raw_parts(input as *const u16, count)),
        4 => encode_ucs4(
            sbuf,
            std::slice::from_raw_parts(input as *const u32, count)),
        _ => {
            write!(sbuf, "Unsupported width: {}", width).unwrap();
            false
        },
    };
    *size_out = sbuf.len();
    *buf_out = sbuf.as_ptr() as *const c_char;
    ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
