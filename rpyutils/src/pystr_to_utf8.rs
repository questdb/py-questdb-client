/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

use std::ffi::c_char;
use std::slice::from_raw_parts;

#[allow(non_camel_case_types)]
pub struct qdb_pystr_buf(Vec<String>);

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct qdb_pystr_pos {
    pub chain: usize,
    pub string: usize,
}

/// Prepare a new buffer. The buffer must be freed with `qdb_pystr_free`.
/// The `qdb_ucsX_to_utf8` functions will write to this buffer.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_new() -> *mut qdb_pystr_buf {
    Box::into_raw(Box::new(qdb_pystr_buf(Vec::new())))
}

/// Get current position. Use in conjunction with `truncate`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_tell(b: *const qdb_pystr_buf) -> qdb_pystr_pos {
    let b = &*b;
    let chain_pos = b.0.len();
    let string_pos = if chain_pos > 0 {
        b.0[chain_pos - 1].len()
    } else {
        0
    };
    qdb_pystr_pos {
        chain: chain_pos,
        string: string_pos,
    }
}

/// Trim the buffer to the given position. Use in conjunction with `tell`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_truncate(b: *mut qdb_pystr_buf, pos: qdb_pystr_pos) {
    let b = &mut *b;
    b.0.truncate(pos.chain);
    if !b.0.is_empty() {
        b.0[pos.chain - 1].truncate(pos.string);
    }
}

/// Reset the converter's buffer to zero length.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_clear(b: *mut qdb_pystr_buf) {
    let b = &mut *b;
    if !b.0.is_empty() {
        b.0.truncate(1);
        b.0[0].clear();
    }
}

/// Free the buffer. Must be called after `qdb_pystr_buf_new`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_free(b: *mut qdb_pystr_buf) {
    if !b.is_null() {
        drop(Box::from_raw(b));
    }
}

const MIN_BUF_LEN: usize = 1024;

/// A carefully crafted buffer with spare capacity for `len` bytes.
/// This is necessary to return "stable" addresses and avoid segfaults.
/// Rust is unaware we are borrowing its memory and could try to free it as
/// part of a reallocation if we were to use a `String` directly.
fn get_dest(chain: &mut Vec<String>, len: usize) -> &mut String {
    if !chain.is_empty() {
        let last = chain.last_mut().unwrap();
        if last.capacity() - last.len() >= len {
            return chain.last_mut().unwrap();
        }
    }
    chain.push(String::with_capacity(std::cmp::max(len, MIN_BUF_LEN)));
    chain.last_mut().unwrap()
}

#[inline(always)]
fn encode_loop<'a, T, F>(
    utf8_mult: usize,
    chain: &'a mut Vec<String>,
    buf: &[T],
    get_char: F,
) -> Result<&'a str, u32>
where
    F: Fn(T) -> Option<char>,
    T: Copy + Into<u32>,
{
    let dest = get_dest(chain, utf8_mult * buf.len());
    let last = dest.len();
    // for &b in buf.iter() {
    //     // Checking for validity is not optional:
    //     // >>> for n in range(2 ** 16):
    //     // >>>     chr(n).encode('utf-8')
    //     // UnicodeEncodeError: 'utf-8' codec can't encode character '\ud800'
    //     //   in position 0: surrogates not allowed
    //     match get_char(b) {
    //         Some(c) => dest.push(c),
    //         None => {
    //             dest.truncate(last);
    //             return Err(b.into());
    //         }
    //     }
    // }
    // Ok(&dest[last..])
    unsafe {
        let v = dest.as_mut_vec();
        v.set_len(v.capacity());
        let mut index = last;

        for &b in buf.iter() {
            let c = match get_char(b) {
                Some(c) => c,
                None => {
                    v.set_len(last);
                    return Err(b.into());
                }
            };
            let utf_c_len = c.len_utf8();
            match utf_c_len {
                1 => {
                    v[index] = c as u8;
                }
                2 => {
                    let mut codepoint_buf = [0; 4];
                    let bytes = c.encode_utf8(&mut codepoint_buf).as_bytes();
                    *v.get_unchecked_mut(index) = *bytes.get_unchecked(0);
                    *v.get_unchecked_mut(index + 1) = *bytes.get_unchecked(1);
                }
                3 => {
                    let mut codepoint_buf = [0; 4];
                    let bytes = c.encode_utf8(&mut codepoint_buf).as_bytes();
                    *v.get_unchecked_mut(index) = *bytes.get_unchecked(0);
                    *v.get_unchecked_mut(index + 1) = *bytes.get_unchecked(1);
                    *v.get_unchecked_mut(index + 2) = *bytes.get_unchecked(2);
                }
                4 => {
                    let mut codepoint_buf = [0; 4];
                    let bytes = c.encode_utf8(&mut codepoint_buf).as_bytes();
                    *v.get_unchecked_mut(index) = *bytes.get_unchecked(0);
                    *v.get_unchecked_mut(index + 1) = *bytes.get_unchecked(1);
                    *v.get_unchecked_mut(index + 2) = *bytes.get_unchecked(2);
                    *v.get_unchecked_mut(index + 3) = *bytes.get_unchecked(3);
                }
                _ => unreachable!(),
            }
            index += utf_c_len;
        }
        v.set_len(index);
    }
    Ok(&dest[last..])
}

/// Convert a Py_UCS1 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed from `b`.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs1_to_utf8(
    b: *mut qdb_pystr_buf,
    count: usize,
    input: *const u8,
    size_out: *mut usize,
    buf_out: *mut *const c_char,
) {
    let b = &mut *b;
    let i = from_raw_parts(input, count);

    // len(chr(2 ** 8 - 1).encode('utf-8')) == 2
    let utf8_mult = 2;
    let res = encode_loop(utf8_mult, &mut b.0, i, |c| Some(c as char)).unwrap();
    *size_out = res.len();
    *buf_out = res.as_ptr() as *const c_char;
}

/// Convert a Py_UCS2 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed from `b`.
/// In case of errors, returns `false` and bad_codepoint_out is set to the
/// offending codepoint.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs2_to_utf8(
    b: *mut qdb_pystr_buf,
    count: usize,
    input: *const u16,
    size_out: *mut usize,
    buf_out: *mut *const c_char,
    bad_codepoint_out: *mut u32,
) -> bool {
    let b = &mut *b;
    let i = from_raw_parts(input, count);

    // len(chr(2 ** 16 - 1).encode('utf-8')) == 3
    let utf8_mult = 3;
    let res = encode_loop(utf8_mult, &mut b.0, i, |c| char::from_u32(c as u32));
    match res {
        Ok(s) => {
            *size_out = s.len();
            *buf_out = s.as_ptr() as *const c_char;
            true
        }
        Err(bad) => {
            *bad_codepoint_out = bad;
            false
        }
    }
}

/// Convert a Py_UCS4 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed from `b`.
/// In case of errors, returns `false` and bad_codepoint_out is set to the
/// offending codepoint.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs4_to_utf8(
    b: *mut qdb_pystr_buf,
    count: usize,
    input: *const u32,
    size_out: *mut usize,
    buf_out: *mut *const c_char,
    bad_codepoint_out: *mut u32,
) -> bool {
    let b = &mut *b;
    let i = from_raw_parts(input, count);

    // Max 4 bytes allowed by RFC: https://www.rfc-editor.org/rfc/rfc3629#page-4
    let utf8_mult = 4;
    let res = encode_loop(utf8_mult, &mut b.0, i, char::from_u32);
    match res {
        Ok(s) => {
            *size_out = s.len();
            *buf_out = s.as_ptr() as *const c_char;
            true
        }
        Err(bad) => {
            *bad_codepoint_out = bad;
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Buf {
        buf: *mut qdb_pystr_buf,
    }

    impl Buf {
        fn new() -> Self {
            Self {
                buf: unsafe { qdb_pystr_buf_new() },
            }
        }

        fn chain(&self) -> &Vec<String> {
            unsafe { &(*self.buf).0 }
        }

        fn chain_mut(&mut self) -> &mut Vec<String> {
            unsafe { &mut (*self.buf).0 }
        }

        fn clear(&mut self) {
            unsafe { qdb_pystr_buf_clear(self.buf) }
        }

        fn tell(&self) -> qdb_pystr_pos {
            unsafe { qdb_pystr_buf_tell(self.buf) }
        }

        fn truncate(&mut self, pos: qdb_pystr_pos) {
            unsafe { qdb_pystr_buf_truncate(self.buf, pos) }
        }

        fn ucs1_to_utf8(&mut self, input: &[u8]) -> &'static str {
            let mut size_out = 0;
            let mut buf_out = std::ptr::null();
            unsafe {
                qdb_ucs1_to_utf8(
                    self.buf,
                    input.len(),
                    input.as_ptr(),
                    &mut size_out,
                    &mut buf_out,
                );
            }
            let slice = unsafe { from_raw_parts(buf_out as *const u8, size_out) };
            std::str::from_utf8(slice).unwrap()
        }

        fn ucs2_to_utf8(&mut self, input: &[u16]) -> Result<&'static str, u32> {
            let mut size_out = 0;
            let mut buf_out = std::ptr::null();
            let mut bad_codepoint = 0u32;
            let ok = unsafe {
                qdb_ucs2_to_utf8(
                    self.buf,
                    input.len(),
                    input.as_ptr(),
                    &mut size_out,
                    &mut buf_out,
                    &mut bad_codepoint,
                )
            };
            if ok {
                let slice = unsafe { from_raw_parts(buf_out as *const u8, size_out) };
                let msg = std::str::from_utf8(slice).unwrap();
                Ok(msg)
            } else {
                Err(bad_codepoint)
            }
        }

        fn ucs4_to_utf8(&mut self, input: &[u32]) -> Result<&'static str, u32> {
            let mut size_out = 0;
            let mut buf_out = std::ptr::null();
            let mut bad_codepoint = 0u32;
            let ok = unsafe {
                qdb_ucs4_to_utf8(
                    self.buf,
                    input.len(),
                    input.as_ptr(),
                    &mut size_out,
                    &mut buf_out,
                    &mut bad_codepoint,
                )
            };
            if ok {
                let slice = unsafe { from_raw_parts(buf_out as *const u8, size_out) };
                let msg = std::str::from_utf8(slice).unwrap();
                Ok(msg)
            } else {
                Err(bad_codepoint)
            }
        }
    }

    impl Drop for Buf {
        fn drop(&mut self) {
            unsafe {
                qdb_pystr_buf_free(self.buf);
            }
        }
    }

    #[test]
    fn test_empty() {
        let b = Buf::new();
        assert_eq!(b.chain().len(), 0);
        let pos = b.tell();
        assert_eq!(pos.chain, 0);
        assert_eq!(pos.string, 0);
    }

    #[test]
    fn test_ucs1() {
        let mut b = Buf::new();
        let s1 = b.ucs1_to_utf8(b"hello");
        assert_eq!(s1, "hello");
        assert_eq!(b.chain_mut().len(), 1);
        assert_eq!(b.chain_mut()[0].as_str().as_ptr(), s1.as_ptr());
        assert_eq!(b.chain()[0], "hello");
        assert_eq!(b.tell().chain, 1);
        assert_eq!(b.tell().string, 5);
        b.clear();
        assert_eq!(b.chain().len(), 1);
        assert_eq!(b.chain()[0], "");
        let s2 = b.ucs1_to_utf8(b"");
        assert_eq!(s2, "");
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 1,
                string: 0
            }
        );
        assert_eq!(s2.as_ptr(), b.chain()[0].as_str().as_ptr());
        let s3 = b.ucs1_to_utf8(b"10\xb5");
        assert_eq!(s3, "10µ");
        assert_eq!(s3.len(), 4); // 3 bytes in UCS-1, 4 bytes in UTF-8.
        assert_eq!(b.chain().len(), 1);
        assert_eq!(s3.as_ptr(), unsafe {
            b.chain()[0].as_str().as_ptr().add(s2.len())
        });
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 1,
                string: s2.len() + s3.len()
            }
        );
    }

    #[test]
    fn test_resize_and_truncate() {
        let mut b = Buf::new();
        let s1 = b.ucs1_to_utf8(b"abcdefghijklmnopqrstuvwxyz");
        assert_eq!(s1, "abcdefghijklmnopqrstuvwxyz");
        assert_eq!(b.chain_mut().len(), 1);
        assert_eq!(b.chain_mut()[0].as_str().as_ptr(), s1.as_ptr());

        let big_string = "hello world".repeat(1000);
        assert!(big_string.len() > MIN_BUF_LEN);
        let s2 = b.ucs1_to_utf8(big_string.as_bytes());
        assert_eq!(s2, big_string);
        assert_eq!(b.chain_mut().len(), 2);
        assert_eq!(b.chain_mut()[0].as_str().as_ptr(), s1.as_ptr());
        assert_eq!(b.chain_mut()[1].as_str().as_ptr(), s2.as_ptr());
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 2,
                string: 11000
            }
        );
        b.truncate(b.tell());
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 2,
                string: 11000
            }
        );

        let spare = b.chain_mut()[1].capacity() - b.chain_mut()[1].len();
        assert!(spare > 4);

        let test_string = "ab";
        let s3 = b.ucs1_to_utf8(test_string.as_bytes());
        assert_eq!(s3, test_string);
        assert_eq!(b.chain_mut().len(), 2);
        assert_eq!(b.chain_mut()[0].as_str().as_ptr(), s1.as_ptr());
        assert_eq!(b.chain_mut()[1].as_str().as_ptr(), s2.as_ptr());
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 2,
                string: 11000 + test_string.len()
            }
        );
    }

    #[test]
    fn test_ucs2() {
        let mut b = Buf::new();

        // We first check code points within the ASCII range.
        let s1 = b.ucs2_to_utf8(&[0x61, 0x62, 0x63, 0x64, 0x65]).unwrap();
        assert_eq!(s1, "abcde");
        assert_eq!(s1.len(), 5);

        // Now chars outside ASCII range, but within UCS-1 range.
        // These will yield two bytes each in UTF-8.
        let s2 = b.ucs2_to_utf8(&[0x00f0, 0x00e3, 0x00b5, 0x00b6]).unwrap();
        assert_eq!(s2, "ðãµ¶");
        assert_eq!(s2.len(), 8);

        // Now chars that actually require two bytes in UCS-2, but also fit in
        // two bytes in UTF-8.
        let s3 = b.ucs2_to_utf8(&[0x0100, 0x069c]).unwrap();
        assert_eq!(s3, "Āڜ");
        assert_eq!(s3.len(), 4);

        // Now chars that require two bytes in UCS-2 and 3 bytes in UTF-8.
        let s4 = b.ucs2_to_utf8(&[0x569c, 0xa4c2]).unwrap();
        assert_eq!(s4, "嚜꓂");
        assert_eq!(s4.len(), 6);

        // Quick check that we're just writing to the same buffer.
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 1,
                string: [s1, s2, s3, s4].iter().map(|s| s.len()).sum()
            }
        );

        // Now we finally check that errors are captured.
        // For this, we use a code point which is valid in a Python string
        // (in UCS-2), but which is not valid when encoded as UTF-8.
        // >>> chr(0xd800).encode('utf-8')
        // Traceback (most recent call last):
        // File "<stdin>", line 1, in <module>
        // UnicodeEncodeError: 'utf-8' codec can't encode character '\ud800'
        //                                     in position 0: surrogates not allowed
        let before_pos = b.tell();
        let s5 = b.ucs2_to_utf8(&[0x061, 0xd800]);
        assert!(s5.is_err());
        assert_eq!(s5.unwrap_err(), 0xd800_u32);

        // Even though 0x061 (ASCII char 'a') was valid and successfully encoded,
        // we also want to be sure that the buffer was not modified and appended to.
        assert_eq!(b.tell(), before_pos);

        // Now we check that the buffer is still in a valid state.
        let s6 = b.ucs2_to_utf8(&[0x062, 0x063]).unwrap();
        assert_eq!(s6, "bc");
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 1,
                string: [s1, s2, s3, s4, s6].iter().map(|s| s.len()).sum()
            }
        );
    }

    #[test]
    fn test_ucs4() {
        let mut b = Buf::new();

        // We first check code points within the ASCII range.
        let s1 = b.ucs4_to_utf8(&[0x61, 0x62, 0x63, 0x64, 0x65]).unwrap();
        assert_eq!(s1, "abcde");
        assert_eq!(s1.len(), 5);

        // Now chars outside ASCII range, but within UCS-1 range.
        // These will yield two bytes each in UTF-8.
        let s2 = b.ucs4_to_utf8(&[0x00f0, 0x00e3, 0x00b5, 0x00b6]).unwrap();
        assert_eq!(s2, "ðãµ¶");
        assert_eq!(s2.len(), 8);

        // Now chars that actually require two bytes in UCS-2, but also fit in
        // two bytes in UTF-8.
        let s3 = b.ucs4_to_utf8(&[0x0100, 0x069c]).unwrap();
        assert_eq!(s3, "Āڜ");
        assert_eq!(s3.len(), 4);

        // Now chars that require two bytes in UCS-2 and 3 bytes in UTF-8.
        let s4 = b.ucs4_to_utf8(&[0x569c, 0xa4c2]).unwrap();
        assert_eq!(s4, "嚜꓂");
        assert_eq!(s4.len(), 6);

        // Now chars that require four bytes in UCS-4 and 4 bytes in UTF-8.
        let s5 = b.ucs4_to_utf8(&[0x1f4a9, 0x1f99e]).unwrap();
        assert_eq!(s5, "💩🦞");
        assert_eq!(s5.len(), 8);

        // Quick check that we're just writing to the same buffer.
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 1,
                string: [s1, s2, s3, s4, s5].iter().map(|s| s.len()).sum()
            }
        );

        // Now we finally check that errors are captured.
        // For this, we use a code point which is valid in a Python string
        // (in UCS-4), but which is not valid when encoded as UTF-8.
        // >>> chr(0xd800).encode('utf-8')
        // Traceback (most recent call last):
        // File "<stdin>", line 1, in <module>
        // UnicodeEncodeError: 'utf-8' codec can't encode character '\ud800'
        //                                     in position 0: surrogates not allowed
        let before_pos = b.tell();
        let s6 = b.ucs4_to_utf8(&[0x061, 0xd800]);
        assert!(s6.is_err());
        assert_eq!(s6.unwrap_err(), 0xd800_u32);

        // Even though 0x061 (ASCII char 'a') was valid and successfully encoded,
        // we also want to be sure that the buffer was not modified and appended to.
        assert_eq!(b.tell(), before_pos);

        // We repeat the same with chars with code points higher than the u16 type.
        let before_pos = b.tell();
        let s7 = b.ucs4_to_utf8(&[0x061, 0x110000]);
        assert!(s7.is_err());
        assert_eq!(s7.unwrap_err(), 0x110000);

        // Even though 0x061 (ASCII char 'a') was valid and successfully encoded,
        // we also want to be sure that the buffer was not modified and appended to.
        assert_eq!(b.tell(), before_pos);

        // Now we check that the buffer is still in a valid state.
        let s8 = b.ucs4_to_utf8(&[0x062, 0x063]).unwrap();
        assert_eq!(s8, "bc");
        assert_eq!(
            b.tell(),
            qdb_pystr_pos {
                chain: 1,
                string: [s1, s2, s3, s4, s5, s8].iter().map(|s| s.len()).sum()
            }
        );
    }
}
