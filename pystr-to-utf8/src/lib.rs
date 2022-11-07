/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
use std::fmt::Write;
use std::slice::from_raw_parts;

#[allow(non_camel_case_types)]
pub struct qdb_pystr_buf(String);

/// Prepare a new buffer. The buffer must be freed with `qdb_pystr_free`.
/// The `qdb_ucsX_to_utf8` functions will write to this buffer.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_new() -> *mut qdb_pystr_buf {
    Box::into_raw(Box::new(qdb_pystr_buf(String::with_capacity(64))))
}

/// Get current position. Use in conjunction with `truncate`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_tell(b: *mut qdb_pystr_buf) -> usize {
    let b = &mut *b;
    b.0.len()
}

/// Trim the buffer to the given length. Use in conjunction with `tell`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_truncate(
        b: *mut qdb_pystr_buf, len: usize) {
    let b = &mut *b;
    b.0.truncate(len)
}

/// Reset the converter's buffer to zero length.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_clear(b: *mut qdb_pystr_buf) {
    let b = &mut *b;
    b.0.clear()
}

/// Free the buffer. Must be called after `qdb_pystr_buf_new`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_free(b: *mut qdb_pystr_buf) {
    if !b.is_null() {
        drop(Box::from_raw(b));
    }
}

#[inline]
fn encode_ucs1(dest: &mut String, buf: &[u8]) {
    // len(chr(2 ** 8 - 1).encode('utf-8')) == 2
    let utf8_mult = 2;
    dest.reserve(utf8_mult * buf.len());
    for &b in buf.iter() {
        dest.push(b as char);
    }
}

/// Convert a Py_UCS1 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed either from `input` or from `b`.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs1_to_utf8(
        b: *mut qdb_pystr_buf,
        count: usize, input: *const u8,
        size_out: *mut usize, buf_out: *mut *const c_char) {
    let b = &mut *b;
    let i = from_raw_parts(input, count);
    let last = b.0.len();
    encode_ucs1(&mut b.0, i);
    *size_out = b.0.len() - last;
    *buf_out = b.0.as_ptr() as *const c_char;
}

#[inline]
fn encode_ucs2(dest: &mut String, buf: &[u16]) -> bool {
    // len(chr(2 ** 16 - 1).encode('utf-8')) == 3
    let utf8_mult = 3;
    dest.reserve(utf8_mult * buf.len());
    for b in buf.iter() {
        // Checking for validity is not optional:
        // >>> for n in range(2 ** 16):
        // >>>     chr(n).encode('utf-8')
        // UnicodeEncodeError: 'utf-8' codec can't encode character '\ud800'
        //   in position 0: surrogates not allowed
        match char::from_u32(*b as u32) {
            Some(c) => dest.push(c),
            None => {
                dest.clear();
                write!(dest, "invalid UCS-2 code point: {}", b).unwrap();
                return false
            }
        }
    }
    true
}

/// Convert a Py_UCS2 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed from `b`.
/// In case of errors, returns `false` and the buffer is an error message.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs2_to_utf8(b: *mut qdb_pystr_buf,
        count: usize, input: *const u16,
        size_out: *mut usize, buf_out: *mut *const c_char) -> bool {
    let b = &mut *b;
    let i = from_raw_parts(input, count);
    let last = b.0.len();
    let ok = encode_ucs2(&mut b.0, i);
    *size_out = b.0.len() - last;
    *buf_out = b.0.as_ptr() as *const c_char;
    ok
}

#[inline]
fn encode_ucs4(dest: &mut String, buf: &[u32]) -> bool {
    // Max 4 bytes allowed by RFC: https://www.rfc-editor.org/rfc/rfc3629#page-4
    let utf8_mult = 4;
    dest.reserve(utf8_mult * buf.len());
    for b in buf.iter() {
        match char::from_u32(*b) {
            Some(c) => dest.push(c),
            None => {
                dest.clear();
                write!(dest, "invalid UCS-4 code point: {}", b).unwrap();
                return false
            }
        }
    }
    true
}

/// Convert a Py_UCS4 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed from `b`.
/// In case of errors, returns `false` and the buffer is an error message.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs4_to_utf8(b: *mut qdb_pystr_buf,
        count: usize, input: *const u32,
        size_out: *mut usize, buf_out: *mut *const c_char) -> bool {
    let b = &mut *b;
    let i = from_raw_parts(input, count);
    let last = b.0.len();
    let ok = encode_ucs4(&mut b.0, i);
    *size_out = b.0.len() - last;
    *buf_out = b.0.as_ptr() as *const c_char;
    ok
}
