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
pub struct qdb_pystr_buf(Vec<String>);

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
pub struct qdb_pystr_pos {
    pub chain: usize,
    pub string: usize
}

/// Prepare a new buffer. The buffer must be freed with `qdb_pystr_free`.
/// The `qdb_ucsX_to_utf8` functions will write to this buffer.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_new() -> *mut qdb_pystr_buf {
    Box::into_raw(Box::new(qdb_pystr_buf(Vec::new())))
}

/// Get current position. Use in conjunction with `truncate`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_tell(
        b: *mut qdb_pystr_buf) -> qdb_pystr_pos {
    let b = &mut *b;
    let chain_pos = b.0.len();
    let string_pos = if chain_pos > 0 {
            b.0[chain_pos - 1].len()
        } else {
            0
        };
    qdb_pystr_pos { chain: chain_pos, string: string_pos }
}

/// Trim the buffer to the given length. Use in conjunction with `tell`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_truncate(
        b: *mut qdb_pystr_buf, pos: qdb_pystr_pos) {
    let b = &mut *b;
    b.0.truncate(pos.chain + 1);
    if !b.0.is_empty() {
        b.0[pos.chain].truncate(pos.string);
    }
}

/// Reset the converter's buffer to zero length.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_clear(b: *mut qdb_pystr_buf) {
    let b = &mut *b;
    if !b.0.is_empty() {
        b.0.clear();
    }
}

/// Free the buffer. Must be called after `qdb_pystr_buf_new`.
#[no_mangle]
pub unsafe extern "C" fn qdb_pystr_buf_free(b: *mut qdb_pystr_buf) {
    if !b.is_null() {
        drop(Box::from_raw(b));
    }
}

const MIN_BUF_LEN: usize = 64;

/// A carefully crafted buffer with spare capacity for `len` bytes.
/// This is necessary to return "stable" addresses and avoid segfaults.
/// Rust is unaware we are borrowing its memory and could try to free it as
/// part of a reallocation if we were to use a `String` directly.
fn get_dest<'a>(chain: &'a mut Vec<String>, len: usize) -> &'a mut String {
    if !chain.is_empty() {
        let last = chain.last_mut().unwrap();
        if last.capacity() - last.len() >= len {
            return chain.last_mut().unwrap();
        }
    }
    chain.push(String::with_capacity(std::cmp::max(len, MIN_BUF_LEN)));
    chain.last_mut().unwrap()
}

#[inline]
fn encode_ucs1<'a, 'b>(chain: &'a mut Vec<String>, buf: &'b [u8]) -> &'a str {
    // len(chr(2 ** 8 - 1).encode('utf-8')) == 2
    let utf8_mult = 2;
    let dest = get_dest(chain, utf8_mult * buf.len());
    let last = dest.len();
    for &b in buf.iter() {
        dest.push(b as char);
    }
    &dest[last..]
}

/// Convert a Py_UCS1 string to UTF-8.
/// Returns a `buf_out` borrowed ptr of `size_out` len.
/// The buffer is borrowed from `b`.
#[no_mangle]
pub unsafe extern "C" fn qdb_ucs1_to_utf8(
        b: *mut qdb_pystr_buf,
        count: usize, input: *const u8,
        size_out: *mut usize, buf_out: *mut *const c_char) {
    let b = &mut *b;
    let i = from_raw_parts(input, count);
    let res = encode_ucs1(&mut b.0, i);
    *size_out = res.len();
    *buf_out = res.as_ptr() as *const c_char;
}

#[inline]
fn encode_ucs2<'a, 'b>(
        chain: &'a mut Vec<String>, buf: &'b [u16]) -> (bool, &'a str) {
    // len(chr(2 ** 16 - 1).encode('utf-8')) == 3
    let utf8_mult = 3;
    let dest = get_dest(chain, utf8_mult * buf.len());
    let last = dest.len();
    let mut ok = true;
    for b in buf.iter() {
        // Checking for validity is not optional:
        // >>> for n in range(2 ** 16):
        // >>>     chr(n).encode('utf-8')
        // UnicodeEncodeError: 'utf-8' codec can't encode character '\ud800'
        //   in position 0: surrogates not allowed
        match char::from_u32(*b as u32) {
            Some(c) => dest.push(c),
            None => {
                dest.truncate(last);
                write!(dest, "invalid UCS-2 code point: {}", b).unwrap();
                ok = false;
                break;
            }
        }
    }
    (ok, &dest[last..])
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
    let (ok, res) = encode_ucs2(&mut b.0, i);
    *size_out = res.len();
    *buf_out = res.as_ptr() as *const c_char;
    ok
}

#[inline]
fn encode_ucs4<'a, 'b>(
        chain: &'a mut Vec<String>, buf: &'b[u32]) -> (bool, &'a str) {
    // Max 4 bytes allowed by RFC: https://www.rfc-editor.org/rfc/rfc3629#page-4
    let utf8_mult = 4;
    let dest = get_dest(chain, utf8_mult * buf.len());
    let last = dest.len();
    let mut ok = true;
    for b in buf.iter() {
        match char::from_u32(*b) {
            Some(c) => dest.push(c),
            None => {
                dest.truncate(last);
                write!(dest, "invalid UCS-4 code point: {}", b).unwrap();
                ok = false;
                break;
            }
        }
    }
    (ok, &dest[last..])
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
    let (ok, res) = encode_ucs4(&mut b.0, i);
    *size_out = res.len();
    *buf_out = res.as_ptr() as *const c_char;
    ok
}
