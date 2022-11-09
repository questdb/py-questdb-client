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
                &mut buf_out);
        }
        let slice = unsafe {
            from_raw_parts(buf_out as *const u8, size_out) };
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
                    &mut bad_codepoint)
            };
        if ok {
            let slice = unsafe {
                from_raw_parts(buf_out as *const u8, size_out) };
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
                    &mut bad_codepoint)
            };
        if ok {
            let slice = unsafe {
                from_raw_parts(buf_out as *const u8, size_out) };
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
    assert_eq!(b.tell(), qdb_pystr_pos { chain: 1, string: 0 });
    assert_eq!(s2.as_ptr(), b.chain()[0].as_str().as_ptr());
    let s3 = b.ucs1_to_utf8(b"10\xb5");
    assert_eq!(s3, "10µ");
    assert_eq!(s3.len(), 4);  // 3 bytes in UCS-1, 4 bytes in UTF-8.
    assert_eq!(b.chain().len(), 1);
    assert_eq!(s3.as_ptr(), unsafe {
        b.chain()[0].as_str().as_ptr().add(s2.len())
    });
    assert_eq!(b.tell(), qdb_pystr_pos {
        chain: 1, string: s2.len() + s3.len() });
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
    assert_eq!(b.tell(), qdb_pystr_pos { chain: 2, string: 11000 });
    b.truncate(b.tell());
    assert_eq!(b.tell(), qdb_pystr_pos { chain: 2, string: 11000 });

    let spare = b.chain_mut()[1].capacity() - b.chain_mut()[1].len();
    assert!(spare > 4);

    let test_string = "ab";
    let s3 = b.ucs1_to_utf8(test_string.as_bytes());
    assert_eq!(s3, test_string);
    assert_eq!(b.chain_mut().len(), 2);
    assert_eq!(b.chain_mut()[0].as_str().as_ptr(), s1.as_ptr());
    assert_eq!(b.chain_mut()[1].as_str().as_ptr(), s2.as_ptr());
    assert_eq!(b.tell(), qdb_pystr_pos {
        chain: 2, string: 11000 + test_string.len() });
}

#[test]
fn test_ucs2() {
    let mut b = Buf::new();

    // We first check code points within the ASCII range.
    let s1 = b.ucs2_to_utf8(
        &[0x61, 0x62, 0x63, 0x64, 0x65]).unwrap();
    assert_eq!(s1, "abcde");
    assert_eq!(s1.len(), 5);
    
    // Now chars outside ASCII range, but within UCS-1 range.
    // These will yield two bytes each in UTF-8.
    let s2 = b.ucs2_to_utf8(
        &[0x00f0, 0x00e3, 0x00b5, 0x00b6])
        .unwrap();
    assert_eq!(s2, "ðãµ¶");
    assert_eq!(s2.len(), 8);

    // Now chars that actually require two bytes in UCS-2, but also fit in
    // two bytes in UTF-8.
    let s3 = b.ucs2_to_utf8(
        &[0x0100, 0x069c])
        .unwrap();
    assert_eq!(s3, "Āڜ");
    assert_eq!(s3.len(), 4);

    // Now chars that require two bytes in UCS-2 and 3 bytes in UTF-8.
    let s4 = b.ucs2_to_utf8(
        &[0x569c, 0xa4c2])
        .unwrap();
    assert_eq!(s4, "嚜꓂");
    assert_eq!(s4.len(), 6);

    // Quick check that we're just writing to the same buffer.
    assert_eq!(b.tell(), qdb_pystr_pos {
        chain: 1,
        string: [s1, s2, s3, s4].iter().map(|s| s.len()).sum() });

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
    assert_eq!(s5.unwrap_err(), 0xd800 as u32);

    // Even though 0x061 (ASCII char 'a') was valid and successfully encoded,
    // we also want to be sure that the buffer was not modified and appended to.
    assert_eq!(b.tell(), before_pos);

    // Now we check that the buffer is still in a valid state.
    let s6 = b.ucs2_to_utf8(&[0x062, 0x063]).unwrap();
    assert_eq!(s6, "bc");
    assert_eq!(b.tell(), qdb_pystr_pos {
        chain: 1,
        string: [s1, s2, s3, s4, s6].iter().map(|s| s.len()).sum() });
}

#[test]
fn test_ucs4() {
    let mut b = Buf::new();

    // We first check code points within the ASCII range.
    let s1 = b.ucs4_to_utf8(
        &[0x61, 0x62, 0x63, 0x64, 0x65]).unwrap();
    assert_eq!(s1, "abcde");
    assert_eq!(s1.len(), 5);
    
    // Now chars outside ASCII range, but within UCS-1 range.
    // These will yield two bytes each in UTF-8.
    let s2 = b.ucs4_to_utf8(
        &[0x00f0, 0x00e3, 0x00b5, 0x00b6])
        .unwrap();
    assert_eq!(s2, "ðãµ¶");
    assert_eq!(s2.len(), 8);

    // Now chars that actually require two bytes in UCS-2, but also fit in
    // two bytes in UTF-8.
    let s3 = b.ucs4_to_utf8(
        &[0x0100, 0x069c])
        .unwrap();
    assert_eq!(s3, "Āڜ");
    assert_eq!(s3.len(), 4);

    // Now chars that require two bytes in UCS-2 and 3 bytes in UTF-8.
    let s4 = b.ucs4_to_utf8(
        &[0x569c, 0xa4c2])
        .unwrap();
    assert_eq!(s4, "嚜꓂");
    assert_eq!(s4.len(), 6);

    // Now chars that require four bytes in UCS-4 and 4 bytes in UTF-8.
    let s5 = b.ucs4_to_utf8(
        &[0x1f4a9, 0x1f99e])
        .unwrap();
    assert_eq!(s5, "💩🦞");
    assert_eq!(s5.len(), 8);

    // Quick check that we're just writing to the same buffer.
    assert_eq!(b.tell(), qdb_pystr_pos {
        chain: 1,
        string: [s1, s2, s3, s4, s5].iter().map(|s| s.len()).sum() });

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
    assert_eq!(s6.unwrap_err(), 0xd800 as u32);

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
    assert_eq!(b.tell(), qdb_pystr_pos {
        chain: 1,
        string: [s1, s2, s3, s4, s5, s8].iter().map(|s| s.len()).sum() });
}