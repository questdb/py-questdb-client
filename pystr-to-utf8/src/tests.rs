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

    fn ucs2_to_utf8(&mut self, input: &[u16]) -> Result<&'static str, &'static str> {
        let mut size_out = 0;
        let mut buf_out = std::ptr::null();
        let ok = unsafe {
                qdb_ucs2_to_utf8(
                    self.buf,
                    input.len(),
                    input.as_ptr(),
                    &mut size_out,
                    &mut buf_out)
            };
        let slice = unsafe {
            from_raw_parts(buf_out as *const u8, size_out) };
        let msg = std::str::from_utf8(slice).unwrap();
        if ok {
            Ok(msg)
        } else {
            Err(msg)
        }
    }

    fn ucs4_to_utf8(&mut self, input: &[u32]) -> Result<&'static str, &'static str> {
        let mut size_out = 0;
        let mut buf_out = std::ptr::null();
        let ok = unsafe {
                qdb_ucs4_to_utf8(
                    self.buf,
                    input.len(),
                    input.as_ptr(),
                    &mut size_out,
                    &mut buf_out)
            };
        let slice = unsafe {
            from_raw_parts(buf_out as *const u8, size_out) };
        let msg = std::str::from_utf8(slice).unwrap();
        if ok {
            Ok(msg)
        } else {
            Err(msg)
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
