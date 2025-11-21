use std::slice;

use i256;

/// Converts an mpdecimal limb array to its big-endian two's complement bytes.
///
/// The limbs originate from Python's `decimal.Decimal` (mpdecimal) and are laid
/// out in little-endian order using the mpdecimal `mpd_uint_t` radix. The limbs
/// are reduced to a single 256-bit signed value, optionally negated, multiplied
/// by `10^exp`, and finally encoded to big-endian bytes. Leading sign-extension
/// bytes are preserved in `out` so the caller can trim the first `out_size`
/// bytes when reading.
///
/// On success the first `out_size` bytes of `out` contain the trimmed
/// big-endian representation (no redundant sign-extension), and the remainder of
/// the buffer is left untouched.
///
/// # Safety
///
/// * `limbs` must either be null (function returns `false`) or point to
///   `limbs_len` valid `mpd_uint_t` limbs.
/// * `out` must point to exactly 32 bytes of writable memory.
/// * `out_size` must point to writable memory for a single `usize`.
/// * `radix` must match the mpdecimal radix used to build the limbs.
#[no_mangle]
pub unsafe extern "C" fn qdb_mpd_to_bigendian(
    limbs: *const usize,
    limbs_len: usize,
    radix: usize,
    exp: u32,
    negative: bool,
    out: *mut u8,
    out_size: *mut usize,
) -> bool {
    let limbs = if limbs.is_null() {
        return false;
    } else {
        unsafe { slice::from_raw_parts(limbs, limbs_len) }
    };
    let out = unsafe { slice::from_raw_parts_mut(out, 32) };
    match mpd_to_bigendian(limbs, radix, exp, negative, out) {
        Some(size) => {
            *out_size = size;
            true
        }
        None => false,
    }
}

fn reduce_limbs(limbs: &[usize], radix: usize) -> Option<i256::i256> {
    let mut value = i256::i256::from(0);
    for limb in limbs.iter().rev() {
        // For negative values mpdecimal gives us a magnitude that may occupy the
        // full 256-bit range, so we accumulate using subtraction to stay within
        // the extended negative range (two's complement has one more negative
        // number than positive).
        value = value
            .checked_mul_u64(radix as u64)?
            .checked_sub_u64(*limb as u64)?;
    }
    Some(value)
}

fn write_trimmed_bytes(value: i256::i256, negative_hint: bool, out: &mut [u8]) -> usize {
    let be = value.to_be_bytes();
    let (pad, sign_bit) = if negative_hint {
        (0xFF, 0x80)
    } else {
        (0x00, 0x00)
    };
    let mut offset = 0;
    // Drop redundant padding bytes as long as the next byte still carries the
    // correct sign bit; this keeps the canonical two's complement encoding while
    // keeping the significant bytes contiguous at the end of `out`.
    while offset < be.len() - 2 && be[offset] == pad && be[offset + 1] & 0x80 == sign_bit {
        offset += 1;
    }
    let len = be.len() - offset;
    out[..len].copy_from_slice(&be[offset..]);
    len
}

/// Converts the provided limbs to a 256-bit big-endian two's complement array.
///
/// Returning `Some(len)` means the conversion fit in 256 bits, the first `len`
/// bytes of `out` (which must be 32 bytes long) now contain the big-endian
/// representation without redundant sign bytes (after multiplying the reduced
/// value by `10^exp` and applying sign), and the rest of the buffer is
/// unmodified. A `None` result indicates either an arithmetic overflow or an
/// invalid `out` size.
fn mpd_to_bigendian(
    limbs: &[usize],
    radix: usize,
    exp: u32,
    negative: bool,
    out: &mut [u8],
) -> Option<usize> {
    debug_assert!(out.len() == 32);

    let mut value = reduce_limbs(limbs, radix)?;
    if !negative {
        value = value.checked_neg()?
    }
    if exp > 0 {
        let pow10 = i256::i256::from(10).checked_pow(exp)?;
        value = value.checked_mul(pow10)?;
    }
    Some(write_trimmed_bytes(value, negative, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_value(bytes: &[u8], negative: bool) -> i256::i256 {
        let fill = if negative { 0xFF } else { 0x00 };
        let mut full = [fill; 32];
        let start = 32 - bytes.len();
        full[start..].copy_from_slice(bytes);
        i256::i256::from_be_bytes(full)
    }

    #[test]
    fn reduces_positive_limbs() {
        let limbs = [12345usize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10, 0, false, &mut out).unwrap();

        assert_eq!(written, 2);
        assert_eq!(&out[..written], &[0x30, 0x39]);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn reduces_negative_limbs() {
        let limbs = [42usize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10, 0, true, &mut out).unwrap();

        assert_eq!(written, 2);
        assert_eq!(&out[..written], &[0xFF, 0xD6]);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn large_number() {
        let limbs = [1usize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10, 12, false, &mut out).unwrap();

        assert_eq!(written, 6);
        assert_eq!(&out[..written], &[0x00, 0xE8, 0xD4, 0xA5, 0x10, 0x00]);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn overflow_by_one() {
        // decimal representation of 2²⁵⁵
        let limbs = [
            8792003956564819968usize,
            3499233282028201972usize,
            7854925043439539266usize,
            7896044618658097711usize,
            5usize,
        ];
        let mut out = [0xAAu8; 32];

        let r = mpd_to_bigendian(&limbs, 10000000000000000000, 0, false, &mut out);
        assert!(r.is_none());
    }

    #[test]
    fn underflow_by_one() {
        // decimal representation of 2²⁵⁵-2
        let limbs = [
            8792003956564819969usize,
            3499233282028201972usize,
            7854925043439539266usize,
            7896044618658097711usize,
            5usize,
        ];
        let mut out = [0xAAu8; 32];

        let r = mpd_to_bigendian(&limbs, 10000000000000000000, 0, true, &mut out);
        assert!(r.is_none());
    }

    #[test]
    fn maximum_value() {
        // decimal representation of 2²⁵⁵-1
        let limbs = [
            8792003956564819967usize,
            3499233282028201972usize,
            7854925043439539266usize,
            7896044618658097711usize,
            5usize,
        ];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10000000000000000000, 0, false, &mut out).unwrap();
        assert_eq!(written, 32);
        assert_eq!(out[0], 0x7F);
        assert!(out[1..].iter().all(|b| *b == 0xFF));
    }

    #[test]
    fn minimum_value() {
        // decimal representation of -2²⁵⁵
        let limbs = [
            8792003956564819968usize,
            3499233282028201972usize,
            7854925043439539266usize,
            7896044618658097711usize,
            5usize,
        ];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10000000000000000000, 0, true, &mut out).unwrap();
        assert_eq!(written, 32);
        assert_eq!(out[0], 0x80);
        assert!(out[1..].iter().all(|b| *b == 0x00));
    }

    #[test]
    fn reduces_multiple_limbs_positive() {
        let radix = 1usize << 16;
        let limbs = [0x0123usize, 0x4567usize, 0x89ABusize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, radix, 0, false, &mut out).expect("conversion");

        assert_eq!(written, 7);
        let expected: [u8; 7] = [0x00, 0x89, 0xAB, 0x45, 0x67, 0x01, 0x23];
        assert_eq!(&out[..written], &expected);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn reduces_multiple_limbs_negative() {
        let radix = 1usize << 16;
        let limbs = [0x0123usize, 0x4567usize, 0x89ABusize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, radix, 0, true, &mut out).expect("conversion");

        assert_eq!(written, 7);
        let expected: [u8; 7] = [0xFF, 0x76, 0x54, 0xBA, 0x98, 0xFE, 0xDD];
        assert_eq!(&out[..written], &expected);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn reduces_many_limbs_radix_1e9() {
        let radix = 1_000_000_000usize;
        let limbs = [123_456_789usize, 987_654_321usize, 202_122_212usize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, radix, 3, false, &mut out).unwrap();

        let decoded = decode_value(&out[..written], false);
        let mut expected = reduce_limbs(&limbs, radix).unwrap().wrapping_neg();
        let pow10 = i256::i256::from(10).checked_pow(3).unwrap();
        expected = expected.checked_mul(pow10).unwrap();
        assert_eq!(decoded, expected);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn reduces_many_limbs_radix_1e19_negative() {
        let radix = 10_000_000_000_000_000_000usize;
        let limbs = [
            123_456_789_012_345_678usize,
            987_654_321_098_765_432usize,
            111_111_111_111_111_111usize,
        ];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, radix, 1, true, &mut out).unwrap();

        let decoded = decode_value(&out[..written], true);
        let mut expected = reduce_limbs(&limbs, radix).unwrap().wrapping_neg();
        expected = unsafe { expected.unchecked_neg() };
        let pow10 = i256::i256::from(10).checked_pow(1).unwrap();
        expected = expected.checked_mul(pow10).unwrap();
        assert_eq!(decoded, expected);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn scales_by_power_of_ten() {
        let limbs = [3usize];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10, 2, false, &mut out).unwrap();

        assert_eq!(written, 2);
        assert_eq!(&out[..written], &[0x01, 0x2C]); // 3 * 10^2 = 300
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn returns_none_on_exponent_overflow() {
        let limbs = [2usize];
        let mut out = [0u8; 32];

        // 10^256 does not fit in 256 bits.
        assert!(mpd_to_bigendian(&limbs, 10, 256, false, &mut out).is_none());
    }

    #[test]
    fn returns_none_on_exponent_overflow2() {
        let limbs = [1_000_000_000usize];
        let mut out = [0u8; 32];

        // 1_000_000_000 * 10^74 does not fit in 256 bits.
        assert!(mpd_to_bigendian(&limbs, 10, 74, false, &mut out).is_none());
    }

    #[test]
    fn returns_none_on_value_overflow() {
        let radix = 256usize;
        let mut limbs = [0usize; 33];
        limbs[32] = 1; // Represents radix^32 == 2^256 which cannot be encoded.
        let mut out = [0u8; 32];

        assert!(mpd_to_bigendian(&limbs, radix, 0, false, &mut out).is_none());
    }

    #[test]
    fn handles_zero_limbs() {
        let limbs: [usize; 0] = [];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10, 0, false, &mut out).unwrap();

        assert_eq!(written, 2);
        assert_eq!(&out[..written], &[0x00, 0x00]);
        assert!(out[written..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn handles_negative_zero_limbs() {
        let limbs: [usize; 0] = [];
        let mut out = [0xAAu8; 32];

        let written = mpd_to_bigendian(&limbs, 10, 0, true, &mut out).unwrap();

        assert_eq!(written, 32);
        assert!(out.iter().all(|b| *b == 0x00));
    }

    #[test]
    fn ffi_wrapper_marshals_arguments() {
        let limbs = vec![7usize, 0usize]; // little-endian limbs
        let mut out = [0xAAu8; 32];
        let mut out_size = 0usize;

        let ok = unsafe {
            qdb_mpd_to_bigendian(
                limbs.as_ptr(),
                limbs.len(),
                10,
                0,
                false,
                out.as_mut_ptr(),
                &mut out_size,
            )
        };

        assert!(ok);
        assert_eq!(out_size, 2);
        assert_eq!(&out[..out_size], &[0x00, 0x07]);
        assert!(out[out_size..].iter().all(|b| *b == 0xAA));
    }

    #[test]
    fn ffi_wrapper_return_false_on_errors() {
        let limbs = [1usize];
        let mut out = [0xAAu8; 32];
        let mut out_size = 0usize;

        let ok = unsafe {
            qdb_mpd_to_bigendian(
                limbs.as_ptr(),
                limbs.len(),
                10,
                256,
                false,
                out.as_mut_ptr(),
                &mut out_size,
            )
        };

        assert!(!ok);
    }

    #[test]
    fn ffi_wrapper_rejects_null_limbs() {
        let mut out = [0u8; 32];
        let mut out_size = 0usize;
        let ok = unsafe {
            qdb_mpd_to_bigendian(
                std::ptr::null_mut(),
                0,
                10,
                0,
                false,
                out.as_mut_ptr(),
                &mut out_size,
            )
        };

        assert!(!ok);
    }
}
