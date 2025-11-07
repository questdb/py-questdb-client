from libc.stdint cimport uint8_t
from libc.stddef cimport size_t
from cpython.object cimport PyObject

# Mirror the subset of libmpdec types that CPython embeds in Decimal objects.
ctypedef size_t mpd_uint_t
ctypedef Py_ssize_t mpd_ssize_t

cdef extern from "mpdecimal_compat.h":
    ctypedef struct mpd_t:
        uint8_t flags
        mpd_ssize_t exp
        mpd_ssize_t digits
        mpd_ssize_t len
        mpd_ssize_t alloc
        mpd_uint_t* data

    mpd_t* decimal_mpd(PyObject* obj)
    mpd_uint_t* decimal_digits(PyObject* obj)
    const mpd_uint_t MPD_RADIX
    const uint8_t MPD_FLAG_SIGN
    const uint8_t MPD_FLAG_SPECIAL_MASK

cdef inline object decimal_pyobj_to_binary(
        PyObject* cell,
        unsigned int* encoded_scale,
        object ingress_error_cls,
        object bad_dataframe_code) except *:
    """Convert a Python ``Decimal`` to ILP binary components."""
    cdef mpd_t* mpd
    cdef mpd_uint_t* digits_ptr
    cdef unsigned long long flag_low
    cdef Py_ssize_t idx
    cdef Py_ssize_t scale_value
    cdef object unscaled_obj

    mpd = decimal_mpd(cell)

    flag_low = mpd.flags & 0xFF
    if (flag_low & MPD_FLAG_SPECIAL_MASK) != 0:
        # NaN/Inf values propagate as ILP nulls (caller will emit empty payload).
        encoded_scale[0] = 0
        return None

    digits_ptr = decimal_digits(cell)

    if mpd.len <= 0:
        unscaled_obj = 0
    else:
        unscaled_obj = digits_ptr[mpd.len - 1]
        for idx in range(mpd.len - 2, -1, -1):
            # Each limb stores MPD_RADIX (10^9 or 10^19) digits in little-endian order.
            unscaled_obj = unscaled_obj * MPD_RADIX + digits_ptr[idx]

    if mpd.exp >= 0:
        # Decimal ILP does not support negative scales; adjust the unscaled value instead.
        if mpd.exp != 0:
            unscaled_obj = unscaled_obj * (10 ** mpd.exp)
        scale_value = 0
    else:
        scale_value = -mpd.exp
        if scale_value > 76:
            raise ingress_error_cls(
                bad_dataframe_code,
                f'Decimal scale {scale_value} exceeds the maximum supported scale of 76')

    if (flag_low & MPD_FLAG_SIGN) != 0:
        unscaled_obj = -unscaled_obj

    encoded_scale[0] = <unsigned int>scale_value
    return unscaled_obj.to_bytes((unscaled_obj.bit_length() + 8) // 8, byteorder='big', signed=True)
