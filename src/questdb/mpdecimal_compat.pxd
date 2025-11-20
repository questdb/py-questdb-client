from libc.stdint cimport uint8_t, uint32_t
from libc.stddef cimport size_t
from cpython.object cimport PyObject
from .rpyutils cimport *

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

# Converts a decimal.Decimal python object to it's two's complement representation
# Returns 0 if no value has to be written, otherwise it returns the number of bytes to be sent
# from the unscaled array (which needs to be at least 32 bytes large).
cdef inline int decimal_pyobj_to_binary(
        PyObject* cell,
        unsigned char* unscaled,
        unsigned int* scale,
        object ingress_error_cls,
        object bad_dataframe_code) noexcept:
    """Convert a Python ``Decimal`` to ILP binary components."""
    cdef mpd_t* mpd
    cdef mpd_uint_t* digits_ptr
    cdef unsigned long long flag_low
    cdef uint32_t exp
    cdef Py_ssize_t out_size

    mpd = decimal_mpd(cell)

    flag_low = mpd.flags & 0xFF
    if (flag_low & MPD_FLAG_SPECIAL_MASK) != 0:
        # NaN/Nulls don't have to be propagated, they end up
        return 0

    digits_ptr = decimal_digits(cell)

    if mpd.exp >= 0:
        # Decimal ILP does not support negative scales; adjust the unscaled value instead.
        exp = mpd.exp
        scale[0] = 0
    else:
        exp = 0
        if -mpd.exp > 76:
            raise ingress_error_cls(
                bad_dataframe_code,
                f'Decimal scale {-mpd.exp} exceeds the maximum supported scale of 76')
        scale[0] = -mpd.exp

    if not qdb_mpd_to_bigendian(digits_ptr, mpd.len, MPD_RADIX, exp, (flag_low & MPD_FLAG_SIGN) != 0, unscaled, <size_t *>&out_size):
        raise ingress_error_cls(
            bad_dataframe_code,
            'Decimal mantissa too large; maximum supported size is 32 bytes.')

    return out_size
