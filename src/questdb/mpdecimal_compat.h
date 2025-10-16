#ifndef MPDECIMAL_COMPAT_H
#define MPDECIMAL_COMPAT_H

#include <Python.h>
#include <stdint.h>
#include <limits.h>

/* Determine the limb type used by CPython's libmpdec build. */
#if SIZE_MAX == UINT64_MAX
typedef uint64_t mpd_uint_t;
typedef int64_t mpd_ssize_t;
#define MPD_RADIX_CONST UINT64_C(10000000000000000000) /* 10**19 */
#elif SIZE_MAX == UINT32_MAX
typedef uint32_t mpd_uint_t;
typedef int32_t mpd_ssize_t;
#define MPD_RADIX_CONST UINT32_C(1000000000) /* 10**9 */
#else
#error "Unsupported platform: mpdecimal compatibility requires 32-bit or 64-bit size_t."
#endif

typedef struct {
    uint8_t flags;
    mpd_ssize_t exp;
    mpd_ssize_t digits;
    mpd_ssize_t len;
    mpd_ssize_t alloc;
    mpd_uint_t* data;
} mpd_t;

typedef struct {
    PyObject_HEAD
    Py_hash_t hash;
    mpd_t dec;
    mpd_uint_t data[4];
} PyDecObject;

static inline mpd_t* decimal_mpd(PyObject* obj) {
    return &((PyDecObject*)obj)->dec;
}

static inline mpd_uint_t* decimal_digits(PyObject* obj) {
    PyDecObject* dec = (PyDecObject*)obj;
    return dec->dec.data != NULL ? dec->dec.data : dec->data;
}

enum {
    MPD_FLAG_SIGN = 0x01,
    MPD_FLAG_INF = 0x02,
    MPD_FLAG_NAN = 0x04,
    MPD_FLAG_SNAN = 0x08,
    MPD_FLAG_SPECIAL_MASK = MPD_FLAG_INF | MPD_FLAG_NAN | MPD_FLAG_SNAN
};

static const mpd_uint_t MPD_RADIX = MPD_RADIX_CONST;

#endif /* MPDECIMAL_COMPAT_H */
