################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2022 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

# distutils: language=c
# cython: language_level=3
# cython: binding=True

"""
API for fast data ingestion into QuestDB.
"""

# For prototypes: https://github.com/cython/cython/tree/master/Cython/Includes
from libc.stdint cimport uint8_t, uint64_t, int64_t, uint32_t, uintptr_t, \
    INT64_MAX, INT64_MIN
from libc.stdlib cimport malloc, calloc, realloc, free, abort, qsort
from libc.string cimport strncmp, memset
from libc.math cimport isnan
from libc.errno cimport errno
# from libc.stdio cimport stderr, fprintf
from cpython.datetime cimport datetime
from cpython.bool cimport bool
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.object cimport PyObject
from cpython.buffer cimport Py_buffer, PyObject_CheckBuffer, \
    PyObject_GetBuffer, PyBuffer_Release, PyBUF_SIMPLE
from cpython.memoryview cimport PyMemoryView_FromMemory

from .line_sender cimport *
from .pystr_to_utf8 cimport *
from .arrow_c_data_interface cimport *
from .extra_cpython cimport *
from .ingress_helper cimport *

# An int we use only for error reporting.
#  0 is success.
# -1 is failure.
ctypedef int void_int

import cython
include "dataframe.pxi"


from enum import Enum
from typing import List, Tuple, Dict, Union, Any, Optional, Callable, \
    Iterable
import pathlib

import sys


cdef bint _has_gil(PyThreadState** gs):
    return gs[0] == NULL


cdef bint _ensure_doesnt_have_gil(PyThreadState** gs):
    """Returns True if previously had the GIL, False otherwise."""
    if _has_gil(gs):
        gs[0] = PyEval_SaveThread()
        return True
    return False


cdef void _ensure_has_gil(PyThreadState** gs):
    if not _has_gil(gs):
        PyEval_RestoreThread(gs[0])
        gs[0] = NULL


class IngressErrorCode(Enum):
    """Category of Error."""
    CouldNotResolveAddr = line_sender_error_could_not_resolve_addr
    InvalidApiCall = line_sender_error_invalid_api_call
    SocketError = line_sender_error_socket_error
    InvalidUtf8 = line_sender_error_invalid_utf8
    InvalidName = line_sender_error_invalid_name
    InvalidTimestamp = line_sender_error_invalid_timestamp
    AuthError = line_sender_error_auth_error
    TlsError = line_sender_error_tls_error
    BadDataFrame = <int>line_sender_error_tls_error + 1

    def __str__(self) -> str:
        """Return the name of the enum."""
        return self.name


class IngressError(Exception):
    """An error whilst using the ``Sender`` or constructing its ``Buffer``."""
    def __init__(self, code, msg):
        super().__init__(msg)
        self._code = code

    @property
    def code(self) -> IngressErrorCode:
        """Return the error code."""
        return self._code


cdef inline object c_err_code_to_py(line_sender_error_code code):
    if code == line_sender_error_could_not_resolve_addr:
        return IngressErrorCode.CouldNotResolveAddr
    elif code == line_sender_error_invalid_api_call:
        return IngressErrorCode.InvalidApiCall
    elif code == line_sender_error_socket_error:
        return IngressErrorCode.SocketError
    elif code == line_sender_error_invalid_utf8:
        return IngressErrorCode.InvalidUtf8
    elif code == line_sender_error_invalid_name:
        return IngressErrorCode.InvalidName
    elif code == line_sender_error_invalid_timestamp:
        return IngressErrorCode.InvalidTimestamp
    elif code == line_sender_error_auth_error:
        return IngressErrorCode.AuthError
    elif code == line_sender_error_tls_error:
        return IngressErrorCode.TlsError
    else:
        raise ValueError('Internal error converting error code.')


cdef inline object c_err_to_code_and_msg(line_sender_error* err):
    """Construct a ``SenderError`` from a C error, which will be freed."""
    cdef line_sender_error_code code = line_sender_error_get_code(err)
    cdef size_t c_len = 0
    cdef const char* c_msg = line_sender_error_msg(err, &c_len)
    cdef object py_err
    cdef object py_msg
    cdef object py_code
    try:
        py_code = c_err_code_to_py(code)
        py_msg = PyUnicode_FromStringAndSize(c_msg, <Py_ssize_t>c_len)
        return (py_code, py_msg)
    finally:
        line_sender_error_free(err)


cdef inline object c_err_to_py(line_sender_error* err):
    """Construct an ``IngressError`` from a C error, which will be freed."""
    cdef object tup = c_err_to_code_and_msg(err)
    return IngressError(tup[0], tup[1])


cdef inline object c_err_to_py_fmt(line_sender_error* err, str fmt):
    """Construct an ``IngressError`` from a C error, which will be freed."""
    cdef object tup = c_err_to_code_and_msg(err)
    return IngressError(tup[0], fmt.format(tup[1]))


cdef object _utf8_decode_error(
        PyObject* string, uint32_t bad_codepoint):
    cdef str s = <str><object>string
    return IngressError(
        IngressErrorCode.InvalidUtf8,
        f'Invalid codepoint 0x{bad_codepoint:x} in string {s!r}: ' +
        'Cannot be encoded as UTF-8.')


cdef str _fqn(type obj):
    if obj.__module__ == 'builtins':
        return obj.__qualname__
    else:
        return f'{obj.__module__}.{obj.__qualname__}'


cdef inline void_int _encode_utf8(
        qdb_pystr_buf* b,
        PyObject* string,
        line_sender_utf8* utf8_out) except -1:
    cdef uint32_t bad_codepoint = 0
    cdef size_t count = <size_t>(PyUnicode_GET_LENGTH(string))
    cdef int kind = PyUnicode_KIND(string)
    if kind == PyUnicode_1BYTE_KIND:
        # No error handling for UCS1: All code points translate into valid UTF8.
        qdb_ucs1_to_utf8(
            b,
            count,
            PyUnicode_1BYTE_DATA(string),
            &utf8_out.len,
            &utf8_out.buf)
    elif kind == PyUnicode_2BYTE_KIND:
        if not qdb_ucs2_to_utf8(
                b,
                count,
                PyUnicode_2BYTE_DATA(string),
                &utf8_out.len,
                &utf8_out.buf,
                &bad_codepoint):
            raise _utf8_decode_error(string, bad_codepoint)
    elif kind == PyUnicode_4BYTE_KIND:
        if not qdb_ucs4_to_utf8(
                b,
                count,

                # This cast is required and is possibly a Cython compiler bug.
                # It doesn't recognize that `const Py_UCS4*`
                # is the same as `const uint32_t*`.
                <const uint32_t*>PyUnicode_4BYTE_DATA(string),

                &utf8_out.len,
                &utf8_out.buf,
                &bad_codepoint):
            raise _utf8_decode_error(string, bad_codepoint)
    else:
        raise ValueError(f'Unknown UCS kind: {kind}.')


cdef void_int str_to_utf8(
        qdb_pystr_buf* b,
        PyObject* string,
        line_sender_utf8* utf8_out) except -1:
    """
    Convert a Python string to a UTF-8 borrowed buffer.
    This is done without allocating new Python `bytes` objects.
    In case the string is an ASCII string, it's also generally zero-copy.
    The `utf8_out` param will point to (borrow from) either the ASCII buffer
    inside the original Python object or a part of memory allocated inside the
    `b` buffer.

    If you need to use `utf8_out` without the GIL, call `qdb_pystr_buf_copy`.
    """
    if not PyUnicode_CheckExact(string):
        raise TypeError(
            'Expected a str object, not an object of type ' +
            _fqn(type(<str><object>string)))
    PyUnicode_READY(string)

    # We optimize the common case of ASCII strings.
    # This avoid memory allocations and copies altogether.
    # We get away with this because ASCII is a subset of UTF-8.
    if PyUnicode_IS_COMPACT_ASCII(string):
        utf8_out.len = <size_t>(PyUnicode_GET_LENGTH(string))
        utf8_out.buf = <const char*>(PyUnicode_1BYTE_DATA(string))
        return 0

    _encode_utf8(b, string, utf8_out)



cdef void_int str_to_utf8_copy(
        qdb_pystr_buf* b,
        PyObject* string,
        line_sender_utf8* utf8_out) except -1:
    """
    Variant of `str_to_utf8` that always copies the string to a new buffer.

    The resulting `utf8_out` can be used when not holding the GIL:
    The pointed-to memory is owned by `b`.
    """
    if not PyUnicode_CheckExact(string):
        raise TypeError(
            'Expected a str object, not an object of type ' +
            _fqn(type(<str><object>string)))

    PyUnicode_READY(string)
    _encode_utf8(b, string, utf8_out)


cdef void_int str_to_table_name(
        qdb_pystr_buf* b,
        PyObject* string,
        line_sender_table_name* name_out) except -1:
    """
    Python string to borrowed C table name.
    Also see `str_to_utf8`.
    """
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    str_to_utf8(b, string, &utf8)
    if not line_sender_table_name_init(name_out, utf8.len, utf8.buf, &err):
        raise c_err_to_py(err)


cdef void_int str_to_table_name_copy(
        qdb_pystr_buf* b,
        PyObject* string,
        line_sender_table_name* name_out) except -1:
    """
    Python string to copied C table name.
    Also see `str_to_utf8_copy`.
    """
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    str_to_utf8_copy(b, string, &utf8)
    if not line_sender_table_name_init(name_out, utf8.len, utf8.buf, &err):
        raise c_err_to_py(err)


cdef void_int str_to_column_name(
        qdb_pystr_buf* b,
        str string,
        line_sender_column_name* name_out) except -1:
    """
    Python string to borrowed C column name.
    Also see `str_to_utf8`.
    """
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    str_to_utf8(b, <PyObject*>string, &utf8)
    if not line_sender_column_name_init(name_out, utf8.len, utf8.buf, &err):
        raise c_err_to_py(err)


cdef void_int str_to_column_name_copy(
        qdb_pystr_buf* b,
        str string,
        line_sender_column_name* name_out) except -1:
    """
    Python string to copied C column name.
    Also see `str_to_utf8_copy`.
    """
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    str_to_utf8_copy(b, <PyObject*>string, &utf8)
    if not line_sender_column_name_init(name_out, utf8.len, utf8.buf, &err):
        raise c_err_to_py(err)


cdef int64_t datetime_to_micros(datetime dt):
    """
    Convert a `datetime.datetime` to microseconds since the epoch.
    """
    return (
        <int64_t>(dt.timestamp()) *
        <int64_t>(1000000) +
        <int64_t>(dt.microsecond))


cdef int64_t datetime_to_nanos(datetime dt):
    """
    Convert a `datetime.datetime` to nanoseconds since the epoch.
    """
    return (
        <int64_t>(dt.timestamp()) *
        <int64_t>(1000000000) +
        <int64_t>(dt.microsecond * 1000))


cdef class TimestampMicros:
    """
    A timestamp in microseconds since the UNIX epoch (UTC).

    You may construct a ``TimestampMicros`` from an integer or a
    ``datetime.datetime``, or simply call the :func:`TimestampMicros.now`
    method.

    .. code-block:: python

        # Recommended way to get the current timestamp.
        TimestampMicros.now()

        # The above is equivalent to:
        TimestampMicros(time.time_ns() // 1000)

        # You can provide a numeric timestamp too. It can't be negative.
        TimestampMicros(1657888365426838)

    ``TimestampMicros`` can also be constructed from a ``datetime.datetime``
    object.

    .. code-block:: python

        TimestampMicros.from_datetime(
            datetime.datetime.now(tz=datetime.timezone.utc))

    We recommend that when using ``datetime`` objects, you explicitly pass in
    the timezone to use. This is because ``datetime`` objects without an
    associated timezone are assumed to be in the local timezone and it is easy
    to make mistakes (e.g. passing ``datetime.datetime.utcnow()`` is a likely
    bug).
    """
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must be a positive integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime):
        """
        Construct a ``TimestampMicros`` from a ``datetime.datetime`` object.
        """
        if not isinstance(dt, datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_micros(dt))

    @classmethod
    def now(cls):
        """
        Construct a ``TimestampMicros`` from the current time as UTC.
        """
        cdef int64_t value = line_sender_now_micros()
        return cls(value)

    @property
    def value(self) -> int:
        """Number of microseconds (Unix epoch timestamp, UTC)."""
        return self._value

    def __repr__(self):
        return f'TimestampMicros.({self._value})'


cdef class TimestampNanos:
    """
    A timestamp in nanoseconds since the UNIX epoch (UTC).

    You may construct a ``TimestampNanos`` from an integer or a
    ``datetime.datetime``, or simply call the :func:`TimestampNanos.now`
    method.

    .. code-block:: python

        # Recommended way to get the current timestamp.
        TimestampNanos.now()

        # The above is equivalent to:
        TimestampNanos(time.time_ns())

        # You can provide a numeric timestamp too. It can't be negative.
        TimestampNanos(1657888365426838016)

    ``TimestampNanos`` can also be constructed from a ``datetime`` object.

    .. code-block:: python

        TimestampNanos.from_datetime(
            datetime.datetime.now(tz=datetime.timezone.utc))

    We recommend that when using ``datetime`` objects, you explicitly pass in
    the timezone to use. This is because ``datetime`` objects without an
    associated timezone are assumed to be in the local timezone and it is easy
    to make mistakes (e.g. passing ``datetime.datetime.utcnow()`` is a likely
    bug).
    """
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must be a positive integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime):
        """
        Construct a ``TimestampNanos`` from a ``datetime.datetime`` object.
        """
        if not isinstance(dt, datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_nanos(dt))

    @classmethod
    def now(cls):
        """
        Construct a ``TimestampNanos`` from the current time as UTC.
        """
        cdef int64_t value = line_sender_now_nanos()
        return cls(value)

    @property
    def value(self) -> int:
        """Number of nanoseconds (Unix epoch timestamp, UTC)."""
        return self._value

    def __repr__(self):
        return f'TimestampNanos({self.value})'


cdef class Sender
cdef class Buffer


cdef void_int may_flush_on_row_complete(Buffer buffer, Sender sender) except -1:
    if sender._auto_flush_enabled:
        if len(buffer) >= sender._auto_flush_watermark:
            sender.flush(buffer)


cdef class Buffer:
    """
    Construct QuestDB-flavored InfluxDB Line Protocol (ILP) messages.

    The :func:`Buffer.row` method is used to add a row to the buffer.

    You can call this many times.

    .. code-block:: python

        from questdb.ingress import Buffer

        buf = Buffer()
        buf.row(
            'table_name1',
            symbols={'s1', 'v1', 's2', 'v2'},
            columns={'c1': True, 'c2': 0.5})

        buf.row(
            'table_name2',
            symbols={'questdb': '❤️'},
            columns={'like': 100000})

        # Append any additional rows then, once ready, call
        sender.flush(buffer)  # a `Sender` instance.

        # The sender auto-cleared the buffer, ready for reuse.

        buf.row(
            'table_name1',
            symbols={'s1', 'v1', 's2', 'v2'},
            columns={'c1': True, 'c2': 0.5})

        # etc.


    Buffer Constructor Arguments:
      * ``init_capacity`` (``int``): Initial capacity of the buffer in bytes.
        Defaults to ``65536`` (64KiB).
      * ``max_name_len`` (``int``): Maximum length of a column name.
        Defaults to ``127`` which is the same default value as QuestDB.
        This should match the ``cairo.max.file.name.length`` setting of the
        QuestDB instance you're connecting to.

    .. code-block:: python

        # These two buffer constructions are equivalent.
        buf1 = Buffer()
        buf2 = Buffer(init_capacity=65536, max_name_len=127)

    To avoid having to manually set these arguments every time, you can call
    the sender's ``new_buffer()`` method instead.

    .. code-block:: python

        from questdb.ingress import Sender, Buffer

        sender = Sender(host='localhost', port=9009,
            init_capacity=16384, max_name_len=64)
        buf = sender.new_buffer()
        assert buf.init_capacity == 16384
        assert buf.max_name_len == 64

    """
    cdef line_sender_buffer* _impl
    cdef qdb_pystr_buf* _b
    cdef size_t _init_capacity
    cdef size_t _max_name_len
    cdef object _row_complete_sender

    def __cinit__(self, init_capacity: int=65536, max_name_len: int=127):
        """
        Create a new buffer with the an initial capacity and max name length.
        :param int init_capacity: Initial capacity of the buffer in bytes.
        :param int max_name_len: Maximum length of a table or column name.
        """
        self._cinit_impl(init_capacity, max_name_len)

    cdef inline _cinit_impl(self, size_t init_capacity, size_t max_name_len):
        self._impl = line_sender_buffer_with_max_name_len(max_name_len)
        self._b = qdb_pystr_buf_new()
        line_sender_buffer_reserve(self._impl, init_capacity)
        self._init_capacity = init_capacity
        self._max_name_len = max_name_len
        self._row_complete_sender = None

    def __dealloc__(self):
        self._row_complete_sender = None
        qdb_pystr_buf_free(self._b)
        line_sender_buffer_free(self._impl)

    @property
    def init_capacity(self) -> int:
        """
        The initial capacity of the buffer when first created.

        This may grow over time, see ``capacity()``.
        """
        return self._init_capacity

    @property
    def max_name_len(self) -> int:
        """Maximum length of a table or column name."""
        return self._max_name_len

    @property
    def max_name_len(self) -> int:
        """Maximum length of a table or column name."""
        return self._max_name_len

    def reserve(self, additional: int):
        """
        Ensure the buffer has at least `additional` bytes of future capacity.

        :param int additional: Additional bytes to reserve.
        """
        if additional < 0:
            raise ValueError('additional must be non-negative.')
        line_sender_buffer_reserve(self._impl, additional)

    def capacity(self) -> int:
        """The current buffer capacity."""
        return line_sender_buffer_capacity(self._impl)

    def clear(self):
        """
        Reset the buffer.

        Note that flushing a buffer will (unless otherwise specified)
        also automatically clear it.

        This method is designed to be called only in conjunction with
        ``sender.flush(buffer, clear=False)``.
        """
        line_sender_buffer_clear(self._impl)
        qdb_pystr_buf_clear(self._b)

    def __len__(self) -> int:
        """
        The current number of bytes currently in the buffer.

        Equivalent (but cheaper) to ``len(str(sender))``.
        """
        return line_sender_buffer_size(self._impl)

    def __str__(self) -> str:
        """Return the constructed buffer as a string. Use for debugging."""
        return self._to_str()

    cdef inline object _to_str(self):
        cdef size_t size = 0
        cdef const char* utf8 = line_sender_buffer_peek(self._impl, &size)
        return PyUnicode_FromStringAndSize(utf8, <Py_ssize_t>size)

    cdef inline void_int _set_marker(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_set_marker(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline void_int _rewind_to_marker(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_rewind_to_marker(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline _clear_marker(self):
        line_sender_buffer_clear_marker(self._impl)

    cdef inline void_int _table(self, str table_name) except -1:
        cdef line_sender_error* err = NULL
        cdef line_sender_table_name c_table_name
        str_to_table_name(
            self._cleared_b(), <PyObject*>table_name, &c_table_name)
        if not line_sender_buffer_table(self._impl, c_table_name, &err):
            raise c_err_to_py(err)

    cdef inline qdb_pystr_buf* _cleared_b(self):
        qdb_pystr_buf_clear(self._b)
        return self._b

    cdef inline void_int _symbol(self, str name, str value) except -1:
        cdef line_sender_error* err = NULL
        cdef line_sender_column_name c_name
        cdef line_sender_utf8 c_value
        str_to_column_name(self._cleared_b(), name, &c_name)
        str_to_utf8(self._b, <PyObject*>value, &c_value)
        if not line_sender_buffer_symbol(self._impl, c_name, c_value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_bool(
            self, line_sender_column_name c_name, bint value) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_bool(self._impl, c_name, value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_i64(
            self, line_sender_column_name c_name, int64_t value) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_i64(self._impl, c_name, value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline void_int _column_f64(
            self, line_sender_column_name c_name, double value) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_f64(self._impl, c_name, value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_str(
            self, line_sender_column_name c_name, str value) except -1:
        cdef line_sender_error* err = NULL
        cdef line_sender_utf8 c_value
        str_to_utf8(self._b, <PyObject*>value, &c_value)
        if not line_sender_buffer_column_str(self._impl, c_name, c_value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_ts(
            self, line_sender_column_name c_name, TimestampMicros ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_ts_micros(self._impl, c_name, ts._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_dt(
            self, line_sender_column_name c_name, datetime dt) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_ts_micros(
                self._impl, c_name, datetime_to_micros(dt), &err):
            raise c_err_to_py(err)

    cdef inline void_int _column(self, str name, object value) except -1:
        cdef line_sender_column_name c_name
        str_to_column_name(self._cleared_b(), name, &c_name)
        if PyBool_Check(<PyObject*>value):
            self._column_bool(c_name, value)
        elif PyLong_CheckExact(<PyObject*>value):
            self._column_i64(c_name, value)
        elif PyFloat_CheckExact(<PyObject*>value):
            self._column_f64(c_name, value)
        elif PyUnicode_CheckExact(<PyObject*>value):
            self._column_str(c_name, value)
        elif isinstance(value, TimestampMicros):
            self._column_ts(c_name, value)
        elif isinstance(value, datetime):
            self._column_dt(c_name, value)
        else:
            valid = ', '.join((
                'bool',
                'int',
                'float',
                'str',
                'TimestampMicros',
                'datetime.datetime'))
            raise TypeError(
                f'Unsupported type: {_fqn(type(value))}. Must be one of: {valid}')

    cdef inline void_int _may_trigger_row_complete(self) except -1:
        cdef line_sender_error* err = NULL
        cdef PyObject* sender = NULL
        if self._row_complete_sender != None:
            sender = PyWeakref_GetObject(self._row_complete_sender)
            if sender != NULL:
                may_flush_on_row_complete(self, <Sender><object>sender)

    cdef inline void_int _at_ts(self, TimestampNanos ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_nanos(self._impl, ts._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at_dt(self, datetime dt) except -1:
        cdef int64_t value = datetime_to_nanos(dt)
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_nanos(self._impl, value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at_now(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_now(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at(self, object ts) except -1:
        if ts is None:
            self._at_now()
        elif isinstance(ts, TimestampNanos):
            self._at_ts(ts)
        elif isinstance(ts, datetime):
            self._at_dt(ts)
        else:
            raise TypeError(
                f'Unsupported type: {_fqn(type(ts))}. Must be one of: ' +
                'TimestampNanos, datetime, None')

    cdef void_int _row(
            self,
            str table_name,
            dict symbols=None,
            dict columns=None,
            object at=None) except -1:
        """
        Add a row to the buffer.
        """
        cdef bint wrote_fields = False
        self._set_marker()
        try:
            self._table(table_name)
            if symbols is not None:
                for name, value in symbols.items():
                    if value is not None:
                        self._symbol(name, value)
                        wrote_fields = True
            if columns is not None:
                for name, value in columns.items():
                    if value is not None:
                        self._column(name, value)
                        wrote_fields = True
            if wrote_fields:
                self._at(at)
                self._clear_marker()
            else:
                self._rewind_to_marker()
        except:
            self._rewind_to_marker()
            raise
        if wrote_fields:
            self._may_trigger_row_complete()

    def row(
            self,
            table_name: str,
            *,
            symbols: Optional[Dict[str, Optional[str]]]=None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros, datetime]]
                ]=None,
            at: Union[None, TimestampNanos, datetime]=None):
        """
        Add a single row (line) to the buffer.

        .. code-block:: python

            # All fields specified.
            buffer.row(
                'table_name',
                symbols={'sym1': 'abc', 'sym2': 'def', 'sym3': None},
                columns={
                    'col1': True,
                    'col2': 123,
                    'col3': 3.14,
                    'col4': 'xyz',
                    'col5': TimestampMicros(123456789),
                    'col6': datetime(2019, 1, 1, 12, 0, 0),
                    'col7': None},
                at=TimestampNanos(123456789))

            # Only symbols specified. Designated timestamp assigned by the db.
            buffer.row(
                'table_name',
                symbols={'sym1': 'abc', 'sym2': 'def'})

            # Float columns and timestamp specified as `datetime.datetime`.
            # Pay special attention to the timezone, which if unspecified is
            # assumed to be the local timezone (and not UTC).
            buffer.row(
                'sensor data',
                columns={
                    'temperature': 24.5,
                    'humidity': 0.5},
                at=datetime.datetime.now(tz=datetime.timezone.utc))


        Python strings passed as values to ``symbols`` are going to be encoded
        as the ``SYMBOL`` type in QuestDB, whilst Python strings passed as
        values to ``columns`` are going to be encoded as the ``STRING`` type.

        Refer to the
        `QuestDB documentation <https://questdb.io/docs/concept/symbol/>`_ to
        understand the difference between the ``SYMBOL`` and ``STRING`` types
        (TL;DR: symbols are interned strings).

        Column values can be specified with Python types directly and map as so:

        .. list-table::
            :header-rows: 1

            * - Python type
              - Serialized as ILP type
            * - ``bool``
              - `BOOLEAN <https://questdb.io/docs/reference/api/ilp/columnset-types#boolean>`_
            * - ``int``
              - `INTEGER <https://questdb.io/docs/reference/api/ilp/columnset-types#integer>`_
            * - ``float``
              - `FLOAT <https://questdb.io/docs/reference/api/ilp/columnset-types#float>`_
            * - ``str``
              - `STRING <https://questdb.io/docs/reference/api/ilp/columnset-types#string>`_
            * - ``datetime.datetime`` and ``TimestampMicros``
              - `TIMESTAMP <https://questdb.io/docs/reference/api/ilp/columnset-types#timestamp>`_
            * - ``None``
              - *Column is skipped and not serialized.*

        If the destination table was already created, then the columns types
        will be cast to the types of the existing columns whenever possible
        (Refer to the QuestDB documentation pages linked above).

        :param table_name: The name of the table to which the row belongs.
        :param symbols: A dictionary of symbol column names to ``str`` values.
            As a convenience, you can also pass a ``None`` value which will
            have the same effect as skipping the key: If the column already
            existed, it will be recorded as ``NULL``, otherwise it will not be
            created.
        :param columns: A dictionary of column names to ``bool``, ``int``,
            ``float``, ``str``, ``TimestampMicros`` or ``datetime`` values.
            As a convenience, you can also pass a ``None`` value which will
            have the same effect as skipping the key: If the column already
            existed, it will be recorded as ``NULL``, otherwise it will not be
            created.
        :param at: The timestamp of the row. If ``None``, timestamp is assigned
            by the server. If ``datetime``, the timestamp is converted to
            nanoseconds. A nanosecond unix epoch timestamp can be passed
            explicitly as a ``TimestampNanos`` object.
        """
        self._row(table_name, symbols, columns, at)
        return self

    def dataframe(
            self,
            df,  # : pd.DataFrame
            *,
            table_name: Optional[str] = None,
            table_name_col: Union[None, int, str] = None,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[None, int, str, TimestampNanos, datetime] = None):
        """
        Add a pandas DataFrame to the buffer.

        Also see the :func:`Sender.dataframe` method if you're
        not using the buffer explicitly. It supports the same parameters
        and also supports auto-flushing.

        This feature requires the ``pandas``, ``numpy`` and ``pyarrow``
        package to be installed.

        :param df: The pandas DataFrame to serialize to the buffer.
        :type df: pandas.DataFrame

        :param table_name: The name of the table to which the rows belong.

            If ``None``, the table name is taken from the ``table_name_col``
            parameter. If both ``table_name`` and ``table_name_col`` are
            ``None``, the table name is taken from the DataFrame's index
            name (``df.index.name`` attribute).
        :type table_name: str or None

        :param table_name_col: The name or index of the column in the DataFrame
            that contains the table name.
            
            If ``None``, the table name is taken
            from the ``table_name`` parameter. If both ``table_name`` and
            ``table_name_col`` are ``None``, the table name is taken from the
            DataFrame's index name (``df.index.name`` attribute).

            If ``table_name_col`` is an integer, it is interpreted as the index
            of the column starting from ``0``. The index of the column can be
            negative, in which case it is interpreted as an offset from the end
            of the DataFrame. E.g. ``-1`` is the last column.
        :type table_name_col: str or int or None

        :param symbols: The columns to be serialized as symbols.
        
            If ``'auto'`` (default), all columns of dtype ``'categorical'`` are
            serialized as symbols. If ``True``, all ``str`` columns are
            serialized as symbols. If ``False``, no columns are serialized as
            symbols.
            
            The list of symbols can also be specified explicitly as a ``list``
            of column names (``str``) or indices (``int``). Integer indices
            start at ``0`` and can be negative, offset from the end of the
            DataFrame. E.g. ``-1`` is the last column.

            Only columns containing strings can be serialized as symbols.

        :type symbols: str or bool or list of str or list of int

        :param at: The designated timestamp of the rows.
        
            You can specify a single value for all rows or column name or index.
            If ``None``, timestamp is assigned by the server for all rows.
            To pass in a timestamp explicity as an integer use the
            ``TimestampNanos`` wrapper type. To get the current timestamp,
            use ``TimestampNanos.now()``.
            When passing a ``datetime.datetime`` object, the timestamp is
            converted to nanoseconds.
            A ``datetime`` object is assumed to be in the local timezone unless
            one is specified explicitly (so call
            ``datetime.datetime.now(tz=datetime.timezone.utc)`` instead
            of ``datetime.datetime.utcnow()`` for the current timestamp to
            avoid bugs).

            To specify a different timestamp for each row, pass in a column name
            (``str``) or index (``int``, 0-based index, negative index
            supported): In this case, the column needs to be of dtype
            ``datetime64[ns]`` (assumed to be in the **UTC timezone** and not
            local, due to differences in Pandas and Python datetime handling) or
            ``datetime64[ns, tz]``. When a timezone is specified in the column,
            it is converted to UTC automatically.

            A timestamp column can also contain ``None`` values. The server will
            assign the current timestamp to those rows.

            **Note**: All timestamps are always converted to nanoseconds and in
            the UTC timezone. Timezone information is dropped before sending and
            QuestDB will not store any timezone information.
        :type at: TimestampNanos, datetime.datetime, int or str or None

        **Note**: It is an error to specify both ``table_name`` and
        ``table_name_col``.

        **Note**: The "index" column of the DataFrame is never serialized,
        even if it is named.

        Example:

        .. code-block:: python

            import pandas as pd
            import questdb.ingress as qi

            buf = qi.Buffer()
            # ...

            df = pd.DataFrame({
                'location': ['London', 'Managua', 'London'],
                'temperature': [24.5, 35.0, 25.5],
                'humidity': [0.5, 0.6, 0.45],
                'ts': pd.date_range('2021-07-01', periods=3)})
            buf.dataframe(
                df, table_name='weather', at='ts', symbols=['location'])

            # ...
            sender.flush(buf)

        **Pandas to ILP datatype mappings**

        .. seealso:: https://questdb.io/docs/reference/api/ilp/columnset-types/

        .. list-table:: Pandas Mappings
            :header-rows: 1

            * - Pandas ``dtype``
              - Nulls
              - ILP Datatype
            * - ``'bool'``
              - N
              - ``BOOLEAN``
            * - ``'boolean'``
              - N **α**
              - ``BOOLEAN``
            * - ``'object'`` (``bool`` objects)
              - N **α**
              - ``BOOLEAN``
            * - ``'uint8'``
              - N
              - ``INTEGER``
            * - ``'int8'``
              - N
              - ``INTEGER``
            * - ``'uint16'``
              - N
              - ``INTEGER``
            * - ``'int16'``
              - N
              - ``INTEGER``
            * - ``'uint32'``
              - N
              - ``INTEGER``
            * - ``'int32'``
              - N
              - ``INTEGER``
            * - ``'uint64'``
              - N
              - ``INTEGER`` **β**
            * - ``'int64'``
              - N
              - ``INTEGER``
            * - ``'UInt8'``
              - Y
              - ``INTEGER``
            * - ``'Int8'``
              - Y
              - ``INTEGER``
            * - ``'UInt16'``
              - Y
              - ``INTEGER``
            * - ``'Int16'``
              - Y
              - ``INTEGER``
            * - ``'UInt32'``
              - Y
              - ``INTEGER``
            * - ``'Int32'``
              - Y
              - ``INTEGER``
            * - ``'UInt64'``
              - Y
              - ``INTEGER`` **β**
            * - ``'Int64'``
              - Y
              - ``INTEGER``
            * - ``'object'`` (``int`` objects)
              - Y
              - ``INTEGER`` **β**
            * - ``'float32'`` **γ**
              - Y (``NaN``)
              - ``FLOAT``
            * - ``'float64'``
              - Y (``NaN``)
              - ``FLOAT``
            * - ``'object'`` (``float`` objects)
              - Y (``NaN``)
              - ``FLOAT``
            * - ``'string'`` (``str`` objects)
              - Y
              - ``STRING`` (default), ``SYMBOL`` via ``symbols`` arg. **δ**
            * - ``'string[pyarrow]'``
              - Y
              - ``STRING`` (default), ``SYMBOL`` via ``symbols`` arg. **δ**
            * - ``'category'`` (``str`` objects) **ε**
              - Y
              - ``SYMBOL`` (default), ``STRING`` via ``symbols`` arg. **δ**
            * - ``'object'`` (``str`` objects)
              - Y
              - ``STRING`` (default), ``SYMBOL`` via ``symbols`` arg. **δ**
            * - ``'datetime64[ns]'``
              - Y
              - ``TIMESTAMP`` **ζ**
            * - ``'datetime64[ns, tz]'``
              - Y
              - ``TIMESTAMP`` **ζ**

        .. note::

            * **α**: Note some pandas dtypes allow nulls (e.g. ``'boolean'``),
              where the QuestDB database does not.

            * **β**: The valid range for integer values is -2^63 to 2^63-1.
              Any ``'uint64'``, ``'UInt64'`` or python ``int`` object values
              outside this range will raise an error during serialization.

            * **γ**: Upcast to 64-bit float during serialization.

            * **δ**: Columns containing strings can also be used to specify the
              table name. See ``table_name_col``.

            * **ε**: We only support categories containing strings. If the
              category contains non-string values, an error will be raised.

            * **ζ**: The '.dataframe()' method only supports datetimes with
              nanosecond precision. The designated timestamp column (see ``at``
              parameter) maintains the nanosecond precision, whilst values
              stored as columns have their precision truncated to microseconds.
              All dates are sent as UTC and any additional timezone information
              is dropped. If no timezone is specified, we follow
              the pandas convention of assuming the timezone is UTC.
              Datetimes before 1970-01-01 00:00:00 UTC are not supported.
              If a datetime value is specified as ``None`` (``NaT``), it is
              interpreted as the current QuestDB server time set on receipt of
              message.

        **Error Handling and Recovery**

        In case an exception is raised during dataframe serialization, the
        buffer is left in its previous state.
        The buffer remains in a valid state and can be used for further calls
        even after an error.

        For clarification, as an example, if an invalid ``None``
        value appears at the 3rd row for a ``bool`` column, neither the 3rd nor
        the preceding rows are added to the buffer.

        **Note**: This differs from the :func:`Sender.dataframe` method, which
        modifies this guarantee due to its ``auto_flush`` logic.

        **Performance Considerations**

        The Python GIL is released during serialization if it is not needed.
        If any column requires the GIL, the entire serialization is done whilst
        holding the GIL.

        Column types that require the GIL are:

        * Columns of ``str``, ``float`` or ``int`` or ``float`` Python objects.
        * The ``'string[python]'`` dtype.
        """
        _dataframe(
            auto_flush_blank(),
            self._impl,
            self._b,
            df,
            table_name,
            table_name_col,
            symbols,
            at)


_FLUSH_FMT = ('{} - See https://py-questdb-client.readthedocs.io/en/'
    'v1.2.0'
    '/troubleshooting.html#inspecting-and-debugging-errors#flush-failed')


cdef class Sender:
    """
    A sender is a client that inserts rows into QuestDB via the ILP protocol.

    **Inserting two rows**

    In this example, data will be flushed and sent at the end of the ``with``
    block.

    .. code-block:: python

        with Sender('localhost', 9009) as sender:
            sender.row(
                'weather_sensor',
                symbols={'id': 'toronto1'},
                columns={'temperature': 23.5, 'humidity': 0.49},
                at=TimestampNanos.now())
            sensor.row(
                'weather_sensor',
                symbols={'id': 'dubai2'},
                columns={'temperature': 41.2, 'humidity': 0.34},
                at=TimestampNanos.now())

    The ``Sender`` object holds an internal buffer. The call to ``.row()``
    simply forwards all arguments to the :func:`Buffer.row` method.


    **Explicit flushing**

    An explicit call to :func:`Sender.flush` will send any pending data
    immediately.

    .. code-block:: python

        with Sender('localhost', 9009) as sender:
            sender.row(
                'weather_sensor',
                symbols={'id': 'toronto1'},
                columns={'temperature': 23.5, 'humidity': 0.49},
                at=TimestampNanos.now())
            sender.flush()
            sender.row(
                'weather_sensor',
                symbols={'id': 'dubai2'},
                columns={'temperature': 41.2, 'humidity': 0.34},
                at=TimestampNanos.now())
            sender.flush()


    **Auto-flushing (on by default, watermark at 63KiB)**

    To avoid accumulating very large buffers, the sender will flush the buffer
    automatically once its buffer reaches a certain byte-size watermark.

    You can control this behavior by setting the ``auto_flush`` argument.

    .. code-block:: python

        # Never flushes automatically.
        sender = Sender('localhost', 9009, auto_flush=False)
        sender = Sender('localhost', 9009, auto_flush=None) # Ditto.
        sender = Sender('localhost', 9009, auto_flush=0)  # Ditto.

        # Flushes automatically when the buffer reaches 1KiB.
        sender = Sender('localhost', 9009, auto_flush=1024)

        # Flushes automatically after every row.
        sender = Sender('localhost', 9009, auto_flush=True)
        sender = Sender('localhost', 9009, auto_flush=1)  # Ditto.


    **Authentication and TLS Encryption**

    This implementation supports authentication and TLS full-connection
    encryption.

    The ``Sender(.., auth=..)`` argument is a tuple of ``(kid, d, x, y)`` as
    documented on the `QuestDB ILP authentication
    <https://questdb.io/docs/reference/api/ilp/authenticate>`_ documentation.
    Authentication is optional and disabled by default.

    The ``Sender(.., tls=..)`` argument is one of:

    * ``False``: No TLS encryption (default).

    * ``True``: TLS encryption, accepting all common certificates as recognized
      by either the `webpki-roots <https://crates.io/crates/webpki-roots>`_ Rust
      crate (which in turn relies on https://mkcert.org/), or the OS-provided
      certificate store.

    * A ``str`` or ``pathlib.Path``: Path to a PEM-encoded certificate authority
      file. This is useful for testing with self-signed certificates.

    * The special ``'os_roots'`` string: Use the OS-provided certificate store.

    * The special ``'webpki_roots'`` string: Use the `webpki-roots
      <https://crates.io/crates/webpki-roots>`_ Rust crate to recognize
      certificates.

    * The special ``'webpki_and_os_roots'`` string: Use both the `webpki-roots
      <https://crates.io/crates/webpki-roots>`_ Rust crate and the OS-provided
      certificate store to recognize certificates. (equivalent to `True`).

    * The special ``'insecure_skip_verify'`` string: Dangerously disable all
      TLS certificate verification (do *NOT* use in production environments).

    **Positional constructor arguments for the Sender(..)**

    * ``host``: Hostname or IP address of the QuestDB server.

    * ``port``: Port number of the QuestDB server.


    **Keyword-only constructor arguments for the Sender(..)**

    * ``interface`` (``str``): Network interface to bind to.
      Set this if you have an accelerated network interface (e.g. Solarflare)
      and want to use it.

    * ``auth`` (``tuple``): Authentication tuple or ``None`` (default).
      *See above for details*.

    * ``tls`` (``bool``, ``pathlib.Path`` or ``str``): TLS configuration or
      ``False`` (default). *See above for details*.

    * ``read_timeout`` (``int``): How long to wait for messages from the QuestDB server
      during the TLS handshake or authentication process.
      This field is expressed in milliseconds. The default is 15 seconds.

    * ``init_capacity`` (``int``): Initial buffer capacity of the internal buffer.
      *Default: 65536 (64KiB).*
      *See Buffer's constructor for more details.*

    * ``max_name_length`` (``int``): Maximum length of a table or column name.
      *See Buffer's constructor for more details.*

    * ``auto_flush`` (``bool`` or ``int``): Whether to automatically flush the
      buffer when it reaches a certain byte-size watermark.
      *Default: 64512 (63KiB).*
      *See above for details.*
    """

    # We need the Buffer held by a Sender can hold a weakref to its Sender.
    # This avoids a circular reference that requires the GC to clean up.
    cdef object __weakref__

    cdef line_sender_opts* _opts
    cdef line_sender* _impl
    cdef Buffer _buffer
    cdef bint _auto_flush_enabled
    cdef ssize_t _auto_flush_watermark
    cdef size_t _init_capacity
    cdef size_t _max_name_len

    def __cinit__(
            self,
            str host,
            object port,
            *,
            str interface=None,
            tuple auth=None,
            object tls=False,
            uint64_t read_timeout=15000,
            uint64_t init_capacity=65536,  # 64KiB
            uint64_t max_name_len=127,
            object auto_flush=64512):  # 63KiB
        cdef line_sender_error* err = NULL

        cdef line_sender_utf8 host_utf8

        cdef str port_str
        cdef line_sender_utf8 port_utf8

        cdef str interface_str
        cdef line_sender_utf8 interface_utf8

        cdef str a_key_id
        cdef bytes a_key_id_owner
        cdef line_sender_utf8 a_key_id_utf8

        cdef str a_priv_key
        cdef bytes a_priv_key_owner
        cdef line_sender_utf8 a_priv_key_utf8

        cdef str a_pub_key_x
        cdef bytes a_pub_key_x_owner
        cdef line_sender_utf8 a_pub_key_x_utf8

        cdef str a_pub_key_y
        cdef bytes a_pub_key_y_owner
        cdef line_sender_utf8 a_pub_key_y_utf8

        cdef line_sender_utf8 ca_utf8

        cdef qdb_pystr_buf* b

        self._opts = NULL
        self._impl = NULL

        self._init_capacity = init_capacity
        self._max_name_len = max_name_len

        self._buffer = Buffer(
            init_capacity=init_capacity,
            max_name_len=max_name_len)

        b = self._buffer._b

        if PyLong_CheckExact(<PyObject*>port):
            port_str = str(port)
        elif PyUnicode_CheckExact(<PyObject*>port):
            port_str = port
        else:
            raise TypeError(
                f'port must be an int or a str, not {_fqn(type(port))}')

        str_to_utf8(b, <PyObject*>host, &host_utf8)
        str_to_utf8(b, <PyObject*>port_str, &port_utf8)
        self._opts = line_sender_opts_new_service(host_utf8, port_utf8)

        if interface is not None:
            str_to_utf8(b, <PyObject*>interface, &interface_utf8)
            line_sender_opts_net_interface(self._opts, interface_utf8)

        if auth is not None:
            (a_key_id,
             a_priv_key,
             a_pub_key_x,
             a_pub_key_y) = auth
            str_to_utf8(b, <PyObject*>a_key_id, &a_key_id_utf8)
            str_to_utf8(b, <PyObject*>a_priv_key, &a_priv_key_utf8)
            str_to_utf8(b, <PyObject*>a_pub_key_x, &a_pub_key_x_utf8)
            str_to_utf8(b, <PyObject*>a_pub_key_y, &a_pub_key_y_utf8)
            line_sender_opts_auth(
                self._opts,
                a_key_id_utf8,
                a_priv_key_utf8,
                a_pub_key_x_utf8,
                a_pub_key_y_utf8)

        if tls:
            if tls is True:
                line_sender_opts_tls_webpki_and_os_roots(self._opts)
            elif isinstance(tls, str):
                if tls == 'webpki_roots':
                    line_sender_opts_tls(self._opts)
                elif tls == 'os_roots':
                    line_sender_opts_tls_os_roots(self._opts)
                elif tls == 'webpki_and_os_roots':
                    line_sender_opts_tls_webpki_and_os_roots(self._opts)
                elif tls == 'insecure_skip_verify':
                    line_sender_opts_tls_insecure_skip_verify(self._opts)
                else:
                    str_to_utf8(b, <PyObject*>tls, &ca_utf8)
                    line_sender_opts_tls_ca(self._opts, ca_utf8)
            elif isinstance(tls, pathlib.Path):
                tls = str(tls)
                str_to_utf8(b, <PyObject*>tls, &ca_utf8)
                line_sender_opts_tls_ca(self._opts, ca_utf8)
            else:
                raise TypeError(
                    'tls must be a bool, a path or string pointing to CA file '
                    f'or "insecure_skip_verify", not {_fqn(type(tls))}')

        if read_timeout is not None:
            line_sender_opts_read_timeout(self._opts, read_timeout)

        self._auto_flush_enabled = not not auto_flush
        self._auto_flush_watermark = int(auto_flush) \
            if self._auto_flush_enabled else 0
        if self._auto_flush_watermark < 0:
            raise ValueError(
                'auto_flush_watermark must be >= 0, '
                f'not {self._auto_flush_watermark}')
        
        qdb_pystr_buf_clear(b)

    def new_buffer(self):
        """
        Make a new configured buffer.

        The buffer is set up with the configured `init_capacity` and
        `max_name_len`.
        """
        return Buffer(
            init_capacity=self._init_capacity,
            max_name_len=self._max_name_len)

    @property
    def init_capacity(self) -> int:
        """The initial capacity of the sender's internal buffer."""
        return self._init_capacity

    @property
    def max_name_len(self) -> int:
        """Maximum length of a table or column name."""
        return self._max_name_len

    def connect(self):
        """
        Connect to the QuestDB server.

        This method is synchronous and will block until the connection is
        established.

        If the connection is set up with authentication and/or TLS, this
        method will return only *after* the handshake(s) is/are complete.
        """
        cdef line_sender_error* err = NULL
        if self._opts == NULL:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'connect() can\'t be called after close().')
        self._impl = line_sender_connect(self._opts, &err)
        if self._impl == NULL:
            raise c_err_to_py(err)
        line_sender_opts_free(self._opts)
        self._opts = NULL

        # Request callbacks when rows are complete.
        if self._buffer is not None:
            self._buffer._row_complete_sender = PyWeakref_NewRef(self, None)

    def __enter__(self) -> Sender:
        """Call :func:`Sender.connect` at the start of a ``with`` block."""
        self.connect()
        return self

    def __str__(self) -> str:
        """
        Inspect the contents of the internal buffer.

        The ``str`` value returned represents the unsent data.

        Also see :func:`Sender.__len__`.
        """
        return str(self._buffer)

    def __len__(self) -> int:
        """
        Number of bytes of unsent data in the internal buffer.

        Equivalent (but cheaper) to ``len(str(sender))``.
        """
        return len(self._buffer)

    def row(self,
            table_name: str,
            *,
            symbols: Optional[Dict[str, str]]=None,
            columns: Optional[Dict[
                str,
                Union[bool, int, float, str, TimestampMicros, datetime]]]=None,
            at: Union[None, TimestampNanos, datetime]=None):
        """
        Write a row to the internal buffer.

        This may be sent automatically depending on the ``auto_flush`` setting
        in the constructor.

        Refer to the :func:`Buffer.row` documentation for details on arguments.
        """
        self._buffer.row(table_name, symbols=symbols, columns=columns, at=at)

    def dataframe(
            self,
            df,  # : pd.DataFrame
            *,
            table_name: Optional[str] = None,
            table_name_col: Union[None, int, str] = None,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[None, int, str, TimestampNanos, datetime] = None):
        """
        Write a Pandas DataFrame to the internal buffer.

        Example:

        .. code-block:: python

            import pandas as pd
            import questdb.ingress as qi

            df = pd.DataFrame({
                'car': pd.Categorical(['Nic 42', 'Eddi', 'Nic 42', 'Eddi']),
                'position': [1, 2, 1, 2],
                'speed': [89.3, 98.2, 3, 4],
                'lat_gforce': [0.1, -0.2, -0.6, 0.4],
                'accelleration': [0.1, -0.2, 0.6, 4.4],
                'tyre_pressure': [2.6, 2.5, 2.6, 2.5],
                'ts': [
                    pd.Timestamp('2022-08-09 13:56:00'),
                    pd.Timestamp('2022-08-09 13:56:01'),
                    pd.Timestamp('2022-08-09 13:56:02'),
                    pd.Timestamp('2022-08-09 13:56:03')]})

            with qi.Sender('localhost', 9000) as sender:
                sender.dataframe(df, table_name='race_metrics', at='ts')

        This method builds on top of the :func:`Buffer.dataframe` method.
        See its documentation for details on arguments.

        Additionally, this method also supports auto-flushing the buffer
        as specified in the ``Sender``'s ``auto_flush`` constructor argument.
        Auto-flushing is implemented incrementally, meanting that when
        calling ``sender.dataframe(df)`` with a large ``df``, the sender may
        have sent some of the rows to the server already whist the rest of the
        rows are going to be sent at the next auto-flush or next explicit call
        to :func:`Sender.flush`.

        In case of data errors with auto-flushing enabled, some of the rows
        may have been transmitted to the server already.
        """
        cdef auto_flush_t af = auto_flush_blank()
        if self._auto_flush_enabled:
            af.sender = self._impl
            af.watermark = self._auto_flush_watermark
        _dataframe(
            af,
            self._buffer._impl,
            self._buffer._b,
            df,
            table_name,
            table_name_col,
            symbols,
            at)

    cpdef flush(self, Buffer buffer=None, bint clear=True):
        """
        If called with no arguments, immediately flushes the internal buffer.

        Alternatively you can flush a buffer that was constructed explicitly
        by passing ``buffer``.

        The buffer will be cleared by default, unless ``clear`` is set to
        ``False``.

        This method does nothing if the provided or internal buffer is empty.

        :param buffer: The buffer to flush. If ``None``, the internal buffer
            is flushed.

        :param clear: If ``True``, the flushed buffer is cleared (default).
            If ``False``, the flushed buffer is left in the internal buffer.
            Note that ``clear=False`` is only supported if ``buffer`` is also
            specified.

        The Python GIL is released during the network IO operation.
        """
        cdef line_sender* sender = self._impl
        cdef line_sender_error* err = NULL
        cdef line_sender_buffer* c_buf = NULL
        cdef PyThreadState* gs = NULL  # GIL state. NULL means we have the GIL.
        cdef bint ok = False

        if buffer is None and not clear:
            raise ValueError('The internal buffer must always be cleared.')

        if sender == NULL:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'flush() can\'t be called: Not connected.')
        if buffer is not None:
            c_buf = buffer._impl
        else:
            c_buf = self._buffer._impl
        if line_sender_buffer_size(c_buf) == 0:
            return

        # We might be blocking on IO, so temporarily release the GIL.
        _ensure_doesnt_have_gil(&gs)
        if clear:
            ok = line_sender_flush(sender, c_buf, &err)
        else:
            ok = line_sender_flush_and_keep(sender, c_buf, &err)
        _ensure_has_gil(&gs)
        if not ok:
            if c_buf == self._buffer._impl:
                # Prevent a follow-up call to `.close(flush=True)` (as is
                # usually called from `__exit__`) to raise after the sender
                # entered an error state following a failed call to `.flush()`.
                # Note: In this case `clear` is always `True`.
                line_sender_buffer_clear(c_buf)
            raise c_err_to_py_fmt(err, _FLUSH_FMT)

    cdef _close(self):
        self._buffer = None
        line_sender_opts_free(self._opts)
        self._opts = NULL
        line_sender_close(self._impl)
        self._impl = NULL

    cpdef close(self, bint flush=True):
        """
        Disconnect.

        This method is idempotent and can be called repeatedly.

        Once a sender is closed, it can't be re-used.

        :param bool flush: If ``True``, flush the internal buffer before closing.
        """
        try:
            if (flush and (self._impl != NULL) and
                    (not line_sender_must_close(self._impl))):
                self.flush(None, True)
        finally:
            self._close()

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        """
        Flush pending and disconnect at the end of a ``with`` block.

        If the ``with`` block raises an exception, any pending data will
        *NOT* be flushed.

        This is implemented by calling :func:`Sender.close`.
        """
        self.close(not exc_type)

    def __dealloc__(self):
        self._close()
