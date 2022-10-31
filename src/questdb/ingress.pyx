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

from libc.stdint cimport uint8_t, uint64_t, int64_t
from cpython.datetime cimport datetime
from cpython.bool cimport bool, PyBool_Check
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.object cimport PyObject
from cpython.float cimport PyFloat_Check
from cpython.int cimport PyInt_Check
from cpython.unicode cimport PyUnicode_Check

from .line_sender cimport *

cdef extern from "Python.h":
    ctypedef uint8_t Py_UCS1  # unicodeobject.h

    ctypedef unsigned int uint

    cdef enum PyUnicode_Kind:
        PyUnicode_1BYTE_KIND
        PyUnicode_2BYTE_KIND
        PyUnicode_4BYTE_KIND

    # Note: Returning an `object` rather than `PyObject` as the function
    # returns a new reference rather than borrowing an existing one.
    object PyUnicode_FromKindAndData(
        int kind, const void* buffer, Py_ssize_t size)

    # Ditto, see comment on why not returning a `PyObject` above.
    str PyUnicode_FromStringAndSize(
        const char* u, Py_ssize_t size)

    # Must be called before accessing data or is compact check.
    int PyUnicode_READY(object o) except -1

    # Is UCS1 and ascii (and therefore valid UTF-8).
    bint PyUnicode_IS_COMPACT_ASCII(object o)

    # Get length.
    Py_ssize_t PyUnicode_GET_LENGTH(object o)

    # Zero-copy access to buffer.
    Py_UCS1* PyUnicode_1BYTE_DATA(object o)

    Py_ssize_t PyBytes_GET_SIZE(object o)

    char* PyBytes_AsString(object o)


from enum import Enum
from typing import List, Tuple, Dict, Union, Any, Optional, Callable, Iterable
import pathlib

import sys

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


cdef bytes str_to_utf8(str string, line_sender_utf8* utf8_out):
    """
    Init the `utf8_out` object from the `string`.
    If the string is held as a UCS1 and is purely ascii, then
    the memory is borrowed.
    Otherwise the string is first encoded to UTF-8 into a bytes object
    and such bytes object is returned to transfer ownership and extend
    the lifetime of the buffer pointed to by `utf8_out`.
    """
    # Note that we bypass `line_sender_utf8_init`.
    cdef bytes owner = None
    PyUnicode_READY(string)
    if PyUnicode_IS_COMPACT_ASCII(string):
        utf8_out.len = <size_t>(PyUnicode_GET_LENGTH(string))
        utf8_out.buf = <const char*>(PyUnicode_1BYTE_DATA(string))
        return owner
    else:
        owner = string.encode('utf-8')
        utf8_out.len = <size_t>(PyBytes_GET_SIZE(owner))
        utf8_out.buf = <const char*>(PyBytes_AsString(owner))
        return owner


cdef bytes str_to_table_name(str string, line_sender_table_name* name_out):
    """
    Python string to borrowed C table name.
    Also see `str_to_utf8`.
    """
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    cdef bytes owner = str_to_utf8(string, &utf8)
    if not line_sender_table_name_init(name_out, utf8.len, utf8.buf, &err):
        raise c_err_to_py(err)
    return owner


cdef bytes str_to_column_name(str string, line_sender_column_name* name_out):
    """
    Python string to borrowed C column name.
    Also see `str_to_utf8`.
    """
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    cdef bytes owner = str_to_utf8(string, &utf8)
    if not line_sender_column_name_init(name_out, utf8.len, utf8.buf, &err):
        raise c_err_to_py(err)
    return owner


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
    A timestamp in microseconds since the UNIX epoch.

    You may construct a ``TimestampMicros`` from an integer or a ``datetime``.

    .. code-block:: python

        # Can't be negative.
        TimestampMicros(1657888365426838016)

        # Careful with the timezeone!
        TimestampMicros.from_datetime(datetime.datetime.utcnow())

    When constructing from a ``datetime``, you should take extra care
    to ensure that the timezone is correct.

    For example, ``datetime.now()`` implies the `local` timezone which
    is probably not what you want.

    When constructing the ``datetime`` object explicity, you pass in the
    timezone to use.

    .. code-block:: python

        TimestampMicros.from_datetime(
            datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))

    """
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must positive integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime):
        """
        Construct a ``TimestampMicros`` from a ``datetime.datetime`` object.
        """
        if not isinstance(dt, datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_micros(dt))

    @property
    def value(self) -> int:
        """Number of microseconds."""
        return self._value


cdef class TimestampNanos:
    """
    A timestamp in nanoseconds since the UNIX epoch.

    You may construct a ``TimestampNanos`` from an integer or a ``datetime``.

    .. code-block:: python

        # Can't be negative.
        TimestampNanos(1657888365426838016)

        # Careful with the timezeone!
        TimestampNanos.from_datetime(datetime.datetime.utcnow())

    When constructing from a ``datetime``, you should take extra care
    to ensure that the timezone is correct.

    For example, ``datetime.now()`` implies the `local` timezone which
    is probably not what you want.

    When constructing the ``datetime`` object explicity, you pass in the
    timezone to use.

    .. code-block:: python

        TimestampMicros.from_datetime(
            datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))

    """
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must positive integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime):
        """
        Construct a ``TimestampNanos`` from a ``datetime.datetime`` object.
        """
        if not isinstance(dt, datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_nanos(dt))

    @property
    def value(self) -> int:
        """Number of nanoseconds."""
        return self._value


cdef class Sender
cdef class Buffer


cdef int may_flush_on_row_complete(Buffer buffer, Sender sender) except -1:
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
        line_sender_buffer_reserve(self._impl, init_capacity)
        self._init_capacity = init_capacity
        self._max_name_len = max_name_len
        self._row_complete_sender = None

    def __dealloc__(self):
        self._row_complete_sender = None
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

    cdef inline int _set_marker(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_set_marker(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline int _rewind_to_marker(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_rewind_to_marker(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline _clear_marker(self):
        line_sender_buffer_clear_marker(self._impl)

    cdef inline int _table(self, str table_name) except -1:
        cdef line_sender_error* err = NULL
        cdef line_sender_table_name c_table_name
        cdef bytes owner = str_to_table_name(table_name, &c_table_name)
        if not line_sender_buffer_table(self._impl, c_table_name, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _symbol(self, str name, str value) except -1:
        cdef line_sender_error* err = NULL
        cdef line_sender_column_name c_name
        cdef line_sender_utf8 c_value
        cdef bytes owner_name = str_to_column_name(name, &c_name)
        cdef bytes owner_value = str_to_utf8(value, &c_value)
        if not line_sender_buffer_symbol(self._impl, c_name, c_value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_bool(
            self, line_sender_column_name c_name, bint value) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_bool(self._impl, c_name, value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_i64(
            self, line_sender_column_name c_name, int64_t value) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_i64(self._impl, c_name, value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_f64(
            self, line_sender_column_name c_name, double value) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_f64(self._impl, c_name, value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_str(
            self, line_sender_column_name c_name, str value) except -1:
        cdef line_sender_error* err = NULL
        cdef line_sender_utf8 c_value
        cdef bytes owner_value = str_to_utf8(value, &c_value)
        if not line_sender_buffer_column_str(self._impl, c_name, c_value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_ts(
            self, line_sender_column_name c_name, TimestampMicros ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_ts(self._impl, c_name, ts._value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_dt(
            self, line_sender_column_name c_name, datetime dt) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_ts(
                self._impl, c_name, datetime_to_micros(dt), &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column(self, str name, object value) except -1:
        cdef line_sender_column_name c_name
        cdef bytes owner_name = str_to_column_name(name, &c_name)
        if PyBool_Check(value):
            return self._column_bool(c_name, value)
        elif PyInt_Check(value):
            return self._column_i64(c_name, value)
        elif PyFloat_Check(value):
            return self._column_f64(c_name, value)
        elif PyUnicode_Check(value):
            return self._column_str(c_name, value)
        elif isinstance(value, TimestampMicros):
            return self._column_ts(c_name, value)
        elif isinstance(value, datetime):
            return self._column_dt(c_name, value)
        else:
            valid = ', '.join((
                'bool',
                'int',
                'float',
                'str',
                'TimestampMicros',
                'datetime.datetime'))
            raise TypeError(
                f'Unsupported type: {type(value)}. Must be one of: {valid}')

    cdef inline int _may_trigger_row_complete(self) except -1:
        cdef line_sender_error* err = NULL
        cdef PyObject* sender = NULL
        if self._row_complete_sender != None:
            sender = PyWeakref_GetObject(self._row_complete_sender)
            if sender != NULL:
                may_flush_on_row_complete(self, <Sender><object>sender)

    cdef inline int _at_ts(self, TimestampNanos ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at(self._impl, ts._value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _at_dt(self, datetime dt) except -1:
        cdef int64_t value = datetime_to_nanos(dt)
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at(self._impl, value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _at_now(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_now(self._impl, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _at(self, object ts) except -1:
        if ts is None:
            return self._at_now()
        elif isinstance(ts, TimestampNanos):
            return self._at_ts(ts)
        elif isinstance(ts, datetime):
            return self._at_dt(ts)
        else:
            raise TypeError(
                f'Unsupported type: {type(ts)}. Must be one of: ' +
                'TimestampNanos, datetime, None')

    cdef int _row(
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
                at=datetime.datetime.utcnow())


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

    # def tabular(
    #         self,
    #         table_name: str,
    #         data: Iterable[Iterable[Union[
    #             bool, int, float, str,
    #             TimestampMicros, TimestampNanos, datetime]]],
    #         *,
    #         header: Optional[List[Optional[str]]]=None,
    #         symbols: Union[bool, List[int]]=False,
    #         at: Union[None, TimestampNanos, datetime]=None):
    #     """
    #     Add multiple rows as an iterable of iterables (e.g. list of lists) to
    #     the buffer.

    #     **Data and header**

    #     The ``data`` argument specifies rows which must all be for the same
    #     table. Column names are provided as the ``header``.

    #     .. code-block:: python

    #         buffer.tabular(
    #             'table_name',
    #             [[True, 123, 3.14, 'xyz'],
    #              [False, 456, 6.28, 'abc'],
    #              [True, 789, 9.87, 'def']],
    #             header=['col1', 'col2', 'col3', 'col4'])

    #     **Designated Timestamp Column**

    #     QuestDB supports a special `designated timestamp
    #     <https://questdb.io/docs/concept/designated-timestamp/>`_ column that it
    #     uses to sort the rows by timestamp.

    #     If the data section contains the same number of columns as the header,
    #     then the designated is going to be
    #     assigned by the server, unless specified for all columns the `at`
    #     argument as either an integer wrapped in a ``TimestampNanos`` object
    #     representing nanoseconds since unix epoch (1970-01-01 00:00:00 UTC) or
    #     as a ``datetime.datetime`` object.

    #     .. code-block:: python

    #         buffer.tabular(
    #             'table_name',
    #             [[True, None, 3.14, 'xyz'],
    #              [False, 123, 6.28, 'abc'],
    #              [True, 456, 9.87, 'def']],
    #             header=['col1', 'col2', 'col3', 'col4'],
    #             at=datetime.datetime.utcnow())

    #             # or ...
    #             # at=TimestampNanos(1657386397157631000))

    #     If the rows need different `designated timestamp
    #     <https://questdb.io/docs/concept/designated-timestamp/>`_ values across
    #     different rows, you can provide them as an additional unlabeled column.
    #     An unlabled column is one that has its name set to ``None``.

    #     .. code-block:: python

    #         ts1 = datetime.datetime.utcnow()
    #         ts2 = (
    #             datetime.datetime.utcnow() +
    #             datetime.timedelta(microseconds=1))
    #         buffer.tabular(
    #             'table_name',
    #             [[True, 123, ts1],
    #              [False, 456, ts2]],
    #             header=['col1', 'col2', None])

    #     Like the ``at`` argument, the designated timestamp column may also be
    #     specified as ``TimestampNanos`` objects.

    #     .. code-block:: python

    #         buffer.tabular(
    #             'table_name',
    #             [[True, 123, TimestampNanos(1657386397157630000)],
    #              [False, 456, TimestampNanos(1657386397157631000)]],
    #             header=['col1', 'col2', None])

    #     The designated timestamp column may appear anywhere positionally.

    #     .. code-block:: python

    #         ts1 = datetime.datetime.utcnow()
    #         ts2 = (
    #             datetime.datetime.utcnow() +
    #             datetime.timedelta(microseconds=1))
    #         buffer.tabular(
    #             'table_name',
    #             [[1000, ts1, 123],
    #              [2000, ts2, 456]],
    #             header=['col1', None, 'col2'])

    #     **Other timestamp columns**

    #     Other columns may also contain timestamps. These columns can take
    #     ``datetime.datetime`` objects or ``TimestampMicros`` (*not nanos*)
    #     objects.

    #     .. code-block:: python

    #         ts1 = datetime.datetime.utcnow()
    #         ts2 = (
    #             datetime.datetime.utcnow() +
    #             datetime.timedelta(microseconds=1))
    #         buffer.tabular(
    #             'table_name',
    #             [[1000, ts1, 123],
    #              [2000, ts2, 456]],
    #             header=['col1', 'col2', 'col3'],
    #             at=datetime.datetime.utcnow())

    #     **Symbol Columns**

    #     QuestDB can represent strings via the ``STRING`` or ``SYMBOL`` types.

    #     If all the columns of type ``str`` are to be treated as ``STRING``, then
    #     specify ``symbols=False`` (default - see exaples above).

    #     If all need to be treated as ``SYMBOL`` specify ``symbols=True``.

    #     .. code-block:: python

    #         buffer.tabular(
    #             'table_name',
    #             [['abc', 123, 3.14, 'xyz'],
    #              ['def', 456, None, 'abc'],
    #              ['ghi', 789, 9.87, 'def']],
    #             header=['col1', 'col2', 'col3', 'col4'],
    #             symbols=True)  # `col1` and `col4` are SYMBOL columns.

    #    Whilst if only a select few are to be treated as ``SYMBOL``, specify a
    #    list of column indices to the ``symbols`` arg.

    #    .. code-block:: python

    #        buffer.tabular(
    #            'table_name',
    #            [['abc', 123, 3.14, 'xyz'],
    #             ['def', 456, 6.28, 'abc'],
    #             ['ghi', 789, 9.87, 'def']],
    #            header=['col1', 'col2', 'col3', 'col4'],
    #            symbols=[0])  # `col1` is SYMBOL; 'col4' is STRING.

    #    Alternatively, you can specify a list of symbol column names.

    #    .. code-block:: python

    #        buffer.tabular(
    #            'table_name',
    #            [['abc', 123, 3.14, 'xyz'],
    #             ['def', 456, 6.28, 'abc'],
    #             ['ghi', 789, 9.87, 'def']],
    #            header=['col1', 'col2', 'col3', 'col4'],
    #            symbols=['col1'])  # `col1` is SYMBOL; 'col4' is STRING.

    #     Note that column indices are 0-based and negative indices are counted
    #     from the end.
    #     """
    #     raise ValueError('nyi')

    # def pandas(
    #         self,
    #         table_name: str,
    #         data: pd.DataFrame,
    #         *,
    #         symbols: Union[bool, List[int]]=False,
    #         at: Union[None, TimestampNanos, datetime]=None):
    #     """
    #     Add a pandas DataFrame to the buffer.
    #     """
    #     raise ValueError('nyi')


_FLUSH_FMT = ('{} - See https://py-questdb-client.readthedocs.io/en/'
    'v1.0.2'
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
                columns={'temperature': 23.5, 'humidity': 0.49})
            sensor.row(
                'weather_sensor',
                symbols={'id': 'dubai2'},
                columns={'temperature': 41.2, 'humidity': 0.34})

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
                columns={'temperature': 23.5, 'humidity': 0.49})
            sender.flush()
            sender.row(
                'weather_sensor',
                symbols={'id': 'dubai2'},
                columns={'temperature': 41.2, 'humidity': 0.34})
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
      by the `webpki-roots <https://crates.io/crates/webpki-roots>`_ Rust crate
      which in turn relies on https://mkcert.org/.

    * A ``str`` or ``pathlib.Path``: Path to a PEM-encoded certificate authority
      file. This is useful for testing with self-signed certificates.

    * A special ``'insecure_skip_verify'`` string: Dangerously disable all
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
        cdef bytes host_owner

        cdef str port_str
        cdef line_sender_utf8 port_utf8
        cdef bytes port_owner

        cdef str interface_str
        cdef line_sender_utf8 interface_utf8
        cdef bytes interface_owner

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

        cdef bytes ca_owner
        cdef line_sender_utf8 ca_utf8

        self._opts = NULL
        self._impl = NULL
        self._buffer = None

        if PyInt_Check(port):
            port_str = str(port)
        elif PyUnicode_Check(port):
            port_str = port
        else:
            raise TypeError(
                f'port must be an integer or a string, not {type(port)}')

        host_owner = str_to_utf8(host, &host_utf8)
        port_owner = str_to_utf8(port_str, &port_utf8)
        self._opts = line_sender_opts_new_service(host_utf8, port_utf8)

        if interface is not None:
            interface_owner = str_to_utf8(interface, &interface_utf8)
            line_sender_opts_net_interface(self._opts, interface_utf8)

        if auth is not None:
            (a_key_id,
             a_priv_key,
             a_pub_key_x,
             a_pub_key_y) = auth
            a_key_id_owner = str_to_utf8(a_key_id, &a_key_id_utf8)
            a_priv_key_owner = str_to_utf8(a_priv_key, &a_priv_key_utf8)
            a_pub_key_x_owner = str_to_utf8(a_pub_key_x, &a_pub_key_x_utf8)
            a_pub_key_y_owner = str_to_utf8(a_pub_key_y, &a_pub_key_y_utf8)
            line_sender_opts_auth(
                self._opts,
                a_key_id_utf8,
                a_priv_key_utf8,
                a_pub_key_x_utf8,
                a_pub_key_y_utf8)

        if tls:
            if tls is True:
                line_sender_opts_tls(self._opts)
            elif isinstance(tls, str):
                if tls == 'insecure_skip_verify':
                    line_sender_opts_tls_insecure_skip_verify(self._opts)
                else:
                    ca_owner = str_to_utf8(tls, &ca_utf8)
                    line_sender_opts_tls_ca(self._opts, ca_utf8)
            elif isinstance(tls, pathlib.Path):
                tls = str(tls)
                ca_owner = str_to_utf8(tls, &ca_utf8)
                line_sender_opts_tls_ca(self._opts, ca_utf8)
            else:
                raise TypeError(
                    'tls must be a bool, a path or string pointing to CA file '
                    f'or "insecure_skip_verify", not {type(tls)}')

        if read_timeout is not None:
            line_sender_opts_read_timeout(self._opts, read_timeout)

        self._init_capacity = init_capacity
        self._max_name_len = max_name_len

        self._buffer = Buffer(
            init_capacity=init_capacity,
            max_name_len=max_name_len)

        self._auto_flush_enabled = not not auto_flush
        self._auto_flush_watermark = int(auto_flush) \
            if self._auto_flush_enabled else 0
        if self._auto_flush_watermark < 0:
            raise ValueError(
                'auto_flush_watermark must be >= 0, '
                f'not {self._auto_flush_watermark}')

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
        """
        if buffer is None and not clear:
            raise ValueError('The internal buffer must always be cleared.')

        cdef line_sender_error* err = NULL
        cdef line_sender_buffer* c_buf = NULL
        if self._impl == NULL:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'flush() can\'t be called: Not connected.')
        if buffer is not None:
            c_buf = buffer._impl
        else:
            c_buf = self._buffer._impl
        if line_sender_buffer_size(c_buf) == 0:
            return

        try:
            if clear:
                if not line_sender_flush(self._impl, c_buf, &err):
                    raise c_err_to_py_fmt(err, _FLUSH_FMT)
            else:
                if not line_sender_flush_and_keep(self._impl, c_buf, &err):
                    raise c_err_to_py_fmt(err, _FLUSH_FMT)
        except:
            # Prevent a follow-up call to `.close(flush=True)` (as is usually
            # called from `__exit__`) to raise after the sender entered an error
            # state following a failed call to `.flush()`.
            if c_buf == self._buffer._impl:
                line_sender_buffer_clear(c_buf)
            raise

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
