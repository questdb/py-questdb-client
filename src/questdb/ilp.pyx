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

from libc.stdint cimport uint8_t, int64_t
from cpython.datetime cimport datetime
from cpython.bool cimport bool, PyBool_Check

from .line_sender cimport *

cdef extern from "Python.h":
    ctypedef uint8_t Py_UCS1  # unicodeobject.h

    ctypedef unsigned int uint

    cdef enum PyUnicode_Kind:
        PyUnicode_1BYTE_KIND
        PyUnicode_2BYTE_KIND
        PyUnicode_4BYTE_KIND

    object PyUnicode_FromKindAndData(
        int kind, const void* buffer, Py_ssize_t size)

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
from typing import List, Tuple, Dict, Union, Any, Optional, Callable
from collections.abc import Iterable

import sys

class IlpErrorCode(Enum):
    """Category of Error."""
    CouldNotResolveAddr = line_sender_error_could_not_resolve_addr
    InvalidApiCall = line_sender_error_invalid_api_call
    SocketError = line_sender_error_socket_error
    InvalidUtf8 = line_sender_error_invalid_utf8
    InvalidName = line_sender_error_invalid_name
    InvalidTimestamp = line_sender_error_invalid_timestamp
    AuthError = line_sender_error_auth_error
    TlsError = line_sender_error_tls_error

    def __str__(self):
        return self.name


class IlpError(Exception):
    """
    An error whilst using the line sender or constructing its buffer.
    """
    def __init__(self, code, msg):
        super().__init__(msg)
        self._code = code

    @property
    def code(self) -> IlpErrorCode:
        return self._code


cdef inline object c_err_code_to_py(line_sender_error_code code):
    if code == line_sender_error_could_not_resolve_addr:
        return IlpErrorCode.CouldNotResolveAddr
    elif code == line_sender_error_invalid_api_call:
        return IlpErrorCode.InvalidApiCall
    elif code == line_sender_error_socket_error:
        return IlpErrorCode.SocketError
    elif code == line_sender_error_invalid_utf8:
        return IlpErrorCode.InvalidUtf8
    elif code == line_sender_error_invalid_name:
        return IlpErrorCode.InvalidName
    elif code == line_sender_error_invalid_timestamp:
        return IlpErrorCode.InvalidTimestamp
    elif code == line_sender_error_auth_error:
        return IlpErrorCode.AuthError
    elif code == line_sender_error_tls_error:
        return IlpErrorCode.TlsError
    else:
        raise ValueError('Internal error converting error code.')


cdef inline object c_err_to_py(line_sender_error* err):
    """Construct a ``SenderError`` from a C error, which will also be freed."""
    cdef line_sender_error_code code = line_sender_error_get_code(err)
    cdef size_t c_len = 0
    cdef const char* c_msg = line_sender_error_msg(err, &c_len)
    cdef object py_err
    cdef object py_msg
    cdef object py_code
    try:
        py_code = c_err_code_to_py(code)
        py_msg = PyUnicode_FromKindAndData(
            PyUnicode_1BYTE_KIND,
            c_msg,
            <Py_ssize_t>c_len)
        return IlpError(py_code, py_msg)
    finally:
        line_sender_error_free(err)


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
    """A timestamp in microseconds since the UNIX epoch."""
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must positive integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime):
        """
        Construct a `TimestampMicros` from a `datetime` object.
        """
        if not isinstance(dt, datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_micros(dt))

    @property
    def value(self) -> int:
        """Number of microseconds."""
        return self._value


cdef class TimestampNanos:
    """A timestamp in nanoseconds since the UNIX epoch."""
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must positive integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime):
        """
        Construct a `TimestampNanos` from a `datetime` object.
        """
        if not isinstance(dt, datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_nanos(dt))

    @property
    def value(self) -> int:
        """Number of nanoseconds."""
        return self._value


ctypedef bint (*row_complete_cb)(
    line_sender_buffer*,
    void* ctx,
    line_sender_error**)


cdef class LineSenderBuffer:
    """
    Construct QuestDB-flavored InfluxDB Line Protocol (ILP) messages.

    .. code-block:: python

        from questdb.ilp import LineSenderBuffer

        buf = LineSenderBuffer()
        buf.row(
            'table_name',
            symbols={'s1', 'v1', 's2', 'v2'},
            columns={'c1': True, 'c2': 0.5})

        # Append any additional rows then, once ready,
        # call `sender.flush(buffer)` on a `LineSender` instance.

    Refer to the
    `QuestDB documentation <https://questdb.io/docs/concept/symbol/>`_ to
    understand the difference between the ``SYMBOL`` and ``STRING`` types
    (TL;DR: symbols are interned strings).

    Appending data:
      * The ``row`` method appends one row at a time.
      * The ``tabular`` method appends multiple rows for a single table.

    Buffer inspection:
      * For the number of bytes in the buffer, call ``len(buffer)``.
      * To see the contents, call ``str(buffer)``.
    """
    cdef line_sender_buffer* _impl
    cdef row_complete_cb _row_complete_cb
    cdef void* _row_complete_ctx

    def __cinit__(self, init_capacity: int = 65536, max_name_len: int = 127):
        """
        Create a new buffer with the an initial capacity and max name length.
        :param int init_capacity: Initial capacity of the buffer in bytes.
        :param int max_name_len: Maximum length of a table or column name.
        """
        self._cinit_impl(init_capacity, max_name_len)

    cdef inline _cinit_impl(self, size_t init_capacity, size_t max_name_len):
        self._impl = line_sender_buffer_with_max_name_len(max_name_len)
        line_sender_buffer_reserve(self._impl, init_capacity)
        self._row_complete_cb = NULL
        self._row_complete_ctx = NULL

    def __dealloc__(self):
        line_sender_buffer_free(self._impl)

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

    def __len__(self):
        """
        The current number of bytes currently in the buffer.
        """
        return line_sender_buffer_size(self._impl)

    def __str__(self):
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
            self, line_sender_column_name c_name, int value) except -1:
        # TODO: Generally audit for int overflows this in the whole codebase.
        # We pretty certainly have one here :-).
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_i64(self._impl, c_name, value, &err):
            raise c_err_to_py(err)
        return 0

    cdef inline int _column_f64(
            self, line_sender_column_name c_name, float value) except -1:
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
        if not line_sender_buffer_column_ts(self._impl, c_name, ts.value, &err):
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
        elif isinstance(value, int):
            return self._column_i64(c_name, value)
        elif isinstance(value, float):
            return self._column_f64(c_name, value)
        elif isinstance(value, str):
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
        if self._row_complete_cb != NULL:
            if not self._row_complete_cb(
                    self._impl,
                    self._row_complete_ctx,
                    &err):
                raise c_err_to_py(err)

    cdef inline int _at_ts(self, TimestampNanos ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at(self._impl, ts.value, &err):
            raise c_err_to_py(err)
        self._may_trigger_row_complete()
        return 0

    cdef inline int _at_dt(self, datetime dt) except -1:
        cdef int64_t value = datetime_to_nanos(dt)
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at(self._impl, value, &err):
            raise c_err_to_py(err)
        self._may_trigger_row_complete()
        return 0

    cdef inline int _at_now(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_now(self._impl, &err):
            raise c_err_to_py(err)
        self._may_trigger_row_complete()
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
        self._set_marker()
        try:
            self._table(table_name)
            if not (symbols or columns):
                raise IlpError(
                    IlpErrorCode.InvalidApiCall,
                    'Must specify at least one symbol or column')
            if symbols is not None:
                for name, value in symbols.items():
                    self._symbol(name, value)
            if columns is not None:
                for name, value in columns.items():
                    self._column(name, value)
            self._at(at)
            self._clear_marker()
        except:
            self._rewind_to_marker()
            raise

    def row(
            self,
            table_name: str,
            *,
            symbols: Optional[dict[str, str]]=None,
            columns: Optional[dict[
                str,
                Union[bool, int, float, str, TimestampMicros, datetime]]]=None,
            at: Union[None, TimestampNanos, datetime]=None):
        """
        Add a single row (line) to the buffer.

        At least one ``symbols`` or ``columns`` must be specified.

        .. code-block:: python

            # All fields specified.
            buffer.row(
                'table_name',
                symbols={'sym1': 'abc', 'sym2': 'def'},
                columns={
                    'col1': True,
                    'col2': 123,
                    'col3': 3.14,
                    'col4': 'xyz',
                    'col5': TimestampMicros(123456789),
                    'col6': datetime(2019, 1, 1, 12, 0, 0)},
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

        :param table_name: The name of the table to which the row belongs.
        :param symbols: A dictionary of symbol column names to ``str`` values.
        :param columns: A dictionary of column names to ``bool``, ``int``,
            ``float``, ``str``, ``TimestampMicros`` or ``datetime`` values.
        :param at: The timestamp of the row. If ``None``, timestamp is assigned
            by the server. If ``datetime``, the timestamp is converted to
            nanoseconds. A nanosecond unix epoch timestamp can be passed
            explicitly as a ``TimestampNanos`` object.
        """
        self._row(table_name, symbols, columns, at)
        return self

    def tabular(
            self,
            table_name: str,
            data: Iterable[Iterable[Union[
                bool, int, float, str,
                TimestampMicros, TimestampNanos, datetime]]],
            *,
            header: Optional[List[Optional[str]]]=None,
            symbols: Union[bool, List[int]]=False,
            at: Union[None, TimestampNanos, datetime]=None):
        """
        Add multiple rows as an iterable of iterables (e.g. list of lists) to
        the buffer.

        **Data and header**

        The ``data`` argument specifies rows which must all be for the same
        table. Column names are provided as the ``header``.

        .. code-block:: python

            buffer.tabular(
                'table_name',
                [[True, 123, 3.14, 'xyz'],
                 [False, 456, 6.28, 'abc'],
                 [True, 789, 9.87, 'def']],
                header=['col1', 'col2', 'col3', 'col4'])

        **Designated Timestamp Column**

        QuestDB supports a special `designated timestamp
        <https://questdb.io/docs/concept/designated-timestamp/>`_ column that it
        uses to sort the rows by timestamp.

        If the data section contains the same number of columns as the header,
        then the designated is going to be
        assigned by the server, unless specified for all columns the `at`
        argument as either an integer wrapped in a ``TimestampNanos`` object
        representing nanoseconds since unix epoch (1970-01-01 00:00:00 UTC) or
        as a ``datetime.datetime`` object.

        .. code-block:: python

            buffer.tabular(
                'table_name',
                [[True, 123, 3.14, 'xyz'],
                 [False, 456, 6.28, 'abc'],
                 [True, 789, 9.87, 'def']],
                header=['col1', 'col2', 'col3', 'col4'],
                at=datetime.datetime.utcnow())

                # or ...
                # at=TimestampNanos(1657386397157631000))

        If the rows need different `designated timestamp
        <https://questdb.io/docs/concept/designated-timestamp/>`_ values across
        different rows, you can provide them as an additional unlabeled column.
        An unlabled column is one that has its name set to ``None``.

        .. code-block:: python

            ts1 = datetime.datetime.utcnow()
            ts2 = (
                datetime.datetime.utcnow() +
                datetime.timedelta(microseconds=1))
            buffer.tabular(
                'table_name',
                [[True, 123, ts1],
                 [False, 456, ts2]],
                header=['col1', 'col2', None])

        Like the ``at`` argument, the designated timestamp column may also be
        specified as ``TimestampNanos`` objects.

        .. code-block:: python

            buffer.tabular(
                'table_name',
                [[True, 123, TimestampNanos(1657386397157630000)],
                 [False, 456, TimestampNanos(1657386397157631000)]],
                header=['col1', 'col2', None])

        The designated timestamp column may appear anywhere positionally.

        .. code-block:: python

            ts1 = datetime.datetime.utcnow()
            ts2 = (
                datetime.datetime.utcnow() +
                datetime.timedelta(microseconds=1))
            buffer.tabular(
                'table_name',
                [[1000, ts1, 123],
                 [2000, ts2, 456]],
                header=['col1', None, 'col2'])

        **Other timestamp columns**

        Other columns may also contain timestamps. These columns can take
        ``datetime.datetime`` objects or ``TimestampMicros`` (*not nanos*)
        objects.

        .. code-block:: python

            ts1 = datetime.datetime.utcnow()
            ts2 = (
                datetime.datetime.utcnow() +
                datetime.timedelta(microseconds=1))
            buffer.tabular(
                'table_name',
                [[1000, ts1, 123],
                 [2000, ts2, 456]],
                header=['col1', 'col2', 'col3'],
                at=datetime.datetime.utcnow())

        **Symbol Columns**

        QuestDB can represent strings via the ``STRING`` or ``SYMBOL`` types.

        If all the columns of type ``str`` are to be treated as ``STRING``, then
        specify ``symbols=False`` (default - see exaples above).

        If all need to be treated as ``SYMBOL`` specify ``symbols=True``.

        .. code-block:: python

            buffer.tabular(
                'table_name',
                [['abc', 123, 3.14, 'xyz'],
                 ['def', 456, 6.28, 'abc'],
                 ['ghi', 789, 9.87, 'def']],
                header=['col1', 'col2', 'col3', 'col4'],
                symbols=True)  # `col1` and `col4` are SYMBOL columns.

        Whilst if only a select few are to be treated as ``SYMBOL``, specify a
        list of column indices to the ``symbols`` arg.

        .. code-block:: python

            buffer.tabular(
                'table_name',
                [['abc', 123, 3.14, 'xyz'],
                 ['def', 456, 6.28, 'abc'],
                 ['ghi', 789, 9.87, 'def']],
                header=['col1', 'col2', 'col3', 'col4'],
                symbols=[0])  # `col1` is SYMBOL; 'col4' is STRING.

        Note that column indices are 0-based and negative indices are counted
        from the end.
        """
        raise ValueError('nyi')

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


cdef class LineSender:
    cdef line_sender_opts* _opts
    cdef line_sender* _impl
    cdef LineSenderBuffer _buffer

    def __cinit__(self, str host, object port):
        cdef line_sender_error* err = NULL
        cdef str port_str
        cdef line_sender_utf8 host_utf8
        cdef bytes host_owner
        cdef line_sender_utf8 port_utf8
        cdef bytes port_owner

        self._opts = NULL
        self._impl = NULL
        self._buffer = None

        if isinstance(port, int):
            port_str = str(port)
        elif isinstance(port, str):
            port_str = port
        else:
            raise TypeError(
                f'port must be an integer or a string, not {type(port)}')

        host_owner = str_to_utf8(host, &host_utf8)
        port_owner = str_to_utf8(port_str, &port_utf8)
        self._opts = line_sender_opts_new_service(host_utf8, port_utf8)
        self._buffer = LineSenderBuffer()

    def connect(self):
        cdef line_sender_error* err = NULL
        if self._opts == NULL:
            raise IlpError(
                IlpErrorCode.InvalidApiCall,
                'connect() can\'t be called after close().')
        self._impl = line_sender_connect(self._opts, &err)
        if self._impl == NULL:
            raise c_err_to_py(err)
        line_sender_opts_free(self._opts)
        self._opts = NULL
        # self._buffer._row_complete_cb = may_flush_on_row_complete
        # self._buffer._row_complete_ctx = self

    def __enter__(self):
        self.connect()
        return self

    @property
    def buffer(self):
        return self._buffer

    cdef flush(self, LineSenderBuffer buffer=None, bint clear=True):
        cdef line_sender_error* err = NULL
        cdef line_sender_buffer* c_buf = NULL
        if self._impl == NULL:
            raise IlpError(
                IlpErrorCode.InvalidApiCall,
                'flush() can\'t be called after close().')
        if buffer is not None:
            c_buf = buffer._impl
        else:
            c_buf = self._buffer._impl
        if line_sender_buffer_size(c_buf) == 0:
            return

        try:
            if clear:
                if not line_sender_flush(self._impl, c_buf, &err):
                    raise c_err_to_py(err)
            else:
                if not line_sender_flush_and_keep(self._impl, c_buf, &err):
                    raise c_err_to_py(err)
        except:
            # Prevent a follow-up call to `.close(flush=True)` (as is usually
            # called from `__exit__`) to raise after the sender entered an error
            # state following a failed call to `.flush()`.
            if c_buf == self._buffer._impl:
                line_sender_buffer_clear(c_buf)
            raise

    cpdef close(self, bint flush=True):
        self._buffer._row_complete_cb = NULL
        self._buffer._row_complete_ctx = NULL
        line_sender_opts_free(self._opts)
        self._opts = NULL
        try:
            if (flush and (self._impl != NULL) and
                    (not line_sender_must_close(self._impl))):
                self.flush(None, True)
        finally:
            line_sender_close(self._impl)
        self._impl = NULL
        self._buffer = None

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        self.close(not exc_type)

    def __dealloc__(self):
        self.close(False)


# cdef int may_flush_on_row_complete(
#         line_sender_buffer* buf,
#         void* ctx) except -1:
#     pass
