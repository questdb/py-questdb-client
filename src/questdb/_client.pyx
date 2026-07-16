################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2024 QuestDB
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
API for fast data ingestion into and querying from QuestDB.
"""

__all__ = [
    'ConnectionEvent',
    'ConnectionEventKind',
    'PooledReader',
    'PooledSender',
    'QuestDB',
    'QuestDBError',
    'QuestDBErrorCode',
    'QuestDBServerRejectionError',
    'Protocol',
    'QueryResult',
    'Sender',
    'QwpWsError',
    'QwpWsErrorCategory',
    'QwpWsErrorPolicy',
    'QwpWsProgress',
    'SenderTransaction',
    'ServerInfo',
    'ServerRole',
    'ServerTimestamp',
    'ServerTimestampType',
    'TimestampMicros',
    'TimestampNanos',
    'TlsCa',
    'UnsupportedDataFrameShapeError',
    'WARN_HIGH_RECONNECTS'
]

# For prototypes: https://github.com/cython/cython/tree/master/Cython/Includes
from libc.stdint cimport uint8_t, uint64_t, int64_t, int32_t, uint32_t, \
    uintptr_t, INT64_MAX, INT64_MIN
from libc.stdlib cimport malloc, calloc, realloc, free, qsort
from libc.string cimport strncmp, memset, memcpy, strlen
from libc.math cimport isnan, floor
from cpython.datetime cimport datetime as cp_datetime
from cpython.datetime cimport timedelta as cp_timedelta
from cpython.datetime cimport (
    PyDateTime_GET_YEAR, PyDateTime_GET_MONTH, PyDateTime_GET_DAY,
    PyDateTime_DATE_GET_HOUR, PyDateTime_DATE_GET_MINUTE,
    PyDateTime_DATE_GET_SECOND, PyDateTime_DATE_GET_MICROSECOND,
)
from cpython.bool cimport bool
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.object cimport PyObject
from cpython.buffer cimport Py_buffer, PyObject_CheckBuffer, \
    PyObject_GetBuffer, PyBuffer_Release, PyBUF_SIMPLE
from cpython.pycapsule cimport (PyCapsule_GetPointer, PyCapsule_IsValid,
                                PyCapsule_New)
from cpython.ref cimport Py_INCREF, Py_DECREF

from .line_sender cimport *
from .rpyutils cimport *
from .conf_str cimport *
from .arrow_c_data_interface cimport *
from .extra_cpython cimport *
from ._client_helper cimport *

# An int we use only for error reporting.
#  0 is success.
# -1 is failure.
ctypedef int void_int

import cython
include "dataframe.pxi"
include "egress.pxi"

from enum import Enum
from typing import List, Dict, Union, Any, Optional
from dataclasses import dataclass
from cpython.bytes cimport (PyBytes_FromStringAndSize,
                            PyBytes_GET_SIZE, PyBytes_AsString)

import datetime
import os
import threading
import time
import uuid
import warnings
import logging

import numpy
cimport numpy as cnp
from numpy cimport NPY_DOUBLE, PyArrayObject

# Functions we need to import as `PyObject` to avoid Cython's `object` type
from .extra_numpy cimport *

cnp.import_array()

cdef bint _dataframe_columnar_count_io_stats = False
cdef uint64_t _dataframe_columnar_flush_calls = 0
cdef uint64_t _dataframe_columnar_flush_ns = 0
cdef uint64_t _dataframe_columnar_sync_calls = 0
cdef uint64_t _dataframe_columnar_sync_ns = 0
cdef uint64_t _dataframe_columnar_flush_retry_syncs = 0

cdef size_t _QWP_MAX_DEFERRED_ARROW_FRAMES = 100

# Default rows per columnar batch. Only a pipelining-granularity default: the
# column sender splits any frame exceeding the negotiated batch cap regardless
# of this size. Mirrors the Rust core's `DEFAULT_MAX_CHUNK_ROWS`; both sides
# pin the value in tests so it cannot drift. Keep them in sync when changing it.
DEFAULT_MAX_CHUNK_ROWS = 16384


# This value is automatically updated by the `bump2version` tool.
# If you need to update it, also update the search definition in
# .bumpversion.cfg.
VERSION = '5.0.0'

WARN_HIGH_RECONNECTS = True


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


class QuestDBErrorCode(Enum):
    """Category of Error."""
    CouldNotResolveAddr = line_sender_error_could_not_resolve_addr
    InvalidApiCall = line_sender_error_invalid_api_call
    SocketError = line_sender_error_socket_error
    InvalidUtf8 = line_sender_error_invalid_utf8
    InvalidName = line_sender_error_invalid_name
    InvalidTimestamp = line_sender_error_invalid_timestamp
    AuthError = line_sender_error_auth_error
    TlsError = line_sender_error_tls_error
    HttpNotSupported = line_sender_error_http_not_supported
    ServerFlushError = line_sender_error_server_flush_error
    ServerRejection = line_sender_error_server_rejection
    RoleMismatch = line_sender_error_role_mismatch
    ConfigError = line_sender_error_config_error
    ArrayError = line_sender_error_array_error
    ProtocolVersionError = line_sender_error_protocol_version_error
    DecimalError = line_sender_error_invalid_decimal
    ArrowUnsupportedColumnKind = line_sender_error_arrow_unsupported_column_kind
    ArrowIngest = line_sender_error_arrow_ingest
    FailoverRetry = line_sender_error_failover_retry
    ConnectTimeout = line_sender_error_connect_timeout
    # Query / reader (egress) categories. The error model is unified across
    # ingest and query, so these are real FFI codes (no longer bucketed).
    HandshakeError = line_sender_error_handshake_error
    UnsupportedServer = line_sender_error_unsupported_server
    ProtocolError = line_sender_error_protocol_error
    InvalidBind = line_sender_error_invalid_bind
    ServerSchemaMismatch = line_sender_error_server_schema_mismatch
    ServerParseError = line_sender_error_server_parse_error
    ServerInternalError = line_sender_error_server_internal_error
    ServerSecurityError = line_sender_error_server_security_error
    LimitExceeded = line_sender_error_limit_exceeded
    ServerLimitExceeded = line_sender_error_server_limit_exceeded
    Cancelled = line_sender_error_cancelled
    FailoverWouldDuplicate = line_sender_error_failover_would_duplicate
    SchemaDrift = line_sender_error_schema_drift
    NoSchema = line_sender_error_no_schema
    ArrowExport = line_sender_error_arrow_export
    BatchTooLarge = line_sender_error_batch_too_large
    StoreResendRequired = line_sender_error_store_resend_required
    # Python-only sentinel with no backing FFI code: raised by the Cython
    # DataFrame-shape validation path. Sits in a reserved high band, disjoint
    # from the contiguous FFI code space, so an appended FFI variant can never
    # collide with it. Compared by identity; never sent over FFI.
    BadDataFrame = 0x10000

    def __str__(self) -> str:
        """Return the name of the enum."""
        return self.name


class QuestDBError(Exception):
    """An error whilst using the QuestDB client."""
    def __init__(self, code, msg, qwp_ws_error=None, *, in_doubt=False):
        super().__init__(msg)
        self._code = code
        self._qwp_ws_error = qwp_ws_error
        self._in_doubt = bool(in_doubt)

    @property
    def code(self) -> QuestDBErrorCode:
        """Return the error code."""
        return self._code

    @property
    def in_doubt(self) -> bool:
        """
        Whether the failed operation may already have delivered its input.

        Retrying the same input when this is true can duplicate rows unless the
        destination table has an appropriate deduplication guarantee.
        """
        return self._in_doubt

    @property
    def qwp_ws_error(self):
        """
        Return the structured QWP/WebSocket diagnostic, if this error carries
        one from a QWP/WebSocket sender failure.
        """
        if self._qwp_ws_error is not None:
            self._qwp_ws_error = _qwp_ws_error_from_raw(self._qwp_ws_error)
        return self._qwp_ws_error


class QuestDBServerRejectionError(QuestDBError):
    """
    A terminal QWP/WebSocket server rejection.

    The structured server payload is available through
    :attr:`QuestDBError.qwp_ws_error`.
    """


class UnsupportedDataFrameShapeError(QuestDBError):
    """
    A DataFrame shape is not supported by the optimized columnar client path.

    ``column_failures`` carries structured per-column rejection details where
    available; each is a ``dict`` with ``column`` (name or ``None`` for a
    whole-frame issue), ``target``, ``source_code``, and ``reason``. The same
    per-column reasons are also folded into ``str(exc)`` so a bare
    ``print(exc)`` explains *why* the frame was rejected, and — when the
    designated timestamp is at fault — how to fix it on
    :meth:`QuestDB.dataframe` (name a valid timestamp column, or pass
    ``ServerTimestamp``).
    """
    def __init__(self, msg, column_failures=None):
        self.column_failures = tuple(column_failures or ())
        super().__init__(
            QuestDBErrorCode.BadDataFrame,
            _format_unsupported_dataframe_msg(msg, self.column_failures))


cdef str _format_unsupported_dataframe_msg(str msg, tuple column_failures):
    # Surface the per-column reasons (otherwise stranded on the
    # `.column_failures` attribute) so a plain `str(exc)` says what to fix
    # instead of a bare "columnar v1". When the designated timestamp is the
    # problem, point at the two client-side remedies (name a valid column,
    # or let the server stamp) — keeping the user on QuestDB.dataframe()
    # rather than steering them off to the row path.
    if not column_failures:
        return msg
    cdef list lines = [msg]
    cdef object failure, column, reason
    cdef bint ts_related = False
    for failure in column_failures:
        if isinstance(failure, dict):
            column = failure.get('column')
            reason = failure.get('reason', failure)
            if (failure.get('target') == 'designated timestamp'
                    or 'ServerTimestamp' in str(reason)):
                ts_related = True
        else:
            column, reason = None, failure
        if column is not None:
            lines.append(f'  - column {column!r}: {reason}')
        else:
            lines.append(f'  - {reason}')
    if ts_related:
        lines.append(
            "  For the designated timestamp, name a valid timestamp column "
            "with at='<column>', or pass at=ServerTimestamp to have the "
            "server assign each row's timestamp on arrival.")
    return '\n'.join(lines)


cdef inline object c_err_code_to_py(line_sender_error_code code):
    if code == line_sender_error_could_not_resolve_addr:
        return QuestDBErrorCode.CouldNotResolveAddr
    elif code == line_sender_error_invalid_api_call:
        return QuestDBErrorCode.InvalidApiCall
    elif code == line_sender_error_socket_error:
        return QuestDBErrorCode.SocketError
    elif code == line_sender_error_invalid_utf8:
        return QuestDBErrorCode.InvalidUtf8
    elif code == line_sender_error_invalid_name:
        return QuestDBErrorCode.InvalidName
    elif code == line_sender_error_invalid_timestamp:
        return QuestDBErrorCode.InvalidTimestamp
    elif code == line_sender_error_auth_error:
        return QuestDBErrorCode.AuthError
    elif code == line_sender_error_tls_error:
        return QuestDBErrorCode.TlsError
    elif code == line_sender_error_http_not_supported:
        return QuestDBErrorCode.HttpNotSupported
    elif code == line_sender_error_server_flush_error:
        return QuestDBErrorCode.ServerFlushError
    elif code == line_sender_error_server_rejection:
        return QuestDBErrorCode.ServerRejection
    elif code == line_sender_error_role_mismatch:
        return QuestDBErrorCode.RoleMismatch
    elif code == line_sender_error_config_error:
        return QuestDBErrorCode.ConfigError
    elif code == line_sender_error_array_error:
        return QuestDBErrorCode.ArrayError
    elif code == line_sender_error_protocol_version_error:
        return QuestDBErrorCode.ProtocolVersionError
    elif code == line_sender_error_invalid_decimal:
        return QuestDBErrorCode.DecimalError
    elif code == line_sender_error_arrow_unsupported_column_kind:
        return QuestDBErrorCode.ArrowUnsupportedColumnKind
    elif code == line_sender_error_arrow_ingest:
        return QuestDBErrorCode.ArrowIngest
    elif code == line_sender_error_failover_retry:
        return QuestDBErrorCode.FailoverRetry
    elif code == line_sender_error_connect_timeout:
        return QuestDBErrorCode.ConnectTimeout
    elif code == line_sender_error_handshake_error:
        return QuestDBErrorCode.HandshakeError
    elif code == line_sender_error_unsupported_server:
        return QuestDBErrorCode.UnsupportedServer
    elif code == line_sender_error_protocol_error:
        return QuestDBErrorCode.ProtocolError
    elif code == line_sender_error_invalid_bind:
        return QuestDBErrorCode.InvalidBind
    elif code == line_sender_error_server_schema_mismatch:
        return QuestDBErrorCode.ServerSchemaMismatch
    elif code == line_sender_error_server_parse_error:
        return QuestDBErrorCode.ServerParseError
    elif code == line_sender_error_server_internal_error:
        return QuestDBErrorCode.ServerInternalError
    elif code == line_sender_error_server_security_error:
        return QuestDBErrorCode.ServerSecurityError
    elif code == line_sender_error_limit_exceeded:
        return QuestDBErrorCode.LimitExceeded
    elif code == line_sender_error_server_limit_exceeded:
        return QuestDBErrorCode.ServerLimitExceeded
    elif code == line_sender_error_cancelled:
        return QuestDBErrorCode.Cancelled
    elif code == line_sender_error_failover_would_duplicate:
        return QuestDBErrorCode.FailoverWouldDuplicate
    elif code == line_sender_error_schema_drift:
        return QuestDBErrorCode.SchemaDrift
    elif code == line_sender_error_no_schema:
        return QuestDBErrorCode.NoSchema
    elif code == line_sender_error_arrow_export:
        return QuestDBErrorCode.ArrowExport
    elif code == line_sender_error_batch_too_large:
        return QuestDBErrorCode.BatchTooLarge
    elif code == line_sender_error_store_resend_required:
        return QuestDBErrorCode.StoreResendRequired
    else:
        raise ValueError('Internal error converting error code.')


cdef inline object c_qwp_ws_error_view_to_raw(
        line_sender_qwpws_error_view view):
    cdef object message
    if view.message == NULL:
        message = ''
    else:
        message = PyUnicode_FromStringAndSize(
            view.message, <Py_ssize_t>view.message_len)
    return (
        <int>view.category,
        <int>view.applied_policy,
        view.status if view.has_status else None,
        message,
        view.message_sequence if view.has_message_sequence else None,
        view.from_fsn,
        view.to_fsn)


cdef inline object c_err_to_fields(questdb_error* err):
    """Construct a ``SenderError`` from a C error, which will be freed."""
    if err == NULL:
        return (
            QuestDBErrorCode.SocketError,
            'Unknown error: the client library reported failure without '
            'a diagnostic.',
            None,
            False)
    cdef questdb_error_code code = questdb_error_get_code(err)
    cdef size_t c_len = 0
    cdef const char* c_msg = questdb_error_msg(err, &c_len)
    cdef line_sender_qwpws_error_view qwp_ws_view
    cdef bint in_doubt = questdb_error_in_doubt(err)
    cdef object py_msg
    cdef object py_code
    cdef object py_qwp_ws_error = None
    try:
        py_code = c_err_code_to_py(code)
        py_msg = PyUnicode_FromStringAndSize(c_msg, <Py_ssize_t>c_len)
        if line_sender_error_qwpws_get_view(err, &qwp_ws_view):
            py_qwp_ws_error = c_qwp_ws_error_view_to_raw(qwp_ws_view)
        return (py_code, py_msg, py_qwp_ws_error, in_doubt)
    finally:
        questdb_error_free(err)


cdef inline object c_err_to_py(line_sender_error* err):
    """Construct an ``QuestDBError`` from a C error, which will be freed."""
    cdef object tup = c_err_to_fields(err)
    if tup[0] == QuestDBErrorCode.ServerRejection:
        return QuestDBServerRejectionError(
            tup[0], tup[1], tup[2], in_doubt=tup[3])
    return QuestDBError(tup[0], tup[1], tup[2], in_doubt=tup[3])


cdef inline object c_err_to_py_fmt(line_sender_error* err, str fmt):
    """Construct an ``QuestDBError`` from a C error, which will be freed."""
    cdef object tup = c_err_to_fields(err)
    if tup[0] == QuestDBErrorCode.ServerRejection:
        return QuestDBServerRejectionError(
            tup[0], fmt.format(tup[1]), tup[2], in_doubt=tup[3])
    return QuestDBError(
        tup[0], fmt.format(tup[1]), tup[2], in_doubt=tup[3])


cdef inline void_int reserve_buffer(
        line_sender_buffer* buffer,
        size_t additional) except -1:
    cdef line_sender_error* err = NULL
    if not line_sender_buffer_reserve(buffer, additional, &err):
        raise c_err_to_py(err)


cdef object _utf8_decode_error(
        PyObject* string, uint32_t bad_codepoint):
    cdef str s = <str><object>string
    return QuestDBError(
        QuestDBErrorCode.InvalidUtf8,
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


cdef object _UTC_EPOCH = datetime.datetime(
    1970, 1, 1, tzinfo=datetime.timezone.utc)


_NAIVE_DATETIME_WARNED = False


cdef object _as_utc_aware(cp_datetime dt):
    global _NAIVE_DATETIME_WARNED
    if dt.tzinfo is not None:
        return dt
    if not _NAIVE_DATETIME_WARNED:
        _NAIVE_DATETIME_WARNED = True
        warnings.warn(
            'Naive datetime interpreted as UTC (questdb 4.x used local '
            'time). If you meant "now", use TimestampNanos.now() or '
            'datetime.now(timezone.utc); pass timezone-aware datetimes '
            'to silence this warning.',
            UserWarning,
            stacklevel=3)
    return dt.replace(tzinfo=datetime.timezone.utc)


cdef int64_t datetime_to_micros(cp_datetime dt):
    """
    Convert a :class:`datetime.datetime` to microseconds since the epoch.

    Naive datetimes are interpreted as UTC.
    """
    cdef object aware = _as_utc_aware(dt)
    cdef object delta = aware - _UTC_EPOCH
    return (
        <int64_t>delta.days * <int64_t>86_400_000_000 +
        <int64_t>delta.seconds * <int64_t>1_000_000 +
        <int64_t>delta.microseconds)


cdef int64_t datetime_to_nanos(cp_datetime dt):
    """
    Convert a `datetime.datetime` to nanoseconds since the epoch.
    """
    cdef int64_t micros = datetime_to_micros(dt)
    if (micros >= <int64_t>9_223_372_036_000_000 or
            micros < <int64_t>-9_223_372_036_000_000):
        raise ValueError(
            'datetime is out of range for a nanosecond timestamp: '
            'must be between 1677-09-21T00:12:44Z and '
            '2262-04-11T23:47:16Z.')
    return micros * 1000


class ServerTimestampType:
    """
    A placeholder value to indicate that the data should be inserted
    using a server-generated-timestamp.

    Don't instantiate this class directly, use the singleton
    :data:`ServerTimestamp` instead.

    This feature is mostly provided for legacy compatibility.
    We recommend always specifying an explicit timestamp.

    Using ``ServerTimestamp`` will prevent QuestDB's deduplication
    feature from working as it would generate unique rows on resubmission.
    """
    pass


#: Singleton instance used to request server-side timestamping.
#: See :class:`ServerTimestampType` for more details.
ServerTimestamp = ServerTimestampType()


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
    the timezone to use. A ``datetime`` object without an associated timezone
    is interpreted as UTC (a ``UserWarning`` is emitted once per process).
    Note that ``datetime.datetime.now()`` is your local wall clock: use
    ``datetime.datetime.now(datetime.timezone.utc)`` or ``now()`` on this
    class for the current instant.
    """
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must be a non-negative integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime.datetime):
        """
        Construct a ``TimestampMicros`` from a :class:`datetime.datetime` object.
        """
        if not isinstance(dt, cp_datetime):
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
        return f'TimestampMicros({self._value})'


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
    the timezone to use. A ``datetime`` object without an associated timezone
    is interpreted as UTC (a ``UserWarning`` is emitted once per process).
    Note that ``datetime.datetime.now()`` is your local wall clock: use
    ``datetime.datetime.now(datetime.timezone.utc)`` or ``now()`` on this
    class for the current instant.
    """
    cdef int64_t _value

    def __cinit__(self, value: int):
        if value < 0:
            raise ValueError('value must be a non-negative integer.')
        self._value = value

    @classmethod
    def from_datetime(cls, dt: datetime.datetime):
        """
        Construct a ``TimestampNanos`` from a ``datetime.datetime`` object.
        """
        if not isinstance(dt, cp_datetime):
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


cdef class QuestDB
cdef class Sender
cdef class PooledSender
cdef class Buffer


cdef void_int may_flush_on_row_complete(Buffer buffer, Sender sender) except -1:
    if should_auto_flush(
            &sender._auto_flush_mode,
            buffer._impl,
            sender._last_flush_ms[0]):
        sender.flush(buffer)


cdef bint _is_tcp_protocol(line_sender_protocol protocol):
    return (
        (protocol == line_sender_protocol_tcp) or
        (protocol == line_sender_protocol_tcps))


cdef bint _is_http_protocol(line_sender_protocol protocol):
    return (
        (protocol == line_sender_protocol_http) or
        (protocol == line_sender_protocol_https))


cdef bint _is_qwp_udp_protocol(line_sender_protocol protocol):
    return protocol == line_sender_protocol_udp


cdef bint _is_qwp_ws_protocol(line_sender_protocol protocol):
    return (
        (protocol == line_sender_protocol_ws) or
        (protocol == line_sender_protocol_wss))


cdef class SenderTransaction:
    """
    A transaction for a specific table.

    Transactions are only supported with ILP/HTTP.

    The sender API can only operate on one transaction at a time.

    To create a transaction:

    .. code-block:: python

        with sender.transaction('table_name') as txn:
            txn.row(..)
            txn.dataframe(..)
    """
    cdef Sender _sender
    cdef str _table_name
    cdef bint _complete

    def __cinit__(self, Sender sender, str table_name):
        if not _is_http_protocol(sender._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Transactions are only supported for ILP/HTTP.')
        self._sender = sender
        self._table_name = table_name
        self._complete = False

    def __enter__(self):
        if self._sender._in_txn:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Already inside a transaction, can\'t start another.')
        if self._sender._buffer is not None and len(self._sender._buffer):
            if self._sender._auto_flush_mode.enabled:
                self._sender.flush()
            else:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'Sender buffer must be clear when starting a ' +
                    'transaction. You must call `.flush()` before this call.')
        self._sender._in_txn = True
        return self

    def __exit__(self, exc_type, _exc_value, _traceback):
        if exc_type is not None:
            if not self._complete:
                self.rollback()
            return False
        else:
            if not self._complete:
                self.commit()
            return True

    def row(
            self,
            *,
            symbols: Optional[Dict[str, Optional[str]]]=None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros, TimestampNanos, datetime.datetime, numpy.ndarray, Decimal]]
                ]=None,
            at: Union[ServerTimestampType, TimestampNanos, datetime.datetime]):
        """
        Write a row for the table in the transaction.

        The table name is taken from the transaction.

        **Note**: Support for NumPy arrays (``numpy.array``) requires QuestDB server version 9.0.0 or higher.
        """
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or ServerTimestamp"
            )

        if self._sender._buffer is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                "row() can\'t be called: Sender is closed."
            )

        self._sender._buffer._row(
            False,  # allow_auto_flush
            self._table_name,
            symbols=symbols,
            columns=columns,
            at=at)
        return self

    def dataframe(
            self,
            df,  # : pd.DataFrame
            *,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[ServerTimestampType, int, str, TimestampNanos, datetime.datetime]):
        """
        Write a dataframe for the table in the transaction.

        The table name is taken from the transaction.
        """
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or ServerTimestamp"
            )
        if self._sender._buffer is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                "dataframe() can\'t be called: Sender is closed."
            )
        _dataframe(
            auto_flush_blank(),
            self._sender._buffer._impl,
            self._sender._buffer._b,
            df,
            self._table_name,
            None, # table_name_col,
            symbols,
            at)
        return self

    def commit(self):
        """
        Commit the transaction.
        
        A commit is also automatic at the end of a successful `with` block.

        This will flush the buffer.
        """
        if self._complete:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Transaction already completed, can\'t commit')
        self._sender._in_txn = False
        self._complete = True
        if len(self._sender._buffer):
            self._sender.flush(transactional=True)

    def rollback(self):
        """
        Roll back the transaction.

        A rollback is also automatic at the end of a failed `with` block.

        This will clear the buffer.
        """
        if self._complete:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Transaction already completed, can\'t rollback.')
        if self._sender._buffer is not None:
            self._sender._buffer.clear()
        self._sender._in_txn = False
        self._complete = True

cdef class Buffer:
    """
    Internal row-serialization buffer, managed by :class:`Sender`.

    Kept importable as ``questdb.ingress.Buffer`` for legacy ILP/HTTP and
    ILP/TCP code that constructs buffers explicitly and flushes them via
    ``sender.flush(buffer)``.
    """
    cdef line_sender_buffer* _impl
    cdef qdb_pystr_buf* _b
    cdef size_t _init_buf_size
    cdef size_t _max_name_len
    cdef bint _qwp
    cdef object _row_complete_sender

    def __cinit__(self):
        self._impl = NULL
        self._b = NULL
        self._init_buf_size = 0
        self._max_name_len = 0
        self._qwp = False
        self._row_complete_sender = None

    def __init__(
            self,
            protocol_version: int,
            init_buf_size: int=65536,
            max_name_len: int=127):
        if self._impl != NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Buffer is already initialized.')
        if protocol_version not in range(1, 4):
            raise QuestDBError(
                QuestDBErrorCode.ProtocolVersionError,
                'Invalid protocol version. Supported versions are 1-3.')
        self._init_ilp_impl(protocol_version, init_buf_size, max_name_len)

    @staticmethod
    def _new_qwp(
            init_buf_size: int=65536,
            max_name_len: int=127):
        cdef Buffer buf = Buffer.__new__(Buffer)
        buf._init_qwp_impl(init_buf_size, max_name_len)
        return buf

    cdef inline _init_ilp_impl(self, line_sender_protocol_version version, size_t init_buf_size, size_t max_name_len):
        self._impl = line_sender_buffer_with_max_name_len(version, max_name_len)
        self._b = qdb_pystr_buf_new()
        reserve_buffer(self._impl, init_buf_size)
        self._init_buf_size = init_buf_size
        self._max_name_len = max_name_len
        self._qwp = False
        self._row_complete_sender = None

    cdef inline _init_qwp_impl(self, size_t init_buf_size, size_t max_name_len):
        self._impl = line_sender_buffer_new_qwp_with_max_name_len(max_name_len)
        self._b = qdb_pystr_buf_new()
        reserve_buffer(self._impl, init_buf_size)
        self._init_buf_size = init_buf_size
        self._max_name_len = max_name_len
        self._qwp = True
        self._row_complete_sender = None


    def __dealloc__(self):
        self._row_complete_sender = None
        qdb_pystr_buf_free(self._b)
        line_sender_buffer_free(self._impl)

    cdef inline void_int _check_impl(self) except -1:
        if self._impl == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Buffer is not initialized.')

    @property
    def init_buf_size(self) -> int:
        """
        The initial capacity of the buffer when first created.

        This may grow over time, see ``capacity()``.
        """
        return self._init_buf_size

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
        self._check_impl()
        reserve_buffer(self._impl, additional)

    def capacity(self) -> int:
        """The current buffer capacity."""
        self._check_impl()
        return line_sender_buffer_capacity(self._impl)

    def clear(self):
        """
        Reset the buffer.

        Note that flushing a buffer will (unless otherwise specified)
        also automatically clear it.

        This method is designed to be called only in conjunction with
        ``sender.flush(buffer, clear=False)``.
        """
        self._check_impl()
        line_sender_buffer_clear(self._impl)
        qdb_pystr_buf_clear(self._b)

    def __len__(self) -> int:
        """
        The current number of bytes currently in the buffer.

        Equivalent (but cheaper) to ``len(bytes(buffer))``.
        """
        self._check_impl()
        return line_sender_buffer_size(self._impl)

    def __bytes__(self) -> bytes:
        """Return the constructed buffer as bytes. Use for debugging."""
        return self._to_bytes()

    cdef inline object _to_bytes(self):
        self._check_impl()
        cdef line_sender_buffer_view view = line_sender_buffer_peek(self._impl)
        return PyBytes_FromStringAndSize(<const char *> view.buf, <Py_ssize_t> view.len)

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

    cdef inline void_int _column_decimal(
            self, line_sender_column_name c_name, object value) except -1:
        return serialize_decimal_py_obj(self._impl, c_name, <PyObject*>value)

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

    cdef inline void_int _column_ts_micros(
            self, line_sender_column_name c_name, TimestampMicros ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_ts_micros(self._impl, c_name, ts._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_ts_nanos(
            self, line_sender_column_name c_name, TimestampNanos ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_ts_nanos(self._impl, c_name, ts._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_numpy(
            self, line_sender_column_name c_name, cnp.ndarray arr) except -1:
        if cnp.PyArray_TYPE(arr) != cnp.NPY_FLOAT64:
            raise QuestDBError(
                QuestDBErrorCode.ArrayError,
                f'Only float64 numpy arrays are supported, got dtype: {arr.dtype}')
        cdef:
            size_t rank = cnp.PyArray_NDIM(arr)
            const double * data_ptr = <const double*> cnp.PyArray_DATA(arr)
            line_sender_error * err = NULL

        if cnp.PyArray_FLAGS(arr) & cnp.NPY_ARRAY_C_CONTIGUOUS != 0:
            if not line_sender_buffer_column_f64_arr_c_major(
                    self._impl,
                    c_name,
                    rank,
                    <const size_t*> cnp.PyArray_DIMS(arr),
                    data_ptr,
                    cnp.PyArray_SIZE(arr),
                    &err):
                raise c_err_to_py(err)
        else:
            if not line_sender_buffer_column_f64_arr_byte_strides(
                    self._impl,
                    c_name,
                    rank,
                    <const size_t*> cnp.PyArray_DIMS(arr),
                    <const ssize_t*> cnp.PyArray_STRIDES(arr), # N.B.: Strides expressed as byte jumps
                    data_ptr,
                    cnp.PyArray_SIZE(arr),
                    &err):
                raise c_err_to_py(err)

    cdef inline void_int _column_dt(
            self, line_sender_column_name c_name, cp_datetime dt) except -1:
        cdef line_sender_error* err = NULL
        # We limit ourselves to micros, since this is the maxium precision
        # exposed by the datetime library in Python.
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
            self._column_ts_micros(c_name, value)
        elif isinstance(value, TimestampNanos):
            self._column_ts_nanos(c_name, value)
        elif PyArray_CheckExact(<PyObject *> value):
            self._column_numpy(c_name, value)
        elif isinstance(value, cp_datetime):
            self._column_dt(c_name, value)
        elif isinstance(value, Decimal):
            self._column_decimal(c_name, value)
        else:
            valid = ', '.join((
                'bool',
                'int',
                'float',
                'str',
                'TimestampMicros',
                'TimestampNanos',
                'datetime.datetime',
                'numpy.ndarray',
                'decimal.Decimal'))
            raise TypeError(
                f'Unsupported type: {_fqn(type(value))}. Must be one of: {valid}')

    cdef inline void_int _may_trigger_row_complete(self) except -1:
        cdef line_sender_error* err = NULL
        cdef PyObject* sender = NULL
        if self._row_complete_sender != None:
            sender = PyWeakref_GetObject(self._row_complete_sender)
            if sender != NULL and <object>sender is not None:
                may_flush_on_row_complete(self, <Sender><object>sender)

    cdef inline void_int _at_ts_us(self, TimestampMicros ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_micros(self._impl, ts._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at_ts_ns(self, TimestampNanos ts) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_nanos(self._impl, ts._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at_dt(self, cp_datetime dt) except -1:
        cdef int64_t value = datetime_to_micros(dt)
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_micros(self._impl, value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at_now(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_at_now(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline void_int _at(self, object ts) except -1:
        if ts is None:
            self._at_now()
        elif isinstance(ts, TimestampMicros):
            self._at_ts_us(ts)
        elif isinstance(ts, TimestampNanos):
            self._at_ts_ns(ts)
        elif isinstance(ts, cp_datetime):
            self._at_dt(ts)
        else:
            raise TypeError(
                f'Unsupported type: {_fqn(type(ts))}. Must be one of: ' +
                'TimestampNanos, datetime, None')

    cdef void_int _row(
            self,
            bint allow_auto_flush,
            str table_name,
            dict symbols=None,
            dict columns=None,
            object at=None) except -1:
        """
        Add a row to the buffer.
        """
        cdef bint wrote_fields = False
        self._check_impl()
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
                self._at(at if not isinstance(at, ServerTimestampType) else None)
                self._clear_marker()
            else:
                self._rewind_to_marker()
        except:
            self._rewind_to_marker()
            raise
        if wrote_fields and allow_auto_flush:
            self._may_trigger_row_complete()

    def row(
            self,
            table_name: str,
            *,
            symbols: Optional[Dict[str, Optional[str]]]=None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros, TimestampNanos, datetime.datetime, numpy.ndarray, Decimal]]
                ]=None,
            at: Union[ServerTimestampType, TimestampNanos, datetime.datetime]):
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
                    'col7': numpy.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]),
                    'col8': None},
                at=TimestampNanos(123456789))

            # Only symbols specified. Designated timestamp assigned by the db.
            buffer.row(
                'table_name',
                symbols={'sym1': 'abc', 'sym2': 'def'}, at=Server.Timestamp)

            # Float columns and timestamp specified as `datetime.datetime`.
            # Pay special attention to the timezone, which if unspecified is
            # interpreted as UTC.
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
        `QuestDB documentation <https://questdb.com/docs/concept/symbol/>`_ to
        understand the difference between the ``SYMBOL`` and ``STRING`` types
        (TL;DR: symbols are interned strings).

        Column values can be specified with Python types directly and map as so:

        .. list-table::
            :header-rows: 1

            * - Python type
              - Serialized as ILP type
            * - ``bool``
              - `BOOLEAN <https://questdb.com/docs/reference/api/ilp/columnset-types#boolean>`_
            * - ``decimal``
              - `DECIMAL <https://questdb.com/docs/reference/api/ilp/columnset-types#decimal>`_
            * - ``int``
              - `INTEGER <https://questdb.com/docs/reference/api/ilp/columnset-types#integer>`_
            * - ``float``
              - `FLOAT <https://questdb.com/docs/reference/api/ilp/columnset-types#float>`_
            * - ``str``
              - `STRING <https://questdb.com/docs/reference/api/ilp/columnset-types#string>`_
            * - ``numpy.ndarray``
              - `ARRAY <https://questdb.com/docs/reference/api/ilp/columnset-types#array>`_
            * - ``datetime.datetime`` and ``TimestampMicros``
              - `TIMESTAMP <https://questdb.com/docs/reference/api/ilp/columnset-types#timestamp>`_
            * - ``None``
              - *Column is skipped and not serialized.*

        **Note**: Support for NumPy arrays (``numpy.array``) requires QuestDB server version 9.0.0 or higher.

        If the destination table was already created, then the columns types
        will be cast to the types of the existing columns whenever possible
        (Refer to the QuestDB documentation pages linked above).

        Adding a row can trigger auto-flushing behaviour.

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
        :param at: The timestamp of the row. This is required!
            If ``ServerTimestamp``, timestamp is assigned by QuestDB.
            If ``datetime``, the timestamp is converted to nanoseconds.
            A nanosecond unix epoch timestamp can be passed
            explicitly as a ``TimestampNanos`` object.
        """
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or ServerTimestamp"
            )
        self._row(
            True,  # allow_auto_flush
            table_name,
            symbols,
            columns,
            at)
        return self

    def dataframe(
            self,
            df,  # : pd.DataFrame
            *,
            table_name: Optional[str] = None,
            table_name_col: Union[None, int, str] = None,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[ServerTimestampType, int, str, TimestampNanos, datetime.datetime]):
        """
        Add a pandas DataFrame to the buffer.

        Also see the :func:`Sender.dataframe` method if you're
        not using the buffer explicitly. It supports the same parameters
        and also supports auto-flushing.

        Requires ``pandas`` and ``numpy``. ``pyarrow`` is only needed
        when the frame contains ``pd.ArrowDtype`` / ``pd.Categorical`` /
        ``string`` dtype columns — purely NumPy / object dtypes work
        without it.

        Adding a dataframe can trigger auto-flushing behaviour,
        even between rows of the same dataframe. To avoid this, you can
        use HTTP and transactions (see :func:`Sender.transaction`).

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

        :type symbols: str or bool or list[str] or list[int]

        :param at: The designated timestamp of the rows.
        
            You can specify a single value for all rows or column name or index.
            If ``ServerTimestamp``, timestamp is assigned by the server for all rows.
            To pass in a timestamp explicitly as an integer use the
            ``TimestampNanos`` wrapper type. To get the current timestamp,
            use ``TimestampNanos.now()``.
            When passing a ``datetime.datetime`` object, the timestamp is
            converted to nanoseconds.
            A naive ``datetime`` object is interpreted as UTC — never
            your machine's local timezone — and a ``UserWarning`` is
            emitted once per process
            (call ``datetime.datetime.now(tz=datetime.timezone.utc)``
            for the current timestamp to
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
            import questdb as qi

            buf = qi.ingress.Buffer(protocol_version=2)
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

        .. seealso:: https://questdb.com/docs/reference/api/ilp/columnset-types/

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
            * - ``'object'`` (``Decimal`` objects)
              - Y (``NaN``)
              - ``DECIMAL``

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
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or ServerTimestamp"
            )
        self._check_impl()
        _dataframe(
            auto_flush_blank(),
            self._impl,
            self._b,
            df,
            table_name,
            table_name_col,
            symbols,
            at)
        return self


_FLUSH_FMT = ('{} - See https://py-questdb-client.readthedocs.io/en/'
    'v' + VERSION +
    '/troubleshooting.html#inspecting-and-debugging-errors#flush-failed')


cdef uint64_t _timedelta_to_millis(cp_timedelta timedelta):
    """
    Convert a timedelta to milliseconds.
    """
    cdef int64_t millis = (
        <int64_t>timedelta.days * 86_400_000 +
        <int64_t>timedelta.seconds * 1000 +
        timedelta.microseconds // 1000)
    if millis < 0:
        raise ValueError(
            f'Negative timedelta not allowed: {timedelta!r}.')
    return millis


cdef bint _is_int_not_bool(object value):
    return isinstance(value, int) and not isinstance(value, bool)


cdef int64_t auto_flush_rows_default(line_sender_protocol protocol):
    if _is_http_protocol(protocol):
        return 75000
    else:
        return 600


cdef void_int _parse_auto_flush(
    line_sender_protocol protocol,
    object auto_flush,
    object auto_flush_rows,
    object auto_flush_bytes,
    object auto_flush_interval,
    auto_flush_mode_t* c_auto_flush,
    size_t max_datagram_size
) except -1:
    # Set defaults.
    if auto_flush_rows is None:
        auto_flush_rows = auto_flush_rows_default(protocol)

    if auto_flush_bytes is None:
        if _is_qwp_udp_protocol(protocol):
            auto_flush_bytes = max_datagram_size if max_datagram_size else 1400
        else:
            auto_flush_bytes = False

    if auto_flush_interval is None:
        auto_flush_interval = 1000

    if isinstance(auto_flush, str):
        if auto_flush == 'off':
            auto_flush = False
        elif auto_flush == 'on':
            auto_flush = True
        else:
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                '"auto_flush" must be None, bool, "on" or "off", ' +
                f'not {auto_flush!r}')

    # Normalise auto_flush parameters to ints or False.
    if isinstance(auto_flush_rows, str):
        if auto_flush_rows == 'on':
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                '"auto_flush_rows" cannot be "on"')
        elif auto_flush_rows == 'off':
            auto_flush_rows = False
        else:
            auto_flush_rows = int(auto_flush_rows)
    elif auto_flush_rows is False or isinstance(auto_flush_rows, int):
        pass
    else:
        raise TypeError(
            '"auto_flush_rows" must be an int, False or "off", ' +
            f'not {auto_flush_rows!r}')

    if isinstance(auto_flush_bytes, str):
        if auto_flush_bytes == 'on':
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                '"auto_flush_bytes" cannot be "on"')
        elif auto_flush_bytes == 'off':
            auto_flush_bytes = False
        else:
            auto_flush_bytes = int(auto_flush_bytes)
    elif auto_flush_bytes is False or isinstance(auto_flush_bytes, int):
        pass
    else:
        raise TypeError(
            '"auto_flush_bytes" must be an int, False or "off", ' +
            f'not {auto_flush_bytes!r}')

    if isinstance(auto_flush_interval, str):
        if auto_flush_interval == 'on':
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                '"auto_flush_interval" cannot be "on"')
        elif auto_flush_interval == 'off':
            auto_flush_interval = False
        else:
            auto_flush_interval = int(auto_flush_interval)
    elif auto_flush_interval is False or isinstance(auto_flush_interval, int):
        pass
    elif isinstance(auto_flush_interval, cp_timedelta):
        auto_flush_interval = _timedelta_to_millis(auto_flush_interval)
    else:
        raise TypeError(
            '"auto_flush_interval" must be an int, timedelta, False or "off", ' +
            f'not {auto_flush_interval!r}')

    # Coerce auto_flush to bool if None.
    if auto_flush is None:
        auto_flush = (
            (auto_flush_rows is not False) or
            (auto_flush_bytes is not False) or
            (auto_flush_interval is not False))
    elif not isinstance(auto_flush, bool):
        raise ValueError(
            '"auto_flush" must be None, bool, "on" or "off", ' +
            f'not {auto_flush!r}')

    # Validate auto_flush parameters.
    if auto_flush and \
            (auto_flush_rows is False) and \
            (auto_flush_bytes is False) and \
            (auto_flush_interval is False):
        raise ValueError(
            '"auto_flush" is enabled but no other auto-flush '
            'parameters are enabled. Please set at least one of '
            '"auto_flush_rows", "auto_flush_bytes" or '
            '"auto_flush_interval".')

    if auto_flush_rows is not False and auto_flush_rows < 1:
        raise ValueError(
            '"auto_flush_rows" must be >= 1, '
            f'not {auto_flush_rows}')

    if auto_flush_bytes is not False and auto_flush_bytes < 1:
        raise ValueError(
            '"auto_flush_bytes" must be >= 1, '
            f'not {auto_flush_bytes}')

    if auto_flush_interval is not False and auto_flush_interval < 1:
        raise ValueError(
            '"auto_flush_interval" must be >= 1, '
            f'not {auto_flush_interval}')

    # Parse individual auto_flush parameters to C struct.
    c_auto_flush.enabled = auto_flush

    if auto_flush_rows is False:
        c_auto_flush.row_count = -1
    else:
        c_auto_flush.row_count = auto_flush_rows

    if auto_flush_bytes is False:
        c_auto_flush.byte_count = -1
    else:
        c_auto_flush.byte_count = auto_flush_bytes

    if auto_flush_interval is False:
        c_auto_flush.interval = -1
    else:
        c_auto_flush.interval = auto_flush_interval


class TaggedEnum(Enum):
    """
    Base class for tagged enums.
    """

    @property
    def tag(self):
        """
        Short name.
        """
        return self.value[0]

    @property
    def c_value(self):
        return self.value[1]

    @classmethod
    def parse(cls, tag):
        """
        Parse from the tag name.
        """
        if tag is None:
            return None
        elif isinstance(tag, cls):
            return tag
        elif isinstance(tag, str):
            for entry in cls:
                if entry.tag == tag:
                    return entry
            raise ValueError(f'Invalid value for {cls.__name__}: {tag!r}')
        else:
            raise ValueError(f'Invalid value for {cls.__name__}: {tag!r}')


class Protocol(TaggedEnum):
    """
    Protocol to use for sending data to QuestDB.

    See :ref:`sender_which_protocol` for more information.
    """
    Tcp = ('tcp', 0)
    Tcps = ('tcps', 1)
    Http = ('http', 2)
    Https = ('https', 3)
    Udp = ('udp', 4)
    Ws = ('ws', 5)
    Wss = ('wss', 6)

    @property
    def tls_enabled(self):
        return self in (Protocol.Tcps, Protocol.Https, Protocol.Wss)


class QwpWsProgress(TaggedEnum):
    """
    Progress mode for QWP/WebSocket senders.
    """
    Background = ('background', LINE_SENDER_QWPWS_PROGRESS_BACKGROUND)
    Manual = ('manual', LINE_SENDER_QWPWS_PROGRESS_MANUAL)


class QwpWsErrorCategory(TaggedEnum):
    """
    Category of a structured QWP/WebSocket diagnostic.
    """
    SchemaMismatch = (
        'schema_mismatch',
        LINE_SENDER_QWPWS_ERROR_SCHEMA_MISMATCH)
    ParseError = ('parse_error', LINE_SENDER_QWPWS_ERROR_PARSE_ERROR)
    InternalError = ('internal_error', LINE_SENDER_QWPWS_ERROR_INTERNAL_ERROR)
    SecurityError = ('security_error', LINE_SENDER_QWPWS_ERROR_SECURITY_ERROR)
    WriteError = ('write_error', LINE_SENDER_QWPWS_ERROR_WRITE_ERROR)
    NotWritable = ('not_writable', LINE_SENDER_QWPWS_ERROR_NOT_WRITABLE)
    ProtocolViolation = (
        'protocol_violation',
        LINE_SENDER_QWPWS_ERROR_PROTOCOL_VIOLATION)
    Unknown = ('unknown', LINE_SENDER_QWPWS_ERROR_UNKNOWN)


class QwpWsErrorPolicy(TaggedEnum):
    """
    Applied policy for a structured QWP/WebSocket diagnostic.
    """
    Retriable = ('retriable', LINE_SENDER_QWPWS_ERROR_RETRIABLE)
    RetriableOther = (
        'retriable_other',
        LINE_SENDER_QWPWS_ERROR_RETRIABLE_OTHER)
    Terminal = ('terminal', LINE_SENDER_QWPWS_ERROR_TERMINAL)


@dataclass(frozen=True)
class QwpWsError:
    """Structured QWP/WebSocket server diagnostic."""
    category: QwpWsErrorCategory
    applied_policy: QwpWsErrorPolicy
    status: Optional[int]
    message: str
    message_sequence: Optional[int]
    from_fsn: int
    to_fsn: int


class ConnectionEventKind(TaggedEnum):
    """
    Connection-state transitions observed by the ingress connection pool.
    """
    #: First successful connect of the pool's lifetime.
    Connected = ('connected', 0)
    #: An active wire connection died.
    Disconnected = ('disconnected', 1)
    #: Reconnect succeeded against the same endpoint after a failure.
    Reconnected = ('reconnected', 2)
    #: Reconnect succeeded against a different endpoint.
    FailedOver = ('failed_over', 3)
    #: One endpoint connect/upgrade attempt failed; the walk moves on.
    EndpointAttemptFailed = ('endpoint_attempt_failed', 4)
    #: Every configured endpoint was attempted and none accepted.
    AllEndpointsUnreachable = ('all_endpoints_unreachable', 5)
    #: Terminal: the server rejected credentials.
    AuthFailed = ('auth_failed', 6)


@dataclass(frozen=True)
class ConnectionEvent:
    """
    One connection-state transition, delivered to the
    ``connection_listener`` registered via :meth:`QuestDB.from_conf`.

    Listeners run on a dedicated dispatcher thread — never on an I/O or
    caller thread — fed by a bounded inbox with a drop-oldest overflow
    policy, so a slow listener cannot stall ingest or reconnects.
    Exceptions raised by the listener are logged and swallowed.
    """
    kind: ConnectionEventKind
    host: Optional[str]
    port: Optional[str]
    #: For :attr:`ConnectionEventKind.FailedOver`, the previously-active
    #: endpoint.
    previous_host: Optional[str]
    previous_port: Optional[str]
    #: Monotonic connect-attempt counter at the time the event fired.
    attempt_number: Optional[int]
    cause_code: Optional[QuestDBErrorCode]
    cause_msg: Optional[str]
    #: Wall-clock time of the event, milliseconds since the Unix epoch.
    timestamp_millis: int


cdef inline object _conn_event_str(const char* buf, size_t buf_len):
    if buf == NULL:
        return None
    return PyUnicode_FromStringAndSize(buf, <Py_ssize_t>buf_len)


cdef void _connection_event_trampoline(
        void* user_data,
        const questdb_connection_event* event) noexcept nogil:
    if qdb_py_is_finalizing():
        return
    with gil:
        try:
            listener = <object>user_data
            kind = ConnectionEventKind.Connected
            for entry in ConnectionEventKind:
                if entry.c_value == <int>event.kind:
                    kind = entry
                    break
            listener(ConnectionEvent(
                kind=kind,
                host=_conn_event_str(event.host, event.host_len),
                port=_conn_event_str(event.port, event.port_len),
                previous_host=_conn_event_str(
                    event.previous_host, event.previous_host_len),
                previous_port=_conn_event_str(
                    event.previous_port, event.previous_port_len),
                attempt_number=event.attempt_number
                    if event.has_attempt else None,
                cause_code=c_err_code_to_py(event.cause_code)
                    if event.has_cause else None,
                cause_msg=_conn_event_str(event.cause_msg, event.cause_msg_len)
                    if event.has_cause else None,
                timestamp_millis=event.timestamp_millis))
        except BaseException:
            logging.getLogger("questdb").exception(
                "connection event listener failed")


class ServerRole(TaggedEnum):
    """
    Cluster role advertised by the server's ``SERVER_INFO`` handshake.
    """
    Standalone = ('standalone', 0)
    Primary = ('primary', 1)
    Replica = ('replica', 2)
    PrimaryCatchup = ('primary_catchup', 3)
    #: Forward-compat: a role byte this client doesn't recognise. The raw
    #: byte is available via :attr:`ServerInfo.role_byte`.
    Other = ('other', 0xFF)


@dataclass(frozen=True)
class ServerInfo:
    """
    Snapshot of the server's ``SERVER_INFO`` handshake, as advertised on
    the connection :meth:`QuestDB.server_info` sampled. Fields mirror the
    Rust reader's ``ServerInfo``.
    """
    #: Cluster role; :attr:`ServerRole.Other` for unrecognised role bytes.
    role: ServerRole
    #: Raw wire role byte; disambiguates ``role == ServerRole.Other``.
    role_byte: int
    #: Monotonic generation counter; increases on failover/role
    #: transitions, useful for fencing replayed batches.
    epoch: int
    #: Bitset of QWP capability flags negotiated with the server.
    capabilities: int
    #: Server wall-clock at handshake, nanoseconds since the Unix epoch
    #: (UTC). Useful for clock-skew detection.
    server_wall_ns: int
    cluster_id: str
    node_id: str
    #: Zone identifier, present iff the server advertised ``CAP_ZONE`` in
    #: ``capabilities``; ``None`` otherwise. Compared case-insensitively
    #: against the client's ``zone=`` connect-string knob during failover.
    zone_id: Optional[str]


def _qwp_ws_error_from_raw(raw):
    if raw is None or isinstance(raw, QwpWsError):
        return raw

    (
        category,
        applied_policy,
        status,
        message,
        message_sequence,
        from_fsn,
        to_fsn,
    ) = raw

    py_category = QwpWsErrorCategory.Unknown
    for entry in QwpWsErrorCategory:
        if entry.c_value == category:
            py_category = entry
            break

    py_policy = QwpWsErrorPolicy.Terminal
    for entry in QwpWsErrorPolicy:
        if entry.c_value == applied_policy:
            py_policy = entry
            break

    return QwpWsError(
        py_category,
        py_policy,
        status,
        message,
        message_sequence,
        from_fsn,
        to_fsn)


def _default_qwp_ws_error_handler(error):
    level = (
        logging.ERROR
        if error.applied_policy is QwpWsErrorPolicy.Terminal
        else logging.WARNING)
    logging.getLogger("questdb").log(
        level,
        "QWP/WebSocket server rejection: "
        "category=%s policy=%s status=%s fsn=[%s,%s] seq=%s message=%s",
        error.category.tag,
        error.applied_policy.tag,
        error.status,
        error.from_fsn,
        error.to_fsn,
        error.message_sequence,
        error.message)


cdef void _qwp_ws_error_trampoline(
        void* user_data,
        const line_sender_qwpws_error_view* view) noexcept nogil:
    if qdb_py_is_finalizing():
        return
    with gil:
        try:
            handler = <object>user_data
            handler(_qwp_ws_error_from_raw(
                c_qwp_ws_error_view_to_raw(view[0])))
        except BaseException:
            logging.getLogger("questdb").exception(
                "QWP/WebSocket error handler failed")


class TlsCa(TaggedEnum):
    """
    Verification mechanism for the server's certificate.

    Here ``webpki`` refers to the
    `WebPKI library <https://github.com/rustls/webpki-roots>`_ and
    ``os`` refers to the operating system's certificate store.

    See :ref:`sender_conf_tls` for more information.
    """
    WebpkiRoots = ('webpki_roots', line_sender_ca_webpki_roots)
    OsRoots = ('os_roots', line_sender_ca_os_roots)
    WebpkiAndOsRoots = ('webpki_and_os_roots', line_sender_ca_webpki_and_os_roots)
    PemFile = ('pem_file', line_sender_ca_pem_file)


cdef object c_parse_conf_err_to_py(questdb_conf_str_parse_err* err):
    cdef str msg = PyUnicode_FromStringAndSize(
        err.msg, <Py_ssize_t>err.msg_len)
    cdef object py_err = QuestDBError(QuestDBErrorCode.ConfigError, msg)
    questdb_conf_str_parse_err_free(err)
    return py_err


cdef object parse_conf_str(
        qdb_pystr_buf* b,
        str conf_str):
    """
    Parse a config string to a tuple of (Protocol, dict[str, str]).
    """
    cdef size_t c_len1
    cdef const char* c_buf1
    cdef size_t c_len2
    cdef const char* c_buf2
    cdef str service
    cdef questdb_conf_str_iter* c_iter
    cdef str key
    cdef str value
    cdef dict params = {}
    cdef line_sender_utf8 c_conf_str_utf8
    cdef questdb_conf_str_parse_err* err
    cdef questdb_conf_str* c_conf_str
    str_to_utf8(b, <PyObject*>conf_str, &c_conf_str_utf8)
    c_conf_str = questdb_conf_str_parse(
        c_conf_str_utf8.buf,
        c_conf_str_utf8.len,
        &err)
    if c_conf_str == NULL:
        raise c_parse_conf_err_to_py(err)

    c_iter = NULL
    try:
        c_buf1 = questdb_conf_str_service(c_conf_str, &c_len1)
        service = PyUnicode_FromStringAndSize(c_buf1, <Py_ssize_t>c_len1)

        c_iter = questdb_conf_str_iter_pairs(c_conf_str)
        while questdb_conf_str_iter_next(c_iter, &c_buf1, &c_len1, &c_buf2, &c_len2):
            key = PyUnicode_FromStringAndSize(c_buf1, <Py_ssize_t>c_len1)
            value = PyUnicode_FromStringAndSize(c_buf2, <Py_ssize_t>c_len2)
            params[key] = value
    finally:
        if c_iter != NULL:
            questdb_conf_str_iter_free(c_iter)
        questdb_conf_str_free(c_conf_str)

    # We now need to parse the various values in the dict from their
    # string values to their Python types, as expected by the overrides
    # API of Sender.from_conf and Sender.from_env.
    # Note that some of these values, such as `tls_ca` or `auto_flush`
    # are kept as strings and are parsed by Sender._set_sender_fields.
    type_mappings = {
        'bind_interface': str,
        'max_datagram_size': int,
        'multicast_ttl': int,
        'username': str,
        'password': str,
        'token': str,
        'token_x': str,
        'token_y': str,
        'auth_timeout': int,
        'tls_verify': str,
        'tls_ca': str,
        'tls_roots': str,
        'tls_roots_password': str,
        'max_buf_size': int,
        'retry_timeout': int,
        'retry_max_backoff_millis': int,
        'request_min_throughput': int,
        'request_timeout': int,
        'auto_flush': str,
        'auto_flush_rows': str,
        'auto_flush_bytes': str,
        'auto_flush_interval': str,
        'init_buf_size': int,
        'max_name_len': int,
        'qwp_ws_progress': str,
    }
    typed_params = {}
    for key, value in params.items():
        converter = type_mappings.get(key, str)
        try:
            typed_params[key] = converter(value)
        except (ValueError, TypeError) as e:
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                f'Invalid value for config key {key!r}: {value!r}') from e
    if service == 'udps':
        raise QuestDBError(
            QuestDBErrorCode.ConfigError,
            'TLS is not supported for UDP.')
    return (Protocol.parse(service), typed_params)


cdef str conf_str_value(object value):
    return str(value).replace(';', ';;')


cdef bint _dataframe_columnar_has_single_contiguous_chunk(
        col_t* col,
        size_t row_count) noexcept nogil:
    cdef ArrowArray* arr
    if col.setup.chunks.n_chunks != 1:
        return False
    if col.setup.chunks.chunks == NULL:
        return False
    arr = &col.setup.chunks.chunks[0]
    return (
        arr.offset == 0 and
        arr.length == <int64_t>row_count and
        arr.buffers != NULL and
        arr.buffers[1] != NULL)


cdef bint _dataframe_columnar_i64_has_nat(
        const int64_t* data,
        size_t row_count) noexcept nogil:
    cdef size_t row_index
    for row_index in range(row_count):
        if data[row_index] == _NAT:
            return True
    return False


cdef bint _dataframe_columnar_i64_has_negative(
        const int64_t* data,
        size_t row_count) noexcept nogil:
    cdef size_t row_index
    for row_index in range(row_count):
        if data[row_index] < 0:
            return True
    return False


cdef const column_sender_validity* _dataframe_columnar_validity(
        ArrowArray* arr,
        size_t row_offset,
        size_t row_count,
        column_sender_validity* validity) except? NULL:
    if arr.null_count == 0:
        return NULL
    if row_offset % 8 != 0:
        raise RuntimeError(
            'Columnar validity slices must start at byte-aligned row offsets.')
    validity.bits = (<const uint8_t*>arr.buffers[0]) + (row_offset // 8)
    validity.bit_len = row_count
    return validity


cdef bint _dataframe_columnar_has_validity(
        ArrowArray* arr) noexcept nogil:
    return arr.null_count == 0 or arr.buffers[0] != NULL


cdef bint _dataframe_columnar_has_utf8_values(
        ArrowArray* arr, bint large_offsets) noexcept nogil:
    if not (arr.n_buffers >= 3 and
            arr.buffers != NULL and
            arr.buffers[1] != NULL):
        return False
    if arr.length == 0 or arr.buffers[2] != NULL:
        return True
    # NULL byte buffer is valid only with zero data bytes (all-null/empty).
    if large_offsets:
        return (<const int64_t*>arr.buffers[1])[arr.offset + arr.length] == 0
    return (<const int32_t*>arr.buffers[1])[arr.offset + arr.length] == 0


cdef bint _dataframe_columnar_has_utf8_dictionary(
        ArrowArray* arr) noexcept nogil:
    cdef ArrowArray* dictionary = arr.dictionary
    if dictionary == NULL:
        return False
    return (
        dictionary.offset == 0 and
        dictionary.n_buffers >= 3 and
        dictionary.buffers != NULL and
        dictionary.buffers[1] != NULL and
        (dictionary.length == 0 or dictionary.buffers[2] != NULL))


cdef bint _dataframe_columnar_plan_has_validity(
        dataframe_plan_t* plan) noexcept nogil:
    """
    True when chunk row boundaries must be byte-aligned (multiples of 8).
    Triggers for:
    - Any Arrow column whose `null_count != 0` (the encoder reads a
      validity bitmap and our slicing requires byte-alignment).
    - Any PyObject source. The planner can't see the nulls until the
      build phase walks the column; we conservatively assume they
      might be present and require alignment.
    - col_source_bool_pyobj specifically packs its VALUES into an
      LSB-first bitmap; the emit shift `row_offset // 8` requires
      alignment whether or not nulls are present.
    """
    cdef size_t col_index
    cdef ArrowArray* arr
    cdef col_t* col
    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if _is_pyobj_source(col.setup.source):
            return True
        arr = &col.setup.chunks.chunks[0]
        if arr.null_count != 0:
            return True
    return False


cdef size_t _dataframe_columnar_rows_per_chunk(
        dataframe_plan_t* plan,
        size_t max_rows_per_chunk) noexcept nogil:
    # Clamp to a hard safety upper bound and align to 8 rows when the plan
    # carries a validity bitmap (chunk boundary must be byte-aligned).
    cdef size_t rows_per_chunk = max_rows_per_chunk
    if rows_per_chunk > 1000000:
        rows_per_chunk = 1000000
    if rows_per_chunk == 0:
        rows_per_chunk = 1
    if _dataframe_columnar_plan_has_validity(plan):
        if rows_per_chunk < 8 and rows_per_chunk < plan.row_count:
            rows_per_chunk = 8
        elif rows_per_chunk > 8:
            rows_per_chunk -= rows_per_chunk % 8
            if rows_per_chunk == 0:
                rows_per_chunk = 8
    return rows_per_chunk


cdef object _dataframe_columnar_global_failure(str reason):
    return {
        'column': None,
        'target': None,
        'source_code': None,
        'reason': reason,
    }


cdef object _dataframe_columnar_col_failure(
        object df,
        col_t* col,
        str reason):
    return {
        'column': df.columns[col.setup.orig_index],
        'target': _TARGET_NAMES[col.setup.target],
        'source_code': <int>col.setup.source,
        'reason': reason,
    }


cdef object _dataframe_columnar_plan_normalizations(
        object df,
        dataframe_plan_t* plan):
    cdef list normalizations = []
    return normalizations


cdef object _dataframe_columnar_plan_failures(
        object df,
        dataframe_plan_t* plan):
    cdef list failures = []
    cdef size_t col_index
    cdef size_t field_count = 0
    cdef col_t* col
    cdef const int64_t* ts_data

    if (plan.col_count == 0) or (plan.row_count == 0):
        return failures

    if plan.c_table_name.buf == NULL:
        failures.append(_dataframe_columnar_global_failure(
            'v1 requires a fixed table_name; table_name_col is not supported.'))

    if (plan.at_value != _AT_IS_SET_BY_COLUMN
            and plan.at_value != _AT_IS_SERVER_NOW
            and plan.at_value < 0):
        failures.append(_dataframe_columnar_global_failure(
            'v1 requires at to be a non-null DataFrame timestamp column, '
            'a fixed timestamp shared by every row, or the explicit '
            'ServerTimestamp sentinel.'))

    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if col.setup.target == col_target_t.col_target_skip:
            continue
        if col.setup.target == col_target_t.col_target_table:
            failures.append(_dataframe_columnar_col_failure(
                df, col, 'table-name columns are not supported in v1.'))
            continue
        if col.setup.target != col_target_t.col_target_at:
            field_count += 1
        if not _dataframe_columnar_has_single_contiguous_chunk(
                col, plan.row_count):
            failures.append(_dataframe_columnar_col_failure(
                df, col, 'v1 requires one contiguous zero-offset buffer.'))
            continue
        if not _dataframe_columnar_has_validity(
                &col.setup.chunks.chunks[0]):
            failures.append(_dataframe_columnar_col_failure(
                df, col, 'v1 requires a zero-offset validity bitmap when '
                'nulls are present.'))
            continue

        if col.setup.target == col_target_t.col_target_column_bool:
            if col.setup.source not in (
                    col_source_t.col_source_bool_pyobj,
                    col_source_t.col_source_bool_numpy):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports object-dtype bool or NumPy bool '
                    'columns; Arrow nullable bool not yet supported.'))
        elif col.setup.target == col_target_t.col_target_column_i64:
            if col.setup.source not in (
                    col_source_t.col_source_i64_numpy,
                    col_source_t.col_source_i8_numpy,
                    col_source_t.col_source_i16_numpy,
                    col_source_t.col_source_i32_numpy,
                    col_source_t.col_source_u8_numpy,
                    col_source_t.col_source_u16_numpy,
                    col_source_t.col_source_u32_numpy,
                    col_source_t.col_source_u64_numpy,
                    col_source_t.col_source_u32_arrow,
                    col_source_t.col_source_i64_arrow,
                    col_source_t.col_source_int_pyobj):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports NumPy signed/unsigned int columns, '
                    'Arrow uint32/int64 columns, or object-dtype int '
                    'columns.'))
        elif col.setup.target == col_target_t.col_target_column_f64:
            if col.setup.source not in (
                    col_source_t.col_source_f64_numpy,
                    col_source_t.col_source_f32_numpy,
                    col_source_t.col_source_f64_arrow,
                    col_source_t.col_source_float_pyobj):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports NumPy float32/float64, Arrow '
                    'float64, or object-dtype float columns.'))
        elif col.setup.target == col_target_t.col_target_column_ts:
            if col.setup.source not in (
                    col_source_t.col_source_dt64ns_numpy,
                    col_source_t.col_source_dt64us_numpy,
                    col_source_t.col_source_dt64ns_tz_arrow,
                    col_source_t.col_source_dt64us_tz_arrow,
                    col_source_t.col_source_datetime_pyobj):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports NumPy datetime64[ns/us], '
                    'tz-aware datetime64/timestamp[pyarrow], or '
                    'object-dtype datetime timestamp field columns.'))
            elif col.setup.source == col_source_t.col_source_dt64ns_numpy:
                # NaT in a datetime64[ns] field is INT64_MIN nanoseconds;
                # the zero-copy ns->micros wire conversion would corrupt
                # that null sentinel into a bogus 1677 timestamp. When
                # pyarrow is available, _dataframe_columnar_promote_cols
                # re-exports the column through Arrow (NULLs intact) before
                # validation, so this branch is reachable only without
                # pyarrow.
                ts_data = <const int64_t*>col.setup.chunks.chunks[0].buffers[1]
                if _dataframe_columnar_i64_has_nat(ts_data, plan.row_count):
                    failures.append(_dataframe_columnar_col_failure(
                        df,
                        col,
                        'v1 datetime64[ns] timestamp field columns cannot '
                        'contain NaT without pyarrow; install pyarrow, or '
                        'use datetime64[us] or an object-dtype datetime '
                        'column for NULL timestamps.'))
        elif col.setup.target == col_target_t.col_target_column_str:
            if col.setup.source == col_source_t.col_source_str_pyobj:
                # PyObject sources are validated by the pre-build phase
                # at row level (one walk catches all rows). The planner
                # has nothing more to check here.
                pass
            elif col.setup.source in (
                    col_source_t.col_source_str_i8_cat,
                    col_source_t.col_source_str_i16_cat,
                    col_source_t.col_source_str_i32_cat):
                if not _dataframe_columnar_has_utf8_dictionary(
                        &col.setup.chunks.chunks[0]):
                    failures.append(_dataframe_columnar_col_failure(
                        df,
                        col,
                        'v1 requires Arrow UTF-8 or LargeUtf8 dictionary '
                        'offsets and byte buffers for categorical columns.'))
            elif col.setup.source not in (
                    col_source_t.col_source_str_utf8_arrow,
                    col_source_t.col_source_str_lrg_utf8_arrow):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports string[pyarrow] columns backed by '
                    'Arrow UTF-8 or LargeUtf8, pandas string Categorical, '
                    'or object-dtype str.'))
            elif not _dataframe_columnar_has_utf8_values(
                    &col.setup.chunks.chunks[0],
                    col.setup.source ==
                        col_source_t.col_source_str_lrg_utf8_arrow):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 requires Arrow UTF-8 or LargeUtf8 offsets and byte buffers.'))
        elif col.setup.target == col_target_t.col_target_symbol:
            if col.setup.source in (
                    col_source_t.col_source_str_i8_cat,
                    col_source_t.col_source_str_i16_cat,
                    col_source_t.col_source_str_i32_cat):
                if not _dataframe_columnar_has_utf8_dictionary(
                        &col.setup.chunks.chunks[0]):
                    failures.append(_dataframe_columnar_col_failure(
                        df,
                        col,
                        'v1 requires Arrow UTF-8 or LargeUtf8 dictionary '
                        'offsets and byte buffers for categorical symbols.'))
            elif col.setup.source in (
                    col_source_t.col_source_str_utf8_arrow,
                    col_source_t.col_source_str_lrg_utf8_arrow):
                if not _dataframe_columnar_has_utf8_values(
                        &col.setup.chunks.chunks[0],
                        col.setup.source ==
                            col_source_t.col_source_str_lrg_utf8_arrow):
                    failures.append(_dataframe_columnar_col_failure(
                        df,
                        col,
                        'v1 requires Arrow UTF-8 or LargeUtf8 offsets and '
                        'byte buffers.'))
            else:
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports pandas string Categorical or '
                    'string[pyarrow] symbol columns.'))
        elif col.setup.target == col_target_t.col_target_at:
            if col.setup.source not in (
                    col_source_t.col_source_dt64ns_numpy,
                    col_source_t.col_source_dt64us_numpy,
                    col_source_t.col_source_dt64ns_tz_arrow,
                    col_source_t.col_source_dt64us_tz_arrow,
                    col_source_t.col_source_dt64ms_tz_arrow,
                    col_source_t.col_source_dt64s_tz_arrow):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports NumPy datetime64[ns/us] or '
                    'tz-aware datetime64/timestamp[pyarrow] '
                    'designated timestamp columns.'))
            elif (col.setup.source in (
                        col_source_t.col_source_dt64ns_tz_arrow,
                        col_source_t.col_source_dt64us_tz_arrow,
                        col_source_t.col_source_dt64ms_tz_arrow,
                        col_source_t.col_source_dt64s_tz_arrow)
                    and col.setup.chunks.chunks[0].null_count != 0):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 designated timestamp columns cannot contain nulls.'))
            else:
                ts_data = <const int64_t*>col.setup.chunks.chunks[0].buffers[1]
                if _dataframe_columnar_i64_has_nat(ts_data, plan.row_count):
                    failures.append(_dataframe_columnar_col_failure(
                        df,
                        col,
                        'v1 designated timestamp columns cannot contain NaT.'))
                elif _dataframe_columnar_i64_has_negative(
                        ts_data, plan.row_count):
                    failures.append(_dataframe_columnar_col_failure(
                        df,
                        col,
                        'v1 designated timestamp columns cannot contain '
                        'timestamps before the Unix epoch.'))
        elif col.setup.target == col_target_t.col_target_column_decimal:
            if col.setup.source not in (
                    col_source_t.col_source_decimal32_arrow,
                    col_source_t.col_source_decimal64_arrow,
                    col_source_t.col_source_decimal128_arrow,
                    col_source_t.col_source_decimal256_arrow):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports Arrow-backed decimal columns '
                    '(pyarrow decimal32/64/128/256); object-dtype Decimal '
                    'columns are not supported on the columnar path.'))
        elif col.setup.target in (
                col_target_t.col_target_column_i8,
                col_target_t.col_target_column_i16,
                col_target_t.col_target_column_i32,
                col_target_t.col_target_column_f32,
                col_target_t.col_target_column_uuid,
                col_target_t.col_target_column_long256,
                col_target_t.col_target_column_ipv4,
                col_target_t.col_target_column_binary,
                col_target_t.col_target_column_arrow):
            # Column-QWP-only targets reached via `_FIELD_TARGETS_QWP`.
            # Each currently reachable target's source-set in
            # `_TARGET_TO_SOURCES` is a singleton, so the source is
            # already constrained by routing. The contiguous-buffer +
            # validity checks above cover layout; the per-type FFI
            # handles the wire encoding. col_target_column_arrow delegates
            # type validation to the Rust importer.
            pass
        else:
            failures.append(_dataframe_columnar_col_failure(
                df,
                col,
                f'v1 does not support {_TARGET_NAMES[col.setup.target]} '
                'columns.'))

    if field_count == 0:
        failures.append(_dataframe_columnar_global_failure(
            'v1 requires at least one non-timestamp data column.'))

    return failures


cdef void_int _dataframe_columnar_validate_plan(
        object df,
        dataframe_plan_t* plan) except -1:
    cdef object failures = _dataframe_columnar_plan_failures(df, plan)
    if failures:
        raise UnsupportedDataFrameShapeError(
            'DataFrame is not supported by QuestDB.dataframe() columnar v1.',
            failures)


cdef object _dataframe_columnar_ndarray_col_to_arrow(object df, col_t* col):
    cdef object series = df.iloc[:, col.setup.orig_index]
    cdef object cell
    cdef PyArrayObject* arr
    cdef bint nested = False
    cdef object col_name = df.columns[col.setup.orig_index]
    for cell in series:
        if _dataframe_is_null_pyobj(<PyObject*>cell):
            continue
        if not PyArray_CheckExact(<PyObject*>cell):
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {col_name!r}: mixed object cells; expected '
                f'every non-null cell to be a numpy array, got '
                f'{_fqn(type(cell))}.')
        arr = <PyArrayObject*>cell
        if PyArray_TYPE(arr) != NPY_DOUBLE:
            raise QuestDBError(
                QuestDBErrorCode.ArrayError,
                f'Bad column {col_name!r}: Only float64 numpy arrays are '
                f'supported, got dtype: {cell.dtype}')
        if PyArray_NDIM(arr) > 1:
            nested = True
    if not nested:
        return _PYARROW.Array.from_pandas(series)
    # pyarrow cannot convert multi-dimensional ndarray cells directly
    # ("Can only convert 1-dimensional array values"); nested python
    # lists infer to the equivalent nested list<...<double>>.
    return _PYARROW.array([
        None if _dataframe_is_null_pyobj(<PyObject*>cell) else cell.tolist()
        for cell in series])


cdef object _dataframe_columnar_col_from_pandas(object df, col_t* col):
    # Re-export a pandas column as a pyarrow Array: pyarrow infers the
    # Arrow type (decimal width/scale, timestamp unit, ...) and carries a
    # validity bitmap for nulls, which the Rust Arrow importer then encodes.
    return _PYARROW.Array.from_pandas(df.iloc[:, col.setup.orig_index])


cdef void_int _dataframe_columnar_promote_cols(
        object df,
        dataframe_plan_t* plan) except -1:
    # Some column shapes have no contiguous native buffer the columnar wire
    # can emit directly. Re-export each as an Arrow array and let the Rust
    # Arrow importer classify it — the same route the all-Arrow capsule path
    # takes:
    #   - object-dtype numpy-array cells -> list<double> (ARRAY(DOUBLE));
    #   - object-dtype Decimal -> DECIMAL (width/scale inferred);
    #   - datetime64[ns] timestamp fields carrying NaT, whose INT64_MIN null
    #     sentinel the zero-copy ns->us path would corrupt into a 1677 value.
    cdef size_t col_index
    cdef col_t* col
    cdef ArrowArray* chunk
    cdef object arrow_array
    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if col.setup.source == col_source_t.col_source_arr_f64_numpyobj:
            _dataframe_require_pyarrow()
            arrow_array = _dataframe_columnar_ndarray_col_to_arrow(df, col)
        elif col.setup.source == col_source_t.col_source_decimal_pyobj:
            _dataframe_require_pyarrow()
            arrow_array = _dataframe_columnar_col_from_pandas(df, col)
        elif (col.setup.target == col_target_t.col_target_column_ts
                and col.setup.source == col_source_t.col_source_dt64ns_numpy
                and _dataframe_columnar_i64_has_nat(
                    <const int64_t*>col.setup.chunks.chunks[0].buffers[1],
                    plan.row_count)):
            if not _dataframe_try_import_pyarrow():
                continue
            arrow_array = _dataframe_columnar_col_from_pandas(df, col)
        else:
            continue
        chunk = &col.setup.chunks.chunks[0]
        if chunk.release != NULL:
            chunk.release(chunk)
        memset(chunk, 0, sizeof(ArrowArray))
        if col.setup.arrow_schema.release != NULL:
            col.setup.arrow_schema.release(&col.setup.arrow_schema)
        arrow_array._export_to_c(
            <uintptr_t>chunk, <uintptr_t>&col.setup.arrow_schema)
        col.setup.source = col_source_t.col_source_arrow_passthrough
        col.setup.target = col_target_t.col_target_column_arrow


cdef bint _is_pyobj_source(col_source_t source) noexcept nogil:
    return (
        source == col_source_t.col_source_str_pyobj or
        source == col_source_t.col_source_int_pyobj or
        source == col_source_t.col_source_float_pyobj or
        source == col_source_t.col_source_bool_pyobj or
        source == col_source_t.col_source_uuid_pyobj or
        source == col_source_t.col_source_ipv4_pyobj or
        source == col_source_t.col_source_datetime_pyobj or
        source == col_source_t.col_source_bytes_pyobj)


cdef inline void _pyobj_set_validity_bit(uint8_t* bitmap, size_t row) noexcept nogil:
    bitmap[row >> 3] |= <uint8_t>(1 << (row & 7))


cdef pyobj_built_t* _dataframe_columnar_build_str_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    """
    Walk a PyObject column once and produce Arrow-Utf8-shaped buffers
    (int32 offsets + uint8 bytes + LSB-packed validity). Encoding uses
    CPython's `PyUnicode_AsUTF8AndSize`, which rejects lone surrogates,
    so every emitted buffer is valid UTF-8.
    """
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef size_t i
    cdef Py_ssize_t utf8_len
    cdef const char* utf8_buf
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t bytes_cap = 16
    cdef uint8_t* new_bytes
    cdef size_t bytes_used = 0

    try:
        b.str_offsets = <int32_t*>calloc(row_count + 1, sizeof(int32_t))
        if b.str_offsets == NULL:
            raise MemoryError()
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        b.str_bytes = <uint8_t*>malloc(bytes_cap)
        if b.str_bytes == NULL:
            raise MemoryError()

        for i in range(row_count):
            cell = access[i]
            if PyUnicode_CheckExact(cell):
                utf8_buf = PyUnicode_AsUTF8AndSize(cell, &utf8_len)
                if bytes_used + <size_t>utf8_len > <size_t>2_147_483_647:
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r}: column total UTF-8 '
                        'bytes exceeds the QWP wire varchar offset table '
                        'limit (2 GiB).')
                while bytes_used + <size_t>utf8_len > bytes_cap:
                    bytes_cap *= 2
                    new_bytes = <uint8_t*>realloc(b.str_bytes, bytes_cap)
                    if new_bytes == NULL:
                        raise MemoryError()
                    b.str_bytes = new_bytes
                if utf8_len > 0:
                    memcpy(b.str_bytes + bytes_used, utf8_buf, <size_t>utf8_len)
                bytes_used += <size_t>utf8_len
                b.str_offsets[i + 1] = <int32_t>bytes_used
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.str_offsets[i + 1] = <int32_t>bytes_used
                b.has_nulls = True
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected str, '
                    f'got {_fqn(type(<object>cell))}.')

        b.str_bytes_len = bytes_used

        # If the column turned out to be all-valid, drop the bitmap so
        # the FFI takes the no-validity hot path.
        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_int_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    """
    Walk a PyObject int column once and produce a contiguous int64
    buffer + LSB-packed validity bitmap. Null cells leave the int64
    slot at 0 with the validity bit cleared.

    Null detection: ``None``, ``pd.NA``, and ``float('nan')`` all count
    as null — the NaN-as-null rule matches the row-path behaviour
    (`_dataframe_is_null_pyobj` in dataframe.pxi). A non-NaN float in
    an int-sniffed column raises ``QuestDBError`` with the row index;
    we accept the asymmetry because column-wide sniff has already
    locked the source type from the first non-null cell.
    """
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef int64_t* values = NULL
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef int64_t value

    try:
        values = <int64_t*>calloc(row_count if row_count > 0 else 1,
                                  sizeof(int64_t))
        if values == NULL:
            raise MemoryError()
        b.data = <void*>values
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        for i in range(row_count):
            cell = access[i]
            # PyBool_Check goes BEFORE PyLong_CheckExact because Python
            # bools are subclasses of int and PyLong_CheckExact returns
            # false for them; treat them as int (matches row-path).
            if PyBool_Check(cell):
                values[i] = 1 if cell == <PyObject*>True else 0
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif PyLong_CheckExact(cell):
                value = PyLong_AsLongLong(cell)
                values[i] = value
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected int, '
                    f'got {_fqn(type(<object>cell))}.')

        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_float_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    """
    Walk a PyObject float column once and produce a contiguous double
    buffer + LSB-packed validity bitmap.
    """
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef double* values = NULL
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef double value

    try:
        values = <double*>calloc(row_count if row_count > 0 else 1,
                                 sizeof(double))
        if values == NULL:
            raise MemoryError()
        b.data = <void*>values
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        for i in range(row_count):
            cell = access[i]
            if PyFloat_CheckExact(cell):
                value = PyFloat_AS_DOUBLE(cell)
                if isnan(value):
                    # pandas NaN-as-null convention matches the row-path.
                    b.has_nulls = True
                else:
                    values[i] = value
                    if b.validity != NULL:
                        _pyobj_set_validity_bit(b.validity, i)
            elif PyLong_CheckExact(cell) or PyBool_Check(cell):
                # Accept widening of int / bool to float, matching how
                # Python implicitly converts when you do float(x).
                values[i] = PyFloat_AsDouble(cell)
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected float, '
                    f'got {_fqn(type(<object>cell))}.')

        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_bool_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    """
    Walk a PyObject bool column once and pack the values into an
    Arrow LSB-first bitmap (one bit per row). Null cells are rejected —
    matches the row-path behaviour (QuestDB BOOLEAN has no null
    representation at the row level).
    """
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef uint8_t* bits = NULL
    cdef size_t bytes = (row_count + 7) // 8
    cdef size_t i

    try:
        if bytes == 0:
            bytes = 1
        bits = <uint8_t*>calloc(bytes, sizeof(uint8_t))
        if bits == NULL:
            raise MemoryError()
        b.data = <void*>bits
        for i in range(row_count):
            cell = access[i]
            if PyBool_Check(cell):
                if cell == <PyObject*>True:
                    bits[i >> 3] |= <uint8_t>(1 << (i & 7))
            elif _dataframe_is_null_pyobj(cell):
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: cannot insert '
                    'null into a boolean column.')
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected bool, '
                    f'got {_fqn(type(<object>cell))}.')
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_uuid_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef uint8_t* buf = NULL
    cdef size_t buf_bytes = row_count * 16 if row_count > 0 else 16
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef object le_bytes
    cdef object uuid_cls = _uuid.UUID

    try:
        buf = <uint8_t*>calloc(buf_bytes, sizeof(uint8_t))
        if buf == NULL:
            raise MemoryError()
        b.data = <void*>buf
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        for i in range(row_count):
            cell = access[i]
            if isinstance(<object>cell, uuid_cls):
                # `.int.to_bytes(16, 'little')` produces exactly the
                # QuestDB UUID wire layout: bytes 0..8 = lo half LE,
                # bytes 8..16 = hi half LE. One C-implemented call +
                # one 16-byte memcpy per row.
                le_bytes = (<object>cell).int.to_bytes(16, 'little')
                memcpy(buf + i * 16, PyBytes_AsString(le_bytes), 16)
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected UUID, '
                    f'got {_fqn(type(<object>cell))}.')

        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_ipv4_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef uint32_t* values = NULL
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef object ipv4_cls = _ipaddress.IPv4Address

    try:
        values = <uint32_t*>calloc(row_count if row_count > 0 else 1,
                                   sizeof(uint32_t))
        if values == NULL:
            raise MemoryError()
        b.data = <void*>values
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        for i in range(row_count):
            cell = access[i]
            if isinstance(<object>cell, ipv4_cls):
                values[i] = <uint32_t>int(<object>cell)
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected '
                    f'ipaddress.IPv4Address, got {_fqn(type(<object>cell))}.')

        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef inline int64_t _days_from_civil(int y, int m, int d) noexcept nogil:
    cdef int y_adj = y - 1 if m <= 2 else y
    cdef int era = (y_adj if y_adj >= 0 else y_adj - 399) // 400
    cdef int yoe = y_adj - era * 400
    cdef int m_adj = m - 3 if m > 2 else m + 9
    cdef int doy = (153 * m_adj + 2) // 5 + d - 1
    cdef int doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return <int64_t>era * 146097 + <int64_t>doe - 719468


cdef pyobj_built_t* _dataframe_columnar_build_datetime_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef int64_t* values = NULL
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef object dt
    cdef object epoch_aware = datetime.datetime(
        1970, 1, 1, tzinfo=datetime.timezone.utc)
    cdef object datetime_cls = datetime.datetime
    cdef object delta
    cdef int year, month, day, hour, minute, second, us
    cdef int64_t days

    try:
        values = <int64_t*>calloc(row_count if row_count > 0 else 1,
                                  sizeof(int64_t))
        if values == NULL:
            raise MemoryError()
        b.data = <void*>values
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        for i in range(row_count):
            cell = access[i]
            if _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
            elif isinstance(<object>cell, datetime_cls):
                dt = <object>cell
                if dt.tzinfo is None:
                    # Fast path: C-level field extraction + Howard
                    # Hinnant days_from_civil; no Python timedelta /
                    # int arithmetic per row.
                    year = PyDateTime_GET_YEAR(dt)
                    month = PyDateTime_GET_MONTH(dt)
                    day = PyDateTime_GET_DAY(dt)
                    hour = PyDateTime_DATE_GET_HOUR(dt)
                    minute = PyDateTime_DATE_GET_MINUTE(dt)
                    second = PyDateTime_DATE_GET_SECOND(dt)
                    us = PyDateTime_DATE_GET_MICROSECOND(dt)
                    days = _days_from_civil(year, month, day)
                    values[i] = (
                        days * 86_400_000_000
                        + <int64_t>hour * 3_600_000_000
                        + <int64_t>minute * 60_000_000
                        + <int64_t>second * 1_000_000
                        + <int64_t>us)
                else:
                    delta = dt - epoch_aware
                    values[i] = <int64_t>(
                        delta.days * 86_400_000_000
                        + delta.seconds * 1_000_000
                        + delta.microseconds)
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected '
                    f'datetime.datetime, got {_fqn(type(<object>cell))}.')

        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_bytes_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef Py_ssize_t blob_len
    cdef const char* blob_buf
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t bytes_cap = 16
    cdef uint8_t* new_bytes
    cdef size_t bytes_used = 0
    cdef size_t i

    try:
        b.str_offsets = <int32_t*>calloc(row_count + 1, sizeof(int32_t))
        if b.str_offsets == NULL:
            raise MemoryError()
        if validity_bytes > 0:
            b.validity = <uint8_t*>calloc(validity_bytes, sizeof(uint8_t))
            if b.validity == NULL:
                raise MemoryError()
        b.str_bytes = <uint8_t*>malloc(bytes_cap)
        if b.str_bytes == NULL:
            raise MemoryError()

        for i in range(row_count):
            cell = access[i]
            if PyBytes_CheckExact(cell):
                blob_len = PyBytes_GET_SIZE(<object>cell)
                blob_buf = PyBytes_AsString(<object>cell)
                if bytes_used + <size_t>blob_len > <size_t>2_147_483_647:
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r}: column total bytes '
                        'exceeds the QWP wire binary offset table '
                        'limit (2 GiB).')
                while bytes_used + <size_t>blob_len > bytes_cap:
                    bytes_cap *= 2
                    new_bytes = <uint8_t*>realloc(b.str_bytes, bytes_cap)
                    if new_bytes == NULL:
                        raise MemoryError()
                    b.str_bytes = new_bytes
                if blob_len > 0:
                    memcpy(b.str_bytes + bytes_used, blob_buf, <size_t>blob_len)
                bytes_used += <size_t>blob_len
                b.str_offsets[i + 1] = <int32_t>bytes_used
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.str_offsets[i + 1] = <int32_t>bytes_used
                b.has_nulls = True
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected bytes, '
                    f'got {_fqn(type(<object>cell))}.')

        b.str_bytes_len = bytes_used
        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef void_int _dataframe_columnar_prebuild_pyobj(
        object df,
        dataframe_plan_t* plan) except -1:
    """
    Walk every PyObject-sourced column once and stash typed buffers on
    `plan.pyobj_built`. Runs after `validate_plan` and before the chunk
    emission loop in `QuestDB.dataframe()`.
    """
    cdef size_t i
    cdef col_t* col
    cdef bint any_pyobj = False

    for i in range(plan.col_count):
        col = &plan.cols.d[i]
        if _is_pyobj_source(col.setup.source):
            any_pyobj = True
            break
    if not any_pyobj:
        return 0

    plan.pyobj_built = <pyobj_built_t**>calloc(
        plan.col_count, sizeof(pyobj_built_t*))
    if plan.pyobj_built == NULL:
        raise MemoryError()

    for i in range(plan.col_count):
        col = &plan.cols.d[i]
        if not _is_pyobj_source(col.setup.source):
            continue
        col_name = df.columns[col.setup.orig_index]
        try:
            if col.setup.source == col_source_t.col_source_str_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_str_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_int_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_int_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_float_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_float_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_bool_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_bool_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_uuid_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_uuid_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_ipv4_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_ipv4_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_datetime_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_datetime_pyobj(
                    col, plan.row_count, col_name)
            elif col.setup.source == col_source_t.col_source_bytes_pyobj:
                plan.pyobj_built[i] = _dataframe_columnar_build_bytes_pyobj(
                    col, plan.row_count, col_name)
        except OverflowError as oe:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {col_name!r}: {oe}') from oe
        except UnicodeEncodeError as ue:
            raise QuestDBError(
                QuestDBErrorCode.InvalidUtf8,
                f'Bad column {col_name!r}: {ue}') from ue


cdef void_int _dataframe_columnar_append_pyobj_str(
        column_sender_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef column_sender_validity validity
    cdef const column_sender_validity* validity_ptr = NULL
    cdef bint ok = False
    cdef int32_t* offsets
    cdef size_t bytes_len

    if prebuilt == NULL:
        raise RuntimeError(
            'PyObject str column missing pre-built buffer; '
            'prebuild phase did not run.')
    if prebuilt.has_nulls:
        if row_offset % 8 != 0:
            raise RuntimeError(
                'PyObject str column with nulls requires byte-aligned '
                'chunk boundaries.')
        validity.bits = prebuilt.validity + (row_offset // 8)
        validity.bit_len = row_count
        validity_ptr = &validity
    offsets = prebuilt.str_offsets + row_offset
    bytes_len = prebuilt.str_bytes_len
    with nogil:
        ok = column_sender_chunk_column_str(
            chunk,
            col.name.buf,
            col.name.len,
            offsets,
            prebuilt.str_bytes,
            bytes_len,
            row_count,
            validity_ptr,
            &err)
    if not ok:
        raise c_err_to_py(err)


cdef void_int _dataframe_columnar_append_pyobj_simple(
        column_sender_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count,
        size_t elem_size,
        column_sender_numpy_dtype dtype) except -1:
    cdef line_sender_error* err = NULL
    cdef column_sender_validity validity
    cdef const column_sender_validity* validity_ptr = NULL
    cdef bint ok = False

    if prebuilt == NULL:
        raise RuntimeError('PyObject column missing pre-built buffer.')
    if prebuilt.has_nulls:
        if row_offset % 8 != 0:
            raise RuntimeError(
                'PyObject column with nulls requires byte-aligned '
                'chunk boundaries.')
        validity.bits = prebuilt.validity + (row_offset // 8)
        validity.bit_len = row_count
        validity_ptr = &validity
    with nogil:
        ok = column_sender_chunk_append_numpy_column(
            chunk,
            col.name.buf,
            col.name.len,
            dtype,
            (<const uint8_t*>prebuilt.data) + row_offset * elem_size,
            row_count * elem_size,
            row_count,
            validity_ptr,
            NULL,
            &err)
    if not ok:
        raise c_err_to_py(err)


cdef void_int _dataframe_columnar_append_pyobj_bytes(
        column_sender_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef column_sender_validity validity
    cdef const column_sender_validity* validity_ptr = NULL
    cdef bint ok = False

    if prebuilt == NULL:
        raise RuntimeError('PyObject bytes column missing pre-built buffer.')
    if prebuilt.has_nulls:
        if row_offset % 8 != 0:
            raise RuntimeError(
                'PyObject bytes column with nulls requires byte-aligned '
                'chunk boundaries.')
        validity.bits = prebuilt.validity + (row_offset // 8)
        validity.bit_len = row_count
        validity_ptr = &validity
    with nogil:
        ok = column_sender_chunk_column_binary(
            chunk,
            col.name.buf,
            col.name.len,
            prebuilt.str_offsets + row_offset,
            prebuilt.str_bytes,
            prebuilt.str_bytes_len,
            row_count,
            validity_ptr,
            &err)
    if not ok:
        raise c_err_to_py(err)


cdef void_int _dataframe_columnar_call_arrow_append(
        column_sender_chunk* chunk,
        col_t* col,
        size_t row_offset,
        size_t row_count,
        column_sender_symbol_mode symbol_mode
            =column_sender_symbol_mode_auto) except -1:
    cdef line_sender_error* err = NULL
    cdef bint ok = False
    cdef column_sender_arrow_import* imported = col.setup.arrow_import
    with nogil:
        if imported == NULL:
            imported = column_sender_arrow_import_new(
                &col.setup.chunks.chunks[0],
                &col.setup.arrow_schema,
                symbol_mode,
                &err)
        if imported != NULL:
            ok = column_sender_chunk_append_arrow_import(
                chunk,
                col.name.buf,
                col.name.len,
                imported,
                row_offset,
                row_count,
                &err)
    col.setup.arrow_import = imported
    if not ok:
        raise c_err_to_py(err)
    return 0


cdef void_int _dataframe_columnar_append_field(
        column_sender_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef ArrowArray* arr = &col.setup.chunks.chunks[0]
    cdef ArrowArray* dictionary
    cdef const void* data = arr.buffers[1]
    cdef int32_t* offsets
    cdef int32_t* dict_offsets
    cdef size_t bytes_len
    cdef size_t dict_offsets_len
    cdef size_t dict_bytes_len
    cdef column_sender_validity validity
    cdef const column_sender_validity* validity_ptr = (
        _dataframe_columnar_validity(arr, row_offset, row_count, &validity))
    cdef bint ok = False

    cdef column_sender_numpy_dtype numpy_dtype
    cdef size_t element_size
    cdef column_sender_numpy_extras extras
    cdef const column_sender_numpy_extras* extras_ptr

    if col.setup.target == col_target_t.col_target_column_bool:
        if col.setup.source == col_source_t.col_source_bool_pyobj:
            if prebuilt == NULL:
                raise RuntimeError(
                    'PyObject bool column missing pre-built bitmap.')
            if row_offset % 8 != 0:
                raise RuntimeError(
                    'PyObject bool column requires byte-aligned chunk boundaries.')
            with nogil:
                ok = column_sender_chunk_column_bool(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    (<const uint8_t*>prebuilt.data) + (row_offset // 8),
                    row_count,
                    NULL,
                    &err)
        elif col.setup.source == col_source_t.col_source_bool_numpy:
            # NumPy bool is byte-per-row; Rust packs to LSB-bitmap
            # inside column_sender_chunk_append_numpy_column.
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    column_sender_numpy_dtype.column_sender_numpy_bool,
                    (<const uint8_t*>data) + row_offset,
                    row_count,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        else:
            raise RuntimeError('Unsupported columnar bool source.')
    elif col.setup.target == col_target_t.col_target_column_i64:
        if col.setup.source == col_source_t.col_source_int_pyobj:
            if prebuilt == NULL:
                raise RuntimeError(
                    'PyObject int column missing pre-built buffer.')
            if prebuilt.has_nulls and row_offset % 8 != 0:
                raise RuntimeError(
                    'PyObject int column with nulls requires byte-aligned '
                    'chunk boundaries.')
            if prebuilt.has_nulls:
                validity.bits = prebuilt.validity + (row_offset // 8)
                validity.bit_len = row_count
                validity_ptr = &validity
            else:
                validity_ptr = NULL
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    column_sender_numpy_dtype.column_sender_numpy_i64,
                    (<const uint8_t*>prebuilt.data) + row_offset * 8,
                    row_count * 8,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        else:
            # Rust widens narrow ints to a sentinel-safe wire (i8/i16 → INT,
            # i32/u32/u64 → LONG); see questdb-rs NumpyDtype::*WidenTo*.
            if col.setup.source in (
                    col_source_t.col_source_i64_numpy,
                    col_source_t.col_source_i64_arrow):
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_i64
                element_size = 8
            elif col.setup.source == col_source_t.col_source_i8_numpy:
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_i8
                element_size = 1
            elif col.setup.source == col_source_t.col_source_i16_numpy:
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_i16
                element_size = 2
            elif col.setup.source == col_source_t.col_source_i32_numpy:
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_i32
                element_size = 4
            elif col.setup.source == col_source_t.col_source_u8_numpy:
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_u8
                element_size = 1
            elif col.setup.source == col_source_t.col_source_u16_numpy:
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_u16
                element_size = 2
            elif col.setup.source in (
                    col_source_t.col_source_u32_numpy,
                    col_source_t.col_source_u32_arrow):
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_u32
                element_size = 4
            elif col.setup.source == col_source_t.col_source_u64_numpy:
                numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_u64
                element_size = 8
            else:
                raise RuntimeError('Unsupported columnar int source.')
            extras_ptr = NULL
            if col.setup.has_override:
                numpy_dtype = col.setup.override_dtype
                if (numpy_dtype
                            == column_sender_numpy_dtype.column_sender_numpy_geohash_i8
                        or numpy_dtype
                            == column_sender_numpy_dtype.column_sender_numpy_geohash_i16
                        or numpy_dtype
                            == column_sender_numpy_dtype.column_sender_numpy_geohash_i32
                        or numpy_dtype
                            == column_sender_numpy_dtype.column_sender_numpy_geohash_i64):
                    memset(&extras, 0, sizeof(column_sender_numpy_extras))
                    extras.geohash_bits = col.setup.override_geohash_bits
                    extras_ptr = &extras
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    numpy_dtype,
                    (<const uint8_t*>data) + row_offset * element_size,
                    row_count * element_size,
                    row_count,
                    validity_ptr,
                    extras_ptr,
                    &err)
    elif col.setup.target == col_target_t.col_target_column_f64:
        if col.setup.source in (
                col_source_t.col_source_f64_numpy,
                col_source_t.col_source_f64_arrow):
            numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_f64
            element_size = 8
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    numpy_dtype,
                    (<const uint8_t*>data) + row_offset * element_size,
                    row_count * element_size,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        elif col.setup.source == col_source_t.col_source_f32_numpy:
            # numpy f32 maps directly to a FLOAT column on the wire; the
            # source stride is 4 bytes per row.
            numpy_dtype = column_sender_numpy_dtype.column_sender_numpy_f32
            element_size = 4
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    numpy_dtype,
                    (<const uint8_t*>data) + row_offset * element_size,
                    row_count * element_size,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        elif col.setup.source == col_source_t.col_source_float_pyobj:
            if prebuilt == NULL:
                raise RuntimeError(
                    'PyObject float column missing pre-built buffer.')
            if prebuilt.has_nulls and row_offset % 8 != 0:
                raise RuntimeError(
                    'PyObject float column with nulls requires byte-aligned '
                    'chunk boundaries.')
            if prebuilt.has_nulls:
                validity.bits = prebuilt.validity + (row_offset // 8)
                validity.bit_len = row_count
                validity_ptr = &validity
            else:
                validity_ptr = NULL
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    column_sender_numpy_dtype.column_sender_numpy_f64,
                    (<const uint8_t*>prebuilt.data) + row_offset * 8,
                    row_count * 8,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        else:
            raise RuntimeError('Unsupported columnar float source.')
    elif col.setup.target == col_target_t.col_target_column_ts:
        if col.setup.source == col_source_t.col_source_dt64ns_numpy:
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    column_sender_numpy_dtype.column_sender_numpy_datetime64_ns,
                    (<const uint8_t*>data) + row_offset * 8,
                    row_count * 8,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        elif col.setup.source == col_source_t.col_source_dt64us_numpy:
            with nogil:
                ok = column_sender_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    column_sender_numpy_dtype.column_sender_numpy_datetime64_us,
                    (<const uint8_t*>data) + row_offset * 8,
                    row_count * 8,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        elif col.setup.source in (
                col_source_t.col_source_dt64ns_tz_arrow,
                col_source_t.col_source_dt64us_tz_arrow):
            _dataframe_columnar_call_arrow_append(
                chunk, col, row_offset, row_count)
            return 0
        elif col.setup.source == col_source_t.col_source_datetime_pyobj:
            _dataframe_columnar_append_pyobj_simple(
                chunk, col, prebuilt, row_offset, row_count, 8,
                column_sender_numpy_dtype.column_sender_numpy_datetime64_us)
            return 0
        else:
            raise RuntimeError('Unsupported columnar timestamp field source.')
    elif col.setup.target in (
            col_target_t.col_target_column_i8,
            col_target_t.col_target_column_i16,
            col_target_t.col_target_column_i32,
            col_target_t.col_target_column_f32,
            col_target_t.col_target_column_long256):
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_uuid:
        if col.setup.source == col_source_t.col_source_uuid_pyobj:
            _dataframe_columnar_append_pyobj_simple(
                chunk, col, prebuilt, row_offset, row_count, 16,
                column_sender_numpy_dtype.column_sender_numpy_s16)
            return 0
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_ipv4:
        if col.setup.source == col_source_t.col_source_ipv4_pyobj:
            _dataframe_columnar_append_pyobj_simple(
                chunk, col, prebuilt, row_offset, row_count, 4,
                column_sender_numpy_dtype.column_sender_numpy_u32_ipv4)
            return 0
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_binary:
        if col.setup.source == col_source_t.col_source_bytes_pyobj:
            _dataframe_columnar_append_pyobj_bytes(
                chunk, col, prebuilt, row_offset, row_count)
            return 0
        raise RuntimeError('Unsupported columnar binary field source.')
    elif col.setup.target == col_target_t.col_target_column_str:
        if col.setup.source == col_source_t.col_source_str_pyobj:
            _dataframe_columnar_append_pyobj_str(
                chunk, col, prebuilt, row_offset, row_count)
            return 0  # err already raised inside on failure
        if col.setup.source in (
                col_source_t.col_source_str_i8_cat,
                col_source_t.col_source_str_i16_cat,
                col_source_t.col_source_str_i32_cat):
            _dataframe_columnar_call_arrow_append(
                chunk, col, row_offset, row_count,
                column_sender_symbol_mode_not_symbol)
            return 0
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_symbol:
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count,
            column_sender_symbol_mode_symbol)
        return 0
    elif col.setup.target == col_target_t.col_target_column_arrow:
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_decimal:
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    else:
        raise RuntimeError('Unsupported columnar field target.')

    if not ok:
        raise c_err_to_py(err)


cdef void_int _dataframe_columnar_append_at(
        column_sender_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef const int64_t* data
    cdef bint ok = False

    if col.setup.source == col_source_t.col_source_datetime_pyobj:
        if prebuilt == NULL:
            raise RuntimeError(
                'PyObject datetime designated TS missing pre-built buffer.')
        if prebuilt.has_nulls:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                'Designated timestamp column cannot contain nulls.')
        data = <const int64_t*>prebuilt.data
        with nogil:
            ok = column_sender_chunk_at_micros(
                chunk,
                data + row_offset,
                row_count,
                &err)
        if not ok:
            raise c_err_to_py(err)
        return 0

    data = <const int64_t*>(col.setup.chunks.chunks[0].buffers[1])

    if col.setup.source in (
            col_source_t.col_source_dt64ns_numpy,
            col_source_t.col_source_dt64ns_tz_arrow):
        with nogil:
            ok = column_sender_chunk_at_nanos(
                chunk,
                data + row_offset,
                row_count,
                &err)
    elif col.setup.source in (
            col_source_t.col_source_dt64us_numpy,
            col_source_t.col_source_dt64us_tz_arrow):
        with nogil:
            ok = column_sender_chunk_at_micros(
                chunk,
                data + row_offset,
                row_count,
                &err)
    elif col.setup.source == col_source_t.col_source_dt64ms_tz_arrow:
        with nogil:
            ok = column_sender_chunk_at_millis(
                chunk,
                data + row_offset,
                row_count,
                &err)
    elif col.setup.source == col_source_t.col_source_dt64s_tz_arrow:
        with nogil:
            ok = column_sender_chunk_at_seconds(
                chunk,
                data + row_offset,
                row_count,
                &err)
    else:
        raise RuntimeError('Unsupported columnar designated timestamp source.')

    if not ok:
        raise c_err_to_py(err)


cdef int _geohash_override_dtype(col_source_t source) noexcept:
    if (source == col_source_t.col_source_u8_numpy
            or source == col_source_t.col_source_i8_numpy):
        return <int>column_sender_numpy_dtype.column_sender_numpy_geohash_i8
    if (source == col_source_t.col_source_u16_numpy
            or source == col_source_t.col_source_i16_numpy):
        return <int>column_sender_numpy_dtype.column_sender_numpy_geohash_i16
    if (source == col_source_t.col_source_u32_numpy
            or source == col_source_t.col_source_i32_numpy):
        return <int>column_sender_numpy_dtype.column_sender_numpy_geohash_i32
    if (source == col_source_t.col_source_u64_numpy
            or source == col_source_t.col_source_i64_numpy):
        return <int>column_sender_numpy_dtype.column_sender_numpy_geohash_i64
    return -1


cdef object _dataframe_normalize_nullable(object df):
    if not _is_pandas_dataframe_object(df):
        return df
    _dataframe_may_import_deps()
    cdef object masked_base = _pandas_masked_dtype()
    convert = []
    for pos, dtype in enumerate(df.dtypes):
        # pyarrow-backed strings keep their Arrow buffers (resolved as
        # str_utf8_arrow), so an all-null column survives as a null VARCHAR
        # instead of collapsing to a skipped all-null object column.
        if isinstance(dtype, masked_base):
            convert.append(pos)
        elif (isinstance(dtype, _PANDAS.StringDtype)
                and getattr(dtype, 'storage', None) != 'pyarrow'):
            convert.append(pos)
    if not convert:
        return df
    out = df.copy(deep=False)
    for pos in convert:
        _dataframe_set_column(out, df, pos, df.iloc[:, pos].astype(object))
    out.attrs = dict(df.attrs)
    return out


cdef void_int _dataframe_set_column(
        object out, object df, object pos, object value) except -1:
    # `out[name] = value` trips pandas' chained-assignment refcount
    # heuristic when called from Cython (the shallow copy looks like a
    # temporary), spamming FutureWarnings under pandas >= 2.2 warn-CoW
    # and erroring under `-W error`. Positional `isetitem` performs the
    # same block replacement without the heuristic; pandas < 1.5 lacks
    # it but also lacks the warning, so plain setitem stays correct.
    if hasattr(out, 'isetitem'):
        out.isetitem(pos, value)
    else:
        out[df.columns[pos]] = value


cdef object _dataframe_normalize_at_timestamp(object df, object at):
    # tz-aware (DatetimeTZ) ms/s designated-`at` columns can't reach the
    # columnar resolver's source override (the shared classifier rejects
    # non-ns/us tz units first), so widen them to us here. ArrowDtype ms/s
    # is widened to micros in Rust by the millis/seconds designated-ts FFI.
    cdef object dtype, new_dtype, out, at_name
    if not _is_pandas_dataframe_object(df):
        return df
    if isinstance(at, str):
        at_name = at
    elif isinstance(at, int) and not isinstance(at, bool):
        try:
            at_name = df.columns[at]
        except Exception:
            return df
        if not isinstance(at_name, str):
            return df
    else:
        return df
    _dataframe_may_import_deps()
    try:
        if at_name not in df.columns:
            return df
        dtype = df[at_name].dtype
    except Exception:
        return df
    if not isinstance(dtype, _PANDAS.DatetimeTZDtype) or dtype.unit not in ('s', 'ms'):
        return df
    new_dtype = _PANDAS.DatetimeTZDtype('us', dtype.tz)
    pos = df.columns.get_loc(at_name)
    if getattr(pos, '__index__', None) is None:
        # Duplicate at-column names: leave the frame for the planner
        # to reject.
        return df
    out = df.copy(deep=False)
    _dataframe_set_column(out, df, pos, df.iloc[:, pos].astype(new_dtype))
    out.attrs = dict(df.attrs)
    return out


cdef void_int _dataframe_apply_roundtrip_overrides(
        object df, dataframe_plan_t* plan) except -1:
    cdef size_t col_index
    cdef col_t* col
    cdef int gh
    for col_index in range(plan.col_count):
        plan.cols.d[col_index].setup.has_override = False
    attrs = getattr(df, 'attrs', None)
    if not attrs:
        return 0
    qmeta = attrs.get('questdb')
    if not qmeta:
        return 0
    cols_meta = qmeta.get('columns')
    if not cols_meta:
        return 0
    df_cols = list(df.columns)
    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if col.setup.orig_index >= <size_t>len(df_cols):
            continue
        meta = cols_meta.get(df_cols[col.setup.orig_index])
        if not meta:
            continue
        kind = meta.get('kind')
        if (kind == 'ipv4'
                and col.setup.source == col_source_t.col_source_u32_numpy):
            col.setup.has_override = True
            col.setup.override_dtype = \
                column_sender_numpy_dtype.column_sender_numpy_u32_ipv4
        elif (kind == 'char'
                and col.setup.source == col_source_t.col_source_u16_numpy):
            col.setup.has_override = True
            col.setup.override_dtype = \
                column_sender_numpy_dtype.column_sender_numpy_u16_char
        elif kind == 'geohash':
            gh = _geohash_override_dtype(col.setup.source)
            bits = meta.get('precision_bits') or 0
            if gh != -1 and 1 <= bits <= 60:
                col.setup.has_override = True
                col.setup.override_dtype = <column_sender_numpy_dtype>gh
                col.setup.override_geohash_bits = <uint8_t>bits
    return 0


cdef void_int _dataframe_columnar_populate_chunk(
        dataframe_plan_t* plan,
        column_sender_chunk* chunk,
        size_t row_offset,
        size_t row_count) except -1:
    cdef size_t col_index
    cdef col_t* col
    cdef col_t* at_col = NULL
    cdef size_t at_col_index = 0
    cdef size_t field_count = 0
    cdef pyobj_built_t* prebuilt = NULL
    cdef pyobj_built_t* at_prebuilt = NULL
    cdef line_sender_error* err = NULL

    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if col.setup.target == col_target_t.col_target_at:
            at_col = col
            at_col_index = col_index
        elif col.setup.target in (
                col_target_t.col_target_column_bool,
                col_target_t.col_target_column_i64,
                col_target_t.col_target_column_f64,
                col_target_t.col_target_column_ts,
                col_target_t.col_target_column_str,
                col_target_t.col_target_symbol,
                col_target_t.col_target_column_i8,
                col_target_t.col_target_column_i16,
                col_target_t.col_target_column_i32,
                col_target_t.col_target_column_f32,
                col_target_t.col_target_column_uuid,
                col_target_t.col_target_column_long256,
                col_target_t.col_target_column_ipv4,
                col_target_t.col_target_column_binary,
                col_target_t.col_target_column_arrow,
                col_target_t.col_target_column_decimal):
            if plan.pyobj_built != NULL:
                prebuilt = plan.pyobj_built[col_index]
            else:
                prebuilt = NULL
            _dataframe_columnar_append_field(
                chunk, col, prebuilt, row_offset, row_count)
            field_count += 1
        elif col.setup.target != col_target_t.col_target_skip:
            raise RuntimeError(
                'Unsupported columnar field target: %d.'
                % <int>col.setup.target)

    if field_count == 0:
        raise RuntimeError(
            'Validated columnar plan has no non-timestamp data columns.')
    if plan.at_value == _AT_IS_SERVER_NOW:
        # Explicit `at=ServerTimestamp` opt-in: the frame carries no
        # designated timestamp column; the server stamps rows on arrival.
        if not column_sender_chunk_at_now(chunk, &err):
            raise c_err_to_py(err)
        return 0
    if plan.at_value >= 0:
        # Fixed scalar designated timestamp shared by every row.
        if not column_sender_chunk_at_scalar_nanos(
                chunk, plan.at_value, &err):
            raise c_err_to_py(err)
        return 0
    if at_col == NULL:
        raise RuntimeError('Validated columnar plan has no timestamp column.')
    if plan.pyobj_built != NULL:
        at_prebuilt = plan.pyobj_built[at_col_index]
    _dataframe_columnar_append_at(
        chunk, at_col, at_prebuilt, row_offset, row_count)


cdef void_int _dataframe_columnar_sync(direct_column_sender* conn) except -1:
    cdef line_sender_error* err = NULL
    cdef bint ok = False
    cdef PyThreadState* gs = NULL
    cdef uint64_t start_ns = 0
    global _dataframe_columnar_sync_calls
    global _dataframe_columnar_sync_ns
    if _dataframe_columnar_count_io_stats:
        start_ns = time.perf_counter_ns()
    _ensure_doesnt_have_gil(&gs)
    ok = direct_column_sender_commit(
        conn,
        qwpws_ack_level.qwpws_ack_level_ok,
        &err)
    _ensure_has_gil(&gs)
    if _dataframe_columnar_count_io_stats:
        _dataframe_columnar_sync_calls += 1
        _dataframe_columnar_sync_ns += time.perf_counter_ns() - start_ns
    if not ok:
        raise c_err_to_py(err)


cdef bint _dataframe_columnar_force_drop_after_error(
        direct_column_sender* conn,
        bint flushed) noexcept:
    # A failed call must not let its data reach the table. The direct pool's
    # return path best-effort-commits pipelined frames, so a connection that
    # saw any flush is dropped (discarding the uncommitted frames) instead of
    # returned. When no flush succeeded there is nothing pipelined and the
    # connection can be returned normally: the FFI return path itself closes
    # (rather than recycles) a conn that latched a terminal transport or
    # protocol error.
    if conn == NULL:
        return False
    return flushed


cdef bint _dataframe_columnar_is_deferred_capacity_error(
        line_sender_error* err) noexcept:
    cdef size_t msg_len = 0
    cdef const char* msg = line_sender_error_msg(err, &msg_len)
    if msg_len < 47:
        return False
    return strncmp(
        msg,
        "column sender deferred flush capacity exhausted",
        47) == 0


cdef void_int _dataframe_columnar_flush(
        direct_column_sender* conn,
        column_sender_chunk* chunk,
        bint retry_after_sync,
        bint* committed_prefix) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_error_code err_code
    cdef bint ok = False
    cdef PyThreadState* gs = NULL
    cdef uint64_t start_ns = 0
    global _dataframe_columnar_flush_calls
    global _dataframe_columnar_flush_ns
    global _dataframe_columnar_flush_retry_syncs

    if _dataframe_columnar_count_io_stats:
        start_ns = time.perf_counter_ns()
    _ensure_doesnt_have_gil(&gs)
    ok = direct_column_sender_flush(conn, chunk, &err)
    _ensure_has_gil(&gs)
    if _dataframe_columnar_count_io_stats:
        _dataframe_columnar_flush_calls += 1
        _dataframe_columnar_flush_ns += time.perf_counter_ns() - start_ns
    if ok:
        return 0

    err_code = line_sender_error_get_code(err)
    if (retry_after_sync and err_code == line_sender_error_invalid_api_call and
            _dataframe_columnar_is_deferred_capacity_error(err)):
        if _dataframe_columnar_count_io_stats:
            _dataframe_columnar_flush_retry_syncs += 1
        line_sender_error_free(err)
        err = NULL
        _dataframe_columnar_sync(conn)
        committed_prefix[0] = True
        if _dataframe_columnar_count_io_stats:
            start_ns = time.perf_counter_ns()
        _ensure_doesnt_have_gil(&gs)
        ok = direct_column_sender_flush(conn, chunk, &err)
        _ensure_has_gil(&gs)
        if _dataframe_columnar_count_io_stats:
            _dataframe_columnar_flush_calls += 1
            _dataframe_columnar_flush_ns += time.perf_counter_ns() - start_ns
        if ok:
            return 0

    raise c_err_to_py(err)


cdef int _arrow_flush_once(
        direct_column_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        ArrowSchema* schema,
        line_sender_column_name* ts_column,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        const column_sender_arrow_override* overrides,
        size_t overrides_len,
        line_sender_error** err) except -1:
    cdef bint ok = False
    cdef PyThreadState* gs = NULL
    cdef uint64_t start_ns = 0
    global _dataframe_columnar_flush_calls
    global _dataframe_columnar_flush_ns

    if _dataframe_columnar_count_io_stats:
        start_ns = time.perf_counter_ns()
    _ensure_doesnt_have_gil(&gs)
    if ts_column != NULL:
        ok = direct_column_sender_flush_arrow_batch_at_column(
            conn, table, array, schema, ts_column[0],
            overrides, overrides_len, err)
    elif at_scalar_set:
        ok = direct_column_sender_flush_arrow_batch_at_scalar_nanos(
            conn, table, array, schema, at_scalar_nanos,
            overrides, overrides_len, err)
    else:
        ok = direct_column_sender_flush_arrow_batch_at_now(
            conn, table, array, schema,
            overrides, overrides_len, err)
    _ensure_has_gil(&gs)
    if _dataframe_columnar_count_io_stats:
        _dataframe_columnar_flush_calls += 1
        _dataframe_columnar_flush_ns += time.perf_counter_ns() - start_ns
    return 1 if ok else 0


cdef void_int _dataframe_arrow_flush_batch(
        direct_column_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        ArrowSchema* schema,
        line_sender_column_name* ts_column,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        const column_sender_arrow_override* overrides,
        size_t overrides_len,
        bint retry_after_sync,
        bint* committed_prefix,
        size_t* deferred_since_sync) except -1:
    cdef line_sender_error* err = NULL
    global _dataframe_columnar_flush_retry_syncs

    if _arrow_flush_once(
            conn, table, array, schema, ts_column,
            at_scalar_set, at_scalar_nanos,
            overrides, overrides_len, &err):
        return 0

    # A batch larger than the server per-batch cap is split into several
    # deferred frames, so the caller's batch counter can undercount the
    # 127-slot in-flight window; commit to drain it and retry once. The
    # failed frame itself never hit the wire (`array` was re-exported;
    # `release` must still be set for the retry to be safe), but earlier
    # frames of a split batch may have: the commit lands them and the
    # retry re-sends them — at-least-once, like any failover replay.
    if (retry_after_sync
            and line_sender_error_get_code(err) ==
                line_sender_error_invalid_api_call
            and _dataframe_columnar_is_deferred_capacity_error(err)
            and array.release != NULL):
        if _dataframe_columnar_count_io_stats:
            _dataframe_columnar_flush_retry_syncs += 1
        line_sender_error_free(err)
        err = NULL
        _dataframe_columnar_sync(conn)
        committed_prefix[0] = True
        deferred_since_sync[0] = 0
        if _arrow_flush_once(
                conn, table, array, schema, ts_column,
                at_scalar_set, at_scalar_nanos,
                overrides, overrides_len, &err):
            return 0

    raise c_err_to_py(err)


def _debug_dataframe_columnar_io_stats(
        object enabled=None,
        bint reset=False):
    """
    Internal benchmark hook for columnar flush/sync timing.
    """
    global _dataframe_columnar_count_io_stats
    global _dataframe_columnar_flush_calls
    global _dataframe_columnar_flush_ns
    global _dataframe_columnar_sync_calls
    global _dataframe_columnar_sync_ns
    global _dataframe_columnar_flush_retry_syncs

    if reset:
        _dataframe_columnar_flush_calls = 0
        _dataframe_columnar_flush_ns = 0
        _dataframe_columnar_sync_calls = 0
        _dataframe_columnar_sync_ns = 0
        _dataframe_columnar_flush_retry_syncs = 0
    if enabled is not None:
        _dataframe_columnar_count_io_stats = bool(enabled)
    return {
        'enabled': _dataframe_columnar_count_io_stats,
        'flush_calls': _dataframe_columnar_flush_calls,
        'flush_s': _dataframe_columnar_flush_ns / 1_000_000_000.0,
        'sync_calls': _dataframe_columnar_sync_calls,
        'sync_s': _dataframe_columnar_sync_ns / 1_000_000_000.0,
        'flush_retry_syncs': _dataframe_columnar_flush_retry_syncs,
    }


def _debug_dataframe_columnar_plan(
        object df,
        *,
        object table_name=None,
        object table_name_col=None,
        object symbols='auto',
        object at=None):
    cdef qdb_pystr_buf* b = qdb_pystr_buf_new()
    cdef dataframe_plan_t plan = dataframe_plan_blank()
    cdef object failures
    try:
        _dataframe_plan_build(
            b,
            df,
            table_name,
            table_name_col,
            symbols,
            at,
            &plan,
            _FIELD_TARGETS_QWP)
        _dataframe_columnar_promote_cols(df, &plan)
        failures = _dataframe_columnar_plan_failures(df, &plan)
        return {
            'supported': not bool(failures),
            'failures': failures,
            'normalizations': _dataframe_columnar_plan_normalizations(
                df,
                &plan),
        }
    finally:
        dataframe_plan_release(&plan)
        qdb_pystr_buf_free(b)


def _bench_dataframe_flush_arrow_batch(
        object arrow_source,
        *,
        object table_name=None,
        object at=None,
        object conf=None,
        size_t iterations=1):
    """
    Internal benchmark hook for `direct_column_sender_flush_arrow_batch_at_now`
    FFI.

    `arrow_source` must expose the Arrow PyCapsule Interface
    (`__arrow_c_stream__`) — pa.RecordBatch, pa.Table, pa.RecordBatchReader,
    pl.DataFrame, or any other Arrow-native container. Pandas frames are
    not accepted here on purpose: this hook benches the Arrow FFI itself,
    not pandas→Arrow conversion. Use `_bench_dataframe_plan_and_populate_
    column_chunks` for the pandas chunk-based path. Intentionally kept out
    of `__all__`.
    """
    cdef size_t iteration
    cdef size_t row_count = 0
    cdef size_t col_count = 0
    cdef size_t completed = 0
    cdef questdb_db* db = NULL
    cdef direct_column_sender* conn = NULL
    cdef line_sender_error* err = NULL
    cdef qdb_pystr_buf* b = NULL
    cdef PyThreadState* gs = NULL
    cdef bytes conf_bytes
    cdef bint any_flushed = False
    cdef bint committed_prefix = False
    cdef size_t deferred_since_sync = 0
    cdef line_sender_table_name c_table_name
    cdef line_sender_column_name c_ts_column
    cdef line_sender_column_name* c_ts_column_ptr = NULL
    cdef ArrowSchema c_schema
    cdef bint at_is_column = False

    if iterations == 0:
        raise ValueError('iterations must be greater than zero')
    if conf is None:
        raise ValueError('conf is required for flush_arrow_batch bench.')
    if not hasattr(arrow_source, '__arrow_c_stream__'):
        raise TypeError(
            '_bench_dataframe_flush_arrow_batch requires an Arrow-native '
            'source exposing __arrow_c_stream__ '
            '(pa.RecordBatch / pa.Table / pl.DataFrame / RecordBatchReader). '
            f'Got {type(arrow_source).__name__}.')
    if not isinstance(table_name, str):
        raise TypeError(
            'table_name must be str for Arrow-native DataFrame input.')
    if at is None or isinstance(at, ServerTimestampType):
        at_is_column = False
    elif isinstance(at, str):
        at_is_column = True
    else:
        raise TypeError(
            'at must be a column name str, ServerTimestamp, or None '
            'for Arrow-native DataFrame input.')

    row_count = int(
        getattr(arrow_source, 'num_rows', None)
        or getattr(arrow_source, 'height', None)
        or 0)
    col_count = int(
        getattr(arrow_source, 'num_columns', None)
        or getattr(arrow_source, 'width', None)
        or 0)

    conf_bytes = conf.encode('utf-8') if isinstance(conf, str) else conf
    _ensure_doesnt_have_gil(&gs)
    db = questdb_db_connect(conf_bytes, len(conf_bytes), &err)
    _ensure_has_gil(&gs)
    if db == NULL:
        raise c_err_to_py(err)
    b = qdb_pystr_buf_new()
    memset(&c_schema, 0, sizeof(ArrowSchema))
    try:
        str_to_table_name(b, <PyObject*>table_name, &c_table_name)
        if at_is_column:
            str_to_column_name(b, at, &c_ts_column)
            c_ts_column_ptr = &c_ts_column

        _ensure_doesnt_have_gil(&gs)
        conn = questdb_db_borrow_direct_column_sender(db, &err)
        _ensure_has_gil(&gs)
        if conn == NULL:
            raise c_err_to_py(err)
        try:
            for iteration in range(iterations):
                _capsule_consume_stream(
                    conn, arrow_source, c_table_name, c_ts_column_ptr,
                    False, 0, &c_schema, NULL, 0, &any_flushed,
                    &deferred_since_sync, &committed_prefix)
            if any_flushed:
                _dataframe_columnar_sync(conn)
            completed = iterations
        except:
            questdb_db_drop_direct_column_sender(db, conn)
            conn = NULL
            raise
        finally:
            if conn != NULL:
                questdb_db_return_direct_column_sender(db, conn)
    finally:
        if c_schema.release != NULL:
            c_schema.release(&c_schema)
        if b != NULL:
            qdb_pystr_buf_free(b)
        if db != NULL:
            questdb_db_close(db)

    return {
        'iterations': iterations,
        'row_count': row_count,
        'col_count': col_count,
        'logical_cells': row_count * col_count,
        'completed': completed,
    }


def _bench_dataframe_plan_and_populate_column_chunks(
        object df,
        *,
        object table_name=None,
        object table_name_col=None,
        object symbols='auto',
        object at=None,
        size_t iterations=1,
        size_t max_rows_per_chunk=16384):
    """
    Internal benchmark hook for Layer 1 pandas columnar work.

    This builds the shared dataframe plan and populates #148 chunks, but it
    never flushes to a sender. It is intentionally kept out of ``__all__``.
    """
    cdef size_t iteration
    cdef qdb_pystr_buf* b = NULL
    cdef dataframe_plan_t plan
    cdef column_sender_chunk* chunk = NULL
    cdef line_sender_error* err = NULL
    cdef uint64_t start_row_path_emissions
    cdef uint64_t end_row_path_emissions
    cdef size_t row_count = 0
    cdef size_t col_count = 0
    cdef size_t populated_rows = 0
    cdef size_t populated_rows_total = 0
    cdef size_t populated_chunks = 0
    cdef size_t rows_per_chunk = 0
    cdef size_t row_offset
    cdef size_t chunk_rows
    global _dataframe_count_row_path_emissions
    global _dataframe_row_path_emissions

    if iterations == 0:
        raise ValueError('iterations must be greater than zero')

    start_row_path_emissions = _dataframe_row_path_emissions
    _dataframe_count_row_path_emissions = True
    try:
        for iteration in range(iterations):
            b = qdb_pystr_buf_new()
            plan = dataframe_plan_blank()
            try:
                _dataframe_plan_build(
                    b,
                    df,
                    table_name,
                    table_name_col,
                    symbols,
                    at,
                    &plan,
                    _FIELD_TARGETS_QWP)
                row_count = plan.row_count
                col_count = plan.col_count
                if (plan.col_count == 0) or (plan.row_count == 0):
                    continue

                _dataframe_columnar_promote_cols(df, &plan)
                _dataframe_columnar_validate_plan(df, &plan)
                _dataframe_columnar_prebuild_pyobj(df, &plan)
                rows_per_chunk = _dataframe_columnar_rows_per_chunk(
                    &plan,
                    max_rows_per_chunk)
                chunk = column_sender_chunk_new(
                    plan.c_table_name.buf,
                    plan.c_table_name.len,
                    &err)
                if chunk == NULL:
                    raise c_err_to_py(err)
                row_offset = 0
                while row_offset < plan.row_count:
                    if not column_sender_chunk_clear(chunk, &err):
                        raise c_err_to_py(err)
                    chunk_rows = rows_per_chunk
                    if chunk_rows > plan.row_count - row_offset:
                        chunk_rows = plan.row_count - row_offset
                    _dataframe_columnar_populate_chunk(
                        &plan,
                        chunk,
                        row_offset,
                        chunk_rows)
                    populated_rows = column_sender_chunk_row_count(chunk, &err)
                    if populated_rows == <size_t>-1:
                        raise c_err_to_py(err)
                    if populated_rows != 0:
                        populated_chunks += 1
                        populated_rows_total += populated_rows
                    row_offset += chunk_rows
            finally:
                if chunk != NULL:
                    column_sender_chunk_free(chunk)
                    chunk = NULL
                dataframe_plan_release(&plan)
                if b != NULL:
                    qdb_pystr_buf_free(b)
                    b = NULL
    finally:
        _dataframe_count_row_path_emissions = False

    end_row_path_emissions = _dataframe_row_path_emissions
    return {
        'iterations': iterations,
        'row_count': row_count,
        'col_count': col_count,
        'logical_cells': row_count * col_count,
        'rows_per_chunk': rows_per_chunk,
        'populated_chunks': populated_chunks,
        'populated_rows_total': populated_rows_total,
        'last_populated_rows': populated_rows,
        'row_path_cell_emissions': (
            end_row_path_emissions - start_row_path_emissions),
    }


cdef object _POLARS = None
cdef object _POLARS_DATAFRAME_T = None
cdef object _POLARS_LAZYFRAME_T = None


cdef bint _try_import_polars():
    global _POLARS, _POLARS_DATAFRAME_T, _POLARS_LAZYFRAME_T
    if _POLARS is not None:
        return True
    try:
        import polars
    except ImportError:
        return False
    _POLARS_DATAFRAME_T = polars.DataFrame
    _POLARS_LAZYFRAME_T = polars.LazyFrame
    _POLARS = polars
    return True


cdef bint _is_polars_dataframe_or_lazy(object obj):
    if not _try_import_polars():
        return False
    return isinstance(obj, (_POLARS_DATAFRAME_T, _POLARS_LAZYFRAME_T))


cdef void_int _capsule_consume_stream(
        direct_column_sender* conn,
        object stream_owner,
        line_sender_table_name c_table_name,
        line_sender_column_name* c_ts_column_ptr,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        ArrowSchema* c_schema,
        const column_sender_arrow_override* c_overrides,
        size_t c_overrides_len,
        bint* any_flushed,
        size_t* deferred_since_sync,
        bint* committed_prefix) except -1:
    # `c_schema` is in/out and owned by the caller: zero-init on first
    # call (this function populates it via get_schema), reused as-is on
    # subsequent calls (Arrow C Data Interface guarantees slices of the
    # same source share schema), and released by the caller.
    cdef object stream_capsule = stream_owner.__arrow_c_stream__()
    if not PyCapsule_IsValid(stream_capsule, b'arrow_array_stream'):
        raise TypeError(
            '__arrow_c_stream__ did not return a valid arrow_array_stream '
            'PyCapsule.')
    cdef ArrowArrayStream* stream = <ArrowArrayStream*>PyCapsule_GetPointer(
        stream_capsule, b'arrow_array_stream')
    cdef ArrowArray batch
    cdef int rc
    cdef const char* stream_err

    if (stream == NULL or stream.get_schema == NULL or
            stream.get_next == NULL or stream.get_last_error == NULL):
        raise TypeError(
            '__arrow_c_stream__ returned a malformed arrow_array_stream '
            'PyCapsule with NULL callbacks.')

    if c_schema.release == NULL:
        rc = stream.get_schema(stream, c_schema)
        if rc != 0:
            stream_err = stream.get_last_error(stream)
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'Arrow stream get_schema failed: '
                f'{stream_err.decode("utf-8", errors="replace") if stream_err != NULL else "unknown"}')

    while True:
        memset(&batch, 0, sizeof(ArrowArray))
        rc = stream.get_next(stream, &batch)
        if rc != 0:
            stream_err = stream.get_last_error(stream)
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'Arrow stream get_next failed: '
                f'{stream_err.decode("utf-8", errors="replace") if stream_err != NULL else "unknown"}')
        if batch.release == NULL:
            break
        try:
            if deferred_since_sync[0] >= _QWP_MAX_DEFERRED_ARROW_FRAMES:
                _dataframe_columnar_sync(conn)
                committed_prefix[0] = True
                deferred_since_sync[0] = 0
            _dataframe_arrow_flush_batch(
                conn, c_table_name, &batch, c_schema, c_ts_column_ptr,
                at_scalar_set, at_scalar_nanos,
                c_overrides, c_overrides_len,
                deferred_since_sync[0] > 0, committed_prefix,
                deferred_since_sync)
            any_flushed[0] = True
            deferred_since_sync[0] += 1
        finally:
            if batch.release != NULL:
                batch.release(&batch)


cdef object _validate_schema_overrides(object schema_overrides):
    """Convert the public schema_overrides dict into a list of
    (name_bytes, kind_int, arg_int) tuples. Returns None if empty.

    Keeping `name_bytes` alive on the Python side lets the C overrides
    array borrow the underlying char* without an extra copy.
    """
    if not schema_overrides:
        return None
    if not isinstance(schema_overrides, dict):
        raise TypeError(
            'schema_overrides must be a dict mapping column name to '
            "one of: 'symbol', 'ipv4', 'char', or ('geohash', bits).")
    cdef list out = []
    cdef object name, override, kind, value
    cdef int kind_int
    cdef int arg_int
    for name, override in schema_overrides.items():
        if not isinstance(name, str):
            raise TypeError(
                f'schema_overrides key must be str, got '
                f'{type(name).__name__}.')
        if isinstance(override, str):
            kind = override
            value = None
        elif isinstance(override, tuple) and len(override) == 2:
            kind, value = override
        else:
            raise TypeError(
                f'schema_overrides[{name!r}] has invalid shape '
                f'{override!r}; expected str or (kind, value) tuple.')
        arg_int = 0
        if kind == 'symbol':
            kind_int = <int>column_sender_arrow_override_symbol
        elif kind == 'ipv4':
            kind_int = <int>column_sender_arrow_override_ipv4
        elif kind == 'char':
            kind_int = <int>column_sender_arrow_override_char
        elif kind == 'geohash':
            if not isinstance(value, int) or value < 1 or value > 60:
                raise ValueError(
                    f'schema_overrides[{name!r}] geohash bits must '
                    f'be int in 1..=60, got {value!r}.')
            kind_int = <int>column_sender_arrow_override_geohash
            arg_int = value
        else:
            raise ValueError(
                f'schema_overrides[{name!r}] kind {kind!r} not '
                "in {'symbol', 'ipv4', 'char', 'geohash'}.")
        out.append((name.encode('utf-8'), kind_int, arg_int))
    return out


cdef object _capsule_get_column_names(object sliceable):
    """Return list of str column names from polars / pyarrow input,
    or None if the input doesn't expose a uniform name list."""
    cdef object names
    names = getattr(sliceable, 'column_names', None)
    if names is not None:
        return list(names)
    names = getattr(sliceable, 'columns', None)
    if names is not None:
        return list(names)
    return None


cdef object _capsule_at_index_to_name(object sliceable, int idx):
    """Resolve an integer `at` column index (negative allowed) to its name,
    mirroring the numpy path's `_bind_col_index`."""
    cdef object names = _capsule_get_column_names(sliceable)
    cdef int orig = idx
    cdef int n
    if names is None:
        raise TypeError(
            'Cannot resolve an integer `at` index for this Arrow-native '
            'input; pass the designated timestamp column name as a str.')
    n = <int>len(names)
    if idx < 0:
        idx += n
    if idx < 0 or idx >= n:
        raise IndexError(f'Bad argument `at`: {orig} index out of range')
    return names[idx]


cdef bint _capsule_polars_dtype_is_string_like(object dtype) except -1:
    """polars: Utf8 / String / Categorical / Enum count as string-like."""
    if _POLARS is None:
        return False
    if dtype == _POLARS.Utf8:
        return True
    if isinstance(dtype, _POLARS.Categorical):
        return True
    cdef object enum_t = getattr(_POLARS, 'Enum', None)
    if enum_t is not None and isinstance(dtype, enum_t):
        return True
    return False


cdef bint _capsule_pyarrow_type_is_string_like(object field_type) except -1:
    """pyarrow: utf8 / large_utf8 / utf8_view, plus Dictionary whose
    value type is one of those."""
    if _PYARROW is None:
        return False
    if (_PYARROW.types.is_string(field_type)
            or _PYARROW.types.is_large_string(field_type)):
        return True
    if _PYARROW.types.is_dictionary(field_type):
        value_type = field_type.value_type
        if (_PYARROW.types.is_string(value_type)
                or _PYARROW.types.is_large_string(value_type)):
            return True
    return False


cdef bint _capsule_pandas_dtype_is_string_like(object dtype) except -1:
    cdef object storage
    cdef object arrow_type
    cdef object cat_dtype
    if _PANDAS is None:
        return False
    if isinstance(dtype, _PANDAS.StringDtype):
        storage = getattr(dtype, 'storage', None)
        return storage == 'pyarrow'
    if isinstance(dtype, _PANDAS.ArrowDtype):
        _dataframe_require_pyarrow()
        arrow_type = dtype.pyarrow_dtype
        return _capsule_pyarrow_type_is_string_like(arrow_type)
    if isinstance(dtype, _PANDAS.CategoricalDtype):
        cat_dtype = dtype.categories.dtype
        if cat_dtype == object:
            return True
        return _capsule_pandas_dtype_is_string_like(cat_dtype)
    return False


cdef object _capsule_get_string_column_names(object sliceable):
    """Return names of all string-like columns (utf8 / large_utf8 /
    utf8_view / dict-of-utf8). Supports polars DataFrame and pyarrow
    Table / RecordBatch. Returns None if schema introspection is not
    available on the input."""
    cdef object schema
    cdef object out
    cdef object name
    cdef object dtype
    cdef object field_type
    cdef int i
    if _is_pandas_dataframe_object(sliceable):
        _dataframe_may_import_deps()
        out = []
        for name, dtype in sliceable.dtypes.items():
            if _capsule_pandas_dtype_is_string_like(dtype):
                out.append(name)
        return out
    if _POLARS is not None and isinstance(sliceable, _POLARS_DATAFRAME_T):
        out = []
        for name, dtype in sliceable.schema.items():
            if _capsule_polars_dtype_is_string_like(dtype):
                out.append(name)
        return out
    if _PYARROW is None:
        try:
            _dataframe_require_pyarrow()
        except ImportError:
            return None
    if isinstance(sliceable, (_PYARROW.Table, _PYARROW.RecordBatch)):
        schema = sliceable.schema
        out = []
        for i in range(len(schema.names)):
            field_type = schema.field(i).type
            if _capsule_pyarrow_type_is_string_like(field_type):
                out.append(schema.names[i])
        return out
    return None


cdef object _capsule_column_is_string_like(object sliceable, str name):
    """Returns True iff `name` is a string-like column on `sliceable`,
    False iff it is some other type, or None if schema introspection
    is not available on the input."""
    cdef object dtype
    cdef object field_type
    if _is_pandas_dataframe_object(sliceable):
        _dataframe_may_import_deps()
        try:
            dtype = sliceable.dtypes[name]
        except KeyError:
            raise KeyError(
                f'symbols column {name!r} not found in the dataframe.')
        return _capsule_pandas_dtype_is_string_like(dtype)
    if _POLARS is not None and isinstance(sliceable, _POLARS_DATAFRAME_T):
        try:
            dtype = sliceable.schema[name]
        except KeyError:
            raise KeyError(
                f'symbols column {name!r} not found in the dataframe.')
        return _capsule_polars_dtype_is_string_like(dtype)
    if _PYARROW is None:
        try:
            _dataframe_require_pyarrow()
        except ImportError:
            return None
    if isinstance(sliceable, (_PYARROW.Table, _PYARROW.RecordBatch)):
        try:
            field_type = sliceable.schema.field(name).type
        except (KeyError, ValueError):
            raise KeyError(
                f'symbols column {name!r} not found in the dataframe.')
        return _capsule_pyarrow_type_is_string_like(field_type)
    return None


cdef object _capsule_get_dict_string_column_names(object sliceable):
    """Return names of dict-encoded string-like columns (polars
    Categorical / Enum or pyarrow Dictionary(*, utf8/large_utf8)).
    Returns None if schema introspection is not available."""
    cdef object schema
    cdef object out
    cdef object name
    cdef object dtype
    cdef object field_type
    cdef object value_type
    cdef object enum_t
    cdef int i
    if _is_pandas_dataframe_object(sliceable):
        _dataframe_may_import_deps()
        out = []
        for name, dtype in sliceable.dtypes.items():
            if (isinstance(dtype, _PANDAS.CategoricalDtype)
                    and _capsule_pandas_dtype_is_string_like(dtype)):
                out.append(name)
            elif isinstance(dtype, _PANDAS.ArrowDtype):
                _dataframe_require_pyarrow()
                field_type = dtype.pyarrow_dtype
                if _PYARROW.types.is_dictionary(field_type):
                    value_type = field_type.value_type
                    if (_PYARROW.types.is_string(value_type)
                            or _PYARROW.types.is_large_string(value_type)):
                        out.append(name)
        return out
    if _POLARS is not None and isinstance(sliceable, _POLARS_DATAFRAME_T):
        out = []
        enum_t = getattr(_POLARS, 'Enum', None)
        for name, dtype in sliceable.schema.items():
            if isinstance(dtype, _POLARS.Categorical):
                out.append(name)
            elif enum_t is not None and isinstance(dtype, enum_t):
                out.append(name)
        return out
    if _PYARROW is None:
        try:
            _dataframe_require_pyarrow()
        except ImportError:
            return None
    if isinstance(sliceable, (_PYARROW.Table, _PYARROW.RecordBatch)):
        schema = sliceable.schema
        out = []
        for i in range(len(schema.names)):
            field_type = schema.field(i).type
            if _PYARROW.types.is_dictionary(field_type):
                value_type = field_type.value_type
                if (_PYARROW.types.is_string(value_type)
                        or _PYARROW.types.is_large_string(value_type)):
                    out.append(schema.names[i])
        return out
    return None


cdef object _resolve_symbols_to_overrides(object sliceable, object symbols):
    """Translate `symbols` into a list of
    (name_bytes, kind, arg) tuples matching the shape returned by
    _validate_schema_overrides. `kind` is `column_sender_arrow_override_symbol`
    to mark a column as SYMBOL or `column_sender_arrow_override_not_symbol`
    to force a dict-encoded column to VARCHAR; `arg` is unused (0) for both.
    Returns:

    - []   for None / 'auto' (no overrides, Rust default applies —
           Dictionary columns auto-classify as SymbolDict).
    - list for True (auto-detect str cols) / False (force NotSymbol
           on every dict-encoded str col) / List[str] / List[int].
    - None if resolution requires introspection not available on the
           input; caller falls back to Manual plan.

    Raises QuestDBError(BadDataFrame) when an explicitly-named symbols
    entry targets a non-string column (matches Manual plan semantics).
    """
    cdef list out
    cdef int symbol_kind = <int>column_sender_arrow_override_symbol
    cdef int not_symbol_kind = <int>column_sender_arrow_override_not_symbol
    cdef object col_names
    cdef object entry
    cdef object name
    cdef object is_str
    cdef int idx
    cdef set listed
    cdef object dict_names

    if symbols is None or symbols == 'auto':
        return []

    if symbols is False:
        col_names = _capsule_get_dict_string_column_names(sliceable)
        if col_names is None:
            return None
        out = []
        for entry in col_names:
            out.append((entry.encode('utf-8'), not_symbol_kind, 0))
        return out

    if symbols is True:
        col_names = _capsule_get_string_column_names(sliceable)
        if col_names is None:
            return None
        out = []
        for entry in col_names:
            out.append((entry.encode('utf-8'), symbol_kind, 0))
        return out

    if not isinstance(symbols, (list, tuple)):
        return None

    out = []
    col_names = None
    listed = set()
    for entry in symbols:
        if isinstance(entry, str):
            name = entry
        elif isinstance(entry, int):
            if col_names is None:
                col_names = _capsule_get_column_names(sliceable)
                if col_names is None:
                    return None
            idx = <int>entry
            if idx < 0:
                idx += len(col_names)
            if idx < 0 or idx >= len(col_names):
                raise ValueError(
                    f'symbols index {entry} out of range '
                    f'(have {len(col_names)} columns).')
            name = col_names[idx]
        else:
            raise TypeError(
                f'symbols entry must be str or int, got '
                f'{type(entry).__name__}.')
        is_str = _capsule_column_is_string_like(sliceable, name)
        if is_str is None:
            return None
        if not is_str:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad argument `symbols`: column {name!r} is not a '
                f'strings column.')
        listed.add(name)
        out.append((name.encode('utf-8'), symbol_kind, 0))

    # Match the row/numpy planner: an explicit symbols list marks only the
    # listed columns as symbols; every other dict-encoded (categorical)
    # column is forced to a plain VARCHAR field rather than auto-symbolized.
    dict_names = _capsule_get_dict_string_column_names(sliceable)
    if dict_names is None:
        return None
    for name in dict_names:
        if name not in listed:
            out.append((name.encode('utf-8'), not_symbol_kind, 0))
    return out


cdef object _merge_capsule_overrides(
        object symbol_overrides, object validated_overrides):
    """Merge symbol overrides into validated schema_overrides.
    schema_overrides take precedence on name collision."""
    cdef set explicit_names
    cdef list merged
    cdef object entry
    if not symbol_overrides and validated_overrides is None:
        return None
    if not symbol_overrides:
        return validated_overrides
    if validated_overrides is None:
        return symbol_overrides
    explicit_names = {entry[0] for entry in validated_overrides}
    merged = list(validated_overrides)
    for entry in symbol_overrides:
        if entry[0] not in explicit_names:
            merged.append(entry)
    return merged


cdef bint _is_pandas_dataframe_object(object obj):
    cdef object cls
    cdef object module
    cdef object name
    if _PANDAS is not None and isinstance(obj, _PANDAS.DataFrame):
        return True
    try:
        for cls in type(obj).__mro__:
            module = getattr(cls, '__module__', '')
            name = getattr(cls, '__name__', '')
            if (name == 'DataFrame' and
                    isinstance(module, str) and
                    (module == 'pandas' or module.startswith('pandas.'))):
                return True
    except Exception:
        return False
    return False


cdef object _MASKED_DTYPE = None
cdef bint _MASKED_DTYPE_READY = False


cdef object _pandas_masked_dtype():
    global _MASKED_DTYPE, _MASKED_DTYPE_READY
    if not _MASKED_DTYPE_READY:
        try:
            from pandas.core.arrays.masked import BaseMaskedDtype
            _MASKED_DTYPE = BaseMaskedDtype
        except Exception:
            _MASKED_DTYPE = ()
        _MASKED_DTYPE_READY = True
    return _MASKED_DTYPE


cdef bint _pandas_dataframe_requires_manual_planner(object df) except -1:
    # A fully Arrow-backed frame takes the zero-copy capsule path; any
    # numpy / object / masked / categorical column routes the whole frame to
    # the manual planner (which ingests those directly and the Arrow-backed
    # columns via the arrow-import path).
    cdef object dtype
    cdef object arrow_dtype
    if not _is_pandas_dataframe_object(df):
        return False
    _dataframe_may_import_deps()
    arrow_dtype = getattr(_PANDAS, 'ArrowDtype', None)
    try:
        for dtype in df.dtypes:
            if arrow_dtype is not None and isinstance(dtype, arrow_dtype):
                continue
            if isinstance(dtype, _PANDAS.StringDtype):
                if getattr(dtype, 'storage', None) == 'pyarrow':
                    continue
            return True
    except Exception:
        return True
    return False


cdef bint _pandas_dataframe_is_timestamp_only_at(
        object df,
        object at) except -1:
    if not _is_pandas_dataframe_object(df) or not isinstance(at, str):
        return False
    try:
        return len(df.columns) == 1 and df.columns[0] == at
    except Exception:
        return False


cdef Py_ssize_t _capsule_row_count(object sliceable) except -2:
    cdef object row_count_obj = getattr(sliceable, 'num_rows', None)
    if row_count_obj is None:
        row_count_obj = getattr(sliceable, 'height', None)
    if row_count_obj is not None:
        return <Py_ssize_t>row_count_obj
    if _is_pandas_dataframe_object(sliceable):
        return <Py_ssize_t>len(sliceable)
    return -1


cdef object _capsule_slice_rows(
        object sliceable,
        Py_ssize_t offset,
        Py_ssize_t row_count):
    if hasattr(sliceable, 'slice'):
        return sliceable.slice(offset, row_count)
    if _is_pandas_dataframe_object(sliceable):
        return sliceable.iloc[offset:offset + row_count]
    return None


cdef struct direct_conn_source_t:
    # Pooled borrow when ``db != NULL``; otherwise a poolless standalone
    # connection opened from ``opts`` per call.
    questdb_db* db
    const line_sender_opts* opts


cdef direct_column_sender* _direct_conn_open(
        direct_conn_source_t* src,
        uint64_t budget_ms,
        line_sender_error** err) noexcept nogil:
    if src.db != NULL:
        if budget_ms == 0:
            return questdb_db_borrow_direct_column_sender(src.db, err)
        return questdb_db_borrow_direct_column_sender_with_retry(
            src.db, budget_ms, err)
    return direct_column_sender_from_opts(src.opts, err)


cdef void _direct_conn_close(
        direct_conn_source_t* src,
        direct_column_sender* conn,
        bint force_drop) noexcept nogil:
    if src.db != NULL:
        if force_drop:
            questdb_db_drop_direct_column_sender(src.db, conn)
        else:
            questdb_db_return_direct_column_sender(src.db, conn)
    elif force_drop:
        questdb_db_drop_direct_column_sender(NULL, conn)
    else:
        direct_column_sender_free(conn)


cdef bint _dataframe_client_try_capsule_path(
        direct_conn_source_t* src,
        uint64_t budget_ms,
        object df,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        size_t max_rows_per_batch,
        object validated_overrides,
        bint* committed_prefix,
        bint* nonreplayable_consumed) except -1:
    cdef qdb_pystr_buf* b = NULL
    cdef direct_column_sender* conn = NULL
    cdef line_sender_error* err = NULL
    cdef PyThreadState* gs = NULL
    cdef object sliceable = None
    cdef bint any_flushed = False
    cdef size_t deferred_since_sync = 0
    cdef bint force_drop_conn = False
    cdef object row_slice = None
    cdef Py_ssize_t total_rows = 0
    cdef Py_ssize_t offset = 0
    cdef Py_ssize_t chunk_rows
    cdef object symbol_overrides
    cdef object merged_overrides
    cdef bint can_slice = False
    cdef line_sender_table_name c_table_name
    cdef line_sender_column_name c_ts_column
    cdef line_sender_column_name* c_ts_column_ptr = NULL
    cdef ArrowSchema c_schema
    cdef column_sender_arrow_override* c_overrides = NULL
    cdef size_t c_overrides_len = 0
    cdef bint at_is_column = False
    cdef bint at_scalar_set = False
    cdef int64_t at_scalar_nanos = 0
    cdef size_t i
    cdef object name_bytes
    cdef int kind_int
    cdef int arg_int

    if _pandas_dataframe_requires_manual_planner(df):
        return False
    if _pandas_dataframe_is_timestamp_only_at(df, at):
        return False
    if table_name_col is not None:
        return False

    # LazyFrame: prefer the streaming engine (polars 1.0+) for lower
    # peak memory. `LazyFrame.collect_batches()` would stream natively
    # but upstream marks it unstable and "much slower than native sinks",
    # so we materialize and slice downstream.
    if _is_polars_dataframe_or_lazy(df) and isinstance(
            df, _POLARS_LAZYFRAME_T):
        try:
            sliceable = df.collect(engine='streaming')
        except TypeError:
            sliceable = df.collect()
    elif hasattr(df, '__arrow_c_stream__'):
        sliceable = df
    elif hasattr(df, '__arrow_c_array__'):
        _dataframe_require_pyarrow()
        sliceable = _PYARROW.Table.from_batches(
            [_PYARROW.record_batch(df)])
    else:
        return False

    total_rows = _capsule_row_count(sliceable)

    if not isinstance(table_name, str):
        raise TypeError(
            'table_name must be str for Arrow-native DataFrame input.')
    if at is None or isinstance(at, ServerTimestampType):
        at_is_column = False
    elif isinstance(at, TimestampNanos):
        at_scalar_set = True
        at_scalar_nanos = (<TimestampNanos>at)._value
    elif isinstance(at, datetime.datetime):
        at_scalar_set = True
        at_scalar_nanos = datetime_to_nanos(at)
    elif isinstance(at, str):
        at_is_column = True
    elif isinstance(at, int) and not isinstance(at, bool):
        at = _capsule_at_index_to_name(sliceable, at)
        at_is_column = True
    else:
        raise TypeError(
            'at must be a column name str, int index, TimestampNanos, '
            'datetime, ServerTimestamp, or None for Arrow-native DataFrame '
            'input.')

    # An empty frame is a no-op: emit nothing and skip symbol-shape
    # validation, which is moot with zero rows.
    if total_rows == 0:
        return True

    symbol_overrides = _resolve_symbols_to_overrides(sliceable, symbols)
    if symbol_overrides is None:
        return False
    merged_overrides = _merge_capsule_overrides(
        symbol_overrides, validated_overrides)

    can_slice = (total_rows >= 0) and (
        hasattr(sliceable, 'slice')
        or _is_pandas_dataframe_object(sliceable))

    b = qdb_pystr_buf_new()
    memset(&c_schema, 0, sizeof(ArrowSchema))
    try:
        str_to_table_name(b, <PyObject*>table_name, &c_table_name)
        if at_is_column:
            str_to_column_name(b, at, &c_ts_column)
            c_ts_column_ptr = &c_ts_column

        if merged_overrides is not None:
            c_overrides_len = len(merged_overrides)
            c_overrides = <column_sender_arrow_override*>calloc(
                c_overrides_len, sizeof(column_sender_arrow_override))
            if c_overrides == NULL:
                raise MemoryError()
            for i in range(c_overrides_len):
                name_bytes, kind_int, arg_int = merged_overrides[i]
                c_overrides[i].column = PyBytes_AsString(name_bytes)
                c_overrides[i].column_len = PyBytes_GET_SIZE(name_bytes)
                c_overrides[i].kind = <uint32_t>kind_int
                c_overrides[i].arg = <uint32_t>arg_int

        _ensure_doesnt_have_gil(&gs)
        conn = _direct_conn_open(src, budget_ms, &err)
        _ensure_has_gil(&gs)
        if conn == NULL:
            raise c_err_to_py(err)

        try:
            if not can_slice:
                # A one-shot stream (e.g. RecordBatchReader) cannot be
                # re-exported for a whole-frame replay: a failed attempt
                # must surface instead of retrying with the drained rest.
                nonreplayable_consumed[0] = True
                _capsule_consume_stream_with_hint(
                    conn, sliceable, c_table_name, c_ts_column_ptr,
                    at_scalar_set, at_scalar_nanos,
                    &c_schema, c_overrides, c_overrides_len,
                    &any_flushed, &deferred_since_sync,
                    committed_prefix, max_rows_per_batch, False)
            else:
                offset = 0
                while offset < total_rows:
                    chunk_rows = max_rows_per_batch
                    if chunk_rows > total_rows - offset:
                        chunk_rows = total_rows - offset
                    row_slice = _capsule_slice_rows(
                        sliceable, offset, chunk_rows)
                    _capsule_consume_stream_with_hint(
                        conn, row_slice, c_table_name, c_ts_column_ptr,
                        at_scalar_set, at_scalar_nanos,
                        &c_schema, c_overrides, c_overrides_len,
                        &any_flushed, &deferred_since_sync,
                        committed_prefix, max_rows_per_batch, True)
                    offset += chunk_rows
            if any_flushed:
                _dataframe_columnar_sync(conn)
        except:
            force_drop_conn = _dataframe_columnar_force_drop_after_error(
                conn, any_flushed)
            raise

        return True
    finally:
        _ensure_has_gil(&gs)
        if conn != NULL:
            with nogil:
                _direct_conn_close(src, conn, force_drop_conn)
        if c_schema.release != NULL:
            c_schema.release(&c_schema)
        if c_overrides != NULL:
            free(c_overrides)
        if b != NULL:
            qdb_pystr_buf_free(b)


cdef void_int _dataframe_numpy_publish(
        direct_conn_source_t* src,
        uint64_t budget_ms,
        qdb_pystr_buf* b,
        dataframe_plan_t* plan,
        object df,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        size_t max_rows_per_batch,
        bint* committed_prefix) except -1:
    cdef column_sender_chunk* chunk = NULL
    cdef direct_column_sender* conn = NULL
    cdef line_sender_error* err = NULL
    cdef PyThreadState* gs = NULL
    cdef bint flushed = False
    cdef bint force_drop_conn = False
    cdef size_t rows_per_chunk
    cdef size_t row_offset
    cdef size_t chunk_rows
    try:
        df = _dataframe_normalize_nullable(df)
        df = _dataframe_normalize_at_timestamp(df, at)
        _dataframe_plan_build(
            b,
            df,
            table_name,
            table_name_col,
            symbols,
            at,
            plan,
            _FIELD_TARGETS_QWP)
        if (plan.col_count == 0) or (plan.row_count == 0):
            return 0

        _dataframe_apply_roundtrip_overrides(df, plan)
        _dataframe_columnar_promote_cols(df, plan)
        _dataframe_columnar_validate_plan(df, plan)
        _dataframe_columnar_prebuild_pyobj(df, plan)
        rows_per_chunk = _dataframe_columnar_rows_per_chunk(
            plan, max_rows_per_batch)

        _ensure_doesnt_have_gil(&gs)
        conn = _direct_conn_open(src, budget_ms, &err)
        _ensure_has_gil(&gs)
        if conn == NULL:
            raise c_err_to_py(err)

        chunk = column_sender_chunk_new(
            plan.c_table_name.buf,
            plan.c_table_name.len,
            &err)
        if chunk == NULL:
            raise c_err_to_py(err)
        try:
            row_offset = 0
            while row_offset < plan.row_count:
                if not column_sender_chunk_clear(chunk, &err):
                    raise c_err_to_py(err)
                chunk_rows = rows_per_chunk
                if chunk_rows > plan.row_count - row_offset:
                    chunk_rows = plan.row_count - row_offset
                _dataframe_columnar_populate_chunk(
                    plan,
                    chunk,
                    row_offset,
                    chunk_rows)
                _dataframe_columnar_flush(
                    conn,
                    chunk,
                    row_offset != 0,
                    committed_prefix)
                flushed = True
                row_offset += chunk_rows

            _dataframe_columnar_sync(conn)
        except:
            force_drop_conn = _dataframe_columnar_force_drop_after_error(
                conn, flushed)
            raise

        return 0
    finally:
        _ensure_has_gil(&gs)
        if conn != NULL:
            with nogil:
                _direct_conn_close(src, conn, force_drop_conn)
        if chunk != NULL:
            column_sender_chunk_free(chunk)
        # The plan is rebuilt on each failover attempt; release this
        # attempt's plan so a re-send starts from a blank plan.
        dataframe_plan_release(plan)
        plan[0] = dataframe_plan_blank()


cdef void_int _direct_dataframe_run(
        direct_conn_source_t* src,
        double reconnect_max_s,
        qdb_pystr_buf* b,
        dataframe_plan_t* plan,
        object df,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        size_t max_rows_per_batch,
        object schema_overrides) except -1:
    cdef uint64_t budget_ms = 0
    cdef double deadline = 0.0
    cdef double remaining = 0.0
    cdef bint committed_prefix = False
    cdef bint nonreplayable_consumed = False
    cdef object validated_overrides = _validate_schema_overrides(
        schema_overrides)
    if max_rows_per_batch <= 0:
        raise ValueError('max_rows_per_batch must be >= 1.')
    if isinstance(at, datetime.datetime):
        if at != at:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                'Bad argument `at`: NaT is not a valid timestamp.')
        try:
            at_micros = datetime_to_micros(at)
        except ValueError as ve:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                f'Bad argument `at`: {ve}') from ve
        if at_micros < 0:
            raise ValueError(
                'Bad argument `at`: Cannot use a datetime before the '
                'Unix epoch (1970-01-01 00:00:00).')
    if not isinstance(
            at,
            (str, ServerTimestampType, TimestampNanos,
             datetime.datetime)) and not (
            isinstance(at, int) and not isinstance(at, bool)):
        raise QuestDBError(
            QuestDBErrorCode.InvalidTimestamp,
            'dataframe() requires `at` to be the designated timestamp '
            'column (by name or index), a fixed timestamp shared by every '
            'row (TimestampNanos / datetime), or the explicit '
            '`ServerTimestamp` sentinel to let the server assign each '
            'row\'s timestamp on arrival.')
    # A zero budget (standalone from-conf source) makes a single attempt:
    # a transient failure surfaces immediately rather than re-dialling.
    deadline = time.monotonic() + reconnect_max_s
    while True:
        # Reclaim string storage from a prior attempt's released plan.
        qdb_pystr_buf_clear(b)
        try:
            if _dataframe_client_try_capsule_path(
                    src,
                    budget_ms,
                    df,
                    table_name,
                    table_name_col,
                    symbols,
                    at,
                    max_rows_per_batch,
                    validated_overrides,
                    &committed_prefix,
                    &nonreplayable_consumed):
                return 0
            if validated_overrides is not None:
                raise UnsupportedDataFrameShapeError(
                    'schema_overrides requires the Arrow columnar path: '
                    'fully Arrow-backed input (pyarrow / polars, or pandas '
                    'where every column uses ArrowDtype) without '
                    '`table_name_col`. This input falls back to the NumPy '
                    'planner, which does not apply schema_overrides; '
                    'convert the frame, e.g. '
                    "df.convert_dtypes(dtype_backend='pyarrow'), or drop "
                    'schema_overrides.')
            _dataframe_numpy_publish(
                src, budget_ms, b, plan, df, table_name,
                table_name_col, symbols, at, max_rows_per_batch,
                &committed_prefix)
            return 0
        except QuestDBError as exc:
            # FailoverRetry = transient flush/sync; SocketError = a
            # re-borrow that has not reached a live primary yet.
            if exc.code not in (
                    QuestDBErrorCode.FailoverRetry,
                    QuestDBErrorCode.SocketError):
                raise
            # The native operation may have committed a split prefix, or an
            # explicit intermediate sync already committed one. Restarting
            # from row 0 would duplicate it.
            if exc.in_doubt or committed_prefix:
                raise
            # A drained one-shot stream has no rows left to replay: retrying
            # would report success while writing nothing.
            if nonreplayable_consumed:
                raise QuestDBError(
                    exc.code,
                    f'{exc} The input stream was already partially '
                    f'consumed and cannot be replayed; retry with a '
                    f'fresh reader.',
                    in_doubt=exc.in_doubt) from exc
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                raise
            budget_ms = <uint64_t>(remaining * 1000.0)


cdef void_int _capsule_consume_stream_with_hint(
        direct_column_sender* conn,
        object stream_owner,
        line_sender_table_name c_table_name,
        line_sender_column_name* c_ts_column_ptr,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        ArrowSchema* c_schema,
        const column_sender_arrow_override* c_overrides,
        size_t c_overrides_len,
        bint* any_flushed,
        size_t* deferred_since_sync,
        bint* committed_prefix,
        size_t max_rows_per_batch,
        bint can_slice) except -1:
    cdef str hint
    try:
        _capsule_consume_stream(
            conn, stream_owner, c_table_name, c_ts_column_ptr,
            at_scalar_set, at_scalar_nanos, c_schema,
            c_overrides, c_overrides_len, any_flushed,
            deferred_since_sync, committed_prefix)
    except QuestDBError as exc:
        if _is_batch_too_large_error(exc):
            if exc.code == QuestDBErrorCode.BatchTooLarge:
                # The core already split the chunk as far as it can: a single
                # row (table schema plus one row's values) still exceeds the
                # server's per-batch cap, so a smaller batch cannot help.
                hint = (
                    'a single row exceeds the server per-batch cap '
                    '(max_buf_size / the server X-QWP-Max-Batch-Size); reduce '
                    'the size of individual values or raise the cap. '
                    '`max_rows_per_batch` does not bound a single row.')
            elif can_slice:
                hint = (
                    f'reduce `max_rows_per_batch` (current: '
                    f'{max_rows_per_batch}) and retry.')
            else:
                hint = (
                    f'this is a streaming Arrow source (e.g. '
                    f'pa.RecordBatchReader); batch size is set by the '
                    f'producer and `max_rows_per_batch` (current: '
                    f'{max_rows_per_batch}) does not bound it. '
                    f'Materialise to a `pa.Table` '
                    f'(`pa.Table.from_batches(reader)`) or re-batch '
                    f'at the source before passing.')
            raise QuestDBError(
                exc.code, f'{exc}\nHint: {hint}') from exc
        raise


cdef bint _is_batch_too_large_error(object exc):
    cdef str msg
    if not isinstance(exc, QuestDBError):
        return False
    if exc.code == QuestDBErrorCode.BatchTooLarge:
        return True
    # Fallback for encoder-level "too large" errors that carry no dedicated
    # code (MAX_CHUNK_ROWS row-count overflow, an oversize single value).
    msg = str(exc).lower()
    return (
        ('row_count' in msg and ('exceeds' in msg or 'too large' in msg))
        or 'batch too large' in msg)


cdef class QuestDB:
    """
    Handle to a QuestDB deployment over QWP/WebSocket.

    Owns the connection pool; lends row-building senders via
    :meth:`sender`, bulk-loads DataFrames via :meth:`dataframe`, runs
    queries via :meth:`query`, and lends reader leases via
    :meth:`reader`. Construct with :func:`questdb.connect`.
    Instances are safe to share across threads.
    """
    cdef questdb_db* _db
    cdef object _conf_str
    cdef object _state_cond
    cdef size_t _active_uses
    cdef bint _closing
    cdef object _connection_listener

    def __cinit__(self):
        self._db = NULL
        self._conf_str = None
        self._state_cond = threading.Condition(threading.RLock())
        self._active_uses = 0
        self._closing = False
        self._connection_listener = None

    cdef questdb_db* _begin_db_use(self, str method) except? NULL:
        cdef questdb_db* db = NULL
        self._state_cond.acquire()
        try:
            db = self._db
            if db == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    f"{method}() can't be called: QuestDB is closed.")
            self._active_uses += 1
            return db
        finally:
            self._state_cond.release()

    cdef void _end_db_use(self) except *:
        self._state_cond.acquire()
        try:
            if self._active_uses == 0:
                raise RuntimeError('QuestDB use counter underflow.')
            self._active_uses -= 1
            if self._active_uses == 0:
                self._state_cond.notify_all()
        finally:
            self._state_cond.release()

    @staticmethod
    def from_conf(
            str conf_str,
            *,
            connection_listener=None,
            connection_event_inbox_capacity: int = 0):
        """
        Construct a handle from a QWP/WebSocket configuration string.

        Prefer the :func:`questdb.connect` module-level factory.

        The underlying connection pool is opened by `questdb_db_connect`, or
        `questdb_db_connect_with_event_handler` when a connection listener is
        supplied.
        Dataframe ingestion always uses the direct (non-store-and-forward)
        QWP/WebSocket column sender, independent of ``sf_dir``. On a transient
        connection failure the frame is re-sent from the caller's DataFrame
        only when the failed operation is provably not delivered. A
        delivery-unknown failure surfaces as :class:`QuestDBError` with
        ``in_doubt`` set, because blindly re-sending could duplicate rows.
        ``request_timeout`` bounds each commit's no-progress ack wait;
        ``request_timeout=0`` disables that deadline, so a stalled but
        connected server can block :meth:`dataframe` indefinitely.

        ``connection_listener``, when set, is a callable receiving one
        :class:`ConnectionEvent` per connection-state transition of the
        pool's ingress connections (initial connect, per-endpoint attempt
        failures, failover, terminal auth rejection). It runs on a
        dedicated dispatcher thread fed by a bounded inbox
        (``connection_event_inbox_capacity``; ``0`` selects the default
        of 64) with a drop-oldest overflow policy, so a slow listener
        cannot stall ingest or reconnects. Exceptions it raises are
        logged and swallowed. Dropped/delivered totals are available via
        :attr:`connection_events_dropped` /
        :attr:`connection_events_delivered`.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_utf8 c_conf
        cdef object protocol
        cdef dict params
        cdef qdb_pystr_buf* b = qdb_pystr_buf_new()
        cdef QuestDB db = QuestDB.__new__(QuestDB)
        cdef PyThreadState* gs = NULL
        cdef void* connection_listener_data = NULL
        try:
            protocol, params = parse_conf_str(b, conf_str)
            if protocol not in (Protocol.Ws, Protocol.Wss):
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    'questdb.connect() requires a QWP/WebSocket '
                    'configuration string: ws:: or wss::.')
            if params.get('addr') is None:
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    'Missing "addr" parameter in config string')

            if connection_listener is not None and not callable(
                    connection_listener):
                raise TypeError(
                    '"connection_listener" must be callable or None, '
                    f'not {_fqn(type(connection_listener))}')
            str_to_utf8(b, <PyObject*>conf_str, &c_conf)
            if connection_listener is not None:
                # Register as part of pool construction so recovery senders
                # pre-opened by connect cannot emit events before the listener
                # exists. The handle keeps the callback target alive until
                # close() has stopped and joined the dispatcher.
                db._connection_listener = connection_listener
                connection_listener_data = <void*>db._connection_listener
            _ensure_doesnt_have_gil(&gs)
            if connection_listener_data == NULL:
                db._db = questdb_db_connect(c_conf.buf, c_conf.len, &err)
            else:
                db._db = questdb_db_connect_with_event_handler(
                    c_conf.buf,
                    c_conf.len,
                    _connection_event_trampoline,
                    connection_listener_data,
                    <size_t>connection_event_inbox_capacity,
                    &err)
            _ensure_has_gil(&gs)
            if db._db == NULL:
                # A failed listener-aware connect fences its dispatcher before
                # returning, so the callback target is now safe to release.
                db._connection_listener = None
                raise c_err_to_py(err)
            db._conf_str = conf_str
            return db
        finally:
            _ensure_has_gil(&gs)
            qdb_pystr_buf_free(b)

    def __enter__(self):
        self._state_cond.acquire()
        try:
            if self._db == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    '__enter__() can\'t be called: QuestDB is closed.')
        finally:
            self._state_cond.release()
        return self

    def sender(self):
        """
        Borrow a context-managed row-building sender from the pool.

        The lease participates in the handle's active-use count until it is
        closed. :meth:`QuestDB.close` therefore waits for outstanding leases.
        """
        cdef questdb_db* db = NULL
        cdef qwp_sender* sender = NULL
        cdef line_sender_error* err = NULL
        cdef Buffer buffer = None
        cdef PooledSender lease = None
        cdef PyThreadState* gs = NULL
        cdef bint db_use = False
        db = self._begin_db_use('sender')
        db_use = True
        try:
            _ensure_doesnt_have_gil(&gs)
            sender = questdb_db_borrow_sender(db, &err)
            _ensure_has_gil(&gs)
            if sender == NULL:
                raise c_err_to_py(err)

            buffer = Buffer.__new__(Buffer)
            buffer._impl = questdb_db_new_buffer(db, &err)
            if buffer._impl == NULL:
                raise c_err_to_py(err)
            buffer._b = qdb_pystr_buf_new()
            buffer._init_buf_size = line_sender_buffer_capacity(buffer._impl)
            buffer._max_name_len = questdb_db_buffer_max_name_len(db)
            buffer._qwp = True

            lease = PooledSender.__new__(PooledSender)
            lease._attach(self, db, sender, buffer)
            sender = NULL
            db_use = False
            return lease
        finally:
            _ensure_has_gil(&gs)
            if sender != NULL:
                questdb_db_return_sender(db, sender)
            if db_use:
                self._end_db_use()

    def dataframe(
            self,
            df,
            *,
            table_name: Optional[str] = None,
            table_name_col: Union[None, int, str] = None,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[ServerTimestampType, int, str, TimestampNanos, datetime.datetime],
            max_rows_per_batch: int = DEFAULT_MAX_CHUNK_ROWS,
            schema_overrides: Optional[Dict[str, object]] = None):
        """
        Ingest a dataframe through the pooled columnar QWP path.

        Ingestion always uses the direct (non-store-and-forward) column
        sender, independent of ``sf_dir``, and returns once the whole frame
        is committed (``AckLevel::Ok``). On a transient connection failure
        the frame is re-sent from the caller's DataFrame within the pool's
        reconnect budget — unless the native client reports delivery as in
        doubt, or an intermediate commit checkpoint (emitted every ~100 batches
        on large frames) has already landed. In either case the error is raised
        immediately and a committed prefix may remain in the table, since a
        blind re-send would duplicate it. Delivery is
        at-least-once: a re-sent frame can duplicate already-committed rows
        (including the first chunk, which commits eagerly on a fresh
        connection) unless the table has ``DEDUP UPSERT KEYS``. Server-side
        rejections (e.g. a schema mismatch) surface as a plain
        :class:`QuestDBError`; the structured ``qwp_ws_error`` diagnostic is
        attached only by the store-and-forward senders.

        ``df`` accepts any of:

        - **pandas** ``pandas.DataFrame``. NumPy-backed columns route
          through the legacy planner; pyarrow-backed columns route
          through the Arrow C Stream capsule path below.
        - **polars** ``polars.DataFrame`` and ``polars.LazyFrame``.
          ``LazyFrame`` is materialised via
          ``.collect(engine='streaming')`` (eager ``.collect()`` on
          polars < 1.0).
        - **pyarrow** ``pa.Table``, ``pa.RecordBatch``, and
          ``pa.RecordBatchReader``.
        - Any object exposing the Arrow C Data Interface — i.e. with
          ``__arrow_c_stream__`` (duckdb / cudf / modin / pyarrow-backed
          pandas 2.2+) or ``__arrow_c_array__`` (single Arrow array
          exporters, wrapped into a one-batch ``pa.Table``).

        ``at`` names the designated timestamp column (by name or index).
        Alternatively pass a fixed ``TimestampNanos`` / ``datetime``
        shared by every row (encoded as a repeated constant; resubmission
        stays idempotent under ``DEDUP UPSERT KEYS``), or the explicit
        :data:`ServerTimestamp` sentinel to let the server assign each
        row's timestamp on arrival — an opt-in mirroring the row API,
        since server-assigned timestamps defeat ``DEDUP UPSERT KEYS`` on
        resubmission.

        Supports a column-QWP v1 subset: fixed ``table_name``, non-null
        designated timestamp column, and the following per-column dtypes:

        - **Numeric**: NumPy ``bool/int{8,16,32,64}/uint{8..64}/float{32,64}``.
          Arrow ``pa.int{8,16,32,64}``, ``pa.float{16,32,64}``, and
          ``pa.uint{8,16,32,64}`` are accepted by the Rust Arrow batch route
          when the frame uses a fixed table name and a designated timestamp
          column name. Unsigned Arrow values follow the
          Rust Arrow policy: ``UInt8`` and ``UInt16`` widen to ``INT``,
          ``UInt32`` to ``LONG``, and ``UInt64`` values up to
          ``i64::MAX`` are accepted as ``LONG``. Larger ``UInt64`` values are
          rejected because QuestDB QWP-WS encodes integers as signed ``i64``.
          Signed ``int8``/``int16`` land as QuestDB ``INT``; the row-oriented
          :meth:`Sender.dataframe` instead widens every integer to ``LONG``.
          Likewise ``float32`` lands here as ``FLOAT`` but is widened to
          ``DOUBLE`` by the row path — ingest a given table through a single
          path to avoid a first-write column-type mismatch.
        - **String / Symbol**: object-dtype ``str``, ``pa.string()``,
          ``pa.large_string()``, ``pd.CategoricalDtype`` of strings.
        - **Timestamp**: NumPy ``datetime64`` units accepted by pandas and
          ``pa.timestamp`` with unit ``s``, ``ms``, ``us``, or ``ns``
          (tz-aware accepted on Arrow-backed columns in the Rust Arrow route).
          Null timestamps (``NaT`` / ``None``) and values before the Unix
          epoch are supported on every timestamp column type; a
          ``datetime64[ns]`` field carrying ``NaT`` is re-exported through
          Arrow so its ``INT64_MIN`` null sentinel is preserved.
        - **Decimal**: Arrow-backed ``pa.decimal{32,64,128,256}`` columns
          (``pa.decimal32``/``pa.decimal64`` require pyarrow >= 18), or
          object-dtype columns of ``decimal.Decimal`` (re-exported through
          Arrow; the decimal width and scale are inferred from the values).
        - **Array**: Arrow ``pa.list_`` / ``pa.large_list`` /
          ``pa.fixed_size_list`` columns with a ``float64`` leaf (nested for
          multi-dimensional), and object-dtype columns of ``float64``
          ``numpy.ndarray`` cells (any rank; requires pyarrow). Both land as
          QuestDB ``ARRAY(DOUBLE)``. Null rows are allowed; null *elements*
          inside an array are not.
        - **UUID**: ``pa.fixed_size_binary(16)`` and the ``arrow.uuid``
          extension type. Bytes are forwarded verbatim as **QuestDB's
          UUID wire layout** ("bytes 0..8 lo half LE, bytes 8..16 hi
          half LE"), matching the convention shared across the
          c-questdb-client family (Rust direct, Polars). Round-trip is
          byte-identity at this layout; users who want
          ``uuid.UUID.bytes`` (RFC 4122 big-endian) round-trip must
          convert at their boundary.

        Server-side coercion handles cross-type writes (e.g. ``pa.string()``
        UUIDs landing in a UUID column are parsed server-side; narrow ints
        landing in a wider column are widened). Failures surface as
        ``QuestDBError`` from the ``flush()``.

        ``schema_overrides`` reclassifies columns by name, mapping each to
        ``'symbol'``, ``'ipv4'``, ``'char'``, or ``'geohash'`` (e.g.
        ``{'venue': 'symbol', 'src_ip': 'ipv4'}``). Unknown column names are
        rejected. It requires the Arrow columnar path (fully Arrow-backed
        input without ``table_name_col``); on input that falls back to the
        NumPy planner it raises :class:`UnsupportedDataFrameShapeError`.
        ``max_rows_per_batch`` bounds the rows sent per columnar batch.
        """
        cdef qdb_pystr_buf* b = qdb_pystr_buf_new()
        cdef dataframe_plan_t plan = dataframe_plan_blank()
        cdef questdb_db* db = NULL
        cdef bint db_use = False
        cdef direct_conn_source_t src
        db = self._begin_db_use('dataframe')
        db_use = True
        try:
            src.db = db
            src.opts = NULL
            _direct_dataframe_run(
                &src,
                questdb_db_reconnect_max_duration_ms(db) / 1000.0,
                b,
                &plan,
                df,
                table_name,
                table_name_col,
                symbols,
                at,
                max_rows_per_batch,
                schema_overrides)
            return self
        finally:
            qdb_pystr_buf_free(b)
            if db_use:
                self._end_db_use()

    def query(
            self,
            str sql,
            object binds=None,
            *,
            bint reset_symbol_dict=True):
        """
        Execute a SQL query and return a :class:`QueryResult`.

        Egress goes through the QuestDB Wire Protocol (QWP/WebSocket)
        ``/read/v1`` endpoint. The reader is borrowed from the same
        connection pool that hosts the ingress writers and is returned to
        the pool when the returned :class:`QueryResult` is consumed or
        closed (a poisoned connection is dropped instead). Auth / TLS
        settings apply to both directions.

        To run several queries on one pooled connection, lease a
        :class:`PooledReader` with :meth:`reader` instead.

        :param sql: SQL text to execute. Forwarded verbatim to QuestDB.

        :param binds: Positional bind parameters matching the ``$1``..``$N``
            placeholders in ``sql``, as a list or tuple. Always prefer binds
            over interpolating values into the SQL text — they take no
            escaping and keep types exact. Supported Python types and their
            QuestDB bind types:

            - ``None`` → SQL NULL (bound as a VARCHAR null)
            - ``bool`` → BOOLEAN
            - ``int`` → LONG (must fit signed 64-bit)
            - ``float`` → DOUBLE
            - ``str`` → VARCHAR
            - ``datetime.datetime`` → TIMESTAMP (microseconds; a naive
              value is interpreted as UTC, the same rule as everywhere else
              in the API)
            - :class:`TimestampMicros` → TIMESTAMP
            - :class:`TimestampNanos` → TIMESTAMP_NS
            - ``uuid.UUID`` → UUID

            Any other type raises :class:`TypeError` naming the placeholder.

            .. code-block:: python

                res = db.query(
                    'SELECT * FROM trades WHERE ts > $1 AND sym = $2',
                    [datetime.datetime(2026, 7, 1), 'BTC-USD'])

        :param reset_symbol_dict: When ``True`` (the default), the server
            resets the connection's SYMBOL dictionary before this query so it
            never inherits symbols from earlier queries on the pooled
            connection (query-scoped dict), avoiding cross-query dictionary
            bloat in ``to_polars()`` / ``to_pandas()``. Set ``False`` to keep
            the dictionary warm across repeated identical queries. No-op
            against servers that predate the capability.

        :return: A :class:`QueryResult`. Materialise it via
            ``to_pandas()``, ``to_arrow()``, ``iter_arrow()``,
            ``iter_pandas()``, or the ``__arrow_c_stream__`` PyCapsule
            protocol.

        Sentinel-value collisions in the result frame round-trip QuestDB's
        contract: ``INT64_MIN`` in a LONG column, NaN in DOUBLE / FLOAT,
        and the sentinel values for INT / DATE / TIMESTAMP /
        TIMESTAMP_NS / CHAR / UUID / LONG256 / IPV4 / GEOHASH are all
        interpreted as NULL by QuestDB and cannot be distinguished from
        legitimate occurrences of those values.
        """
        # Borrow a reader from the same `questdb_db` pool that hosts
        # the ingress writers. The pool amortises TCP+TLS handshake
        # cost across many `QuestDB.query()` calls: the first call
        # opens a connection, subsequent calls hit the idle-list
        # cache. See `c-questdb-client/questdb-rs/src/ingress/
        # column_sender/db.rs` for the pool's structure.
        cdef _ReaderHandle reader_handle
        cdef _CursorHandle cursor_handle
        cdef questdb_db* db
        if sql is None:
            raise TypeError(
                'query() requires a sql string; to run several queries '
                'on one pooled connection, lease a PooledReader with '
                'reader() instead.')
        if binds is not None and not isinstance(binds, (list, tuple)):
            raise TypeError(
                '"binds" must be a list or tuple of positional bind '
                f'parameters (or None), not {_fqn(type(binds))}')
        db = self._begin_db_use('query')
        try:
            reader_handle = _borrow_reader_from_pool(db)
            cursor_handle = _execute_query(
                reader_handle, sql, binds, reset_symbol_dict)
        finally:
            self._end_db_use()
        return QueryResult(cursor_handle)

    def reader(self):
        """
        Borrow a context-managed :class:`PooledReader` lease from the pool.

        The read-side twin of :meth:`sender`: the lease holds one pooled
        reader connection for its lifetime and runs queries on it
        sequentially via :meth:`PooledReader.query`.

        .. code-block:: python

            with db.reader() as r:
                r1 = r.query('SELECT * FROM t1').to_pandas()
                r2 = r.query(
                    'SELECT * FROM t2',
                    reset_symbol_dict=False).to_pandas()

        The lease participates in the handle's active-use count until it
        is closed. :meth:`QuestDB.close` therefore waits for outstanding
        leases.
        """
        cdef _ReaderHandle reader_handle
        cdef PooledReader lease
        cdef questdb_db* db
        cdef bint db_use = False
        db = self._begin_db_use('reader')
        db_use = True
        try:
            reader_handle = _borrow_reader_from_pool(db)
            lease = PooledReader.__new__(PooledReader)
            lease._attach(self, reader_handle)
            db_use = False
            return lease
        finally:
            if db_use:
                self._end_db_use()

    def server_info(self) -> ServerInfo:
        """
        Return a :class:`ServerInfo` snapshot of the server's
        ``SERVER_INFO`` handshake: cluster role, failover epoch,
        negotiated capabilities, handshake wall-clock, and cluster/node
        identifiers.

        A reader is borrowed from the connection pool (opening one on
        first use, exactly like :meth:`query`), sampled, and returned to
        the pool. The snapshot describes that connection at its last
        handshake; a later failover is reflected only in snapshots taken
        after it.
        """
        cdef _ReaderHandle reader_handle
        cdef questdb_db* db
        db = self._begin_db_use('server_info')
        try:
            reader_handle = _borrow_reader_from_pool(db)
            info = _snapshot_server_info(reader_handle)
            # No cursor was opened, so the reader is pristine: return it
            # to the pool instead of the default drop-on-dealloc.
            reader_handle._must_close = False
            reader_handle._close()
        finally:
            self._end_db_use()
        return info

    @property
    def connection_events_dropped(self) -> int:
        """
        Total connection events discarded by the listener inbox's
        drop-oldest policy. ``0`` when no listener is registered.
        """
        cdef questdb_db* db = self._begin_db_use('connection_events_dropped')
        try:
            return questdb_db_connection_events_dropped(db)
        finally:
            self._end_db_use()

    @property
    def connection_events_delivered(self) -> int:
        """
        Total connection events delivered to the listener. ``0`` when no
        listener is registered.
        """
        cdef questdb_db* db = self._begin_db_use('connection_events_delivered')
        try:
            return questdb_db_connection_events_delivered(db)
        finally:
            self._end_db_use()

    def reap_idle(self):
        """
        Manually reap idle above-pool-size connections.
        """
        cdef size_t closed
        cdef PyThreadState* gs = NULL
        cdef questdb_db* db = NULL
        cdef bint db_use = False
        db = self._begin_db_use('reap_idle')
        db_use = True
        try:
            _ensure_doesnt_have_gil(&gs)
            closed = questdb_db_reap_idle(db)
            _ensure_has_gil(&gs)
            return closed
        finally:
            _ensure_has_gil(&gs)
            if db_use:
                self._end_db_use()

    cpdef close(self):
        """
        Close the client and its connection pool.

        This method is idempotent.
        """
        cdef questdb_db* db = NULL
        cdef PyThreadState* gs = NULL
        self._state_cond.acquire()
        try:
            db = self._db
            if db == NULL:
                while self._closing:
                    self._state_cond.wait()
                return
            self._db = NULL
            self._conf_str = None
            self._closing = True
            while self._active_uses != 0:
                if (not self._state_cond.wait(timeout=5.0)
                        and self._active_uses != 0):
                    warnings.warn(
                        'QuestDB.close() is waiting for '
                        f'{self._active_uses} outstanding lease(s) to be '
                        'released.',
                        UserWarning)
        finally:
            self._state_cond.release()
        try:
            _ensure_doesnt_have_gil(&gs)
            # `questdb_db_close` drains both the writer and reader free
            # lists in one shot (see `db.rs::DbInner::Drop`).
            questdb_db_close(db)
        finally:
            _ensure_has_gil(&gs)
            self._state_cond.acquire()
            try:
                self._closing = False
                self._state_cond.notify_all()
            finally:
                self._state_cond.release()

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        self.close()

    def __dealloc__(self):
        cdef questdb_db* db
        cdef PyThreadState* gs = NULL
        if self._db != NULL:
            db = self._db
            self._db = NULL
            _ensure_doesnt_have_gil(&gs)
            questdb_db_close(db)
            _ensure_has_gil(&gs)


cdef class Sender:
    """
    Ingest data into QuestDB.

    See the :ref:`sender` documentation for more information.
    """

    # We need the Buffer held by a Sender can hold a weakref to its Sender.
    # This avoids a circular reference that requires the GC to clean up.
    cdef object __weakref__

    cdef line_sender_protocol _c_protocol
    cdef line_sender_opts* _opts
    cdef line_sender* _impl
    cdef Buffer _buffer
    cdef object _qwp_ws_error_handler
    cdef object _connection_listener
    cdef auto_flush_mode_t _auto_flush_mode
    cdef int64_t* _last_flush_ms
    cdef size_t _init_buf_size
    cdef bint _in_txn
    cdef int64_t _slot_id
    # A clone of the fully-configured opts for QWP/WebSocket senders, retained
    # so dataframe() can open a poolless direct columnar connection per call
    # (carrying auth/TLS). NULL for other protocols.
    cdef line_sender_opts* _qwp_ws_opts

    cdef void_int _set_sender_fields(
            self,
            qdb_pystr_buf* b,
            object protocol,
            str bind_interface,
            str username,
            str password,
            str token,
            str token_x,
            str token_y,
            object auth_timeout,
            object tls_verify,
            object tls_ca,
            object tls_roots,
            object tls_roots_password,
            object max_buf_size,
            object retry_timeout,
            object retry_max_backoff,
            object request_min_throughput,
            object request_timeout,
            object auto_flush,
            object auto_flush_rows,
            object auto_flush_bytes,
            object auto_flush_interval,
            object max_datagram_size,
            object multicast_ttl,
            object protocol_version,
            object qwp_ws_progress,
            object qwp_ws_error_handler,
            object connection_listener,
            object connection_event_inbox_capacity,
            object init_buf_size,
            object max_name_len) except -1:
        """
        Set optional parameters for the sender.
        """
        cdef line_sender_error* err = NULL
        cdef str user_agent = 'questdb/python/' + VERSION
        cdef line_sender_utf8 c_user_agent
        cdef line_sender_utf8 c_bind_interface
        cdef line_sender_utf8 c_username
        cdef line_sender_utf8 c_password
        cdef line_sender_utf8 c_token
        cdef line_sender_utf8 c_token_x
        cdef line_sender_utf8 c_token_y
        cdef uint64_t c_auth_timeout
        cdef bint c_tls_verify
        cdef line_sender_ca c_tls_ca
        cdef line_sender_utf8 c_tls_roots
        cdef line_sender_utf8 c_tls_roots_password
        cdef uint64_t c_max_buf_size
        cdef uint64_t c_retry_timeout
        cdef uint64_t c_retry_max_backoff
        cdef uint64_t c_request_min_throughput
        cdef uint64_t c_request_timeout
        cdef size_t c_max_name_len
        cdef size_t c_max_datagram_size = 0
        cdef uint32_t c_multicast_ttl = 0
        cdef line_sender_qwpws_progress c_qwp_ws_progress

        self._c_protocol = protocol.c_value

        # It's OK to override this setting.
        str_to_utf8(b, <PyObject*>user_agent, &c_user_agent)
        if not line_sender_opts_user_agent(self._opts, c_user_agent, &err):
            raise c_err_to_py(err)

        if bind_interface is not None:
            str_to_utf8(b, <PyObject*>bind_interface, &c_bind_interface)
            if not line_sender_opts_bind_interface(
                    self._opts, c_bind_interface, &err):
                raise c_err_to_py(err)

        if max_datagram_size is not None:
            if not _is_qwp_udp_protocol(self._c_protocol):
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    '"max_datagram_size" is only supported for QWP/UDP senders.')
            if not isinstance(max_datagram_size, int) or isinstance(max_datagram_size, bool):
                raise TypeError(
                    '"max_datagram_size" must be a positive int, '
                    f'not {_fqn(type(max_datagram_size))}')
            if max_datagram_size <= 0 or max_datagram_size > 65507:
                raise ValueError(
                    '"max_datagram_size" must be an int between 1 and 65507, '
                    f'not {max_datagram_size!r}')
            c_max_datagram_size = max_datagram_size
            if not line_sender_opts_max_datagram_size(
                    self._opts, c_max_datagram_size, &err):
                raise c_err_to_py(err)

        if multicast_ttl is not None:
            if not _is_qwp_udp_protocol(self._c_protocol):
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    '"multicast_ttl" is only supported for QWP/UDP senders.')
            if not isinstance(multicast_ttl, int) or isinstance(multicast_ttl, bool):
                raise TypeError(
                    '"multicast_ttl" must be an int (0-255), '
                    f'not {_fqn(type(multicast_ttl))}')
            if multicast_ttl < 0 or multicast_ttl > 255:
                raise ValueError(
                    '"multicast_ttl" must be an int (0-255), '
                    f'not {multicast_ttl!r}')
            c_multicast_ttl = multicast_ttl
            if not line_sender_opts_multicast_ttl(
                    self._opts, c_multicast_ttl, &err):
                raise c_err_to_py(err)

        if qwp_ws_progress is not None:
            if not _is_qwp_ws_protocol(self._c_protocol):
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    '"qwp_ws_progress" is only supported for QWP/WebSocket senders.')
            try:
                c_qwp_ws_progress = QwpWsProgress.parse(qwp_ws_progress).c_value
            except ValueError:
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    f'"qwp_ws_progress" has invalid value: {qwp_ws_progress!r}')
            if not line_sender_opts_qwpws_progress(
                    self._opts, c_qwp_ws_progress, &err):
                raise c_err_to_py(err)

        if qwp_ws_error_handler is not None and not callable(qwp_ws_error_handler):
            raise TypeError(
                '"qwp_ws_error_handler" must be callable or None, '
                f'not {_fqn(type(qwp_ws_error_handler))}')
        if qwp_ws_error_handler is not None and not _is_qwp_ws_protocol(self._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'qwp_ws_error_handler is only supported for QWP/WebSocket senders.')
        if _is_qwp_ws_protocol(self._c_protocol):
            if qwp_ws_error_handler is None:
                qwp_ws_error_handler = _default_qwp_ws_error_handler
            self._qwp_ws_error_handler = qwp_ws_error_handler
            if not line_sender_opts_qwpws_error_handler(
                    self._opts,
                    <line_sender_qwpws_error_cb>_qwp_ws_error_trampoline,
                    <void*>self._qwp_ws_error_handler,
                    &err):
                self._qwp_ws_error_handler = None
                raise c_err_to_py(err)

        if connection_listener is not None and not callable(
                connection_listener):
            raise TypeError(
                '"connection_listener" must be callable or None, '
                f'not {_fqn(type(connection_listener))}')
        if connection_listener is not None and not _is_qwp_ws_protocol(
                self._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'connection_listener is only supported for QWP/WebSocket '
                'senders.')
        if connection_listener is not None:
            # The Sender owns the only strong reference the trampoline
            # relies on; the dispatcher joins its thread when the sender
            # closes, so no delivery outlives this reference.
            self._connection_listener = connection_listener
            if not line_sender_opts_connection_event_handler(
                    self._opts,
                    <questdb_connection_event_cb>_connection_event_trampoline,
                    <void*>self._connection_listener,
                    <size_t>(connection_event_inbox_capacity or 0),
                    &err):
                self._connection_listener = None
                raise c_err_to_py(err)

        if username is not None:
            str_to_utf8(b, <PyObject*>username, &c_username)
            if not line_sender_opts_username(self._opts, c_username, &err):
                raise c_err_to_py(err)

        if password is not None:
            str_to_utf8(b, <PyObject*>password, &c_password)
            if not line_sender_opts_password(self._opts, c_password, &err):
                raise c_err_to_py(err)

        if token is not None:
            str_to_utf8(b, <PyObject*>token, &c_token)
            if not line_sender_opts_token(self._opts, c_token, &err):
                raise c_err_to_py(err)

        if token_x is not None:
            str_to_utf8(b, <PyObject*>token_x, &c_token_x)
            if not line_sender_opts_token_x(self._opts, c_token_x, &err):
                raise c_err_to_py(err)

        if token_y is not None:
            str_to_utf8(b, <PyObject*>token_y, &c_token_y)
            if not line_sender_opts_token_y(self._opts, c_token_y, &err):
                raise c_err_to_py(err)

        if protocol_version is not None:
            if protocol_version == 'auto':
                pass
            elif (protocol_version == 1) or (protocol_version == '1'):
                if not line_sender_opts_protocol_version(
                        self._opts, line_sender_protocol_version_1, &err):
                    raise c_err_to_py(err)
            elif (protocol_version == 2) or (protocol_version == '2'):
                if not line_sender_opts_protocol_version(
                        self._opts, line_sender_protocol_version_2, &err):
                    raise c_err_to_py(err)
            elif (protocol_version == 3) or (protocol_version == '3'):
                if not line_sender_opts_protocol_version(
                        self._opts, line_sender_protocol_version_3, &err):
                    raise c_err_to_py(err)
            else:
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    '"protocol_version" must be None, "auto", 1-3' +
                    f' not {protocol_version!r}')

        if auth_timeout is not None:
            if _is_int_not_bool(auth_timeout):
                c_auth_timeout = auth_timeout
            elif isinstance(auth_timeout, cp_timedelta):
                c_auth_timeout = _timedelta_to_millis(auth_timeout)
            else:
                raise TypeError(
                    '"auth_timeout" must be an int or a timedelta, '
                    f'not {_fqn(type(auth_timeout))}')
            if not line_sender_opts_auth_timeout(self._opts, c_auth_timeout, &err):
                raise c_err_to_py(err)

        if tls_verify is not None:
            if (tls_verify is True) or (tls_verify == 'on'):
                c_tls_verify = True
            elif (tls_verify is False) or (tls_verify == 'unsafe_off'):
                c_tls_verify = False
            else:
                raise ValueError(
                    '"tls_verify" must be a bool, "on" or "unsafe_off", '
                    f'not {tls_verify!r}')
            if not line_sender_opts_tls_verify(self._opts, c_tls_verify, &err):
                raise c_err_to_py(err)

        if tls_roots is not None:
            tls_roots = str(tls_roots)
            str_to_utf8(b, <PyObject*>tls_roots, &c_tls_roots)
            if not line_sender_opts_tls_roots(self._opts, c_tls_roots, &err):
                raise c_err_to_py(err)

        if tls_roots_password is not None:
            str_to_utf8(b, <PyObject*>tls_roots_password, &c_tls_roots_password)
            if not line_sender_opts_tls_roots_password(
                    self._opts, c_tls_roots_password, &err):
                raise c_err_to_py(err)

        if tls_ca is not None:
            c_tls_ca = TlsCa.parse(tls_ca).c_value
            if not line_sender_opts_tls_ca(self._opts, c_tls_ca, &err):
                raise c_err_to_py(err)
        elif protocol.tls_enabled and tls_roots is None:
            # Set different default for Python than the the Rust default.
            # We don't set it if `tls_roots` is set, as it would override it.
            c_tls_ca = line_sender_ca_webpki_and_os_roots
            if not line_sender_opts_tls_ca(self._opts, c_tls_ca, &err):
                raise c_err_to_py(err)

        if max_buf_size is not None:
            if not _is_int_not_bool(max_buf_size):
                raise TypeError(
                    '"max_buf_size" must be an int, '
                    f'not {_fqn(type(max_buf_size))}')
            c_max_buf_size = max_buf_size
            if not line_sender_opts_max_buf_size(self._opts, c_max_buf_size, &err):
                raise c_err_to_py(err)

        if retry_timeout is not None:
            if _is_int_not_bool(retry_timeout):
                c_retry_timeout = retry_timeout
                if not line_sender_opts_retry_timeout(self._opts, c_retry_timeout, &err):
                    raise c_err_to_py(err)
            elif isinstance(retry_timeout, cp_timedelta):
                c_retry_timeout = _timedelta_to_millis(retry_timeout)
                if not line_sender_opts_retry_timeout(self._opts, c_retry_timeout, &err):
                    raise c_err_to_py(err)
            else:
                raise TypeError(
                    '"retry_timeout" must be an int or a timedelta, '
                    f'not {_fqn(type(retry_timeout))}')

        if retry_max_backoff is not None:
            if _is_int_not_bool(retry_max_backoff):
                c_retry_max_backoff = retry_max_backoff
                if not line_sender_opts_retry_max_backoff(
                        self._opts, c_retry_max_backoff, &err):
                    raise c_err_to_py(err)
            elif isinstance(retry_max_backoff, cp_timedelta):
                c_retry_max_backoff = _timedelta_to_millis(retry_max_backoff)
                if not line_sender_opts_retry_max_backoff(
                        self._opts, c_retry_max_backoff, &err):
                    raise c_err_to_py(err)
            else:
                raise TypeError(
                    '"retry_max_backoff" must be an int or a timedelta, '
                    f'not {_fqn(type(retry_max_backoff))}')

        if request_min_throughput is not None:
            if not _is_int_not_bool(request_min_throughput):
                raise TypeError(
                    '"request_min_throughput" must be an int, '
                    f'not {_fqn(type(request_min_throughput))}')
            c_request_min_throughput = request_min_throughput
            if not line_sender_opts_request_min_throughput(self._opts, c_request_min_throughput, &err):
                raise c_err_to_py(err)

        if max_name_len is not None:
            if not _is_int_not_bool(max_name_len):
                raise TypeError(
                    '"max_name_len" must be an int, '
                    f'not {_fqn(type(max_name_len))}')
            c_max_name_len = max_name_len
            if not line_sender_opts_max_name_len(self._opts, c_max_name_len, &err):
                raise c_err_to_py(err)

        if request_timeout is not None:
            if _is_int_not_bool(request_timeout):
                c_request_timeout = request_timeout
                if not line_sender_opts_request_timeout(self._opts, c_request_timeout, &err):
                    raise c_err_to_py(err)
            elif isinstance(request_timeout, cp_timedelta):
                c_request_timeout = _timedelta_to_millis(request_timeout)
                if not line_sender_opts_request_timeout(self._opts, c_request_timeout, &err):
                    raise c_err_to_py(err)
            else:
                raise TypeError(
                    '"request_timeout" must be an int or a timedelta, '
                    f'not {_fqn(type(request_timeout))}')

        _parse_auto_flush(
            self._c_protocol,
            auto_flush,
            auto_flush_rows,
            auto_flush_bytes,
            auto_flush_interval,
            &self._auto_flush_mode,
            c_max_datagram_size)

        self._init_buf_size = init_buf_size or 65536
        self._last_flush_ms = <int64_t*>calloc(1, sizeof(int64_t))
        if self._last_flush_ms == NULL:
            raise MemoryError()

        # Retain a clone of the fully-configured opts (auth/TLS included) so a
        # ws sender's dataframe() can open a poolless direct columnar
        # connection per call, independent of how it was built.
        if _is_qwp_ws_protocol(self._c_protocol) and self._opts != NULL:
            self._qwp_ws_opts = line_sender_opts_clone(self._opts)

    def __cinit__(self):
        self._c_protocol = line_sender_protocol_tcp
        self._opts = NULL
        self._impl = NULL
        self._buffer = None
        self._qwp_ws_error_handler = None
        self._connection_listener = None
        self._auto_flush_mode.enabled = False
        self._last_flush_ms = NULL
        self._init_buf_size = 0
        self._in_txn = False
        self._slot_id = -1
        self._qwp_ws_opts = NULL

    def __init__(
            self,
            object protocol,
            str host,
            object port,
            *,
            str bind_interface=None,
            str username=None,
            str password=None,
            str token=None,
            str token_x=None,
            str token_y=None,
            object auth_timeout=None,  # default: 15000 milliseconds
            object tls_verify=None,  # default: True
            object tls_ca=None,  # default: TlsCa.WebpkiRoots
            object tls_roots=None,
            str tls_roots_password=None,
            object max_buf_size=None,  # 100 * 1024 * 1024 - 100MiB
            object retry_timeout=None,  # default: 10000 milliseconds
            object retry_max_backoff=None,  # default: 1000 milliseconds
            object request_min_throughput=None, # default: 100 * 1024 - 100KiB/s
            object request_timeout=None,
            object auto_flush=None,  # Default True
            object auto_flush_rows=None,  # Default 75000 (HTTP) or 600 (TCP)
            object auto_flush_bytes=None,  # Default off
            object auto_flush_interval=None,  # Default 1000 milliseconds
            object max_datagram_size=None,  # Default 1400 for QWP/UDP
            object multicast_ttl=None,  # Default 1 for QWP/UDP
            object qwp_ws_progress=None,  # Default background for QWP/WebSocket
            object qwp_ws_error_handler=None,
            object connection_listener=None,
            object connection_event_inbox_capacity=0,
            object protocol_version=None,  # Default auto
            object init_buf_size=None,  # 64KiB
            object max_name_len=None):  # 127

        cdef line_sender_utf8 c_host
        cdef str port_str
        cdef line_sender_protocol c_protocol
        cdef line_sender_utf8 c_port
        cdef qdb_pystr_buf* b
        if (self._opts != NULL or self._impl != NULL or
                self._last_flush_ms != NULL):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Sender is already initialized.')
        b = qdb_pystr_buf_new()
        try:
            protocol = Protocol.parse(protocol)
            c_protocol = protocol.c_value
            if PyLong_CheckExact(<PyObject*>port):
                port_str = str(port)
            elif PyUnicode_CheckExact(<PyObject*>port):
                port_str = port
            else:
                raise TypeError(
                    f'port must be an int or a str, not {_fqn(type(port))}')
            str_to_utf8(b, <PyObject*>host, &c_host)
            str_to_utf8(b, <PyObject*>port_str, &c_port)
            self._opts = line_sender_opts_new_service(c_protocol, c_host, c_port)

            self._set_sender_fields(
                b,
                protocol,
                bind_interface,
                username,
                password,
                token,
                token_x,
                token_y,
                auth_timeout,
                tls_verify,
                tls_ca,
                tls_roots,
                tls_roots_password,
                max_buf_size,
                retry_timeout,
                retry_max_backoff,
                request_min_throughput,
                request_timeout,
                auto_flush,
                auto_flush_rows,
                auto_flush_bytes,
                auto_flush_interval,
                max_datagram_size,
                multicast_ttl,
                protocol_version,
                qwp_ws_progress,
                qwp_ws_error_handler,
                connection_listener,
                connection_event_inbox_capacity,
                init_buf_size,
                max_name_len)
        finally:
            qdb_pystr_buf_free(b)

    @staticmethod
    def from_conf(
            str conf_str,
            *,
            str bind_interface=None,
            str username=None,
            str password=None,
            str token=None,
            str token_x=None,
            str token_y=None,
            object auth_timeout=None,  # default: 15000 milliseconds
            object tls_verify=None,  # default: True
            object tls_ca=None,  # default: TlsCa.WebpkiRoots
            object tls_roots=None,
            str tls_roots_password=None,
            object max_buf_size=None,  # 100 * 1024 * 1024 - 100MiB
            object retry_timeout=None,  # default: 10000 milliseconds
            object retry_max_backoff=None,  # default: 1000 milliseconds
            object request_min_throughput=None, # default: 100 * 1024 - 100KiB/s
            object request_timeout=None,
            object auto_flush=None,  # Default True
            object auto_flush_rows=None,  # Default 75000 (HTTP) or 600 (TCP)
            object auto_flush_bytes=None,  # Default off
            object auto_flush_interval=None,  # Default 1000 milliseconds
            object max_datagram_size=None,  # Default 1400 for QWP/UDP
            object multicast_ttl=None,  # Default 1 for QWP/UDP
            object qwp_ws_progress=None,  # Default background for QWP/WebSocket
            object qwp_ws_error_handler=None,
            object connection_listener=None,
            object connection_event_inbox_capacity=0,
            object protocol_version=None,  # Default auto
            object init_buf_size=None,  # 64KiB
            object max_name_len=None):  # 127
        """
        Construct a sender from a :ref:`configuration string <sender_conf>`.

        The additional arguments are used to specify additional parameters
        which are not present in the configuration string.

        Note that any parameters already present in the configuration string
        cannot be overridden.
        """

        cdef line_sender_error* err = NULL
        cdef object protocol
        cdef Sender sender
        cdef str synthetic_conf_str
        cdef line_sender_utf8 c_synthetic_conf_str
        cdef dict params
        cdef qdb_pystr_buf* b = qdb_pystr_buf_new()
        try:
            protocol, params = parse_conf_str(b, conf_str)

            addr = params.get('addr')
            if addr is None:
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    'Missing "addr" parameter in config string')
            
            # add fields to the dictionary, so long as they aren't already
            # present in the params dictionary
            for override_key, override_value in {
                'bind_interface': bind_interface,
                'username': username,
                'password': password,
                'token': token,
                'token_x': token_x,
                'token_y': token_y,
                'auth_timeout': auth_timeout,
                'tls_verify': tls_verify,
                'tls_ca': tls_ca,
                'tls_roots': tls_roots,
                'tls_roots_password': tls_roots_password,
                'max_buf_size': max_buf_size,
                'retry_timeout': retry_timeout,
                'retry_max_backoff_millis': retry_max_backoff,
                'request_min_throughput': request_min_throughput,
                'request_timeout': request_timeout,
                'auto_flush': auto_flush,
                'auto_flush_rows': auto_flush_rows,
                'auto_flush_bytes': auto_flush_bytes,
                'auto_flush_interval': auto_flush_interval,
                'max_datagram_size': max_datagram_size,
                'multicast_ttl': multicast_ttl,
                'qwp_ws_progress': qwp_ws_progress,
                'protocol_version': protocol_version,
                'init_buf_size': init_buf_size,
                'max_name_len': max_name_len,
            }.items():
                if override_value is None:
                    continue
                if override_key in params:
                    raise ValueError(
                        f'"{override_key}" is already present in the conf_str '
                        'and cannot be overridden.')
                params[override_key] = override_value

            sender = Sender.__new__(Sender)

            python_handled_keys = {
                'addr',
                'bind_interface',
                'username',
                'password',
                'token',
                'token_x',
                'token_y',
                'auth_timeout',
                'tls_verify',
                'tls_ca',
                'tls_roots',
                'tls_roots_password',
                'max_buf_size',
                'retry_timeout',
                'retry_max_backoff_millis',
                'request_min_throughput',
                'request_timeout',
                'auto_flush',
                'auto_flush_rows',
                'auto_flush_bytes',
                'auto_flush_interval',
                'max_datagram_size',
                'multicast_ttl',
                'qwp_ws_progress',
                'protocol_version',
                'init_buf_size',
                'max_name_len',
            }
            synthetic_params = {'addr': addr}
            if protocol in (Protocol.Ws, Protocol.Wss):
                for key, value in params.items():
                    if key not in python_handled_keys:
                        synthetic_params[key] = value
            synthetic_conf_str = protocol.tag + '::' + ''.join(
                f'{key}={conf_str_value(value)};'
                for key, value in synthetic_params.items())
            str_to_utf8(b, <PyObject*>synthetic_conf_str, &c_synthetic_conf_str)
            sender._opts = line_sender_opts_from_conf(
                c_synthetic_conf_str, &err)
            if sender._opts == NULL:
                raise c_err_to_py(err)

            sender._set_sender_fields(
                b,
                protocol,
                params.get('bind_interface'),
                params.get('username'),
                params.get('password'),
                params.get('token'),
                params.get('token_x'),
                params.get('token_y'),
                params.get('auth_timeout'),
                params.get('tls_verify'),
                params.get('tls_ca'),
                params.get('tls_roots'),
                params.get('tls_roots_password'),
                params.get('max_buf_size'),
                params.get('retry_timeout'),
                params.get('retry_max_backoff_millis'),
                params.get('request_min_throughput'),
                params.get('request_timeout'),
                params.get('auto_flush'),
                params.get('auto_flush_rows'),
                params.get('auto_flush_bytes'),
                params.get('auto_flush_interval'),
                params.get('max_datagram_size'),
                params.get('multicast_ttl'),
                params.get('protocol_version'),
                params.get('qwp_ws_progress'),
                qwp_ws_error_handler,
                connection_listener,
                connection_event_inbox_capacity,
                params.get('init_buf_size'),
                params.get('max_name_len'))

            return sender
        finally:
            qdb_pystr_buf_free(b)

    @staticmethod
    def from_env(
            *,
            str bind_interface=None,
            str username=None,
            str password=None,
            str token=None,
            str token_x=None,
            str token_y=None,
            object auth_timeout=None,  # default: 15000 milliseconds
            object tls_verify=None,  # default: True
            object tls_ca=None,  # default: TlsCa.WebpkiRoots
            object tls_roots=None,
            str tls_roots_password=None,
            object max_buf_size=None,  # 100 * 1024 * 1024 - 100MiB
            object retry_timeout=None,  # default: 10000 milliseconds
            object retry_max_backoff=None,  # default: 1000 milliseconds
            object request_min_throughput=None, # default: 100 * 1024 - 100KiB/s
            object request_timeout=None,
            object auto_flush=None,  # Default True
            object auto_flush_rows=None,  # Default 75000 (HTTP) or 600 (TCP)
            object auto_flush_bytes=None,  # Default off
            object auto_flush_interval=None,  # Default 1000 milliseconds
            object max_datagram_size=None,  # Default 1400 for QWP/UDP
            object multicast_ttl=None,  # Default 1 for QWP/UDP
            object qwp_ws_progress=None,  # Default background for QWP/WebSocket
            object qwp_ws_error_handler=None,
            object connection_listener=None,
            object connection_event_inbox_capacity=0,
            object protocol_version=None,  # Default auto
            object init_buf_size=None,  # 64KiB
            object max_name_len=None):  # 127
        """
        Construct a sender from the ``QDB_CLIENT_CONF`` environment variable.

        The environment variable must be set to a valid
        :ref:`configuration string <sender_conf>`.

        The additional arguments are used to specify additional parameters
        which are not present in the configuration string.

        Note that any parameters already present in the configuration string
        cannot be overridden.
        """
        cdef str conf_str = os.environ.get('QDB_CLIENT_CONF')
        if conf_str is None:
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                'Environment variable QDB_CLIENT_CONF is not set.')
        return Sender.from_conf(
            conf_str,
            bind_interface=bind_interface,
            username=username,
            password=password,
            token=token,
            token_x=token_x,
            token_y=token_y,
            auth_timeout=auth_timeout,
            tls_verify=tls_verify,
            tls_ca=tls_ca,
            tls_roots=tls_roots,
            tls_roots_password=tls_roots_password,
            max_buf_size=max_buf_size,
            retry_timeout=retry_timeout,
            retry_max_backoff=retry_max_backoff,
            request_min_throughput=request_min_throughput,
            request_timeout=request_timeout,
            auto_flush=auto_flush,
            auto_flush_rows=auto_flush_rows,
            auto_flush_bytes=auto_flush_bytes,
            auto_flush_interval=auto_flush_interval,
            max_datagram_size=max_datagram_size,
            multicast_ttl=multicast_ttl,
            qwp_ws_progress=qwp_ws_progress,
            qwp_ws_error_handler=qwp_ws_error_handler,
            connection_listener=connection_listener,
            connection_event_inbox_capacity=connection_event_inbox_capacity,
            protocol_version=protocol_version,
            init_buf_size=init_buf_size,
            max_name_len=max_name_len)


    cdef inline object _new_buffer_for_sender(self):
        cdef Buffer buf = Buffer.__new__(Buffer)
        buf._impl = line_sender_buffer_new_for_sender(self._impl)
        buf._b = qdb_pystr_buf_new()
        reserve_buffer(buf._impl, self._init_buf_size)
        buf._init_buf_size = self._init_buf_size
        buf._max_name_len = line_sender_get_max_name_len(self._impl)
        buf._qwp = (
            _is_qwp_udp_protocol(self._c_protocol) or
            _is_qwp_ws_protocol(self._c_protocol))
        return buf

    def new_buffer(self):
        """
        Make a new configured buffer.

        The buffer is set up with the configured `init_buf_size` and
        `max_name_len`, and matches the sender's protocol.

        Must be called after :func:`Sender.establish` and before
        :func:`Sender.close`; otherwise raises
        :class:`QuestDBError` (``InvalidApiCall``).
        """
        if self._impl == NULL:
            if self._opts == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'new_buffer() can\'t be called: Sender is closed.')
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'new_buffer() can\'t be called before establish().')
        return self._new_buffer_for_sender()

    @property
    def init_buf_size(self) -> int:
        """The initial capacity of the sender's internal buffer."""
        return self._init_buf_size

    @property
    def max_name_len(self) -> int:
        """Maximum length of a table or column name."""
        if self._impl == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'max_name_len() can\'t be called: Sender is closed.')
        return line_sender_get_max_name_len(self._impl)

    @property
    def auto_flush(self) -> bint:
        """
        Auto-flushing is enabled.
        
        Consult the `.auto_flush_rows`, `.auto_flush_bytes` and
        `.auto_flush_interval` properties for the current active thresholds.
        """
        return self._auto_flush_mode.enabled

    @property
    def auto_flush_rows(self) -> Optional[int]:
        """
        Row count threshold for the auto-flush logic, or None if disabled.
        """
        if not self._auto_flush_mode.enabled:
            return None
        if self._auto_flush_mode.row_count == -1:
            return None
        return self._auto_flush_mode.row_count

    @property
    def auto_flush_bytes(self) -> Optional[int]:
        """
        Byte-count threshold for the auto-flush logic, or None if disabled.
        """
        if not self._auto_flush_mode.enabled:
            return None
        if self._auto_flush_mode.byte_count == -1:
            return None
        return self._auto_flush_mode.byte_count
    
    @property
    def auto_flush_interval(self) -> Optional[datetime.timedelta]:
        """
        Time interval threshold for the auto-flush logic, or None if disabled.
        """
        if not self._auto_flush_mode.enabled:
            return None
        if self._auto_flush_mode.interval == -1:
            return None
        return cp_timedelta(milliseconds=self._auto_flush_mode.interval)

    @property
    def protocol_version(self) -> int:
        """
        The protocol version used by the sender.

        Protocol version 1 is retained for backwards compatibility with
        older QuestDB versions.

        Protocol version 2 introduces binary floating point support and
        the array datatype.
        """
        if self._impl == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'protocol_version() can\'t be called: Sender is closed.')
        if _is_qwp_udp_protocol(self._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'protocol_version is not applicable for QWP/UDP senders.')
        return <int>line_sender_get_protocol_version(self._impl)

    def establish(self):
        """
        Prepare the sender for use.

        If using ILP/HTTP this will initialize the HTTP connection pool.

        If using ILP/TCP this will cause connection to the server and 
        block until the connection is established.

        If the TCP connection is set up with authentication and/or TLS, this
        method will return only *after* the handshake(s) is/are complete.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState * gs = NULL
        cdef line_sender* failed_impl = NULL
        if self._opts == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'establish() can\'t be called after close().')

        # We disable the GIL when calling `line_sender_build` since for HTTP
        # it can make HTTP requests to auto-detect the protocol version.
        _ensure_doesnt_have_gil(&gs)
        self._impl = line_sender_build(self._opts, &err)
        _ensure_has_gil(&gs)

        if self._impl == NULL:
            raise c_err_to_py(err)

        if self._buffer is None:
            try:
                self._buffer = self._new_buffer_for_sender()
            except:
                failed_impl = self._impl
                self._impl = NULL
                _ensure_doesnt_have_gil(&gs)
                line_sender_close(failed_impl)
                _ensure_has_gil(&gs)
                raise

        line_sender_opts_free(self._opts)
        self._opts = NULL

        # Request callbacks when rows are complete.
        self._buffer._row_complete_sender = PyWeakref_NewRef(self, None)
        self._last_flush_ms[0] = line_sender_now_micros() // 1000

        # Track and warn about overly quick reconnections to the server.
        cdef bint warn = False
        if WARN_HIGH_RECONNECTS:
            self._slot_id = <int32_t> qdb_active_senders_track_established(&warn)
            if warn:
                warnings.warn(
                    "questdb.Sender: "
                    f"Detected a burst of reconnections. "
                    "This may indicate an inefficient coding pattern where the sender is "
                    "frequently created and destroyed. "
                    "Consider reusing sender instance whenever possible."
                    "See: https://py-questdb-client.readthedocs.io/en/latest/sender.html#reuse-sender-objects",
                    UserWarning,
                    stacklevel=1
                )

    def __enter__(self) -> Sender:
        """Call :func:`Sender.establish` at the start of a ``with`` block."""
        self.establish()
        return self

    def __bytes__(self) -> bytes:
        """
        Inspect the contents of the internal buffer.

        The ``bytes`` value returned represents the unsent data.

        For QWP/UDP senders this always returns ``b''`` because encoding
        is deferred to flush. Use :func:`Sender.__len__` instead for a
        size estimate.

        Also see :func:`Sender.__len__`.
        """
        if self._buffer is None:
            return b''
        else:
            return bytes(self._buffer)

    def __len__(self) -> int:
        """
        Number of bytes of unsent data in the internal buffer.

        Equivalent (but cheaper) to ``len(bytes(sender))``.

        For QWP/UDP senders this returns an estimated size hint, not the
        exact serialized byte count.
        """
        if self._buffer is None:
            return 0
        else:
            return len(self._buffer)

    def transaction(self, table_name: str):
        """
        Start a :ref:`sender_transaction` block.
        """
        return SenderTransaction(self, table_name)

    def row(self,
            table_name: str,
            *,
            symbols: Optional[Dict[str, str]]=None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros, TimestampNanos, datetime.datetime, numpy.ndarray, Decimal]]]=None,
            at: Union[TimestampNanos, datetime.datetime, ServerTimestampType]):
        """
        Write a row to the internal buffer.

        This may be sent automatically depending on the ``auto_flush`` setting
        in the constructor.

        Refer to the :func:`Buffer.row` documentation for details on arguments.

        **Note**: Support for NumPy arrays (``numpy.array``) requires QuestDB server version 9.0.0 or higher.
        """
        if self._in_txn:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Cannot append rows explicitly inside a transaction')
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or ServerTimestamp"
            )
        if self._buffer is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                "row() can\'t be called: Sender is closed."
            )

        self._buffer.row(table_name, symbols=symbols, columns=columns, at=at)
        return self

    def dataframe(
            self,
            df,  # : pd.DataFrame
            *,
            table_name: Optional[str] = None,
            table_name_col: Union[None, int, str] = None,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[ServerTimestampType, int, str, TimestampNanos, datetime.datetime],
            max_rows_per_batch: int = DEFAULT_MAX_CHUNK_ROWS,
            schema_overrides: Optional[Dict[str, object]] = None):
        """
        Write a Pandas DataFrame to QuestDB.

        Over ILP/HTTP and ILP/TCP the frame is serialized into the internal
        row buffer; over QWP/UDP into fire-and-forget datagrams, with the
        same delivery caveats as ``row()``. Over QWP/WebSocket the frame is
        bulk-loaded through a poolless direct columnar connection opened from
        this sender's configuration for the call (the same direct path as
        :meth:`QuestDB.dataframe`, carrying the sender's auth/TLS regardless
        of how it was constructed); ``max_rows_per_batch`` applies only on
        that path, and passing ``schema_overrides`` on any other protocol
        raises. The direct load has no
        ordering relationship with rows buffered via :meth:`row` and does not
        flush them.

        Example:

        .. code-block:: python

            import pandas as pd
            import questdb as qi

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

            with qi.Sender.from_env() as sender:
                sender.dataframe(df, table_name='race_metrics', at='ts')

        See the buffer-level ``dataframe`` documentation for details on
        the supported column types and arguments.

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
        cdef direct_conn_source_t src
        cdef qdb_pystr_buf* ws_b = NULL
        cdef dataframe_plan_t ws_plan
        if _is_qwp_ws_protocol(self._c_protocol):
            if self._qwp_ws_opts == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    "dataframe() can't be called: Sender is closed.")
            src.db = NULL
            src.opts = self._qwp_ws_opts
            ws_b = qdb_pystr_buf_new()
            ws_plan = dataframe_plan_blank()
            try:
                _direct_dataframe_run(
                    &src,
                    0.0,
                    ws_b,
                    &ws_plan,
                    df,
                    table_name,
                    table_name_col,
                    symbols,
                    at,
                    max_rows_per_batch,
                    schema_overrides)
                return self
            finally:
                qdb_pystr_buf_free(ws_b)
        if schema_overrides is not None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'schema_overrides is only supported over QWP/WebSocket; '
                'the row-serializing protocols ignore it. Drop the '
                'argument or connect over ws:: / wss::.')
        if self._in_txn:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Cannot append rows explicitly inside a transaction')
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or ServerTimestamp"
            )
        if self._auto_flush_mode.enabled:
            af.sender = self._impl
            af.mode = self._auto_flush_mode
            af.last_flush_ms = self._last_flush_ms

        if self._buffer is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                "dataframe() can\'t be called: Sender is closed."
            )
        _dataframe(
            af,
            self._buffer._impl,
            self._buffer._b,
            df,
            table_name,
            table_name_col,
            symbols,
            at)
        return self

    cpdef flush(
            self,
            Buffer buffer=None,
            bint clear=True,
            bint transactional=False):
        """
        If called with no arguments, immediately flushes the internal buffer.

        Alternatively you can flush a buffer that was constructed explicitly
        by passing ``buffer``.

        The buffer will be cleared by default, unless ``clear`` is set to
        ``False``.

        This method does nothing if the provided or internal buffer is empty.

        :param buffer: The buffer to flush. If ``None``, the internal buffer
            is flushed.

        With QWP/WebSocket, this publishes the buffer into the local sender
        queue and returns before the server necessarily ACKs the frame. Later
        terminal diagnostics fail subsequent sender calls and are available as
        :attr:`QuestDBError.qwp_ws_error`. Server diagnostics are also
        available through :func:`Sender.poll_qwp_ws_error`.

        :param clear: If ``True``, the flushed buffer is cleared (default).
            If ``False``, the flushed buffer is left in the internal buffer.
            Note that ``clear=False`` is only supported if ``buffer`` is also
            specified.

        :param transactional: If ``True`` ensures that the flushed buffer
            contains row for a single table, ensuring all data can be written
            transactionally. This feature requires ILP/HTTP and is not available
            when connecting over TCP. *Default: False.*

        The Python GIL is released during the network IO operation.
        """
        cdef line_sender* sender = self._impl
        cdef line_sender_error* err = NULL
        cdef line_sender_buffer* c_buf = NULL
        cdef PyThreadState* gs = NULL  # GIL state. NULL means we have the GIL.
        cdef bint ok = False

        if self._in_txn:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Cannot flush explicitly inside a transaction')

        if buffer is None and not clear:
            raise ValueError('The internal buffer must always be cleared.')

        if sender == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'flush() can\'t be called: Sender is closed.')
        if buffer is not None:
            buffer._check_impl()
            self._check_buffer_protocol(buffer)
            c_buf = buffer._impl
        else:
            c_buf = self._buffer._impl
        if line_sender_buffer_size(c_buf) == 0 and not _is_qwp_ws_protocol(self._c_protocol):
            return

        # We might be blocking on IO, so temporarily release the GIL.
        _ensure_doesnt_have_gil(&gs)
        if transactional:
            ok = line_sender_flush_and_keep_with_flags(
                    sender,
                    c_buf,
                    transactional,
                    &err)
            if ok and clear:
                line_sender_buffer_clear(c_buf)
        elif clear:
            ok = line_sender_flush(sender, c_buf, &err)
        else:
            ok = line_sender_flush_and_keep(sender, c_buf, &err)
        _ensure_has_gil(&gs)
        if ok and c_buf == self._buffer._impl:
            self._last_flush_ms[0] = line_sender_now_micros() // 1000
        if not ok:
            if c_buf == self._buffer._impl:
                # Prevent a follow-up call to `.close(flush=True)` (as is
                # usually called from `__exit__`) to raise after the sender
                # entered an error state following a failed call to `.flush()`.
                # Note: In this case `clear` is always `True`.
                line_sender_buffer_clear(c_buf)
            if _is_tcp_protocol(self._c_protocol):
                # Provide further context pointing to the logs.
                raise c_err_to_py_fmt(err, _FLUSH_FMT)
            else:
                raise c_err_to_py(err)

    cdef inline void_int _check_qwp_ws(self, str method) except -1:
        if self._impl == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'{method}() can\'t be called: Sender is closed.')
        if not _is_qwp_ws_protocol(self._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'{method}() is only supported for QWP/WebSocket senders.')

    cdef inline void_int _check_buffer_protocol(self, Buffer buffer) except -1:
        cdef bint need_qwp = (
            _is_qwp_udp_protocol(self._c_protocol) or
            _is_qwp_ws_protocol(self._c_protocol))
        if need_qwp and not buffer._qwp:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'QWP sender requires a QWP buffer. Use Sender.new_buffer() '
                'to build a matching buffer.')
        if buffer._qwp and not need_qwp:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'ILP sender requires an ILP buffer. Use Sender.new_buffer() '
                'to build a matching buffer.')

    def flush_and_get_fsn(self, Buffer buffer=None):
        """
        Publish a QWP/WebSocket buffer locally, clear it on success, and return
        the assigned frame sequence number.
        """
        cdef line_sender* sender = self._impl
        cdef line_sender_error* err = NULL
        cdef line_sender_buffer* c_buf = NULL
        cdef line_sender_qwpws_fsn fsn
        cdef PyThreadState* gs = NULL
        cdef bint ok = False

        if self._in_txn:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Cannot flush explicitly inside a transaction')
        self._check_qwp_ws('flush_and_get_fsn')
        if buffer is not None:
            buffer._check_impl()
            self._check_buffer_protocol(buffer)
            c_buf = buffer._impl
        else:
            c_buf = self._buffer._impl

        _ensure_doesnt_have_gil(&gs)
        ok = line_sender_qwpws_flush_and_get_fsn(sender, c_buf, &fsn, &err)
        _ensure_has_gil(&gs)
        if not ok:
            if c_buf == self._buffer._impl:
                line_sender_buffer_clear(c_buf)
            raise c_err_to_py(err)
        if c_buf == self._buffer._impl:
            self._last_flush_ms[0] = line_sender_now_micros() // 1000
        if fsn.has_value:
            return fsn.value
        return None

    def flush_and_keep_and_get_fsn(self, Buffer buffer=None):
        """
        Publish a QWP/WebSocket buffer locally without clearing it and return
        the assigned frame sequence number.
        """
        cdef line_sender* sender = self._impl
        cdef line_sender_error* err = NULL
        cdef line_sender_buffer* c_buf = NULL
        cdef line_sender_qwpws_fsn fsn
        cdef PyThreadState* gs = NULL
        cdef bint ok = False

        if self._in_txn:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'Cannot flush explicitly inside a transaction')
        self._check_qwp_ws('flush_and_keep_and_get_fsn')
        if buffer is not None:
            buffer._check_impl()
            self._check_buffer_protocol(buffer)
            c_buf = buffer._impl
        else:
            c_buf = self._buffer._impl

        _ensure_doesnt_have_gil(&gs)
        ok = line_sender_qwpws_flush_and_keep_and_get_fsn(
            sender, c_buf, &fsn, &err)
        _ensure_has_gil(&gs)
        if not ok:
            if c_buf == self._buffer._impl:
                line_sender_buffer_clear(c_buf)
            raise c_err_to_py(err)
        if c_buf == self._buffer._impl:
            self._last_flush_ms[0] = line_sender_now_micros() // 1000
        if fsn.has_value:
            return fsn.value
        return None

    def published_fsn(self):
        """
        Highest QWP/WebSocket frame sequence number published locally.
        """
        cdef line_sender_qwpws_fsn fsn
        cdef line_sender_error* err = NULL

        self._check_qwp_ws('published_fsn')
        if not line_sender_qwpws_published_fsn(self._impl, &fsn, &err):
            raise c_err_to_py(err)
        if fsn.has_value:
            return fsn.value
        return None

    def acked_fsn(self):
        """
        Highest QWP/WebSocket frame sequence number completed by ACK or
        drop-and-continue rejection.
        """
        cdef line_sender_qwpws_fsn fsn
        cdef line_sender_error* err = NULL

        self._check_qwp_ws('acked_fsn')
        if not line_sender_qwpws_acked_fsn(self._impl, &fsn, &err):
            raise c_err_to_py(err)
        if fsn.has_value:
            return fsn.value
        return None

    def await_acked_fsn(self, fsn, timeout_millis):
        """
        Wait until the QWP/WebSocket completion watermark reaches ``fsn``.

        Returns ``True`` once every frame published so far (which includes
        ``fsn``) has been acknowledged, or ``False`` if the no-progress
        timeout elapsed before the acknowledgement watermark reached ``fsn``.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef uint64_t c_fsn
        cdef uint64_t c_timeout_millis
        cdef line_sender_qwpws_fsn acked
        cdef bint ok = False

        self._check_qwp_ws('await_acked_fsn')
        if not isinstance(fsn, int) or isinstance(fsn, bool):
            raise TypeError('"fsn" must be a non-negative int.')
        if fsn < 0:
            raise ValueError('"fsn" must be a non-negative int.')
        if not isinstance(timeout_millis, int) or isinstance(timeout_millis, bool):
            raise TypeError('"timeout_millis" must be a non-negative int.')
        if timeout_millis < 0:
            raise ValueError('"timeout_millis" must be a non-negative int.')
        c_fsn = fsn
        c_timeout_millis = timeout_millis

        # Fast path: the completion watermark may already cover ``fsn``.
        if not line_sender_qwpws_acked_fsn(self._impl, &acked, &err):
            raise c_err_to_py(err)
        if acked.has_value and acked.value >= c_fsn:
            return True

        # `line_sender_qwpws_wait` drains every frame published so far (a
        # superset of ``fsn``) up to the "ok" ack level. The no-progress
        # deadline (`timeout_millis`; 0 == wait indefinitely) surfaces as a
        # `line_sender_error_failover_retry`, which we translate back into the
        # historical ``reached == False`` return rather than raising.
        _ensure_doesnt_have_gil(&gs)
        ok = line_sender_qwpws_wait(
            self._impl,
            qwpws_ack_level.qwpws_ack_level_ok,
            c_timeout_millis,
            &err)
        _ensure_has_gil(&gs)
        if not ok:
            if line_sender_error_get_code(err) == \
                    line_sender_error_failover_retry:
                line_sender_error_free(err)
                return False
            raise c_err_to_py(err)

        # Re-read the watermark now that the wait has drained in-flight frames.
        err = NULL
        if not line_sender_qwpws_acked_fsn(self._impl, &acked, &err):
            raise c_err_to_py(err)
        return bool(acked.has_value and acked.value >= c_fsn)

    def drive_once(self):
        """
        Drive one QWP/WebSocket progress step for manual progress senders.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef cbool progressed = False
        cdef bint ok = False

        self._check_qwp_ws('drive_once')
        _ensure_doesnt_have_gil(&gs)
        ok = line_sender_qwpws_drive_once(self._impl, &progressed, &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise c_err_to_py(err)
        return bool(progressed)

    def poll_qwp_ws_error(self):
        """
        Poll the next structured QWP/WebSocket diagnostic.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_qwpws_error* qwp_err = NULL
        cdef line_sender_qwpws_error_view view

        self._check_qwp_ws('poll_qwp_ws_error')
        if not line_sender_qwpws_poll_error(self._impl, &qwp_err, &err):
            raise c_err_to_py(err)
        if qwp_err == NULL:
            return None
        try:
            view = line_sender_qwpws_error_get_view(qwp_err)
            return _qwp_ws_error_from_raw(c_qwp_ws_error_view_to_raw(view))
        finally:
            line_sender_qwpws_error_free(qwp_err)

    def qwp_ws_errors_dropped(self):
        """
        Number of QWP/WebSocket diagnostics dropped from the bounded ring.
        """
        cdef line_sender_error* err = NULL
        cdef uint64_t dropped = 0

        self._check_qwp_ws('qwp_ws_errors_dropped')
        if not line_sender_qwpws_errors_dropped(self._impl, &dropped, &err):
            raise c_err_to_py(err)
        return dropped

    @property
    def connection_events_dropped(self) -> int:
        """
        Total connection events discarded by the listener inbox's
        drop-oldest policy. ``0`` when no listener is registered.
        """
        if self._impl == NULL:
            return 0
        return line_sender_connection_events_dropped(self._impl)

    @property
    def connection_events_delivered(self) -> int:
        """
        Total connection events delivered to the listener. ``0`` when no
        listener is registered.
        """
        if self._impl == NULL:
            return 0
        return line_sender_connection_events_delivered(self._impl)

    def close_drain(self):
        """
        Stop accepting new QWP/WebSocket publications and wait for already
        published frames to resolve.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef bint ok = False

        self._check_qwp_ws('close_drain')
        _ensure_doesnt_have_gil(&gs)
        ok = line_sender_qwpws_close_drain(self._impl, &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise c_err_to_py(err)

    cdef _close(self):
        cdef PyThreadState* gs = NULL
        cdef line_sender* impl = self._impl
        cdef line_sender_opts* opts = self._opts
        cdef line_sender_opts* qwp_ws_opts = self._qwp_ws_opts
        self._impl = NULL
        self._opts = NULL
        self._qwp_ws_opts = NULL
        if impl != NULL or opts != NULL or qwp_ws_opts != NULL:
            _ensure_doesnt_have_gil(&gs)
            if impl != NULL:
                line_sender_close(impl)
            line_sender_opts_free(opts)
            line_sender_opts_free(qwp_ws_opts)
            _ensure_has_gil(&gs)
        self._buffer = None
        self._qwp_ws_error_handler = None
        self._connection_listener = None
        if self._slot_id != -1:
            qdb_active_senders_track_closed(<uint32_t>self._slot_id)
            self._slot_id = -1

    cpdef close(self, bint flush=True):
        """
        Disconnect.

        This method is idempotent and can be called repeatedly.

        Once a sender is closed, it can't be re-used.

        :param bool flush: If ``True``, flush the internal buffer before closing.
            For QWP/WebSocket, this also drains already-published frames before
            closing.
        """
        try:
            if (flush and (self._impl != NULL) and
                    (not line_sender_must_close(self._impl))):
                self.flush(None, True)
                if _is_qwp_ws_protocol(self._c_protocol):
                    self.close_drain()
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
        free(self._last_flush_ms)


cdef class PooledSender:
    """
    A row-building sender borrowed from a :class:`QuestDB` pool.

    Obtain a lease with :meth:`QuestDB.sender`; ``close()`` returns the
    native sender to the pool. Rows publish into an ordered,
    store-and-forward-covered QWP stream over one pooled connection.

    The surface is deliberately small: ``row()``, ``flush()``, ``wait()``
    and ``close()``. ``len(sender)`` is the number of buffered rows.
    DataFrame bulk loads go through :meth:`QuestDB.dataframe`.
    """
    cdef qwp_sender* _qwp
    cdef questdb_db* _db
    cdef QuestDB _handle
    cdef Buffer _buffer
    cdef object _lock

    def __cinit__(self):
        self._qwp = NULL
        self._db = NULL
        self._handle = None
        self._buffer = None
        self._lock = threading.RLock()

    cdef void _attach(
            self,
            QuestDB handle,
            questdb_db* db,
            qwp_sender* sender,
            Buffer buffer) noexcept:
        self._handle = handle
        self._db = db
        self._qwp = sender
        self._buffer = buffer

    cdef void_int _check_open(self, str method) except -1:
        if self._qwp == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f"{method}() can't be called: Sender is closed.")

    cdef void_int _wait_locked(self, uint64_t timeout_millis) except -1:
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef bint ok = False
        _ensure_doesnt_have_gil(&gs)
        ok = qwp_sender_wait(
            self._qwp,
            qwpws_ack_level.qwpws_ack_level_ok,
            timeout_millis,
            &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise c_err_to_py(err)

    cdef void_int _flush_locked(self, bint wait) except -1:
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef bint ok = False
        self._check_open('flush')
        if line_sender_buffer_row_count(self._buffer._impl) == 0:
            if wait:
                self._wait_locked(0)
            return 0
        _ensure_doesnt_have_gil(&gs)
        if wait:
            ok = qwp_sender_flush_buffer_and_wait(
                self._qwp,
                self._buffer._impl,
                qwpws_ack_level.qwpws_ack_level_ok,
                &err)
        else:
            ok = qwp_sender_flush_buffer(
                self._qwp, self._buffer._impl, &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise c_err_to_py(err)
        qdb_pystr_buf_clear(self._buffer._b)

    cdef void _release_locked(self) except *:
        cdef qwp_sender* sender = self._qwp
        cdef questdb_db* db = self._db
        cdef QuestDB handle = self._handle
        cdef PyThreadState* gs = NULL
        if sender == NULL:
            return
        self._qwp = NULL
        self._db = NULL
        self._buffer = None
        self._handle = None
        _ensure_doesnt_have_gil(&gs)
        questdb_db_return_sender(db, sender)
        _ensure_has_gil(&gs)
        if handle is not None:
            handle._end_db_use()

    def __enter__(self):
        with self._lock:
            self._check_open('__enter__')
        return self

    def row(
            self,
            table_name: str,
            *,
            symbols: Optional[Dict[str, Optional[str]]] = None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros,
                      TimestampNanos, datetime.datetime, numpy.ndarray,
                      Decimal]]] = None,
            at: Union[ServerTimestampType, TimestampNanos,
                      datetime.datetime]):
        """Append one row to this sender's QWP buffer."""
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or "
                "ServerTimestamp")
        with self._lock:
            self._check_open('row')
            self._buffer._row(
                False, table_name, symbols, columns, at)
        return self

    def dataframe(
            self,
            df,
            *,
            table_name: Optional[str] = None,
            table_name_col: Union[None, int, str] = None,
            symbols: Union[str, bool, List[int], List[str]] = 'auto',
            at: Union[ServerTimestampType, int, str, TimestampNanos,
                      datetime.datetime],
            max_rows_per_batch: int = DEFAULT_MAX_CHUNK_ROWS,
            schema_overrides: Optional[Dict[str, object]] = None):
        """
        Bulk-load a whole DataFrame over a direct columnar connection
        borrowed from the pool for the duration of this call.

        This is the pooled equivalent of :meth:`QuestDB.dataframe`: the frame
        is committed over its own connection and becomes visible to SQL
        immediately, without waiting for rows appended to this sender with
        :meth:`row` to drain. There is therefore **no ordering relationship**
        between ``dataframe()`` and buffered rows — ``dataframe()`` does not
        flush them; publish those with :meth:`flush`.

        Arguments mirror :meth:`QuestDB.dataframe`.
        """
        cdef QuestDB handle
        with self._lock:
            self._check_open('dataframe')
            handle = self._handle
        handle.dataframe(
            df,
            table_name=table_name,
            table_name_col=table_name_col,
            symbols=symbols,
            at=at,
            max_rows_per_batch=max_rows_per_batch,
            schema_overrides=schema_overrides)
        return self

    def __len__(self):
        """Number of buffered (unpublished) rows."""
        with self._lock:
            self._check_open('__len__')
            return line_sender_buffer_row_count(self._buffer._impl)

    def flush(self, *, bint wait=False):
        """
        Publish and clear buffered rows.

        By default this returns after local store-and-forward acceptance.
        Pass ``wait=True`` to wait for the server's OK acknowledgement.
        """
        with self._lock:
            self._flush_locked(wait)
        return self

    def wait(self, timeout_millis: int=0):
        """
        Wait for every publication on this sender to receive an OK ack.

        ``timeout_millis`` is a no-progress timeout; ``0`` waits indefinitely.
        """
        if timeout_millis < 0:
            raise ValueError('timeout_millis must be non-negative.')
        with self._lock:
            self._check_open('wait')
            self._wait_locked(<uint64_t>timeout_millis)
        return self

    def close(self, flush: bool=True, wait: bool=False):
        """
        Return this sender to its pool. Idempotent.

        Pending rows are published by default. ``wait=True`` additionally
        waits for an OK ack before returning the sender to the pool.
        """
        with self._lock:
            try:
                if flush and self._qwp != NULL:
                    self._flush_locked(wait)
            finally:
                self._release_locked()

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        self.close(exc_type is None, False)

    def __dealloc__(self):
        if self._lock is not None:
            with self._lock:
                self._release_locked()


cdef class PooledReader:
    """
    A reader lease borrowed from a :class:`QuestDB` pool.

    The read-side twin of :meth:`QuestDB.sender`: obtain a lease with
    :meth:`QuestDB.reader`; it holds one pooled reader connection for
    its lifetime and runs queries on it sequentially via :meth:`query`.
    ``close()`` (or leaving the ``with`` block) releases the
    connection: back to the pool if the last query was drained cleanly,
    dropped otherwise.

    Queries are strictly sequential — one result at a time. Fully drain
    (or ``close()``) each :class:`QueryResult` before calling
    :meth:`query` again; running the next query while the previous
    result is still open raises ``QuestDBError``. Closing a result
    before draining it tears down the lease's connection (the server
    may still be streaming into it), after which the lease is terminal:
    ``close()`` it and obtain a fresh one with ``QuestDB.reader()``.

    Because every query shares one connection, passing
    ``reset_symbol_dict=False`` to follow-up queries keeps the
    connection's SYMBOL dictionary warm across them instead of
    resetting it per query.

    The lease counts as an active use of the :class:`QuestDB` handle
    (``QuestDB.close()`` waits for it) and has thread affinity: use one
    lease per thread, on the thread that created it.
    """
    cdef QuestDB _handle
    cdef _ReaderHandle _reader
    cdef _CursorHandle _last_cursor
    cdef object _lock

    def __cinit__(self):
        self._handle = None
        self._reader = None
        self._last_cursor = None
        self._lock = threading.RLock()

    cdef void _attach(
            self,
            QuestDB handle,
            _ReaderHandle reader) noexcept:
        self._handle = handle
        self._reader = reader

    cdef void_int _check_open(self, str method) except -1:
        if self._reader is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f"{method}() can't be called: the reader lease is closed.")

    cdef void _release_locked(self) except *:
        cdef _ReaderHandle reader = self._reader
        cdef _CursorHandle last = self._last_cursor
        cdef QuestDB handle = self._handle
        if reader is None:
            return
        self._reader = None
        self._last_cursor = None
        self._handle = None
        if last is not None:
            last._free()
        reader._close()
        if handle is not None:
            handle._end_db_use()

    def __enter__(self):
        with self._lock:
            self._check_open('__enter__')
        return self

    def query(
            self,
            str sql,
            object binds=None,
            *,
            bint reset_symbol_dict=True) -> QueryResult:
        """
        Execute a SQL query on the lease's connection and return a
        :class:`QueryResult`.

        ``sql``, ``binds`` and ``reset_symbol_dict`` behave exactly as
        on :meth:`QuestDB.query`, except the query runs on the reader
        this lease holds instead of a per-call pool borrow. The
        previous query's result must be fully drained (or closed)
        first; ``reset_symbol_dict=False`` reuses the connection's
        SYMBOL dictionary built up by the lease's earlier queries.
        """
        cdef _CursorHandle cursor_handle
        cdef _CursorHandle last
        if binds is not None and not isinstance(binds, (list, tuple)):
            raise TypeError(
                '"binds" must be a list or tuple of positional bind '
                f'parameters (or None), not {_fqn(type(binds))}')
        with self._lock:
            self._check_open('query')
            last = self._last_cursor
            if last is not None:
                if last._cursor != NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall,
                        'the previous QueryResult is still open: drain '
                        'it fully or close() it before running the next '
                        'query on this lease.')
                if self._reader._must_close:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall,
                        "the lease's connection is terminal: the "
                        'previous query was not drained to its clean '
                        'end, so its transport was torn down. close() '
                        'this lease and obtain a new one with '
                        'QuestDB.reader().')
            cursor_handle = _execute_query(
                self._reader, sql, binds, reset_symbol_dict)
            cursor_handle._owns_reader = False
            self._last_cursor = cursor_handle
        return QueryResult(cursor_handle)

    def close(self):
        """
        Release the lease's reader connection. Idempotent.

        A still-open (undrained) last result is freed first, which
        tears down the connection; a cleanly-drained connection is
        returned to the pool, any other is dropped and the pool refills
        on demand.
        """
        with self._lock:
            self._release_locked()

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        self.close()

    def __dealloc__(self):
        if self._lock is not None:
            with self._lock:
                self._release_locked()


