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
    'Char',
    'ConnectionEvent',
    'ConnectionEventKind',
    'DateMillis',
    'Geohash',
    'Long256',
    'PooledReader',
    'PooledSender',
    'Protocol',
    'QueryResult',
    'QuestDB',
    'QuestDBError',
    'QuestDBErrorCode',
    'QuestDBServerRejectionError',
    'QwpWsProgress',
    'Sender',
    'SenderError',
    'SenderErrorCategory',
    'SenderErrorPolicy',
    'SenderTransaction',
    'ServerInfo',
    'ServerRole',
    'ServerTimestamp',
    'ServerTimestampType',
    'TimestampMicros',
    'TimestampNanos',
    'TlsCa',
    'UnsupportedDataFrameShapeError',
    'WARN_HIGH_RECONNECTS',
]

# For prototypes: https://github.com/cython/cython/tree/master/Cython/Includes
from libc.stdint cimport uint8_t, uint16_t, uint64_t, int64_t, int32_t, uint32_t, \
    uintptr_t, INT64_MAX, INT64_MIN
from libc.stdlib cimport malloc, calloc, realloc, free, qsort
from libc.string cimport strncmp, memset, memcpy, memcmp, strlen
from libc.math cimport isnan, floor
from cpython.datetime cimport datetime as cp_datetime
from cpython.datetime cimport timedelta as cp_timedelta
from cpython.datetime cimport (
    PyDateTime_GET_YEAR, PyDateTime_GET_MONTH, PyDateTime_GET_DAY,
    PyDateTime_DATE_GET_HOUR, PyDateTime_DATE_GET_MINUTE,
    PyDateTime_DATE_GET_SECOND, PyDateTime_DATE_GET_MICROSECOND,
)
from cpython.bool cimport bool
from cpython.ref cimport Py_XDECREF
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetRef
from cpython.object cimport PyObject, PyTypeObject, PyObject_TypeCheck
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

# Imported before the includes because `dataframe.pxi` binds two of its
# classes at module-init time.
import ipaddress

include "dataframe.pxi"
include "egress.pxi"

from enum import Enum
from typing import List, Dict, Union, Any, Optional, Tuple
from dataclasses import dataclass
from cpython.bytes cimport (PyBytes_FromStringAndSize,
                            PyBytes_GET_SIZE, PyBytes_AsString, PyBytes_Check)
from cpython.bytearray cimport (PyByteArray_AsString, PyByteArray_Size,
                               PyByteArray_Check)
from cpython.long cimport PyLong_AsLongLongAndOverflow
from cpython.memoryview cimport PyMemoryView_Check

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
    SymbolDictFull = line_sender_error_symbol_dict_full
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
    def __init__(self, code, msg, sender_error=None, *, in_doubt=False):
        super().__init__(msg)
        self._code = code
        self._sender_error = sender_error
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
    def sender_error(self):
        """
        Return the structured QWP/WebSocket diagnostic, if this error carries
        one from a QWP/WebSocket sender failure.
        """
        if self._sender_error is not None:
            self._sender_error = _sender_error_from_raw(self._sender_error)
        return self._sender_error


class QuestDBServerRejectionError(QuestDBError):
    """
    A terminal QWP/WebSocket server rejection.

    The structured server payload is available through
    :attr:`QuestDBError.sender_error`.
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
    elif code == line_sender_error_symbol_dict_full:
        return QuestDBErrorCode.SymbolDictFull
    else:
        raise ValueError('Internal error converting error code.')


def _debug_error_code_to_py(int raw_code):
    """Internal ABI test hook for the native-to-Python error mapping."""
    return c_err_code_to_py(<line_sender_error_code>raw_code)


cdef inline object c_sender_error_view_to_raw(
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
    """Extract ``QuestDBError`` fields from a C error, which will be freed."""
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
    cdef object py_sender_error = None
    try:
        py_code = c_err_code_to_py(code)
        py_msg = PyUnicode_FromStringAndSize(c_msg, <Py_ssize_t>c_len)
        if line_sender_error_qwpws_get_view(err, &qwp_ws_view):
            py_sender_error = c_sender_error_view_to_raw(qwp_ws_view)
        return (py_code, py_msg, py_sender_error, in_doubt)
    finally:
        questdb_error_free(err)


cdef inline object c_err_to_py(line_sender_error* err):
    """Construct a ``QuestDBError`` from a C error, which will be freed."""
    cdef object tup = c_err_to_fields(err)
    if tup[0] == QuestDBErrorCode.ServerRejection:
        return QuestDBServerRejectionError(
            tup[0], tup[1], tup[2], in_doubt=tup[3])
    return QuestDBError(tup[0], tup[1], tup[2], in_doubt=tup[3])


cdef inline object c_err_to_py_fmt(line_sender_error* err, str fmt):
    """Construct a ``QuestDBError`` from a C error, which will be freed."""
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
    if dt != dt:
        raise ValueError('NaT is not a valid timestamp.')
    if not _NAIVE_DATETIME_WARNED:
        _NAIVE_DATETIME_WARNED = True
        # cdef frames and this module's def entry points are invisible to
        # the warnings stack walker, so level 1 is already the user's call.
        warnings.warn(
            'Naive datetime interpreted as UTC (questdb 4.x used local '
            'time). If you meant "now", use TimestampNanos.now() or '
            'datetime.now(timezone.utc); pass timezone-aware datetimes '
            'to silence this warning.',
            UserWarning,
            stacklevel=1)
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
    # INT64_MAX // 1000 == 9_223_372_036_854_775 is the largest microsecond
    # value whose nanosecond product still fits in an int64.
    if (micros > <int64_t>9_223_372_036_854_775 or
            micros < <int64_t>-9_223_372_036_854_775):
        raise ValueError(
            'datetime is out of range for a nanosecond timestamp: '
            'must be between 1677-09-21T00:12:43.145225Z and '
            '2262-04-11T23:47:16.854775Z.')
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

    def __cinit__(self, value):
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

    def __cinit__(self, value):
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


cdef class Char:
    """A QuestDB CHAR value stored as one UTF-16 code unit.

    ``'\\x00'`` is stored as code unit 0. CHAR has no physical ``NULL``
    representation: QWP/Arrow egress returns it as 0, while text output
    renders it as empty. Some QuestDB SQL operations treat code unit 0 as
    CHAR's null/absent marker.
    """
    cdef uint16_t _value

    def __cinit__(self, value):
        if not isinstance(value, str):
            raise TypeError('value must be a str.')
        if len(value) != 1:
            raise ValueError('value must contain exactly one code point.')
        code_point = ord(value)
        if code_point > 0xFFFF:
            raise ValueError(
                'a supplementary character needs a surrogate pair and '
                'cannot fit CHAR; use str/VARCHAR instead.')
        self._value = <uint16_t>code_point

    @property
    def value(self) -> str:
        """The single Python code point represented by this CHAR."""
        return chr(self._value)

    def __repr__(self):
        return f'Char({self.value!r})'


cdef class DateMillis:
    """A QuestDB DATE value in milliseconds since the Unix epoch (UTC).

    QuestDB DATE is a millisecond timestamp, not a civil date. The full signed
    64-bit range is accepted, including pre-epoch values. ``INT64_MIN`` is
    accepted but reads back from QuestDB as ``NULL``.

    This is how :func:`Buffer.row <questdb.ingress.Buffer.row>` writes a
    DATE column. DataFrames claim DATE from the column's Arrow type
    instead — ``pa.timestamp('ms')`` (naive or tz-aware),
    ``pa.date32()``, or ``pa.date64()`` — so there is no DATE cell type
    and no ``'date'`` kind for ``schema_overrides``. A NumPy
    ``datetime64[ms]`` dtype has no route of its own to DATE and widens
    to a microsecond TIMESTAMP, unless the frame carries a
    ``df.attrs['questdb']`` claim naming the column DATE, which puts the
    Arrow type back on it first. Like the other QWP-only types, DATE
    needs a QWP sender; ILP senders have no DATE type, so the datetime
    columns they accept all land as TIMESTAMP.
    """
    cdef int64_t _value

    def __cinit__(self, millis):
        if isinstance(millis, bool) or not isinstance(millis, int):
            raise TypeError('millis must be an int.')
        if millis < INT64_MIN or millis > INT64_MAX:
            raise ValueError('millis must fit in a signed 64-bit integer.')
        self._value = <int64_t>millis

    @classmethod
    def from_datetime(cls, dt: datetime.datetime):
        """Construct a ``DateMillis`` by flooring a datetime to milliseconds."""
        if not isinstance(dt, cp_datetime):
            raise TypeError('dt must be a datetime object.')
        return cls(datetime_to_micros(dt) // 1000)

    @classmethod
    def now(cls):
        """Construct a ``DateMillis`` from the current time as UTC."""
        return cls(line_sender_now_micros() // 1000)

    @property
    def value(self) -> int:
        """Milliseconds since the Unix epoch (UTC)."""
        return self._value

    def __repr__(self):
        return f'DateMillis({self._value})'


cdef class Long256:
    """An unsigned 256-bit integer for a QuestDB LONG256 column.

    The value whose four 64-bit limbs are all ``0x8000000000000000`` is
    accepted but reads back from QuestDB as ``NULL``.
    """
    cdef bytes _bytes

    def __cinit__(self, value):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError('value must be an int.')
        if value < 0 or value >= (1 << 256):
            raise ValueError('value must be in the range 0 <= value < 2**256.')
        # The column write reads exactly 32 bytes from the pointer. The
        # unbound `int.to_bytes` keeps a subclass override out of the way,
        # so the result is always a `bytes` of exactly that width.
        self._bytes = int.to_bytes(value, 32, 'little')

    @property
    def value(self) -> int:
        """The unsigned integer value."""
        return int.from_bytes(self._bytes, 'little')

    def __repr__(self):
        return f'Long256({self.value})'


cdef class Geohash:
    """A QuestDB GEOHASH value represented by bits and precision.

    Precision is pinned per column within one buffer's worth of rows.
    The first GEOHASH cell written to a column fixes the precision for
    the rest of that batch, and a later cell at a different precision
    is rejected by :func:`Buffer.row <questdb.ingress.Buffer.row>`
    itself, with the buffer rewound to what it held before that row --
    the bad row is not left waiting for a flush.

    The pin goes with the buffer: a flush clears it and the next batch
    starts over. A precision the server's column does not have is
    therefore the server's to reject, at flush time.
    """
    cdef uint64_t _bits
    cdef uint8_t _precision

    def __cinit__(self, bits, precision):
        if isinstance(bits, bool) or not isinstance(bits, int):
            raise TypeError('bits must be an int.')
        if isinstance(precision, bool) or not isinstance(precision, int):
            raise TypeError('precision must be an int.')
        if precision < 1 or precision > 60:
            raise ValueError('precision must be in the range 1..60.')
        if bits < 0 or bits >= (1 << precision):
            raise ValueError('bits must be in the range 0 <= bits < 2**precision.')
        self._bits = <uint64_t>bits
        self._precision = <uint8_t>precision

    @classmethod
    def from_string(cls, value):
        """Construct a GEOHASH from one to twelve base32 characters."""
        if not isinstance(value, str):
            raise TypeError('value must be a str.')
        if len(value) < 1 or len(value) > 12:
            raise ValueError('geohash string must contain 1 to 12 characters.')
        bits = 0
        for char in value:
            digit = (
                '0123456789bcdefghjkmnpqrstuvwxyz'.find(char.lower())
                if char.isascii() else -1)
            if digit < 0:
                raise ValueError(
                    f'invalid geohash character {char!r}; expected the base32 '
                    'alphabet 0123456789bcdefghjkmnpqrstuvwxyz.')
            bits = bits * 32 + digit
        return cls(bits, 5 * len(value))

    @property
    def bits(self) -> int:
        """The packed geohash bits."""
        return self._bits

    @property
    def precision(self) -> int:
        """The precision in bits."""
        return self._precision

    def __repr__(self):
        return f'Geohash({self._bits}, {self._precision})'


# The two value unions the `row()` family accepts, defined here as well
# as in the stub. Annotating with them used to type-check and then
# `ImportError` at runtime, because they existed only in `_client.pyi`.
# Not exported: they name a parameter type, they are not part of the
# ingestion surface.
TransactionColumnValue = Union[
    None, bool, int, float, str, TimestampMicros, TimestampNanos,
    datetime.datetime, cnp.ndarray, Decimal]
RowColumnValue = Union[
    None, bool, int, float, str, TimestampMicros, TimestampNanos,
    datetime.datetime, cnp.ndarray, Decimal, uuid.UUID,
    ipaddress.IPv4Address, bytes, bytearray, memoryview, Char, DateMillis,
    Long256, Geohash]
# What `schema_overrides` accepts: a kind on its own, or a kind and its
# argument. Only 'geohash' takes one, the precision in bits.
SchemaOverrides = Dict[str, Union[str, Tuple[str, int]]]


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

        UUID, IPV4, BINARY, CHAR, DATE, LONG256, and GEOHASH columns are
        QWP-only and are not supported by ``SenderTransaction``, which is
        ILP/HTTP-only by construction.
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
            self._sender._buffer,
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
        if self._sender._buffer is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                "commit() can't be called: Sender is closed.")
        # Checked here rather than left to the flush below, which is
        # skipped when the buffer is empty. `dataframe()` counts into
        # `_row_depth` before it writes anything, so a commit re-entered
        # from the plan build would find nothing to flush, end the
        # transaction, and leave the frame's rows to go out afterwards
        # outside it.
        self._sender._buffer._check_not_in_row('commit')
        # `_in_txn` has to come down first, because an explicit flush
        # inside a transaction is refused.
        self._sender._in_txn = False
        try:
            if len(self._sender._buffer):
                self._sender.flush(transactional=True)
        except:
            # A flush that reached the wire clears the buffer whether it
            # succeeded or not, so there is nothing left to commit and
            # the transaction is over -- saying otherwise would strand
            # the sender inside it and make the next `close(flush=True)`
            # raise over the caller's own error. A flush refused before
            # it got that far left the rows where they were, and the
            # transaction is still the caller's to finish or roll back.
            if (self._sender._buffer is not None
                    and len(self._sender._buffer)):
                self._sender._in_txn = True
            else:
                self._complete = True
            raise
        self._complete = True

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
    Internal row-serialization buffer, managed by :class:`Sender <questdb.Sender>`.

    Kept importable as ``questdb.ingress.Buffer`` for legacy ILP/HTTP and
    ILP/TCP code that constructs buffers explicitly and flushes them via
    ``sender.flush(buffer)``.
    """
    cdef line_sender_buffer* _impl
    cdef qdb_pystr_buf* _b
    cdef size_t _init_buf_size
    cdef size_t _max_name_len
    cdef bint _qwp
    cdef bint _marker_set
    cdef int _row_depth
    cdef object _row_complete_sender

    def __cinit__(self):
        self._impl = NULL
        self._b = NULL
        self._init_buf_size = 0
        self._max_name_len = 0
        self._qwp = False
        self._marker_set = False
        self._row_depth = 0
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

    def reserve(self, additional):
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

        Raises :class:`QuestDBError <questdb.QuestDBError>`
        (``InvalidApiCall``) if a
        :func:`Buffer.row <questdb.ingress.Buffer.row>` or
        :func:`Buffer.dataframe <questdb.ingress.Buffer.dataframe>` call
        on this buffer is still in progress.
        """
        self._check_impl()
        self._check_not_in_row('clear')
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

    cdef inline void_int _check_not_in_row(self, str method) except -1:
        """
        Refuse a call that arrives while a row is part-way through
        being written into this buffer.

        `row()` and the row-serializing `dataframe()` both hold a
        rewind point across values whose conversion runs Python code,
        and that code re-enters on the same thread, so nothing else
        stands between it and the buffer. The native buffer refuses a
        half-written row anyway; what it cannot do is stop the caller's
        own error handling from acting on the refusal.
        """
        if self._row_depth != 0:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f"{method}() can't be called while a row is being "
                "written into this buffer. `row()` or `dataframe()` is "
                "part-way through and something it called has come "
                "back here, most likely a column value whose "
                "conversion runs Python code.")

    cdef inline void_int _set_marker(self) except -1:
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_set_marker(self._impl, &err):
            raise c_err_to_py(err)
        self._marker_set = True

    cdef inline void_int _rewind_to_marker(self) except -1:
        cdef line_sender_error* err = NULL
        # The rewind point is spent either way: a successful rewind
        # consumes it, and a failed one leaves nothing worth keeping.
        self._marker_set = False
        if not line_sender_buffer_rewind_to_marker(self._impl, &err):
            raise c_err_to_py(err)

    cdef inline _clear_marker(self):
        line_sender_buffer_clear_marker(self._impl)
        self._marker_set = False

    cdef inline _rewind_after_failure(self):
        """
        Drop the part-written row while an error is already on its way
        out to the caller.

        A flush the row itself triggered takes the rewind point with it,
        and so does anything that re-enters the buffer while the row is
        being assembled. Either way there is no rewind point left, the
        part-written row is gone with it, and the error already being
        raised is the one the caller needs to see.
        """
        cdef line_sender_error* err = NULL
        if not self._marker_set:
            return
        self._marker_set = False
        if not line_sender_buffer_rewind_to_marker(self._impl, &err):
            line_sender_error_free(err)

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

    cdef inline void_int _require_qwp_column(
            self, str type_name) except -1:
        if not self._qwp:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f"{type_name} columns require a QWP sender "
                "(protocol 'udp', 'ws' or 'wss'); this buffer uses "
                "the ILP protocol (tcp/tcps/http/https).")

    cdef inline void_int _column_binary(
            self, line_sender_column_name c_name, object value) except -1:
        self._require_qwp_column('BINARY')
        cdef line_sender_error* err = NULL
        cdef const uint8_t* data
        cdef size_t data_len
        cdef Py_buffer view
        cdef bint release_view = False
        if isinstance(value, bytes):
            data = <const uint8_t*>PyBytes_AsString(value)
            data_len = <size_t>PyBytes_GET_SIZE(value)
        elif isinstance(value, bytearray):
            data = <const uint8_t*>PyByteArray_AsString(value)
            data_len = <size_t>PyByteArray_Size(value)
        else:
            if value.itemsize != 1 or not value.c_contiguous:
                raise ValueError(
                    'memoryview BINARY values must be C-contiguous with '
                    'one-byte items.')
            PyObject_GetBuffer(value, &view, PyBUF_SIMPLE)
            release_view = True
            data = <const uint8_t*>view.buf
            data_len = <size_t>view.len
        try:
            if not line_sender_buffer_column_binary(
                    self._impl, c_name, data, data_len, &err):
                raise c_err_to_py(err)
        finally:
            if release_view:
                PyBuffer_Release(&view)

    cdef inline void_int _column_uuid(
            self, str name, object value) except -1:
        self._require_qwp_column('UUID')
        cdef line_sender_error* err = NULL
        cdef line_sender_column_name c_name
        # Reading `value.int` runs Python code: `UUID` accepts subclasses and
        # a subclass may define `int` as a property. That code can recycle
        # the string arena an encoded column name borrows from, so the value
        # is reduced to C scalars first and the name is encoded after.
        cdef object i = value.int
        cdef uint64_t lo = <uint64_t>(i & 0xFFFFFFFFFFFFFFFF)
        cdef uint64_t hi = <uint64_t>(i >> 64)
        str_to_column_name(self._cleared_b(), name, &c_name)
        if not line_sender_buffer_column_uuid(
                self._impl, c_name, lo, hi, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_ipv4(
            self, str name, object value) except -1:
        self._require_qwp_column('IPV4')
        cdef line_sender_error* err = NULL
        cdef line_sender_column_name c_name
        # `IPv4Address.__int__` is pure Python and can recycle the string
        # arena an encoded column name borrows from, so the value is reduced
        # to a C scalar first and the name is encoded after.
        cdef uint32_t bits = <uint32_t>int(value)
        str_to_column_name(self._cleared_b(), name, &c_name)
        if not line_sender_buffer_column_ipv4(
                self._impl, c_name, bits, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_char(
            self, line_sender_column_name c_name, Char value) except -1:
        self._require_qwp_column('CHAR')
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_char(
                self._impl, c_name, value._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_date_millis(
            self, line_sender_column_name c_name, DateMillis value) except -1:
        self._require_qwp_column('DATE')
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_date(
                self._impl, c_name, value._value, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_long256(
            self, line_sender_column_name c_name, Long256 value) except -1:
        self._require_qwp_column('LONG256')
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_long256(
                self._impl, c_name,
                <const uint8_t*>PyBytes_AsString(value._bytes), &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_geohash(
            self, line_sender_column_name c_name, Geohash value) except -1:
        self._require_qwp_column('GEOHASH')
        cdef line_sender_error* err = NULL
        if not line_sender_buffer_column_geohash(
                self._impl, c_name, value._bits, value._precision, &err):
            raise c_err_to_py(err)

    cdef inline void_int _column_int(
            self, line_sender_column_name c_name, str name,
            object value) except -1:
        # A plain `int` goes to a LONG column, which holds 64 bits. A
        # wider value fits only in a LONG256 column, and the `Long256`
        # wrapper is what puts it there, so the error names the wrapper.
        # `PyLong_AsLongLongAndOverflow` sets a flag instead of raising,
        # which is what lets the message be written here.
        #
        # Only QWP senders have a LONG256 column. On an ILP buffer there
        # is nothing to point the user at, so the value goes straight to
        # `_column_i64` and the coercion to `int64_t` raises.
        cdef int overflow = 0
        cdef int64_t as_i64
        if not self._qwp:
            return self._column_i64(c_name, value)
        as_i64 = <int64_t>PyLong_AsLongLongAndOverflow(value, &overflow)
        if overflow != 0:
            raise OverflowError(
                f'Bad column {name!r}: integer out of range for a LONG '
                f'column (-2**63 .. 2**63-1). Wrap it in `Long256(...)` '
                f'to store it in a LONG256 column.')
        return self._column_i64(c_name, as_i64)

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
        # `c_name` borrows storage that `Buffer.clear()` recycles, so it stays
        # valid only while no Python code runs. A branch whose value needs
        # Python to convert takes `name` instead and encodes it itself, once
        # the conversion is done.
        str_to_column_name(self._cleared_b(), name, &c_name)
        if PyBool_Check(<PyObject*>value):
            self._column_bool(c_name, value)
        elif PyLong_CheckExact(<PyObject*>value):
            self._column_int(c_name, name, value)
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
        elif _is_decimal(value):
            self._column_decimal(c_name, value)
        else:
            self._column_qwp_only(c_name, name, value)

    cdef void_int _column_qwp_only(
            self, line_sender_column_name c_name, str name,
            object value) except -1:
        # The QWP-only cell types and the unsupported-type error live
        # out of line, and deliberately not `inline`. `_column` is
        # inlined into the loop in `_row`, so every branch here would
        # otherwise be code the common bool / int / float / str /
        # datetime cells have to be laid out around.
        #
        # Within this function the four wrapper classes come before
        # `uuid.UUID` and the IPV4 test. They are the cheapest checks in
        # the chain -- an exact type test against a cdef class -- and
        # none of them can be a UUID or an address, so nothing becomes
        # reachable only through the more expensive pair.
        if isinstance(value, (bytes, bytearray, memoryview)):
            self._column_binary(c_name, value)
        elif isinstance(value, Char):
            self._column_char(c_name, value)
        elif isinstance(value, DateMillis):
            self._column_date_millis(c_name, value)
        elif isinstance(value, Long256):
            self._column_long256(c_name, value)
        elif isinstance(value, Geohash):
            self._column_geohash(c_name, value)
        elif isinstance(value, uuid.UUID):
            self._column_uuid(name, value)
        elif _is_ipv4_address(value):
            self._column_ipv4(name, value)
        else:
            if isinstance(value, ipaddress.IPv6Address):
                raise TypeError(
                    'IPv6 is not supported; QuestDB has no IPv6 column type.')
            if isinstance(value, ipaddress.IPv4Interface):
                # It subclasses IPv4Address, so the list below would
                # otherwise appear to contain the thing just rejected.
                raise TypeError(
                    _IPV4_INTERFACE_REASON
                    + ' Pass its address instead, as `value.ip`.')
            valid = ', '.join((
                'bool',
                'int',
                'float',
                'str',
                'TimestampMicros',
                'TimestampNanos',
                'datetime.datetime',
                'numpy.ndarray',
                'decimal.Decimal',
                'bytes',
                'bytearray',
                'memoryview',
                'uuid.UUID',
                'ipaddress.IPv4Address',
                'Char',
                'DateMillis',
                'Long256',
                'Geohash'))
            raise TypeError(
                f'Unsupported type: {_fqn(type(value))}. Must be one of: {valid}')

    cdef inline void_int _may_trigger_row_complete(self) except -1:
        cdef PyObject* sender = NULL
        if self._row_complete_sender != None:
            if PyWeakref_GetRef(self._row_complete_sender, &sender):
                try:
                    may_flush_on_row_complete(
                        self, <Sender><object>sender)
                finally:
                    Py_XDECREF(sender)

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
            object at=None,
            bint keep_marker=False) except -1:
        """
        Add a row to the buffer.
        """
        cdef bint wrote_fields = False
        self._check_impl()
        self._set_marker()
        # A column value whose conversion runs Python code can call back
        # into whatever owns this buffer. `_row_depth` is how those owners
        # tell "a row is part-way through" from "a rewind point is held":
        # `PooledSender.row` deliberately keeps the marker set across its
        # own flush, so `_marker_set` cannot answer that question.
        self._row_depth += 1
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
                if not keep_marker:
                    self._clear_marker()
            else:
                self._rewind_to_marker()
        except:
            self._rewind_after_failure()
            raise
        finally:
            self._row_depth -= 1
        if wrote_fields and allow_auto_flush:
            self._may_trigger_row_complete()

    def row(
            self,
            table_name: str,
            *,
            symbols: Optional[Dict[str, Optional[str]]]=None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros,
                      TimestampNanos, datetime.datetime, numpy.ndarray,
                      Decimal, uuid.UUID, ipaddress.IPv4Address, bytes,
                      bytearray, memoryview, Char, DateMillis, Long256,
                      Geohash]]
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
            * - ``uuid.UUID``
              - UUID (QWP-only)
            * - ``ipaddress.IPv4Address``
              - IPV4 (QWP-only)
            * - ``bytes``, ``bytearray``, or ``memoryview``
              - BINARY (QWP-only)
            * - ``Char``
              - CHAR (QWP-only)
            * - ``DateMillis``
              - DATE (QWP-only)
            * - ``Long256``
              - LONG256 (QWP-only)
            * - ``Geohash``
              - GEOHASH (QWP-only)
            * - ``None``
              - *Column is skipped and not serialized.*

        **Note**: Support for NumPy arrays (``numpy.array``) requires QuestDB server version 9.0.0 or higher.

        The seven QWP-only types require protocol ``udp``, ``ws``, or ``wss``
        and are rejected by ILP buffers used by ``tcp``, ``tcps``, ``http``,
        and ``https``. They require QuestDB 10 or newer.

        QuestDB reserves these values as ``NULL`` sentinels, but the client
        deliberately accepts them: IPV4 ``0.0.0.0``, DATE ``INT64_MIN``, UUID
        ``80000000-0000-0000-8000-000000000000``, and a LONG256 whose four
        64-bit limbs are all ``0x8000000000000000``. CHAR has no physical
        ``NULL`` sentinel: ``'\\x00'`` is stored as code unit 0, although some
        SQL operations treat it as CHAR's null/absent marker. GEOHASH has no
        sentinel collision. Empty BINARY ``b''`` is a real empty value and is
        distinct from ``NULL``.

        A bare ``int`` is a 64-bit LONG, so a value outside
        ``-2**63 .. 2**63-1`` raises ``OverflowError``. On a QWP buffer the
        message names :class:`Long256 <questdb.Long256>`, the wrapper that
        sends it as a 256-bit LONG256 instead; an ILP buffer, which has no
        LONG256 to offer, reports the bare conversion failure.

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
        :param columns: A dictionary mapping column names to the supported
            column values listed above.
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

        Also see the :func:`Sender.dataframe <questdb.Sender.dataframe>` method if you're
        not using the buffer explicitly. It supports the same parameters
        and also supports auto-flushing.

        Requires ``pandas`` and ``numpy``. ``pyarrow`` is only needed
        when the frame contains ``pd.ArrowDtype`` / ``pd.Categorical`` /
        ``string`` dtype columns — purely NumPy / object dtypes work
        without it.

        Adding a dataframe can trigger auto-flushing behaviour,
        even between rows of the same dataframe. To avoid this, you can
        use HTTP and transactions (see :func:`Sender.transaction <questdb.Sender.transaction>`).

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

        **Note**: This differs from the :func:`Sender.dataframe <questdb.Sender.dataframe>` method, which
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
            self,
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
    if millis == 0 and (
            timedelta.days or timedelta.seconds or timedelta.microseconds):
        # Never silently turn a positive finite duration into 0ms, which
        # several options interpret as "no deadline".
        return 1
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
    elif auto_flush_rows is False or _is_int_not_bool(auto_flush_rows):
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
    elif auto_flush_bytes is False or _is_int_not_bool(auto_flush_bytes):
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
    elif auto_flush_interval is False or _is_int_not_bool(auto_flush_interval):
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


cdef void_int _parse_pooled_auto_flush(
    line_sender_protocol protocol,
    object auto_flush,
    object auto_flush_rows,
    object auto_flush_bytes,
    object auto_flush_interval,
    auto_flush_mode_t* c_auto_flush,
    bint* c_auto_flush_bytes_dynamic,
) except -1:
    """Parse the QWP pool's Java-builder-compatible auto-flush policy."""
    cdef bint default_auto_flush_bytes = auto_flush_bytes is None
    if auto_flush_rows is None:
        auto_flush_rows = 1000
    if auto_flush_bytes is None:
        # The pooled path derives its byte threshold from the negotiated frame
        # cap. Give the generic parser an enabled placeholder so the byte
        # trigger alone still turns auto-flush on; the separate flag records
        # that this placeholder is dynamic rather than a literal threshold.
        auto_flush_bytes = 1
    if auto_flush_interval is None:
        auto_flush_interval = 100
    _parse_auto_flush(
        protocol,
        auto_flush,
        auto_flush_rows,
        auto_flush_bytes,
        auto_flush_interval,
        c_auto_flush,
        0)
    c_auto_flush_bytes_dynamic[0] = default_auto_flush_bytes


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


class SenderErrorCategory(TaggedEnum):
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


class SenderErrorPolicy(TaggedEnum):
    """
    Applied policy for a structured QWP/WebSocket diagnostic.
    """
    Retriable = ('retriable', LINE_SENDER_QWPWS_ERROR_RETRIABLE)
    RetriableOther = (
        'retriable_other',
        LINE_SENDER_QWPWS_ERROR_RETRIABLE_OTHER)
    Terminal = ('terminal', LINE_SENDER_QWPWS_ERROR_TERMINAL)


@dataclass(frozen=True)
class SenderError:
    """Structured QWP/WebSocket server diagnostic."""
    category: SenderErrorCategory
    applied_policy: SenderErrorPolicy
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
    Successful events are queued only after negotiated connection state,
    including the server-advertised frame cap, is committed. They are not
    data-delivery or acknowledgement barriers.
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


_DISPATCH_THREAD = threading.local()


# Module-level strong root for callback targets registered with the native
# dispatchers. It keeps each handler/listener function object reachable from a
# GC root for as long as its dispatchers can still fire, so the cycle collector
# never clears a live callback's internals (which would crash the interpreter
# when a dispatch calls it). A handle abandoned in a reference cycle through its
# own callback therefore leaks rather than crashing; explicit close() removes
# the entry once the dispatchers are joined and lets the cycle collect.
_LIVE_CALLBACK_REFS = {}


# The registry is keyed by a value captured while the owner is alive and
# released through that key alone: `tp_dealloc` must never pass the dying
# owner back into Python-level calls (PyPy's cpyext aborts on reviving a
# dying object mid-dealloc).
cdef size_t _retain_callback_refs(
        object owner, object error_handler, object listener):
    cdef size_t key = id(owner)
    _LIVE_CALLBACK_REFS[key] = (error_handler, listener)
    return key


cdef _release_callback_refs(size_t key):
    if key != 0:
        _LIVE_CALLBACK_REFS.pop(key, None)


cdef list _dispatch_target_stack():
    stack = getattr(_DISPATCH_THREAD, 'targets', None)
    if stack is None:
        stack = []
        _DISPATCH_THREAD.targets = stack
    return stack


cdef bint _on_dispatch_thread_for(object handler, object listener):
    stack = getattr(_DISPATCH_THREAD, 'targets', None)
    if not stack:
        return False
    for target in stack:
        if target is handler or target is listener:
            return True
    return False


cdef void _connection_event_dispatch(
        void* user_data,
        const questdb_connection_event* event) noexcept with gil:
    listener = <object>user_data
    stack = _dispatch_target_stack()
    stack.append(listener)
    try:
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
    finally:
        stack.pop()


cdef void _connection_event_trampoline(
        void* user_data,
        const questdb_connection_event* event) noexcept nogil:
    # No object locals here: the finalizing early return must not touch
    # the GIL, which a Cython epilogue with object cleanup would.
    if qdb_py_is_finalizing():
        return
    _connection_event_dispatch(user_data, event)


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


def _sender_error_from_raw(raw):
    if raw is None or isinstance(raw, SenderError):
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

    py_category = SenderErrorCategory.Unknown
    for entry in SenderErrorCategory:
        if entry.c_value == category:
            py_category = entry
            break

    py_policy = SenderErrorPolicy.Terminal
    for entry in SenderErrorPolicy:
        if entry.c_value == applied_policy:
            py_policy = entry
            break

    return SenderError(
        py_category,
        py_policy,
        status,
        message,
        message_sequence,
        from_fsn,
        to_fsn)


def _default_error_handler(error):
    level = (
        logging.ERROR
        if error.applied_policy is SenderErrorPolicy.Terminal
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


cdef void _sender_error_dispatch(
        void* user_data,
        const line_sender_qwpws_error_view* view) noexcept with gil:
    handler = <object>user_data
    stack = _dispatch_target_stack()
    stack.append(handler)
    try:
        handler(_sender_error_from_raw(
            c_sender_error_view_to_raw(view[0])))
    except BaseException:
        logging.getLogger("questdb").exception(
            "QWP/WebSocket error handler failed")
    finally:
        stack.pop()


cdef void _sender_error_trampoline(
        void* user_data,
        const line_sender_qwpws_error_view* view) noexcept nogil:
    # No object locals here: the finalizing early return must not touch
    # the GIL, which a Cython epilogue with object cleanup would.
    if qdb_py_is_finalizing():
        return
    _sender_error_dispatch(user_data, view)


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
    # pyarrow allocates exactly `n_buffers` pointers, so `buffers[1]` is
    # past the allocation for a struct array (1) or a null array (0).
    return (
        arr.offset == 0 and
        arr.length == <int64_t>row_count and
        arr.n_buffers >= 2 and
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


cdef bint _dataframe_columnar_col_has_nat(
        col_t* col,
        size_t row_count) noexcept nogil:
    if not col.setup.nat_scan_done:
        col.setup.nat_found = _dataframe_columnar_i64_has_nat(
            <const int64_t*>col.setup.chunks.chunks[0].buffers[1],
            row_count)
        col.setup.nat_scan_done = True
    return col.setup.nat_found


cdef bint _dataframe_columnar_i64_has_negative(
        const int64_t* data,
        size_t row_count) noexcept nogil:
    cdef size_t row_index
    for row_index in range(row_count):
        if data[row_index] < 0:
            return True
    return False


cdef const qwp_validity* _dataframe_columnar_validity(
        ArrowArray* arr,
        size_t row_offset,
        size_t row_count,
        qwp_validity* validity) except? NULL:
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
    # A null array carries no buffer pointers at all, so the bitmap
    # read is bounded by `n_buffers` before it happens. This is the
    # gate `_dataframe_columnar_validity` relies on having run.
    return arr.null_count == 0 or (
        arr.n_buffers >= 1
        and arr.buffers != NULL
        and arr.buffers[0] != NULL)


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
            elif (col.setup.source == col_source_t.col_source_dt64ns_numpy
                    and _PYARROW is None):
                # NaT in a datetime64[ns] field is INT64_MIN nanoseconds;
                # the zero-copy ns->micros wire conversion would corrupt
                # that null sentinel into a bogus 1677 timestamp. With
                # pyarrow, _dataframe_columnar_promote_cols already
                # re-exported any NaT-carrying column through Arrow (and
                # imported pyarrow doing so), leaving nothing to reject.
                if _dataframe_columnar_col_has_nat(col, plan.row_count):
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
            if col.setup.source == col_source_t.col_source_decimal_pyobj:
                # _dataframe_columnar_promote_cols re-exports these through
                # Arrow; reaching this branch means pyarrow is unavailable.
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'object-dtype Decimal columns require pyarrow on the '
                    'columnar path; install pyarrow, or back the column '
                    'with an Arrow decimal type.'))
            elif col.setup.source not in (
                    col_source_t.col_source_decimal32_arrow,
                    col_source_t.col_source_decimal64_arrow,
                    col_source_t.col_source_decimal128_arrow,
                    col_source_t.col_source_decimal256_arrow):
                failures.append(_dataframe_columnar_col_failure(
                    df,
                    col,
                    'v1 only supports Arrow-backed decimal columns '
                    '(pyarrow decimal32/64/128/256).'))
        elif col.setup.target in (
                col_target_t.col_target_column_i8,
                col_target_t.col_target_column_i16,
                col_target_t.col_target_column_i32,
                col_target_t.col_target_column_f32,
                col_target_t.col_target_column_uuid,
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


cdef inline bint _geohash_slot_out_of_range(
        const void* data,
        size_t row,
        size_t elem_size,
        bint is_signed,
        int64_t max_value) noexcept nogil:
    cdef int64_t value
    if is_signed:
        if elem_size == 8:
            value = (<const int64_t*>data)[row]
        elif elem_size == 4:
            value = (<const int32_t*>data)[row]
        elif elem_size == 2:
            value = (<const int16_t*>data)[row]
        else:
            value = (<const int8_t*>data)[row]
        return value < 0 or value > max_value
    if elem_size == 8:
        return (<const uint64_t*>data)[row] > <uint64_t>max_value
    if elem_size == 4:
        value = (<const uint32_t*>data)[row]
    elif elem_size == 2:
        value = (<const uint16_t*>data)[row]
    else:
        value = (<const uint8_t*>data)[row]
    return value > max_value


cdef bint _dataframe_columnar_geohash_scan(
        const void* data,
        const uint8_t* validity,
        size_t elem_size,
        bint is_signed,
        size_t row_count,
        int64_t max_value,
        size_t offset,
        size_t* bad_row) noexcept nogil:
    """The first row holding a value the claimed precision cannot hold,
    if there is one. A null slot carries no value and is skipped.

    `offset` is the Arrow array's own start row, which a sliced batch
    carries instead of rebasing its buffers. It shifts both the value
    slot and the validity bit, so a bitmap that does not start on a
    byte boundary is read correctly. `bad_row` is reported relative to
    the array, not to the buffer, so it is the row the caller named.
    """
    cdef size_t row
    cdef size_t slot
    for row in range(row_count):
        slot = offset + row
        if (validity != NULL
                and not ((validity[slot >> 3] >> (slot & 7)) & 1)):
            continue
        if _geohash_slot_out_of_range(
                data, slot, elem_size, is_signed, max_value):
            bad_row[0] = row
            return True
    return False


cdef void_int _dataframe_columnar_check_geohash_ranges(
        object df,
        dataframe_plan_t* plan) except -1:
    """Refuse a claimed GEOHASH value that its precision cannot hold.

    A GEOHASH column keeps the claimed number of low bits, and the high
    bits are the coarse position, so a wider value reaches the database
    as a valid geohash for somewhere else entirely — the one claimed
    type whose range is narrower than the integer carrying it, and so
    the one that can be silently rewritten this way. IPV4 and CHAR fit
    their storage width exactly and need no scan.

    Object columns are checked value by value as they are built. The
    columns that reach the wire as raw buffers are checked here, once
    for the whole frame and before a connection is opened, so the
    refusal arrives with the rest of the shape errors rather than
    mid-flush.
    """
    cdef size_t col_index
    cdef col_t* col
    cdef ArrowArray* arr
    cdef const void* data
    cdef const uint8_t* validity
    cdef size_t elem_size
    cdef bint is_signed
    cdef int64_t max_value
    cdef size_t bad_row = 0
    cdef bint bad = False
    if plan.row_count == 0:
        return 0
    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if not col.setup.has_override:
            continue
        if col.setup.override_dtype not in (
                qwp_numpy_dtype.qwp_numpy_geohash_i8,
                qwp_numpy_dtype.qwp_numpy_geohash_i16,
                qwp_numpy_dtype.qwp_numpy_geohash_i32,
                qwp_numpy_dtype.qwp_numpy_geohash_i64):
            continue
        # `_dataframe_columnar_build_int_pyobj` already walks an object
        # column value by value and checks each one as it goes.
        if col.setup.source == col_source_t.col_source_int_pyobj:
            continue
        if col.setup.source in (
                col_source_t.col_source_i8_numpy,
                col_source_t.col_source_u8_numpy):
            elem_size = 1
        elif col.setup.source in (
                col_source_t.col_source_i16_numpy,
                col_source_t.col_source_u16_numpy):
            elem_size = 2
        elif col.setup.source in (
                col_source_t.col_source_i32_numpy,
                col_source_t.col_source_u32_numpy):
            elem_size = 4
        elif col.setup.source in (
                col_source_t.col_source_i64_numpy,
                col_source_t.col_source_u64_numpy,
                col_source_t.col_source_i64_arrow):
            elem_size = 8
        else:
            raise RuntimeError(
                'Unsupported columnar GEOHASH source: %d.'
                % <int>col.setup.source)
        is_signed = col.setup.source in (
            col_source_t.col_source_i8_numpy,
            col_source_t.col_source_i16_numpy,
            col_source_t.col_source_i32_numpy,
            col_source_t.col_source_i64_numpy,
            col_source_t.col_source_i64_arrow)
        max_value = (<int64_t>1 << col.setup.override_geohash_bits) - 1
        arr = &col.setup.chunks.chunks[0]
        data = arr.buffers[1]
        validity = NULL
        if arr.null_count != 0:
            validity = <const uint8_t*>arr.buffers[0]
        with nogil:
            bad = _dataframe_columnar_geohash_scan(
                data,
                validity,
                elem_size,
                is_signed,
                plan.row_count,
                max_value,
                # `_dataframe_columnar_validate_plan` has already held
                # every column to a zero-offset chunk.
                0,
                &bad_row)
        if bad:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {df.columns[col.setup.orig_index]!r} at row '
                f'{bad_row}: '
                f'GEOHASH({col.setup.override_geohash_bits}b) values must '
                f'be in the range 0 .. {max_value}.')
    return 0


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
    try:
        return _PYARROW.Array.from_pandas(df.iloc[:, col.setup.orig_index])
    except (TypeError, ValueError) as e:
        col_name = df.columns[col.setup.orig_index]
        raise QuestDBError(
            QuestDBErrorCode.BadDataFrame,
            f'Bad column {col_name!r}: {e}') from e


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
            if not _dataframe_try_import_pyarrow():
                continue
            arrow_array = _dataframe_columnar_col_from_pandas(df, col)
        elif (col.setup.target == col_target_t.col_target_column_ts
                and col.setup.source == col_source_t.col_source_dt64ns_numpy
                and _dataframe_columnar_has_single_contiguous_chunk(
                    col, plan.row_count)
                and _dataframe_columnar_col_has_nat(col, plan.row_count)):
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
    Walk a PyObject int column once and produce a contiguous buffer +
    LSB-packed validity bitmap. Null cells leave the slot at 0 with the
    validity bit cleared.

    The slot is an int64 unless an IPV4 or CHAR round-trip claim
    narrows it to the 32- or 16-bit unsigned width that wire type
    needs; a GEOHASH claim keeps the int64 slot and carries its
    precision separately. Each is stored through its own pointer type,
    so the buffer holds native-order values whichever width it is.

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
    cdef uint8_t* values = NULL
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef int64_t value
    cdef int overflow = 0
    cdef size_t elem_size = 8
    cdef int64_t narrow_max = 0
    cdef str narrow_type = None

    if col.setup.has_override:
        if col.setup.override_dtype == qwp_numpy_dtype.qwp_numpy_u32_ipv4:
            elem_size = 4
            narrow_max = 0xFFFFFFFF
            narrow_type = 'IPV4'
        elif col.setup.override_dtype == qwp_numpy_dtype.qwp_numpy_u16_char:
            elem_size = 2
            narrow_max = 0xFFFF
            narrow_type = 'CHAR'
        elif col.setup.override_dtype in (
                qwp_numpy_dtype.qwp_numpy_geohash_i8,
                qwp_numpy_dtype.qwp_numpy_geohash_i16,
                qwp_numpy_dtype.qwp_numpy_geohash_i32,
                qwp_numpy_dtype.qwp_numpy_geohash_i64):
            # The slot stays 64 bits wide; the claimed precision is what
            # bounds the value. A value past it is truncated to the low
            # bits on the wire, putting the row at a different point on
            # the planet, so it is refused here with the other claims.
            narrow_max = (
                (<int64_t>1 << col.setup.override_geohash_bits) - 1)
            narrow_type = (
                f'GEOHASH({col.setup.override_geohash_bits}b)')

    try:
        values = <uint8_t*>calloc(
            row_count * elem_size if row_count > 0 else elem_size,
            sizeof(uint8_t))
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
                value = 1 if cell == <PyObject*>True else 0
            elif PyLong_CheckExact(cell):
                value = <int64_t>PyLong_AsLongLongAndOverflow(
                    <object>cell, &overflow)
                if overflow != 0:
                    if narrow_type is not None:
                        raise QuestDBError(
                            QuestDBErrorCode.BadDataFrame,
                            f'Bad column {df_col_name!r} at row {i}: '
                            f'{narrow_type} values must be in the range '
                            f'0 .. {narrow_max}.')
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r} at row {i}: integer '
                        f'out of range for a LONG column '
                        f'(-2**63 .. 2**63-1). A 256-bit value needs a '
                        f'LONG256 column, which this column claims with '
                        f"df.attrs['questdb'] = {{'version': 1, "
                        f"'columns': {{{df_col_name!r}: "
                        f"{{'kind': 'long256'}}}}}}.")
            elif _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
                continue
            else:
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: expected int, '
                    f'got {_fqn(type(<object>cell))}.')

            if narrow_type is not None and (value < 0 or value > narrow_max):
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: '
                    f'{narrow_type} values must be in the range '
                    f'0 .. {narrow_max}.')
            if elem_size == 4:
                (<uint32_t*>values)[i] = <uint32_t>value
            elif elem_size == 2:
                (<uint16_t*>values)[i] = <uint16_t>value
            else:
                (<int64_t*>values)[i] = value
            if b.validity != NULL:
                _pyobj_set_validity_bit(b.validity, i)

        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef object _LONG256_LIMIT = 1 << 256


cdef pyobj_built_t* _dataframe_columnar_build_long256_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name) except NULL:
    """
    Walk a PyObject int column claimed as LONG256 and produce 32 bytes
    per row + LSB-packed validity bitmap.

    ``qwp_numpy_s32`` puts the row on the wire verbatim, and the wire
    order for LONG256 is little-endian — the same order
    ``QueryResult.to_pandas()`` read the integer out of, so a value that
    came from a query goes back unchanged.
    """
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef uint8_t* buf = NULL
    cdef size_t buf_bytes = row_count * 32 if row_count > 0 else 32
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef object py_cell
    cdef bytes le_bytes
    # Called unbound so that an `int` subclass overriding `to_bytes`
    # cannot hand back something other than the 32 bytes the column
    # write copies.
    cdef object int_to_bytes = int.to_bytes

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
            if PyLong_CheckExact(cell):
                py_cell = <object>cell
                if py_cell < 0 or py_cell >= _LONG256_LIMIT:
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r} at row {i}: LONG256 '
                        f'is unsigned and 256 bits wide, so the value '
                        f'must be in the range 0 <= value < 2**256.')
                le_bytes = int_to_bytes(py_cell, 32, 'little')
                memcpy(buf + i * 32, PyBytes_AsString(le_bytes), 32)
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
    cdef bytes be_bytes
    cdef object uuid_cls = _uuid.UUID
    cdef object int_to_bytes = int.to_bytes

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
                # `qwp_numpy_s16` reads canonical RFC 4122 big-endian
                # rows and byte-swaps them into QWP wire order itself.
                # `int.to_bytes` is called unbound so that a replaced
                # `UUID.int` cannot narrow the result: it is always the
                # 16 bytes `UUID.bytes` would give, in one C-implemented
                # call plus one 16-byte memcpy per row.
                be_bytes = int_to_bytes((<object>cell).int, 16, 'big')
                memcpy(buf + i * 16, PyBytes_AsString(be_bytes), 16)
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
            if _is_ipv4_address(<object>cell):
                values[i] = <uint32_t>int(<object>cell)
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            elif _dataframe_is_null_pyobj(cell):
                b.has_nulls = True
            elif isinstance(<object>cell, _IPV4_INTERFACE):
                raise QuestDBError(
                    QuestDBErrorCode.BadDataFrame,
                    f'Bad column {df_col_name!r} at row {i}: '
                    + _ipv4_interface_df_message(df_col_name))
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
    cdef object py_cell
    cdef Py_buffer view
    cdef bint release_view
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
            release_view = False
            try:
                if PyBytes_Check(<object>cell):
                    blob_len = PyBytes_GET_SIZE(<object>cell)
                    blob_buf = PyBytes_AsString(<object>cell)
                elif PyByteArray_Check(<object>cell):
                    blob_len = PyByteArray_Size(<object>cell)
                    blob_buf = PyByteArray_AsString(<object>cell)
                elif PyMemoryView_Check(<object>cell):
                    py_cell = <object>cell
                    # A released memoryview raises ValueError from the
                    # `itemsize` and `c_contiguous` reads, not from
                    # `PyObject_GetBuffer`, so those reads sit inside the
                    # handler that names the column and the row.
                    try:
                        if (py_cell.itemsize != 1
                                or not py_cell.c_contiguous):
                            raise QuestDBError(
                                QuestDBErrorCode.BadDataFrame,
                                f'Bad column {df_col_name!r} at row {i}: '
                                'memoryview BINARY values must be '
                                'C-contiguous with one-byte items.')
                        PyObject_GetBuffer(py_cell, &view, PyBUF_SIMPLE)
                    except (BufferError, ValueError) as exc:
                        raise QuestDBError(
                            QuestDBErrorCode.BadDataFrame,
                            f'Bad column {df_col_name!r} at row {i}: '
                            f'invalid memoryview BINARY value: {exc}') from exc
                    release_view = True
                    blob_len = view.len
                    blob_buf = <const char*>view.buf
                elif _dataframe_is_null_pyobj(cell):
                    b.str_offsets[i + 1] = <int32_t>bytes_used
                    b.has_nulls = True
                    continue
                else:
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r} at row {i}: expected '
                        'bytes, bytearray, or memoryview, '
                        f'got {_fqn(type(<object>cell))}.')

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
            finally:
                if release_view:
                    PyBuffer_Release(&view)

        b.str_bytes_len = bytes_used
        if not b.has_nulls and b.validity != NULL:
            free(b.validity)
            b.validity = NULL
    except:
        pyobj_built_free(b)
        raise

    return b


cdef pyobj_built_t* _dataframe_columnar_build_fsb_pyobj(
        col_t* col,
        size_t row_count,
        object df_col_name,
        size_t width,
        str type_name) except NULL:
    """
    Walk an object column of `bytes` claimed as UUID or LONG256 and
    produce `width` bytes per row + LSB-packed validity bitmap.

    ``to_pandas(dtype_backend='numpy_nullable')`` hands a fixed-size
    binary column back as object-dtype `bytes`, so the raw rows are
    already in the order the wire wants: canonical RFC 4122 big-endian
    for UUID, little-endian limbs for LONG256. Every non-null cell must
    be exactly `width` bytes — a column whose values no longer are is
    refused rather than written to a column of the claimed type.
    """
    cdef pyobj_built_t* b = <pyobj_built_t*>calloc(1, sizeof(pyobj_built_t))
    if b == NULL:
        raise MemoryError()
    b.row_count = row_count

    cdef PyObject** access = <PyObject**>col.setup.chunks.chunks[0].buffers[1]
    cdef PyObject* cell
    cdef uint8_t* buf = NULL
    cdef size_t buf_bytes = row_count * width if row_count > 0 else width
    cdef size_t validity_bytes = (row_count + 7) // 8
    cdef size_t i
    cdef Py_ssize_t blob_len
    cdef const char* blob_buf
    cdef object py_cell
    cdef Py_buffer view
    cdef bint release_view

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
            release_view = False
            try:
                if PyBytes_Check(<object>cell):
                    blob_len = PyBytes_GET_SIZE(<object>cell)
                    blob_buf = PyBytes_AsString(<object>cell)
                elif PyByteArray_Check(<object>cell):
                    blob_len = PyByteArray_Size(<object>cell)
                    blob_buf = PyByteArray_AsString(<object>cell)
                elif PyMemoryView_Check(<object>cell):
                    py_cell = <object>cell
                    try:
                        if (py_cell.itemsize != 1
                                or not py_cell.c_contiguous):
                            raise QuestDBError(
                                QuestDBErrorCode.BadDataFrame,
                                f'Bad column {df_col_name!r} at row {i}: '
                                'memoryview values must be C-contiguous '
                                'with one-byte items.')
                        PyObject_GetBuffer(py_cell, &view, PyBUF_SIMPLE)
                    except (BufferError, ValueError) as exc:
                        raise QuestDBError(
                            QuestDBErrorCode.BadDataFrame,
                            f'Bad column {df_col_name!r} at row {i}: '
                            f'invalid memoryview value: {exc}') from exc
                    release_view = True
                    blob_len = view.len
                    blob_buf = <const char*>view.buf
                elif _dataframe_is_null_pyobj(cell):
                    b.has_nulls = True
                    continue
                else:
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r} at row {i}: expected '
                        'bytes, bytearray, or memoryview, '
                        f'got {_fqn(type(<object>cell))}.')

                if <size_t>blob_len != width:
                    raise QuestDBError(
                        QuestDBErrorCode.BadDataFrame,
                        f'Bad column {df_col_name!r} at row {i}: a '
                        f'{type_name} value is exactly {width} bytes, '
                        f'got {blob_len}.')
                memcpy(buf + i * width, blob_buf, width)
                if b.validity != NULL:
                    _pyobj_set_validity_bit(b.validity, i)
            finally:
                if release_view:
                    PyBuffer_Release(&view)

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
                if (col.setup.has_override
                        and col.setup.override_dtype
                            == qwp_numpy_dtype.qwp_numpy_s32):
                    plan.pyobj_built[i] = \
                        _dataframe_columnar_build_long256_pyobj(
                            col, plan.row_count, col_name)
                else:
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
                if (col.setup.has_override
                        and col.setup.override_dtype
                            == qwp_numpy_dtype.qwp_numpy_s16):
                    plan.pyobj_built[i] = _dataframe_columnar_build_fsb_pyobj(
                        col, plan.row_count, col_name, 16, 'UUID')
                elif (col.setup.has_override
                        and col.setup.override_dtype
                            == qwp_numpy_dtype.qwp_numpy_s32):
                    plan.pyobj_built[i] = _dataframe_columnar_build_fsb_pyobj(
                        col, plan.row_count, col_name, 32, 'LONG256')
                else:
                    plan.pyobj_built[i] = \
                        _dataframe_columnar_build_bytes_pyobj(
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
        qwp_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef qwp_validity validity
    cdef const qwp_validity* validity_ptr = NULL
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
        ok = qwp_chunk_column_str(
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
        qwp_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count,
        size_t elem_size,
        qwp_numpy_dtype dtype) except -1:
    cdef line_sender_error* err = NULL
    cdef qwp_validity validity
    cdef const qwp_validity* validity_ptr = NULL
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
        ok = qwp_chunk_append_numpy_column(
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
        qwp_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef qwp_validity validity
    cdef const qwp_validity* validity_ptr = NULL
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
        ok = qwp_chunk_column_binary(
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
        qwp_chunk* chunk,
        col_t* col,
        size_t row_offset,
        size_t row_count,
        qwp_symbol_mode symbol_mode
            =qwp_symbol_mode_auto) except -1:
    cdef line_sender_error* err = NULL
    cdef bint ok = False
    cdef qwp_arrow_import* imported = col.setup.arrow_import
    with nogil:
        if imported == NULL:
            imported = qwp_arrow_import_new(
                &col.setup.chunks.chunks[0],
                &col.setup.arrow_schema,
                symbol_mode,
                &err)
        if imported != NULL:
            ok = qwp_chunk_append_arrow_import(
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


cdef str _columnar_col_name(const col_t* col):
    """A planned column's name as text, for a message naming it."""
    if col.name.buf == NULL:
        return '?'
    return PyUnicode_FromStringAndSize(
        col.name.buf, <Py_ssize_t>col.name.len)


cdef void_int _dataframe_columnar_append_field(
        qwp_chunk* chunk,
        col_t* col,
        pyobj_built_t* prebuilt,
        size_t row_offset,
        size_t row_count) except -1:
    cdef line_sender_error* err = NULL
    cdef ArrowArray* arr = &col.setup.chunks.chunks[0]
    cdef ArrowArray* dictionary
    cdef const void* data = NULL
    cdef int32_t* offsets
    cdef int32_t* dict_offsets
    cdef size_t bytes_len
    cdef size_t dict_offsets_len
    cdef size_t dict_bytes_len
    cdef qwp_validity validity
    cdef const qwp_validity* validity_ptr = (
        _dataframe_columnar_validity(arr, row_offset, row_count, &validity))
    cdef bint ok = False

    cdef qwp_numpy_dtype numpy_dtype
    cdef size_t element_size
    cdef qwp_numpy_extras extras
    cdef const qwp_numpy_extras* extras_ptr

    # pyarrow allocates exactly `n_buffers` pointers, so reading the
    # value buffer of a column that has fewer reads past the
    # allocation. The planner turns such a column away, and saying so
    # here keeps that a property of this read rather than of the checks
    # upstream of it.
    if arr.buffers == NULL or arr.n_buffers < 2:
        raise QuestDBError(
            QuestDBErrorCode.BadDataFrame,
            f'Bad column {_columnar_col_name(col)!r}: it arrived '
            f'with {arr.n_buffers} Arrow buffers, too few to read '
            f'values from.')
    data = arr.buffers[1]

    if col.setup.target == col_target_t.col_target_column_bool:
        if col.setup.source == col_source_t.col_source_bool_pyobj:
            if prebuilt == NULL:
                raise RuntimeError(
                    'PyObject bool column missing pre-built bitmap.')
            if row_offset % 8 != 0:
                raise RuntimeError(
                    'PyObject bool column requires byte-aligned chunk boundaries.')
            with nogil:
                ok = qwp_chunk_column_bool(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    (<const uint8_t*>prebuilt.data) + (row_offset // 8),
                    row_count,
                    NULL,
                    &err)
        elif col.setup.source == col_source_t.col_source_bool_numpy:
            # NumPy bool is byte-per-row; Rust packs to LSB-bitmap
            # inside qwp_chunk_append_numpy_column.
            with nogil:
                ok = qwp_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    qwp_numpy_dtype.qwp_numpy_bool,
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
            # A round-trip claim decides the slot width the prebuild
            # wrote: IPV4 and CHAR narrow it, `long256` widens it to 32
            # raw bytes, and GEOHASH keeps the int64 slot and carries
            # its precision in `extras`. Without a claim the column is
            # an int64 LONG.
            numpy_dtype = qwp_numpy_dtype.qwp_numpy_i64
            element_size = 8
            extras_ptr = NULL
            if col.setup.has_override:
                numpy_dtype = col.setup.override_dtype
                if numpy_dtype == qwp_numpy_dtype.qwp_numpy_u32_ipv4:
                    element_size = 4
                elif numpy_dtype == qwp_numpy_dtype.qwp_numpy_u16_char:
                    element_size = 2
                elif numpy_dtype == qwp_numpy_dtype.qwp_numpy_s32:
                    element_size = 32
                elif numpy_dtype == qwp_numpy_dtype.qwp_numpy_geohash_i64:
                    memset(&extras, 0, sizeof(qwp_numpy_extras))
                    extras.geohash_bits = col.setup.override_geohash_bits
                    extras_ptr = &extras
            with nogil:
                ok = qwp_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    numpy_dtype,
                    (<const uint8_t*>prebuilt.data) + row_offset * element_size,
                    row_count * element_size,
                    row_count,
                    validity_ptr,
                    extras_ptr,
                    &err)
        else:
            # Rust widens narrow ints to a sentinel-safe wire (i8/i16 → INT,
            # i32/u32/u64 → LONG); see questdb-rs NumpyDtype::*WidenTo*.
            if col.setup.source in (
                    col_source_t.col_source_i64_numpy,
                    col_source_t.col_source_i64_arrow):
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_i64
                element_size = 8
            elif col.setup.source == col_source_t.col_source_i8_numpy:
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_i8
                element_size = 1
            elif col.setup.source == col_source_t.col_source_i16_numpy:
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_i16
                element_size = 2
            elif col.setup.source == col_source_t.col_source_i32_numpy:
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_i32
                element_size = 4
            elif col.setup.source == col_source_t.col_source_u8_numpy:
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_u8
                element_size = 1
            elif col.setup.source == col_source_t.col_source_u16_numpy:
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_u16
                element_size = 2
            elif col.setup.source in (
                    col_source_t.col_source_u32_numpy,
                    col_source_t.col_source_u32_arrow):
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_u32
                element_size = 4
            elif col.setup.source == col_source_t.col_source_u64_numpy:
                numpy_dtype = qwp_numpy_dtype.qwp_numpy_u64
                element_size = 8
            else:
                raise RuntimeError('Unsupported columnar int source.')
            extras_ptr = NULL
            if col.setup.has_override:
                numpy_dtype = col.setup.override_dtype
                if (numpy_dtype
                            == qwp_numpy_dtype.qwp_numpy_geohash_i8
                        or numpy_dtype
                            == qwp_numpy_dtype.qwp_numpy_geohash_i16
                        or numpy_dtype
                            == qwp_numpy_dtype.qwp_numpy_geohash_i32
                        or numpy_dtype
                            == qwp_numpy_dtype.qwp_numpy_geohash_i64):
                    memset(&extras, 0, sizeof(qwp_numpy_extras))
                    extras.geohash_bits = col.setup.override_geohash_bits
                    extras_ptr = &extras
            with nogil:
                ok = qwp_chunk_append_numpy_column(
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
            numpy_dtype = qwp_numpy_dtype.qwp_numpy_f64
            element_size = 8
            with nogil:
                ok = qwp_chunk_append_numpy_column(
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
            numpy_dtype = qwp_numpy_dtype.qwp_numpy_f32
            element_size = 4
            with nogil:
                ok = qwp_chunk_append_numpy_column(
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
                ok = qwp_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    qwp_numpy_dtype.qwp_numpy_f64,
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
                ok = qwp_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    qwp_numpy_dtype.qwp_numpy_datetime64_ns,
                    (<const uint8_t*>data) + row_offset * 8,
                    row_count * 8,
                    row_count,
                    validity_ptr,
                    NULL,
                    &err)
        elif col.setup.source == col_source_t.col_source_dt64us_numpy:
            with nogil:
                ok = qwp_chunk_append_numpy_column(
                    chunk,
                    col.name.buf,
                    col.name.len,
                    qwp_numpy_dtype.qwp_numpy_datetime64_us,
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
                qwp_numpy_dtype.qwp_numpy_datetime64_us)
            return 0
        else:
            raise RuntimeError('Unsupported columnar timestamp field source.')
    elif col.setup.target in (
            col_target_t.col_target_column_i8,
            col_target_t.col_target_column_i16,
            col_target_t.col_target_column_i32,
            col_target_t.col_target_column_f32):
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_uuid:
        if col.setup.source == col_source_t.col_source_uuid_pyobj:
            _dataframe_columnar_append_pyobj_simple(
                chunk, col, prebuilt, row_offset, row_count, 16,
                qwp_numpy_dtype.qwp_numpy_s16)
            return 0
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_ipv4:
        if col.setup.source == col_source_t.col_source_ipv4_pyobj:
            _dataframe_columnar_append_pyobj_simple(
                chunk, col, prebuilt, row_offset, row_count, 4,
                qwp_numpy_dtype.qwp_numpy_u32_ipv4)
            return 0
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_column_binary:
        if col.setup.source == col_source_t.col_source_bytes_pyobj:
            # A UUID or LONG256 round-trip claim turns the same object
            # column into fixed-width rows instead of an offsets table.
            if (col.setup.has_override
                    and col.setup.override_dtype
                        == qwp_numpy_dtype.qwp_numpy_s16):
                _dataframe_columnar_append_pyobj_simple(
                    chunk, col, prebuilt, row_offset, row_count, 16,
                    qwp_numpy_dtype.qwp_numpy_s16)
                return 0
            if (col.setup.has_override
                    and col.setup.override_dtype
                        == qwp_numpy_dtype.qwp_numpy_s32):
                _dataframe_columnar_append_pyobj_simple(
                    chunk, col, prebuilt, row_offset, row_count, 32,
                    qwp_numpy_dtype.qwp_numpy_s32)
                return 0
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
                qwp_symbol_mode_not_symbol)
            return 0
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count)
        return 0
    elif col.setup.target == col_target_t.col_target_symbol:
        _dataframe_columnar_call_arrow_append(
            chunk, col, row_offset, row_count,
            qwp_symbol_mode_symbol)
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
        qwp_chunk* chunk,
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
            ok = qwp_chunk_at_micros(
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
            ok = qwp_chunk_at_nanos(
                chunk,
                data + row_offset,
                row_count,
                &err)
    elif col.setup.source in (
            col_source_t.col_source_dt64us_numpy,
            col_source_t.col_source_dt64us_tz_arrow):
        with nogil:
            ok = qwp_chunk_at_micros(
                chunk,
                data + row_offset,
                row_count,
                &err)
    elif col.setup.source == col_source_t.col_source_dt64ms_tz_arrow:
        with nogil:
            ok = qwp_chunk_at_millis(
                chunk,
                data + row_offset,
                row_count,
                &err)
    elif col.setup.source == col_source_t.col_source_dt64s_tz_arrow:
        with nogil:
            ok = qwp_chunk_at_seconds(
                chunk,
                data + row_offset,
                row_count,
                &err)
    else:
        raise RuntimeError('Unsupported columnar designated timestamp source.')

    if not ok:
        raise c_err_to_py(err)


# What `_geohash_override_dtype` answers with besides a
# `qwp_numpy_dtype` slot.
cdef int _GEOHASH_DTYPE_NONE = -1
cdef int _GEOHASH_DTYPE_UNSIGNED = -2


cdef int _geohash_override_dtype(col_source_t source) noexcept:
    """The GEOHASH slot a column of this source goes out in,
    ``_GEOHASH_DTYPE_UNSIGNED`` for an unsigned integer column, or
    ``_GEOHASH_DTYPE_NONE`` for anything else.

    A GEOHASH rides on a signed integer: `qwp_numpy_dtype` names only
    signed geohash slots, and the native Arrow importer refuses an
    unsigned column outright, so an unsigned column is one no planner
    can carry the kind on. It gets its own answer because that is a
    claim guaranteed to do nothing, which is worth saying out loud
    rather than dropping in silence.
    """
    if (source == col_source_t.col_source_u8_numpy
            or source == col_source_t.col_source_u16_numpy
            or source == col_source_t.col_source_u32_numpy
            or source == col_source_t.col_source_u64_numpy):
        return _GEOHASH_DTYPE_UNSIGNED
    if source == col_source_t.col_source_i8_numpy:
        return <int>qwp_numpy_dtype.qwp_numpy_geohash_i8
    if source == col_source_t.col_source_i16_numpy:
        return <int>qwp_numpy_dtype.qwp_numpy_geohash_i16
    if source == col_source_t.col_source_i32_numpy:
        return <int>qwp_numpy_dtype.qwp_numpy_geohash_i32
    if (source == col_source_t.col_source_i64_numpy
            or source == col_source_t.col_source_i64_arrow
            or source == col_source_t.col_source_int_pyobj):
        # An object column of Python ints — what a masked pandas dtype
        # becomes — is widened to the 64-bit geohash; the claim carries
        # the precision, so the storage width no longer has to.
        #
        # `i64_arrow` is here because it is the one Arrow integer width
        # that resolves to `col_target_column_i64` and so reaches the
        # wire as a raw buffer the append call names a dtype for. The
        # narrower Arrow widths take their own targets and go to the
        # native importer as Arrow arrays, which carry no type hint;
        # `_dataframe_normalize_claimed_arrow` reshapes those instead.
        return <int>qwp_numpy_dtype.qwp_numpy_geohash_i64
    return _GEOHASH_DTYPE_NONE


cdef int _geohash_dtype_max_bits(int gh) noexcept:
    """The widest precision a GEOHASH slot of this width can hold.

    A precision fills its bits, so an 8-bit geohash spans 0..255 --
    every bit of a byte, and one more than a signed byte can express.
    The slots are signed and the range check reads them as signed, so
    each width stops one bit short of its own size and a precision that
    needs the last bit belongs in the next slot up. Accepting the full
    width instead promised a range the column could not hold: no value
    from 128 to 255 could reach an 8-bit claim on `int8`, which is the
    only way `int8` has of spelling them.

    The 64-bit slot stops at 60 because that is the widest GEOHASH
    QuestDB has, well inside a signed 64-bit value.

    `_attrs_override_fits` states the same four numbers against Arrow
    types, and `egress.pxi`'s read-back table picks the slot a
    precision comes back in by them.
    """
    if gh == <int>qwp_numpy_dtype.qwp_numpy_geohash_i8:
        return 7
    if gh == <int>qwp_numpy_dtype.qwp_numpy_geohash_i16:
        return 15
    if gh == <int>qwp_numpy_dtype.qwp_numpy_geohash_i32:
        return 31
    if gh == <int>qwp_numpy_dtype.qwp_numpy_geohash_i64:
        return 60
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


cdef object _claimed_arrow_col_reshape_dtype(
        object types, object raw_ty, object ty, str kind):
    """The dtype an Arrow-backed claimed column has to be copied to for
    the manual planner to honour its claim, or None where the column
    carries it as it stands.

    A claim reaches the wire as the NumPy dtype named in the append
    call, so it only lands on columns the planner sends as raw buffers.
    A column handed to the native Arrow importer instead carries no type
    hint, and `qwp_chunk_append_arrow_column` takes no per-column type
    override, so those are the ones that need reshaping.

    The matching NumPy width is the copy to make where the claim has
    one: it keeps the column a raw buffer and costs a memcpy, where
    object dtype costs a boxed Python int per row -- about 100 times
    more for a column of any size. The caller downgrades a NumPy width
    to object for a column holding a null, which a NumPy integer dtype
    cannot express.

    `_attrs_override_fits` has already held each kind to the Arrow
    types listed here, so the widths not named are the ones it rejects.
    """
    if kind == 'ipv4':
        # uint32 resolves to the i64 target and goes out as a raw
        # buffer, claim and all.
        return None
    if kind == 'geohash':
        # int64 does the same. int8/16/32 take the narrow BYTE/SHORT/INT
        # targets, which reach the importer as Arrow arrays; the same
        # width in NumPy takes the i64 target and the raw-buffer route.
        if types.is_int64(ty):
            return None
        if types.is_int8(ty):
            return 'int8'
        if types.is_int16(ty):
            return 'int16'
        return 'int32'
    if kind == 'char':
        # An Arrow uint16 column goes to the importer, which has no CHAR
        # hint to read; NumPy uint16 takes the raw-buffer route.
        return 'uint16'
    if kind == 'uuid':
        # A `pa.uuid()` column states its own type to the importer and
        # already lands as UUID. A bare 16-byte column is refused by the
        # planner outright, which reshaping avoids.
        if (isinstance(raw_ty, _PYARROW.lib.BaseExtensionType)
                and raw_ty.extension_name == _ARROW_EXT_UUID):
            return None
        return object
    # LONG256 is a 32-byte fixed binary, a width NumPy has no integer
    # dtype for, so object is the only shape left.
    return object


cdef object _dataframe_normalize_claimed_arrow(object df):
    """NumPy-dtype copies of the Arrow-backed columns whose round-trip
    claim the manual planner cannot otherwise honour.

    A frame mixing Arrow-backed and NumPy columns routes whole to this
    planner, where a claim rides on the NumPy dtype named in the append
    call. Columns handed to the native Arrow importer have nowhere to
    put it, so their claim would be dropped and the column would land as
    its storage type — IPV4, CHAR and GEOHASH as plain integers, UUID
    and LONG256 refused — auto-creating the destination table with the
    wrong column types.

    An integer claim copies to the NumPy width that carries it, which
    stays a raw buffer the whole way to the wire. UUID, LONG256, and any
    column holding a null copy to object dtype instead: that is the
    shape `_dataframe_apply_roundtrip_overrides` covers for every
    claimed kind, and what `dtype_backend='numpy_nullable'` hands these
    columns back as, so both backends write the same bytes again.

    Only a frame that already fell back from the zero-copy Arrow path
    reaches here, and within it only the claimed columns that have no
    buffer-shaped route are copied, so the shapes that can carry their
    claim natively stay zero-copy.
    """
    cdef object cols_meta, arrow_dtype, types, dtype, raw_ty, ty, meta, out
    cdef object target_dtype
    cdef str kind
    cdef list convert
    if not _is_pandas_dataframe_object(df):
        return df
    cols_meta = _roundtrip_columns_meta(df)
    if not cols_meta:
        return df
    _dataframe_may_import_deps()
    arrow_dtype = getattr(_PANDAS, 'ArrowDtype', None)
    if arrow_dtype is None:
        return df
    if not _dataframe_try_import_pyarrow():
        return df
    types = _PYARROW.types
    convert = []
    # Walked by position rather than by label: `dtypes[name]` on a frame
    # with duplicate column names hands back a Series, not a dtype.
    for pos, (name, dtype) in enumerate(zip(df.columns, df.dtypes)):
        if not isinstance(dtype, arrow_dtype):
            continue
        meta = cols_meta.get(name)
        kind = _roundtrip_kind(meta)
        if kind is None or kind not in _ATTRS_OVERRIDE_KINDS:
            continue
        raw_ty = dtype.pyarrow_dtype
        ty = raw_ty
        if isinstance(ty, _PYARROW.lib.BaseExtensionType):
            ty = ty.storage_type
        # A claim that no longer fits the column is dropped rather than
        # reshaped: refusing value by value later would answer a retyped
        # column with an error where the contract promises the write
        # goes ahead. It says which claim it dropped on the way past,
        # because a type that can never carry the kind is a mistake
        # rather than drift, and the two look the same from here.
        if not _attrs_override_fits(
                types, ty, kind, meta.get('precision_bits') or 0):
            _warn_roundtrip_claim_dropped(name, kind, dtype)
            continue
        target_dtype = _claimed_arrow_col_reshape_dtype(
            types, raw_ty, ty, kind)
        if target_dtype is None:
            continue
        if target_dtype is not object and df.iloc[:, pos].isna().any():
            # A NumPy integer dtype has no null to copy a null into.
            target_dtype = object
        convert.append((pos, target_dtype))
    if not convert:
        return df
    out = df.copy(deep=False)
    for pos, target_dtype in convert:
        _dataframe_set_column(
            out, df, pos, df.iloc[:, pos].astype(target_dtype))
    out.attrs = dict(df.attrs)
    return out


cdef object _dataframe_normalize_claimed_date(object df):
    """Arrow-backed copies of the millisecond datetime columns claimed
    as DATE.

    A DATE column is claimed by the column's own Arrow type, and the
    NumPy planner has no DATE target of any kind: it reaches DATE only
    through `col_target_column_arrow`, which needs the column to carry
    a `pa.timestamp('ms')`. Plain `to_pandas()` hands a DATE column back
    as a NumPy `datetime64[ms]`, which the planner widens to a
    microsecond TIMESTAMP -- so reading a table and writing the frame
    straight back created the destination table with a TIMESTAMP column
    where the source had DATE, and said nothing.

    The claim carries the missing half. `'date'` is not in
    `_ATTRS_OVERRIDE_KINDS` because there is no Arrow override to
    restore DATE with; what restores it is the column's type, so the
    claim reshapes the column and the type does the rest. Both Arrow
    backends already hand the column back as `pa.timestamp('ms')`, so
    this is what makes the three backends write the same column type.

    A tz-aware millisecond column is reshaped the same way. On its own
    the planner refuses that dtype outright, so a claimed one turns a
    hard rejection into the type the claim names.

    pyarrow is what carries a column to DATE, and without it the frame
    has no route there to be put back on -- the same early return the
    rest of the claim machinery takes.
    """
    cdef object cols_meta, arrow_dtype, dtype, meta, out, unit, tz
    cdef str kind
    cdef list convert
    if not _is_pandas_dataframe_object(df):
        return df
    cols_meta = _roundtrip_columns_meta(df)
    if not cols_meta:
        return df
    _dataframe_may_import_deps()
    arrow_dtype = getattr(_PANDAS, 'ArrowDtype', None)
    if arrow_dtype is None:
        return df
    if not _dataframe_try_import_pyarrow():
        return df
    convert = []
    # Walked by position rather than by label: `dtypes[name]` on a frame
    # with duplicate column names hands back a Series, not a dtype.
    for pos, (name, dtype) in enumerate(zip(df.columns, df.dtypes)):
        meta = cols_meta.get(name)
        kind = _roundtrip_kind(meta)
        if kind != 'date':
            continue
        # An Arrow-backed column already states the type itself.
        if isinstance(dtype, arrow_dtype):
            continue
        if getattr(dtype, 'kind', None) != 'M':
            continue
        tz = getattr(dtype, 'tz', None)
        unit = getattr(dtype, 'unit', None)
        if unit is None:
            unit = numpy.datetime_data(dtype)[0]
        # Only a millisecond column holds what a DATE column held. A
        # column retyped to another unit since it was read is drift, and
        # drift is written as the column's own type implies.
        if unit != 'ms':
            continue
        convert.append((
            pos,
            arrow_dtype(
                _PYARROW.timestamp('ms')
                if tz is None
                else _PYARROW.timestamp('ms', str(tz)))))
    if not convert:
        return df
    out = df.copy(deep=False)
    for pos, target_dtype in convert:
        _dataframe_set_column(
            out, df, pos, df.iloc[:, pos].astype(target_dtype))
    out.attrs = dict(df.attrs)
    return out


cdef void_int _dataframe_claim_all_null_source(
        col_t* col, str kind, object bits) except -1:
    """Give an all-null claimed column the object-column shape its
    claim names.

    Every cell being null leaves the planner nothing to sniff a type
    from, so such a column is skipped and never reaches the wire. A
    round-trip claim states the type anyway, and a destination table
    auto-created without the column is the outcome the claim exists to
    prevent -- so the claim picks the source, and the object-column
    builder writes a column of nulls in the claimed width. This is the
    same source the shape would have taken with one non-null cell in
    it, and what the Arrow path emits for the same frame.

    A claim the column cannot carry -- a geohash precision outside
    1..=60 -- leaves the column skipped, and the tail of
    `_dataframe_apply_roundtrip_overrides` says so, as it does for any
    claim no source took.
    """
    if kind == 'uuid':
        col.setup.source = col_source_t.col_source_bytes_pyobj
        col.setup.target = col_target_t.col_target_column_binary
    elif kind in ('ipv4', 'char', 'long256', 'geohash'):
        if kind == 'geohash' and not (
                _is_int_not_bool(bits) and 1 <= bits <= 60):
            return 0
        col.setup.source = col_source_t.col_source_int_pyobj
        col.setup.target = col_target_t.col_target_column_i64
    else:
        return 0
    col.dispatch_code = <col_dispatch_code_t>(
        <int>col.setup.source + <int>col.setup.target)
    return 0


cdef _warn_roundtrip_claim_dropped(object name, str kind, object shape):
    """Say that a column's ``df.attrs['questdb']`` claim was not applied.

    A claim that no longer matches its column is not an error. The frame
    may have been retyped since it was read, and the write goes ahead as
    the column's own type implies -- rejecting it would answer ordinary
    schema drift with a failure.

    A claim quietly doing nothing is also how a column reaches the
    database as the wrong type, though, and the two are indistinguishable
    from the outside. Drift is the case the silence is for; a type that
    can never carry the kind -- an unsigned integer under ``geohash``,
    say, which the native Arrow importer only accepts signed -- is a
    mistake the caller wants to hear about. Naming the claim and the type
    that turned it away tells them apart without failing either.
    """
    warnings.warn(
        f'questdb: column {name!r} carries a '
        f"df.attrs['questdb'] claim of kind {kind!r}, which a column of "
        f'type {shape} cannot carry. The claim is ignored, and the '
        f'column goes out as its own type decides -- which for some '
        f'shapes means it is left out of the write, or refused by it. '
        f'Cast the column to a type the kind fits, state the type '
        f'outright with schema_overrides, or drop the claim to silence '
        f'this.',
        UserWarning,
        stacklevel=1)


cdef bint _roundtrip_claim_already_carried(
        str kind, const col_t* col) noexcept:
    """Whether the column carries the claimed kind without an override.

    An object column of `uuid.UUID` or `ipaddress.IPv4Address` is
    already written as UUID or IPV4 by the source alone -- which is the
    shape plain `to_pandas()` hands back -- so no override is set for
    it and none is missing. Without this, the claim such a frame comes
    back with would be reported as dropped on every write, on the one
    path the claim exists to serve.
    """
    if kind == 'uuid':
        return col.setup.source == col_source_t.col_source_uuid_pyobj
    if kind == 'ipv4':
        return col.setup.source == col_source_t.col_source_ipv4_pyobj
    return False


cdef void_int _dataframe_apply_roundtrip_overrides(
        object df, dataframe_plan_t* plan) except -1:
    cdef size_t col_index
    cdef col_t* col
    cdef int gh
    cdef object arrow_dtype, dtype
    for col_index in range(plan.col_count):
        plan.cols.d[col_index].setup.has_override = False
    cols_meta = _roundtrip_columns_meta(df)
    if not cols_meta:
        return 0
    df_cols = list(df.columns)
    _dataframe_may_import_deps()
    arrow_dtype = getattr(_PANDAS, 'ArrowDtype', None)
    for col_index in range(plan.col_count):
        col = &plan.cols.d[col_index]
        if col.setup.orig_index >= <size_t>len(df_cols):
            continue
        meta = cols_meta.get(df_cols[col.setup.orig_index])
        kind = _roundtrip_kind(meta)
        if kind is None:
            continue
        if col.setup.source == col_source_t.col_source_nulls:
            _dataframe_claim_all_null_source(
                col, kind, meta.get('precision_bits') or 0)
        # `col_source_int_pyobj` is what a pandas masked dtype turns
        # into: `to_pandas(dtype_backend='numpy_nullable')` returns
        # IPV4 / CHAR / GEOHASH as UInt32 / UInt16 / Int* extension
        # columns, and `_dataframe_normalize_nullable` converts every
        # masked column to object-dtype Python ints before planning.
        # `col_source_bytes_pyobj` is the same story for the two binary
        # kinds, which that backend hands back as object-dtype `bytes`.
        # `col_source_u32_arrow` is an Arrow uint32 column, which
        # resolves to `col_target_column_i64` and so reaches the wire as
        # a raw buffer with the append call naming its dtype — the same
        # place the claim lands for the NumPy and object shapes.
        if (kind == 'ipv4'
                and col.setup.source in (
                    col_source_t.col_source_u32_numpy,
                    col_source_t.col_source_u32_arrow,
                    col_source_t.col_source_int_pyobj)):
            col.setup.has_override = True
            col.setup.override_dtype = \
                qwp_numpy_dtype.qwp_numpy_u32_ipv4
        elif (kind == 'char'
                and col.setup.source in (
                    col_source_t.col_source_u16_numpy,
                    col_source_t.col_source_int_pyobj)):
            col.setup.has_override = True
            col.setup.override_dtype = \
                qwp_numpy_dtype.qwp_numpy_u16_char
        elif (kind == 'uuid'
                and col.setup.source == col_source_t.col_source_bytes_pyobj):
            col.setup.has_override = True
            col.setup.override_dtype = qwp_numpy_dtype.qwp_numpy_s16
        elif (kind == 'long256'
                and col.setup.source in (
                    col_source_t.col_source_int_pyobj,
                    col_source_t.col_source_bytes_pyobj)):
            # Plain `to_pandas()` hands a LONG256 column back as Python
            # ints — the only shape wide enough to hold one without
            # pyarrow — and `numpy_nullable` as raw 32-byte `bytes`.
            # Either way the claim is what tells the 32-byte encoder
            # from the LONG or BINARY column the shape alone implies.
            col.setup.has_override = True
            col.setup.override_dtype = qwp_numpy_dtype.qwp_numpy_s32
        elif kind == 'geohash':
            gh = _geohash_override_dtype(col.setup.source)
            bits = meta.get('precision_bits') or 0
            # The precision is held to the column's own width, not just
            # to 1..=60. A claim wider than the slot carrying it is one
            # the column cannot express, so it is dropped rather than
            # left for the native writer to refuse mid-flush, with the
            # connection already open and a message naming neither the
            # column nor the claim.
            if (gh != _GEOHASH_DTYPE_NONE
                    and gh != _GEOHASH_DTYPE_UNSIGNED
                    and _is_int_not_bool(bits)
                    and 1 <= bits <= _geohash_dtype_max_bits(gh)):
                col.setup.has_override = True
                col.setup.override_dtype = <qwp_numpy_dtype>gh
                col.setup.override_geohash_bits = <uint8_t>bits
        if (not col.setup.has_override
                and kind in _ATTRS_OVERRIDE_KINDS
                and not _roundtrip_claim_already_carried(kind, col)):
            # The claim names a kind this column cannot carry -- a
            # width that cannot hold the precision, an unsigned column,
            # or a type with no route to the kind at all. That is a
            # mistake rather than drift, so it is said out loud, and
            # saying it here rather than per kind is what makes all
            # five answer the way the Arrow path already answers them.
            # The write still goes ahead as the column's own type
            # implies.
            #
            # An Arrow-backed column has already been through
            # `_dataframe_normalize_claimed_arrow`, which says the same
            # thing against the Arrow type and is the one that knows
            # whether the claim could be reshaped. Saying it twice for
            # one claim would be worse than either.
            dtype = df.dtypes.iloc[col.setup.orig_index]
            if arrow_dtype is None or not isinstance(dtype, arrow_dtype):
                _warn_roundtrip_claim_dropped(
                    df_cols[col.setup.orig_index], kind, dtype)
    return 0


cdef void_int _dataframe_columnar_populate_chunk(
        dataframe_plan_t* plan,
        qwp_chunk* chunk,
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
        if not qwp_chunk_at_now(chunk, &err):
            raise c_err_to_py(err)
        return 0
    if plan.at_value >= 0:
        # Fixed scalar designated timestamp shared by every row.
        if not qwp_chunk_at_scalar_nanos(
                chunk, plan.at_value, &err):
            raise c_err_to_py(err)
        return 0
    if at_col == NULL:
        raise RuntimeError('Validated columnar plan has no timestamp column.')
    if plan.pyobj_built != NULL:
        at_prebuilt = plan.pyobj_built[at_col_index]
    _dataframe_columnar_append_at(
        chunk, at_col, at_prebuilt, row_offset, row_count)


cdef void_int _dataframe_columnar_sync(qwp_direct_sender* conn) except -1:
    cdef line_sender_error* err = NULL
    cdef bint ok = False
    cdef PyThreadState* gs = NULL
    cdef uint64_t start_ns = 0
    global _dataframe_columnar_sync_calls
    global _dataframe_columnar_sync_ns
    if _dataframe_columnar_count_io_stats:
        start_ns = time.perf_counter_ns()
    _ensure_doesnt_have_gil(&gs)
    ok = qwp_direct_sender_commit(
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
        qwp_direct_sender* conn,
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
        qwp_direct_sender* conn,
        qwp_chunk* chunk,
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
    ok = qwp_direct_sender_flush(conn, chunk, &err)
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
        ok = qwp_direct_sender_flush(conn, chunk, &err)
        _ensure_has_gil(&gs)
        if _dataframe_columnar_count_io_stats:
            _dataframe_columnar_flush_calls += 1
            _dataframe_columnar_flush_ns += time.perf_counter_ns() - start_ns
        if ok:
            return 0

    raise c_err_to_py(err)


cdef int _arrow_flush_once(
        qwp_direct_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        ArrowSchema* schema,
        line_sender_column_name* ts_column,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        const qwp_arrow_override* overrides,
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
        ok = qwp_direct_sender_flush_arrow_batch_at_column(
            conn, table, array, schema, ts_column[0],
            overrides, overrides_len, err)
    elif at_scalar_set:
        ok = qwp_direct_sender_flush_arrow_batch_at_scalar_nanos(
            conn, table, array, schema, at_scalar_nanos,
            overrides, overrides_len, err)
    else:
        ok = qwp_direct_sender_flush_arrow_batch_at_now(
            conn, table, array, schema,
            overrides, overrides_len, err)
    _ensure_has_gil(&gs)
    if _dataframe_columnar_count_io_stats:
        _dataframe_columnar_flush_calls += 1
        _dataframe_columnar_flush_ns += time.perf_counter_ns() - start_ns
    return 1 if ok else 0


cdef void_int _dataframe_arrow_flush_batch(
        qwp_direct_sender* conn,
        line_sender_table_name table,
        ArrowArray* array,
        ArrowSchema* schema,
        line_sender_column_name* ts_column,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        const qwp_arrow_override* overrides,
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
    Internal benchmark hook for `qwp_direct_sender_flush_arrow_batch_at_now`
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
    cdef qwp_direct_sender* conn = NULL
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
        conn = questdb_db_borrow_direct_sender(db, &err)
        _ensure_has_gil(&gs)
        if conn == NULL:
            raise c_err_to_py(err)
        try:
            for iteration in range(iterations):
                _capsule_consume_stream(
                    conn, arrow_source, c_table_name, c_ts_column_ptr,
                    False, 0, &c_schema, NULL, 0, &any_flushed,
                    &deferred_since_sync, &committed_prefix, 0)
            if any_flushed:
                _dataframe_columnar_sync(conn)
            completed = iterations
        except:
            questdb_db_drop_direct_sender(db, conn)
            conn = NULL
            raise
        finally:
            if conn != NULL:
                questdb_db_return_direct_sender(db, conn)
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
    cdef qwp_chunk* chunk = NULL
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
                chunk = qwp_chunk_new(
                    plan.c_table_name.buf,
                    plan.c_table_name.len,
                    &err)
                if chunk == NULL:
                    raise c_err_to_py(err)
                row_offset = 0
                while row_offset < plan.row_count:
                    if not qwp_chunk_clear(chunk, &err):
                        raise c_err_to_py(err)
                    chunk_rows = rows_per_chunk
                    if chunk_rows > plan.row_count - row_offset:
                        chunk_rows = plan.row_count - row_offset
                    _dataframe_columnar_populate_chunk(
                        &plan,
                        chunk,
                        row_offset,
                        chunk_rows)
                    populated_rows = qwp_chunk_row_count(chunk, &err)
                    if populated_rows == <size_t>-1:
                        raise c_err_to_py(err)
                    if populated_rows != 0:
                        populated_chunks += 1
                        populated_rows_total += populated_rows
                    row_offset += chunk_rows
            finally:
                if chunk != NULL:
                    qwp_chunk_free(chunk)
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


cdef void_int _reject_polars_object_columns(object frame) except -1:
    """polars exports an ``Object`` column as ``fixed_size_binary(8)``
    holding in-process handles, which the server would store as BINARY
    blobs of raw memory addresses. Reject such a column before the
    Arrow export, mirroring the Rust polars API."""
    cdef object object_dtype
    cdef object name
    cdef object dtype
    if not _try_import_polars():
        return 0
    if not isinstance(frame, _POLARS_DATAFRAME_T):
        return 0
    object_dtype = getattr(_POLARS, 'Object', None)
    if object_dtype is None:
        return 0
    for name, dtype in frame.schema.items():
        if dtype == object_dtype:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {name!r}: polars Object dtype is not '
                f'supported; cast it to a supported dtype before ingest.')
    return 0


cdef const char* _ARROW_MD_KEY_COLUMN_TYPE = "questdb.column_type"
cdef size_t _ARROW_MD_KEY_COLUMN_TYPE_LEN = 19
cdef const char* _ARROW_MD_KEY_GEOHASH_BITS = "questdb.geohash_bits"
cdef size_t _ARROW_MD_KEY_GEOHASH_BITS_LEN = 20
cdef const char* _ARROW_MD_VALUE_GEOHASH = "geohash"
cdef const char* _ARROW_FMT_INT64 = "l"
# `questdb.geohash_bits` is written as decimal digits.
cdef char _ASCII_ZERO = 48
cdef char _ASCII_NINE = 57
cdef char _ASCII_PLUS = 43
# The largest value `u8::from_str` yields before it overflows.
cdef int _U8_MAX = 255
# How far a metadata walk may go. The Arrow C data interface hands over
# a bare pointer with no total length, so the blob's own pair count and
# lengths are the only thing steering the walk, and a producer that
# gets them wrong steers it off the end of the allocation. These two
# bounds sit well above any schema a producer writes on purpose, and
# past either one the lookup stops reading and says so.
cdef int32_t _ARROW_MD_MAX_PAIRS = 4096
cdef size_t _ARROW_MD_MAX_BYTES = 1 << 20

# What a metadata lookup answers with. "Stopped short" is its own
# answer because the native importer reads the same keys with no such
# bound: a blob this walk gives up on is one the importer still acts
# on, so "did not find it" would be a claim this side cannot make.
cdef int _ARROW_MD_MISSING = 0
cdef int _ARROW_MD_FOUND = 1
cdef int _ARROW_MD_STOPPED_SHORT = -1

# What `_arrow_column_geohash_bits` answers with besides a precision.
cdef int _GEOHASH_BITS_NONE = -1
cdef int _GEOHASH_BITS_UNREADABLE = -2


cdef int _arrow_md_lookup(
        const char* metadata,
        const char* key,
        size_t key_len,
        const char** value_out,
        int32_t* value_len_out) noexcept nogil:
    """One value out of an ``ArrowSchema`` metadata blob.

    The blob is the Arrow C data interface's packed form: a pair count,
    then each key and each value as a length followed by its bytes.
    None of those lengths is aligned, so each is copied out rather than
    read through a cast.

    The walk is bounded by ``_ARROW_MD_MAX_PAIRS`` and by a running
    byte budget, so how far it reads depends on those bounds rather
    than on numbers the producer wrote. Reaching either bound answers
    ``_ARROW_MD_STOPPED_SHORT``, which is what the caller turns into a
    refusal: the key may well be there, and the importer -- which walks
    the same blob unbounded -- would act on it.
    """
    cdef int32_t n_pairs = 0
    cdef int32_t pair
    cdef int32_t len_bytes = 0
    cdef size_t budget = _ARROW_MD_MAX_BYTES
    cdef const char* pos = metadata
    # A field with no metadata blob carries no keys: that is the one
    # "not found" this walk can state without having read anything.
    if metadata == NULL:
        return _ARROW_MD_MISSING
    memcpy(&n_pairs, pos, 4)
    pos += 4
    if n_pairs < 0 or n_pairs > _ARROW_MD_MAX_PAIRS:
        return _ARROW_MD_STOPPED_SHORT
    for pair in range(n_pairs):
        if budget < 4:
            return _ARROW_MD_STOPPED_SHORT
        budget -= 4
        memcpy(&len_bytes, pos, 4)
        pos += 4
        if len_bytes < 0 or <size_t>len_bytes > budget:
            return _ARROW_MD_STOPPED_SHORT
        budget -= <size_t>len_bytes
        if (<size_t>len_bytes == key_len
                and strncmp(pos, key, key_len) == 0):
            pos += len_bytes
            if budget < 4:
                return _ARROW_MD_STOPPED_SHORT
            budget -= 4
            memcpy(&len_bytes, pos, 4)
            pos += 4
            if len_bytes < 0 or <size_t>len_bytes > budget:
                return _ARROW_MD_STOPPED_SHORT
            value_out[0] = pos
            value_len_out[0] = len_bytes
            return _ARROW_MD_FOUND
        pos += len_bytes
        if budget < 4:
            return _ARROW_MD_STOPPED_SHORT
        budget -= 4
        memcpy(&len_bytes, pos, 4)
        pos += 4
        if len_bytes < 0 or <size_t>len_bytes > budget:
            return _ARROW_MD_STOPPED_SHORT
        budget -= <size_t>len_bytes
        pos += len_bytes
    return _ARROW_MD_MISSING


cdef int _arrow_md_geohash_bits(
        const char* value, int32_t value_len) noexcept nogil:
    """A ``questdb.geohash_bits`` value as an int, or -1 where it is not
    a number in 1..=60.

    The native importer reads this key with ``u8::from_str``, so every
    spelling it accepts -- a leading ``+``, any run of leading zeros --
    names a precision here too, and a column carrying one is held to
    it. What is left behind a -1 is what the importer refuses with a
    message of its own, so no value leaves unchecked either way.
    """
    cdef int32_t i = 0
    cdef int out = 0
    cdef char ch
    cdef bint any_digit = False
    if value_len > 0 and value[0] == _ASCII_PLUS:
        i = 1
    while i < value_len:
        ch = value[i]
        if ch < _ASCII_ZERO or ch > _ASCII_NINE:
            return -1
        out = out * 10 + <int>(ch - _ASCII_ZERO)
        if out > _U8_MAX:
            return -1
        any_digit = True
        i += 1
    if not any_digit:
        return -1
    if out < 1 or out > 60:
        return -1
    return out


cdef int _arrow_column_geohash_bits(
        const ArrowSchema* field,
        const qwp_arrow_override* overrides,
        size_t overrides_len) noexcept nogil:
    """The GEOHASH precision this column goes out at,
    ``_GEOHASH_BITS_NONE`` where it is not a GEOHASH column, or
    ``_GEOHASH_BITS_UNREADABLE`` where the metadata walk stopped short
    of an answer.

    An override wins over the field's own metadata, and an override
    naming some other type takes the column out of GEOHASH altogether
    -- the precedence ``qwp_arrow_override`` documents. An override is
    read straight off the caller's own words, so a column carrying one
    is answered without walking any metadata at all.

    In the field's own metadata it is ``questdb.geohash_bits`` that
    claims the type, which is the order the native importer reads them
    in: those bits alone make the column a GEOHASH, whatever else the
    field carries. A blob too long for the walk therefore leaves the
    kind unknown, and ``_GEOHASH_BITS_UNREADABLE`` says so: the
    importer walks the same blob unbounded and would honour the claim.

    ``questdb.column_type`` only rules the column out, and only by
    naming something other than a ``geohash`` spelling -- a pairing the
    importer refuses outright, so the column never reaches the wire for
    an unread one to have let anything through, and a walk that stops
    short of it leaves the precision standing.
    """
    cdef size_t i
    cdef const char* value = NULL
    cdef int32_t value_len = 0
    cdef size_t name_len
    cdef int bits
    cdef int found
    if field.name != NULL:
        name_len = strlen(field.name)
        for i in range(overrides_len):
            if (overrides[i].column == NULL
                    or overrides[i].column_len != name_len
                    or strncmp(overrides[i].column, field.name,
                               name_len) != 0):
                continue
            if overrides[i].kind == <uint32_t>qwp_arrow_override_geohash:
                bits = <int>overrides[i].arg
                # The same 1..=60 range `_arrow_md_geohash_bits` holds
                # a metadata claim to. Both callers of this path check
                # it before they build the override, and checking it
                # again here is what lets the caller shift by `bits`
                # without knowing which of them it came from.
                if bits < 1 or bits > 60:
                    return _GEOHASH_BITS_NONE
                return bits
            return _GEOHASH_BITS_NONE
    found = _arrow_md_lookup(
        field.metadata, _ARROW_MD_KEY_GEOHASH_BITS,
        _ARROW_MD_KEY_GEOHASH_BITS_LEN, &value, &value_len)
    if found == _ARROW_MD_STOPPED_SHORT:
        return _GEOHASH_BITS_UNREADABLE
    if found != _ARROW_MD_FOUND:
        return _GEOHASH_BITS_NONE
    bits = _arrow_md_geohash_bits(value, value_len)
    if bits < 0:
        return _GEOHASH_BITS_NONE
    if _arrow_md_lookup(
            field.metadata, _ARROW_MD_KEY_COLUMN_TYPE,
            _ARROW_MD_KEY_COLUMN_TYPE_LEN,
            &value, &value_len) == _ARROW_MD_FOUND:
        # The importer reads any `geohash`-prefixed spelling as the
        # kind, so the prefix is what agrees with the bits here too.
        if value_len < 7 or strncmp(
                value, _ARROW_MD_VALUE_GEOHASH, 7) != 0:
            return _GEOHASH_BITS_NONE
    return bits


cdef int _arrow_signed_int_width(const char* fmt) noexcept nogil:
    """The byte width of a signed Arrow integer format, or 0 for
    anything else. GEOHASH rides only on the signed widths, so an
    unsigned or non-integer column is the native importer's to refuse.
    """
    if fmt == NULL:
        return 0
    # Compared with the terminator, so a one-character format matches
    # exactly and a wider one that merely starts with it does not.
    if strncmp(fmt, _ARROW_FMT_INT8, 2) == 0:
        return 1
    if strncmp(fmt, _ARROW_FMT_INT16, 2) == 0:
        return 2
    if strncmp(fmt, _ARROW_FMT_INT32, 2) == 0:
        return 4
    if strncmp(fmt, _ARROW_FMT_INT64, 2) == 0:
        return 8
    return 0


cdef int _arrow_geohash_width_max_bits(int elem_size) noexcept nogil:
    """The widest precision an Arrow integer of this byte width can
    carry, or 0 for a width that carries none.

    A precision fills its bits, so a width carries a precision its own
    size -- an `int32` carries a 32-bit geohash, whose top values sit in
    the column as negative numbers. The 64-bit slot stops at 60, the
    widest GEOHASH QuestDB has.

    `_geohash_dtype_max_bits` and `_attrs_override_fits` state one bit
    less per width, because the round-trip claim they answer has to
    survive a read back into a signed column and so cannot use the
    sign bit.
    """
    if elem_size == 1:
        return 8
    if elem_size == 2:
        return 16
    if elem_size == 4:
        return 32
    if elem_size == 8:
        return 60
    return 0


cdef str _arrow_field_name(const ArrowSchema* field):
    """A field's name as text, for a message naming the column."""
    if field == NULL or field.name == NULL:
        return '?'
    return field.name.decode('utf-8', 'replace')


cdef bint _arrow_schema_claims_geohash(
        const ArrowSchema* schema,
        const qwp_arrow_override* overrides,
        size_t overrides_len) noexcept nogil:
    """Whether any column of this schema goes out as a GEOHASH, or
    leaves the question unanswered.

    Read off the schema alone, which the stream hands over once and
    keeps for every batch, so a frame that claims no GEOHASH is
    answered without looking at a single batch. A column whose
    metadata the walk stopped short of counts here, so the scan runs
    and gets to name it.
    """
    cdef int64_t child_index
    cdef const ArrowSchema* field
    if schema.n_children == 0 or schema.children == NULL:
        return False
    for child_index in range(schema.n_children):
        field = schema.children[child_index]
        if field == NULL:
            continue
        if (_arrow_column_geohash_bits(field, overrides, overrides_len)
                != _GEOHASH_BITS_NONE):
            return True
    return False


cdef void_int _arrow_batch_check_geohash_ranges(
        const ArrowArray* batch,
        const ArrowSchema* schema,
        const qwp_arrow_override* overrides,
        size_t overrides_len,
        size_t row_base) except -1:
    """Refuse a GEOHASH value the claimed precision cannot hold.

    A GEOHASH column keeps only the claimed low bits, and the high bits
    are the coarse position, so a value that does not fit reaches the
    database as a valid geohash for somewhere else entirely. The wire
    encoder writes the low ``ceil(bits/8)`` bytes of whatever it is
    handed and reports nothing, so the value has to be turned away on
    this side.

    Every batch this path sends passes through here -- pandas, polars,
    ``pa.Table``, ``pa.RecordBatch`` and one-shot streams alike, sliced
    or whole -- so there is one rule and one place that applies it. A
    frame claiming no GEOHASH is answered off the schema, which is
    fetched once and shared by every batch of a stream, and its values
    are never touched.

    ``row_base`` is the number of rows of this frame already sent, so
    the row named is the caller's own rather than its position inside a
    batch they never chose.

    Once a frame does claim a GEOHASH column, a shape this scan cannot
    read stops the send rather than passing unread: nothing downstream
    would catch what went by, so "could not check" and "checked, and it
    was fine" have to end differently. A frame claiming no GEOHASH is
    left alone whatever shape it is in -- the importer holds it to its
    own rules.
    """
    cdef int64_t child_index
    cdef const ArrowSchema* field
    cdef const ArrowArray* col
    cdef int bits
    cdef int elem_size
    cdef int64_t max_value
    cdef const void* data
    cdef const uint8_t* validity
    cdef size_t bad_row = 0
    cdef bint bad = False
    cdef int64_t start
    if batch.length == 0:
        return 0
    if not _arrow_schema_claims_geohash(schema, overrides, overrides_len):
        return 0
    if (batch.n_children != schema.n_children
            or batch.children == NULL or schema.children == NULL):
        raise QuestDBError(
            QuestDBErrorCode.BadDataFrame,
            f'Bad dataframe: a GEOHASH column cannot be checked because '
            f'the batch carries {batch.n_children} columns against the '
            f'schema\'s {schema.n_children}.')
    # A struct-level offset shifts every child: the importer slices each
    # column by it before reading, so the scan starts there too and the
    # rows it reads are the rows that go out.
    if batch.offset < 0:
        raise QuestDBError(
            QuestDBErrorCode.BadDataFrame,
            f'Bad dataframe: a GEOHASH column cannot be checked because '
            f'the batch starts at row {batch.offset}.')
    for child_index in range(schema.n_children):
        field = schema.children[child_index]
        col = batch.children[child_index]
        if field == NULL or col == NULL:
            continue
        elem_size = _arrow_signed_int_width(field.format)
        if elem_size == 0:
            # A GEOHASH rides only on a signed Int8/16/32/64, and the
            # importer refuses the kind on anything else, naming the
            # type it got. Settling that from the format alone keeps a
            # column that can never carry one out of the metadata walk
            # below, whose bound is not a property of this column: a
            # string column with a large blob of its own metadata would
            # otherwise stop the send over a claim it cannot hold.
            continue
        bits = _arrow_column_geohash_bits(field, overrides, overrides_len)
        if bits == _GEOHASH_BITS_UNREADABLE:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {_arrow_field_name(field)!r}: whether it '
                f'claims a GEOHASH cannot be read, because its Arrow '
                f'metadata runs past what this scan walks (at most '
                f'{_ARROW_MD_MAX_PAIRS} pairs and '
                f'{_ARROW_MD_MAX_BYTES} bytes). The server reads that '
                f'metadata with no such bound, so a claim hiding past '
                f'it would ship unchecked. Name the type through '
                f'`schema_overrides`, which is read before the '
                f'metadata.')
        if bits == _GEOHASH_BITS_NONE:
            continue
        if bits > _arrow_geohash_width_max_bits(elem_size):
            # `schema_overrides` bounds the precision to 1..60 without
            # seeing the column, so a claim wider than the column can
            # carry arrives here. Refusing it names the width; letting
            # it through would refuse every value with the top bit set
            # and quote a range the column cannot express.
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {_arrow_field_name(field)!r}: a '
                f'GEOHASH({bits}b) claim needs more bits than a '
                f'{elem_size * 8}-bit column can carry (at most '
                f'{_arrow_geohash_width_max_bits(elem_size)}).')
        # `col.offset` is the array's own start row and is cast to
        # `size_t` for the scan, so a negative one would wrap to about
        # 2**64 and index the value buffer far outside it.
        if (col.n_buffers < 2 or col.buffers == NULL
                or col.offset < 0 or col.buffers[1] == NULL):
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {_arrow_field_name(field)!r}: its '
                f'GEOHASH({bits}b) values cannot be held to the '
                f'precision because the column arrived without readable '
                f'value buffers.')
        # An Arrow length is the row count measured from the array's own
        # start, and a struct-level offset picks the row each column is
        # read from, so the batch's rows are in range when
        # `batch.offset + batch.length` fits the column's length.
        if batch.offset + batch.length > col.length:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {_arrow_field_name(field)!r}: its '
                f'GEOHASH({bits}b) values cannot be held to the '
                f'precision because the column holds {col.length} rows '
                f'and the batch asks for {batch.length} from row '
                f'{batch.offset}.')
        start = col.offset + batch.offset
        data = col.buffers[1]
        # A column with no validity buffer has no nulls, whatever it
        # says its null count is -- the count is allowed to be -1,
        # meaning nobody has counted, and a column that never had a
        # null has no bitmap to count from. Reading the pair as "has
        # nulls, cannot see them" left every value in an ordinary
        # bitmap-free column unread.
        validity = NULL
        if col.null_count != 0 and col.buffers[0] != NULL:
            validity = <const uint8_t*>col.buffers[0]
        max_value = (<int64_t>1 << bits) - 1
        # A precision that fills the column's width uses every bit,
        # including the one that reads as a sign, so its top half sits
        # there as negative numbers. Reading such a column as signed
        # refused every one of those values while quoting a range the
        # column cannot express. A narrower claim leaves the sign bit
        # clear, so a negative there is genuinely out of range.
        with nogil:
            bad = _dataframe_columnar_geohash_scan(
                data,
                validity,
                <size_t>elem_size,
                bits < elem_size * 8,
                <size_t>batch.length,
                max_value,
                <size_t>start,
                &bad_row)
        if bad:
            raise QuestDBError(
                QuestDBErrorCode.BadDataFrame,
                f'Bad column {_arrow_field_name(field)!r} at row '
                f'{row_base + bad_row}: '
                f'GEOHASH({bits}b) values must be in the range '
                f'0 .. {max_value}.')
    return 0


cdef void_int _capsule_consume_stream(
        qwp_direct_sender* conn,
        object stream_owner,
        line_sender_table_name c_table_name,
        line_sender_column_name* c_ts_column_ptr,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        ArrowSchema* c_schema,
        const qwp_arrow_override* c_overrides,
        size_t c_overrides_len,
        bint* any_flushed,
        size_t* deferred_since_sync,
        bint* committed_prefix,
        size_t row_base) except -1:
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
            # Before the flush: a value the claimed GEOHASH precision
            # cannot hold is truncated by the encoder without a word,
            # so it has to be caught while it is still on this side.
            try:
                _arrow_batch_check_geohash_ranges(
                    &batch, c_schema, c_overrides, c_overrides_len,
                    row_base)
            except QuestDBError as gh_exc:
                # The scan runs batch by batch, and a large enough frame
                # syncs a checkpoint every
                # `_QWP_MAX_DEFERRED_ARROW_FRAMES` batches. A refusal
                # past the first checkpoint therefore arrives with rows
                # already stored, and retrying the whole frame after
                # correcting it would duplicate them.
                if committed_prefix[0]:
                    raise QuestDBError(
                        gh_exc.code,
                        f'{gh_exc} Rows from earlier batches of this '
                        f'dataframe are already stored, so retrying the '
                        f'whole dataframe would duplicate them; resume '
                        f'from the named row instead.',
                        # The scan raises with `in_doubt` false, being
                        # a local refusal -- but rows really are stored
                        # by this point, and `in_doubt` is what a caller
                        # branches on to decide whether replaying is
                        # safe. Saying otherwise leaves only the message
                        # text to carry it.
                        in_doubt=True) from gh_exc
                raise
            if deferred_since_sync[0] >= _QWP_MAX_DEFERRED_ARROW_FRAMES:
                _dataframe_columnar_sync(conn)
                committed_prefix[0] = True
                deferred_since_sync[0] = 0
            _dataframe_arrow_flush_batch(
                conn, c_table_name, &batch, c_schema, c_ts_column_ptr,
                at_scalar_set, at_scalar_nanos,
                c_overrides, c_overrides_len,
                True, committed_prefix,
                deferred_since_sync)
            any_flushed[0] = True
            deferred_since_sync[0] += 1
            row_base += <size_t>batch.length
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
            "one of: 'symbol', 'ipv4', 'char', 'uuid', 'long256', or "
            "('geohash', bits).")
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
            kind_int = <int>qwp_arrow_override_symbol
        elif kind == 'ipv4':
            kind_int = <int>qwp_arrow_override_ipv4
        elif kind == 'char':
            kind_int = <int>qwp_arrow_override_char
        elif kind == 'uuid':
            kind_int = <int>qwp_arrow_override_uuid
        elif kind == 'long256':
            kind_int = <int>qwp_arrow_override_long256
        elif kind == 'geohash':
            if not _is_int_not_bool(value) or value < 1 or value > 60:
                raise ValueError(
                    f'schema_overrides[{name!r}] geohash bits must '
                    f'be int in 1..=60, got {value!r}.')
            kind_int = <int>qwp_arrow_override_geohash
            arg_int = value
        else:
            raise ValueError(
                f'schema_overrides[{name!r}] kind {kind!r} not '
                "in {'symbol', 'ipv4', 'char', 'uuid', 'long256', "
                "'geohash'}.")
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
    _validate_schema_overrides. `kind` is `qwp_arrow_override_symbol`
    to mark a column as SYMBOL or `qwp_arrow_override_not_symbol`
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
    cdef int symbol_kind = <int>qwp_arrow_override_symbol
    cdef int not_symbol_kind = <int>qwp_arrow_override_not_symbol
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
        object weaker, object stronger):
    """Merge two override lists, `stronger` winning on name collision.

    The three sources rank: `schema_overrides` states a type outright,
    `symbols` states one for string columns, and `df.attrs['questdb']`
    only recalls what a column held when it was read back out of
    QuestDB.
    """
    cdef set stronger_names
    cdef list merged
    cdef object entry
    if not weaker and stronger is None:
        return None
    if not weaker:
        return stronger
    if stronger is None:
        return weaker
    stronger_names = {entry[0] for entry in stronger}
    merged = list(stronger)
    for entry in weaker:
        if entry[0] not in stronger_names:
            merged.append(entry)
    return merged


# `df.attrs['questdb']` column kinds that an Arrow override can restore.
# The other kinds either survive on their storage type alone (VARCHAR,
# SYMBOL, TIMESTAMP, ...) or have no override to restore them with
# (BYTE / SHORT / INT, which the Arrow classifier widens). DATE is the
# one kind restored by reshaping the column rather than by an override,
# in `_dataframe_normalize_claimed_date`: no Arrow override names it,
# and the Arrow type it needs is the whole claim.
cdef dict _ATTRS_OVERRIDE_KINDS = {
    'ipv4': <int>qwp_arrow_override_ipv4,
    'char': <int>qwp_arrow_override_char,
    'uuid': <int>qwp_arrow_override_uuid,
    'long256': <int>qwp_arrow_override_long256,
    'geohash': <int>qwp_arrow_override_geohash,
}


cdef object _capsule_pandas_arrow_type(
        object dtypes, object arrow_dtype, object name):
    """The Arrow storage type backing column `name`, or None where the
    column is missing or not Arrow-backed.

    Takes the frame's `dtypes` rather than the frame: reading
    `frame.dtypes` builds a fresh Series every time, so looking it up
    per column was quadratic in the column count, on a path a streaming
    `iter_pandas` feeds per batch.
    """
    cdef object dtype
    cdef object ty
    if arrow_dtype is None:
        return None
    try:
        dtype = dtypes[name]
    except Exception:
        return None
    if not isinstance(dtype, arrow_dtype):
        return None
    ty = dtype.pyarrow_dtype
    if isinstance(ty, _PYARROW.lib.BaseExtensionType):
        ty = ty.storage_type
    return ty


cdef bint _attrs_override_fits(
        object types, object ty, str kind, object bits) except -1:
    """Whether the Arrow type can carry the claimed kind.

    Mirrors what the native client accepts, so a claim that no longer
    matches the column is dropped here instead of erroring there. UUID
    and LONG256 are held to their fixed widths: those are what the
    egress emits, and a variable-width binary claim would only be
    caught value by value at encode time.

    `types` is `pyarrow.types`, passed in so the caller reads it once
    for the whole frame rather than once per column.
    """
    if kind == 'ipv4':
        return types.is_uint32(ty)
    if kind == 'char':
        return types.is_uint16(ty)
    if kind == 'uuid':
        return types.is_fixed_size_binary(ty) and ty.byte_width == 16
    if kind == 'long256':
        return types.is_fixed_size_binary(ty) and ty.byte_width == 32
    if kind == 'geohash':
        if not _is_int_not_bool(bits) or bits < 1 or bits > 60:
            return False
        # One bit short of each width: the slot is signed and the range
        # check reads it as signed, so a precision that fills the width
        # names values the column has no way of spelling. The same four
        # numbers as `_geohash_dtype_max_bits`.
        if types.is_int8(ty):
            return bits <= 7
        if types.is_int16(ty):
            return bits <= 15
        if types.is_int32(ty):
            return bits <= 31
        if types.is_int64(ty):
            return bits <= 60
        return False
    return False


cdef object _capsule_roundtrip_overrides(object frame):
    """Arrow overrides rebuilt from the `df.attrs['questdb']` metadata
    that `QueryResult.to_pandas()` attaches.

    A pandas dtype holds an Arrow type and no field, so the
    `questdb.column_type` claim the egress stamps on the field is gone
    by the time a frame comes back in; without it a UUID or LONG256
    column would go back out as BINARY and IPV4 / CHAR / GEOHASH as
    plain integers. The claim recalls what the frame held when it was
    read, so a column since dropped, renamed or retyped is skipped
    rather than rejected.
    """
    cdef list out = []
    cdef object cols_meta, meta, ty, bits, dtypes, arrow_dtype, types
    cdef str kind
    cdef int kind_int
    cdef int arg_int
    if not _is_pandas_dataframe_object(frame):
        return out
    cols_meta = _roundtrip_columns_meta(frame)
    if not cols_meta:
        return out
    _dataframe_may_import_deps()
    if not _dataframe_try_import_pyarrow():
        return out
    # Read once for the whole frame, not once per column.
    dtypes = frame.dtypes
    arrow_dtype = getattr(_PANDAS, 'ArrowDtype', None)
    types = _PYARROW.types
    for name, meta in cols_meta.items():
        if not isinstance(name, str):
            continue
        kind = _roundtrip_kind(meta)
        if kind is None or kind not in _ATTRS_OVERRIDE_KINDS:
            continue
        kind_int = <int>_ATTRS_OVERRIDE_KINDS[kind]
        bits = meta.get('precision_bits') or 0
        ty = _capsule_pandas_arrow_type(dtypes, arrow_dtype, name)
        if ty is None:
            # The column is gone, renamed, or not Arrow-backed. That is
            # the drift the claim is meant to survive quietly.
            continue
        if not _attrs_override_fits(types, ty, kind, bits):
            _warn_roundtrip_claim_dropped(name, kind, ty)
            continue
        # Only GEOHASH takes an argument, and `_attrs_override_fits` has
        # already held it to 1..=60.
        arg_int = <int>bits if kind == 'geohash' else 0
        out.append((name.encode('utf-8'), kind_int, arg_int))
    return out


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


cdef qwp_direct_sender* _direct_conn_open(
        direct_conn_source_t* src,
        uint64_t budget_ms,
        line_sender_error** err) noexcept nogil:
    if src.db != NULL:
        if budget_ms == 0:
            return questdb_db_borrow_direct_sender(src.db, err)
        return questdb_db_borrow_direct_sender_with_retry(
            src.db, budget_ms, err)
    return qwp_direct_sender_from_opts(src.opts, err)


cdef void _direct_conn_close(
        direct_conn_source_t* src,
        qwp_direct_sender* conn,
        bint force_drop) noexcept nogil:
    if src.db != NULL:
        if force_drop:
            questdb_db_drop_direct_sender(src.db, conn)
        else:
            questdb_db_return_direct_sender(src.db, conn)
    elif force_drop:
        # Destroying a poolless sender after a failure must discard its
        # uncommitted pipelined frames, and `qwp_direct_sender_free`
        # commits them best-effort, so the drop entry point is the only
        # bound way to get that. It ignores its `db` argument
        # (`qwp_sender.h`: "`db` is currently ignored - the sender
        # carries its own reference to the pool"), which is what makes
        # NULL safe for a handle that never came from a pool.
        questdb_db_drop_direct_sender(NULL, conn)
    else:
        qwp_direct_sender_free(conn)


cdef bint _dataframe_client_try_capsule_path(
        direct_conn_source_t* src,
        uint64_t budget_ms,
        object df,
        object table_name,
        object symbols,
        object at,
        size_t max_rows_per_batch,
        object validated_overrides,
        bint* committed_prefix,
        bint* nonreplayable_consumed) except -1:
    cdef qdb_pystr_buf* b = NULL
    cdef qwp_direct_sender* conn = NULL
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
    cdef qwp_arrow_override* c_overrides = NULL
    cdef size_t c_overrides_len = 0
    cdef bint at_is_column = False
    cdef bint at_scalar_set = False
    cdef int64_t at_scalar_nanos = 0
    cdef size_t i
    cdef object name_bytes
    cdef object roundtrip_overrides
    cdef int kind_int
    cdef int arg_int

    if _pandas_dataframe_requires_manual_planner(df):
        return False
    if _pandas_dataframe_is_timestamp_only_at(df, at):
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

    _reject_polars_object_columns(sliceable)

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
    roundtrip_overrides = _capsule_roundtrip_overrides(sliceable)
    merged_overrides = _merge_capsule_overrides(
        roundtrip_overrides, merged_overrides)

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
            c_overrides = <qwp_arrow_override*>calloc(
                c_overrides_len, sizeof(qwp_arrow_override))
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
                    committed_prefix, max_rows_per_batch, False, 0)
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
                        committed_prefix, max_rows_per_batch, True,
                        <size_t>offset)
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
    cdef qwp_chunk* chunk = NULL
    cdef qwp_direct_sender* conn = NULL
    cdef line_sender_error* err = NULL
    cdef PyThreadState* gs = NULL
    cdef bint flushed = False
    cdef bint force_drop_conn = False
    cdef size_t rows_per_chunk
    cdef size_t row_offset
    cdef size_t chunk_rows
    try:
        df = _dataframe_normalize_nullable(df)
        df = _dataframe_normalize_claimed_arrow(df)
        df = _dataframe_normalize_claimed_date(df)
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
        _dataframe_columnar_check_geohash_ranges(df, plan)
        _dataframe_columnar_prebuild_pyobj(df, plan)
        rows_per_chunk = _dataframe_columnar_rows_per_chunk(
            plan, max_rows_per_batch)

        _ensure_doesnt_have_gil(&gs)
        conn = _direct_conn_open(src, budget_ms, &err)
        _ensure_has_gil(&gs)
        if conn == NULL:
            raise c_err_to_py(err)

        chunk = qwp_chunk_new(
            plan.c_table_name.buf,
            plan.c_table_name.len,
            &err)
        if chunk == NULL:
            raise c_err_to_py(err)
        try:
            row_offset = 0
            while row_offset < plan.row_count:
                if not qwp_chunk_clear(chunk, &err):
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
                    True,
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
            qwp_chunk_free(chunk)
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
    if table_name is not None and table_name_col is not None:
        raise ValueError(
            'Can specify only one of `table_name` or `table_name_col`.')
    if table_name_col is not None:
        # This path writes one table per call, so it has nowhere to put
        # a table-name column. The check runs before the planner looks
        # at any column, so the error always says the same thing. Later
        # on, a column the planner cannot type by itself — a 32-byte
        # binary with no type claim, say — would raise first and point
        # at the wrong problem.
        raise UnsupportedDataFrameShapeError(
            'QWP column ingestion writes one table per call and does '
            'not accept `table_name_col`. That is QuestDB.dataframe() '
            'and Sender.dataframe() over ws:: / wss::. Split the frame '
            'on that column and make one call per table, e.g. '
            "`for name, group in df.groupby('tbl'): "
            "dataframe(group.drop(columns='tbl'), table_name=name, "
            "at='ts')`. Those calls take `symbols` and "
            "`schema_overrides` and read `df.attrs['questdb']`, so "
            'LONG256 and the other column types work as normal. '
            'Sender.dataframe() over tcp:: / http:: does accept '
            '`table_name_col`: it serializes row by row, so it can '
            'change table between rows.')
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
                    'where every column uses ArrowDtype). This input falls '
                    'back to the NumPy planner, which does not apply '
                    'schema_overrides; convert the frame, e.g. '
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
        qwp_direct_sender* conn,
        object stream_owner,
        line_sender_table_name c_table_name,
        line_sender_column_name* c_ts_column_ptr,
        bint at_scalar_set,
        int64_t at_scalar_nanos,
        ArrowSchema* c_schema,
        const qwp_arrow_override* c_overrides,
        size_t c_overrides_len,
        bint* any_flushed,
        size_t* deferred_since_sync,
        bint* committed_prefix,
        size_t max_rows_per_batch,
        bint can_slice,
        size_t row_base) except -1:
    cdef str hint
    try:
        _capsule_consume_stream(
            conn, stream_owner, c_table_name, c_ts_column_ptr,
            at_scalar_set, at_scalar_nanos, c_schema,
            c_overrides, c_overrides_len, any_flushed,
            deferred_since_sync, committed_prefix, row_base)
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


# no_gc_clear keeps tp_clear from nulling fields the native dispatchers
# still target. The callback targets are additionally pinned in
# _LIVE_CALLBACK_REFS until the dispatchers are joined, so the cycle collector
# cannot clear a live handler even when the handle is reached only through it.
@cython.no_gc_clear
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
    cdef object _error_handler
    cdef size_t _cb_refs_key
    cdef auto_flush_mode_t _auto_flush_mode
    cdef bint _auto_flush_bytes_dynamic

    def __cinit__(self):
        self._db = NULL
        self._conf_str = None
        self._state_cond = threading.Condition(threading.RLock())
        self._active_uses = 0
        self._closing = False
        self._connection_listener = None
        self._error_handler = None
        self._cb_refs_key = 0
        self._auto_flush_mode.enabled = False
        self._auto_flush_mode.interval = -1
        self._auto_flush_mode.row_count = -1
        self._auto_flush_mode.byte_count = -1
        self._auto_flush_bytes_dynamic = False

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
            connection_event_inbox_capacity=0,
            error_handler=None,
            error_event_inbox_capacity=0):
        """
        Construct a handle from a QWP/WebSocket configuration string.

        Prefer the :func:`questdb.connect` module-level factory.

        By default the handle connects eagerly: construction pre-opens the
        warm minimums (``sender_pool_min`` sender and ``query_pool_min``
        reader connections, one of each by default), so an unreachable
        server or bad credentials fail here, fast. With
        ``lazy_connect=true`` construction opens no connection: sender
        leases buffer locally and connect in the background, readers
        connect on first use (``query_pool_min`` defaults to 0), and
        errors surface from the first operation instead. Combining
        ``lazy_connect=true`` with a blocking ``initial_connect_retry``
        or a positive ``query_pool_min`` is a configuration conflict.

        Pooled row auto-flush is enabled by default at 1,000 rows, 100
        milliseconds, or an estimated encoded size at 90% of the effective
        frame cap. Before a server cap is known, or when the server omits it,
        the byte threshold is the lower of 8 MiB and 90% of the local
        store-and-forward frame cap. Set
        ``auto_flush_bytes=off`` to disable only the byte trigger, or
        ``auto_flush=off`` to disable all automatic publishing.
        The mode is immutable and shared by every lease; the interval starts
        when the first row enters an empty lease buffer. Auto-triggered
        publishes do not wait for server acknowledgement.

        The underlying connection pool is opened by
        `questdb_db_connect_with_handlers`.
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
        :attr:`connection_events_delivered`. Successful events are queued
        only after negotiated state, including the server frame cap, is
        committed; they do not acknowledge data.

        ``error_handler``, when set, is a callable receiving one
        :class:`SenderError` per server rejection recorded by any of the
        pool's store-and-forward connections — including rejections for
        rows published through a :class:`PooledSender` that was already
        closed. It runs on its own dedicated dispatcher thread fed by a
        bounded inbox (``error_event_inbox_capacity``; ``0`` selects the
        default of 64, overflow drops the oldest event). Exceptions it
        raises are logged and swallowed. Without a handler every rejection
        is logged through the ``questdb`` logger instead — ``ERROR`` for
        terminal rejections, ``WARNING`` for retriable ones (the affected
        rows are replayed, not lost) — so rejections are never silent.
        A terminal rejection is queued only after the connection's terminal
        latch and pollable diagnostic have been committed.
        Delivered/dropped totals are available via
        :attr:`error_events_delivered` /
        :attr:`error_events_dropped`. Use the handler for
        dead-lettering, alerting, and metrics; terminal failures also
        surface as :class:`QuestDBError` from the sender calls themselves.

        When a handler or listener closes over the returned handle, call
        :meth:`close` explicitly (or use ``with``): until closed, such a
        handle is pinned alive rather than collected, so an abandoned one
        leaks instead of crashing. The pin is dropped at interpreter
        shutdown, so an unclosed self-referential handle can still crash
        during finalization.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_utf8 c_conf
        cdef line_sender_protocol c_protocol
        cdef object protocol
        cdef dict params
        cdef set auto_flush_keys = {
            'auto_flush',
            'auto_flush_rows',
            'auto_flush_bytes',
            'auto_flush_interval',
        }
        cdef bint auto_flush_configured
        cdef str native_conf_str
        cdef qdb_pystr_buf* b = qdb_pystr_buf_new()
        cdef QuestDB db = QuestDB.__new__(QuestDB)
        cdef PyThreadState* gs = NULL
        cdef void* connection_listener_data = NULL
        cdef questdb_connection_event_cb connection_event_cb = NULL
        cdef size_t c_event_inbox_capacity
        cdef size_t c_error_inbox_capacity
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

            # The pooled native core does not implement row-buffer
            # auto-flushing: it rejects the threshold keys and accepts only
            # auto_flush=off. Keep this Python-owned handle configuration out
            # of the native connect string. Pooled QWP uses its own defaults,
            # while validation stays shared with the standalone Sender.
            c_protocol = protocol.c_value
            auto_flush_configured = not auto_flush_keys.isdisjoint(params)
            _parse_pooled_auto_flush(
                c_protocol,
                params.get('auto_flush'),
                params.get('auto_flush_rows'),
                params.get('auto_flush_bytes'),
                params.get('auto_flush_interval'),
                &db._auto_flush_mode,
                &db._auto_flush_bytes_dynamic)
            if auto_flush_configured:
                native_conf_str = protocol.tag + '::' + ''.join(
                    f'{key}={conf_str_value(value)};'
                    for key, value in params.items()
                    if key not in auto_flush_keys)
            else:
                # Preserve the native configuration path byte for byte when
                # there are no Python-owned auto-flush settings to remove.
                native_conf_str = conf_str

            if connection_listener is not None and not callable(
                    connection_listener):
                raise TypeError(
                    '"connection_listener" must be callable or None, '
                    f'not {_fqn(type(connection_listener))}')
            if error_handler is not None and not callable(
                    error_handler):
                raise TypeError(
                    '"error_handler" must be callable or None, '
                    f'not {_fqn(type(error_handler))}')
            str_to_utf8(b, <PyObject*>native_conf_str, &c_conf)
            if connection_listener is not None:
                # Register as part of pool construction so recovery senders
                # pre-opened by connect cannot emit events before the listener
                # exists. The handle keeps the callback target alive until
                # close() has stopped and joined the dispatcher.
                db._connection_listener = connection_listener
                connection_listener_data = <void*>db._connection_listener
                connection_event_cb = _connection_event_trampoline
            # A rejection handler is always installed (defaulting to the
            # `questdb` logger) because the native default logs through the
            # Rust `log` facade, which is not bridged into Python logging.
            if error_handler is None:
                error_handler = _default_error_handler
            db._error_handler = error_handler
            # Convert to C integers while still holding the GIL: a bad
            # value must raise here, not inside the nogil region below.
            c_event_inbox_capacity = connection_event_inbox_capacity
            c_error_inbox_capacity = error_event_inbox_capacity
            _ensure_doesnt_have_gil(&gs)
            db._db = questdb_db_connect_with_handlers(
                c_conf.buf,
                c_conf.len,
                connection_event_cb,
                connection_listener_data,
                c_event_inbox_capacity,
                _sender_error_trampoline,
                <void*>db._error_handler,
                c_error_inbox_capacity,
                &err)
            _ensure_has_gil(&gs)
            if db._db == NULL:
                # A failed handler-aware connect fences its dispatchers before
                # returning, so the callback targets are now safe to release.
                db._connection_listener = None
                db._error_handler = None
                raise c_err_to_py(err)
            db._conf_str = conf_str
            db._cb_refs_key = _retain_callback_refs(
                db, db._error_handler, db._connection_listener)
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
        sender, independent of ``sf_dir``. On success, the call returns only
        after every DataFrame batch has been committed. Most loads queue their
        batches and commit once at the end. Large Arrow inputs checkpoint about
        every 100 batches to keep memory bounded. The client may checkpoint
        earlier if the connection cannot queue another batch or if a batch must
        be split to fit. If a later batch fails, the exception means that the
        load did not finish, not necessarily that no rows landed. Any already
        committed prefix from this call remains, and retrying the whole
        DataFrame can duplicate it unless the destination table uses suitable
        ``DEDUP UPSERT KEYS``.

        On a transient connection failure, the client re-sends from the
        original DataFrame only when it knows that no rows landed. Otherwise it
        raises instead of risking a blind retry. Server-side rejections (e.g. a
        schema mismatch) surface as a plain
        :class:`QuestDBError`; the structured ``sender_error`` diagnostic
        is attached only by the store-and-forward senders.

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

        The columnar path loads one table per call: name it via
        ``table_name`` — or, for NumPy-backed pandas input, the
        dataframe's index name (``df.index.name``); Arrow-native input
        (polars, pyarrow, pyarrow-backed pandas) requires an explicit
        ``table_name``. ``table_name_col`` raises
        :class:`UnsupportedDataFrameShapeError` — split multi-table
        frames (e.g. ``df.groupby(col)``) and load each group.

        Supports a column-QWP v1 subset: a single per-call table name,
        non-null designated timestamp column, and the following
        per-column dtypes:

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
          ``pa.timestamp`` with unit ``s``, ``us``, or ``ns`` (tz-aware
          accepted on Arrow-backed columns in the Rust Arrow route). An
          Arrow ``ms`` timestamp field lands as DATE instead — see
          **DATE** below — while a NumPy ``datetime64[ms]`` field is
          widened to a microsecond TIMESTAMP unless a
          ``df.attrs['questdb']`` claim names it DATE.
          Timestamp *field* columns accept null timestamps (``NaT`` /
          ``None``) and values before the Unix epoch; a ``datetime64[ns]``
          field carrying ``NaT`` is re-exported through Arrow so its
          ``INT64_MIN`` null sentinel is preserved (this needs pyarrow).
          The *designated* timestamp column (``at=``) must be non-null
          and at or after the epoch.
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
        - **UUID**: object-dtype columns of ``uuid.UUID``, the
          ``arrow.uuid`` extension type over ``pa.fixed_size_binary(16)``,
          or any 16-byte binary column claimed with
          ``schema_overrides={'col': 'uuid'}``. Bytes are **canonical
          RFC 4122 big-endian** — exactly ``uuid.UUID.bytes`` — and the
          client byte-swaps them into QWP wire order. Round-trip through
          :meth:`query <questdb.QuestDB.query>` is byte-identity.

          The ``arrow.uuid`` route requires pyarrow >= 18, which is where
          that canonical extension type is registered, and the column
          must carry the extension **type** — build it from ``pa.uuid()``.
          Writing ``ARROW:extension:name`` as plain field metadata is not
          equivalent: pyarrow leaves such a key on the field, and a
          pandas ``ArrowDtype`` carries a type and no field, so a frame
          built that way reaches the NumPy planner as a bare
          ``pa.fixed_size_binary(16)`` and lands as BINARY. Frames
          imported over the C data interface or Arrow IPC do get the
          extension type rebuilt from that key, which is why the same
          metadata can route two ways. The other two routes —
          ``uuid.UUID`` cells and ``schema_overrides`` — are free of both
          conditions, and a ``{'kind': 'uuid'}`` entry in
          ``df.attrs['questdb']`` claims an object-dtype column of
          16-byte ``bytes``, the shape
          ``to_pandas(dtype_backend='numpy_nullable')`` returns. On
          pyarrow < 18, where no column can carry the label at all, a
          bare ``pa.fixed_size_binary(16)`` column is rejected on the
          NumPy planner rather than quietly landing as BINARY.
        - **LONG256**: 32-byte binary columns. Bytes are little-endian
          limbs, least-significant limb first, forwarded verbatim. Three
          routes claim the type, and they need different inputs.
          ``schema_overrides={'col': 'long256'}`` needs a fully
          Arrow-backed frame — every column an ArrowDtype, e.g.
          ``df.convert_dtypes(dtype_backend='pyarrow')``.
          ``questdb.column_type=long256`` *field* metadata needs a
          ``pa.Table`` or ``pa.RecordBatch`` handed straight to this
          method: field metadata lives on the field, a pandas
          ``ArrowDtype`` carries a type and no field, so a column that
          passed through pandas has already lost the claim. A
          ``{'kind': 'long256'}`` entry in ``df.attrs['questdb']`` claims
          an object-dtype column of Python ints or of 32-byte ``bytes``
          — the two shapes :meth:`QueryResult.to_pandas` hands a LONG256
          column back in, plain and ``dtype_backend='numpy_nullable'``
          respectively. Ints are encoded as 32 unsigned little-endian
          bytes, so every non-null value must satisfy
          ``0 <= value < 2**256``; ``bytes`` cells go out verbatim and
          must be exactly 32 bytes. LONG256 has no Arrow extension type,
          so a ``pa.fixed_size_binary(32)`` column reaching the NumPy
          planner claims nothing by itself and is rejected rather than
          sent as opaque bytes — see **Binary** below for why that
          planner refuses where the Arrow columnar path accepts.
        - **Binary**: object-dtype columns of ``bytes``, ``bytearray``, or
          C-contiguous one-byte-item ``memoryview`` cells land as BINARY,
          the same value types :func:`Buffer.row <questdb.ingress.Buffer.row>`
          accepts. Arrow ``pa.binary()``, ``pa.large_binary()``, and
          ``pa.fixed_size_binary(n)`` columns also land as BINARY: a
          16- or 32-byte width on its own claims nothing, so an
          unlabeled fixed-size column is opaque bytes rather than a
          UUID or a LONG256.

          The two planners differ here, and deliberately. On the Arrow
          columnar path an unlabeled 16- or 32-byte column lands as
          BINARY, because ``schema_overrides`` is there to say otherwise
          when that is not what you meant. The NumPy planner has no such
          argument, so it cannot tell *I want opaque bytes* from *I meant
          UUID and the claim did not survive pandas*; it refuses both
          widths rather than auto-create a table with the wrong column
          type and say nothing. The refusal does not depend on the
          pyarrow version — which remedies the message can name does,
          since ``pa.uuid()`` needs pyarrow 18, but which columns are
          accepted does not. To send either width as BINARY on that
          planner, pass the values as an object-dtype column of
          ``bytes``. Requires QuestDB 10 or newer.
        - **DATE**: Arrow ``pa.timestamp('ms')`` (tz-aware or naive),
          ``pa.date32()``, and ``pa.date64()`` columns land as DATE. The
          Arrow type is itself the claim, so there is no DATE cell type
          and no ``'date'`` kind for ``schema_overrides``. Such a column
          lands as DATE whether the frame is fully Arrow-backed or mixed
          with NumPy columns. What has no route of its own to DATE is
          the NumPy ``datetime64[ms]`` dtype, which widens to a
          microsecond TIMESTAMP, and its tz-aware ``datetime64[ms, tz]``
          form, which is rejected outright. A millisecond column named
          by ``at=`` becomes the table's designated TIMESTAMP, like any
          other ``at`` column. Round-tripping keeps the type: :meth:`query
          <questdb.QuestDB.query>` returns a DATE column as
          ``pa.timestamp('ms', 'UTC')``, which ``to_arrow()`` and
          ``to_pandas(dtype_backend='pyarrow')`` feed straight back as
          DATE. Plain ``to_pandas()`` gives a NumPy ``datetime64[ms]``
          column, and the ``df.attrs['questdb']`` claim it comes with
          names the column DATE, so this method puts the Arrow type back
          on before writing and that frame lands as DATE too.
          Row-at-a-time, DATE is written with :class:`DateMillis
          <questdb.DateMillis>` through :func:`Buffer.row
          <questdb.ingress.Buffer.row>`.

        Server-side coercion handles cross-type writes (e.g. ``pa.string()``
        UUIDs landing in a UUID column are parsed server-side; narrow ints
        landing in a wider column are widened). Failures surface as
        ``QuestDBError`` from the ``flush()``.

        ``schema_overrides`` reclassifies columns by name, mapping each to
        ``'symbol'``, ``'ipv4'``, ``'char'``, ``'uuid'``, ``'long256'``, or
        ``('geohash', bits)`` (e.g.
        ``{'venue': 'symbol', 'src_ip': 'ipv4'}``). Unknown column names are
        rejected. An override wins over any Arrow field metadata on its
        column. ``'uuid'`` and ``'long256'`` apply to fixed-size and
        variable-length binary columns alike — the route polars frames take,
        since polars has no fixed-size binary dtype — and every non-null
        value must be exactly 16 or 32 bytes respectively. It requires the
        Arrow columnar path (fully Arrow-backed input without
        ``table_name_col``); on input that falls back to the NumPy planner
        it raises :class:`UnsupportedDataFrameShapeError`.

        A pandas frame that came out of :meth:`QueryResult.to_pandas`
        carries its source column types in ``df.attrs['questdb']``, and
        this method reads them back. That is what keeps UUID, LONG256,
        IPV4, CHAR, and GEOHASH columns their own type on a
        read-modify-write round trip: their claim lives in Arrow *field*
        metadata, which a pandas dtype — an Arrow type and no field —
        cannot hold. The metadata recalls what the frame held when it was
        read, so a column since dropped, renamed, or retyped simply loses
        its claim, and ``symbols`` / ``schema_overrides`` outrank it.
        BYTE, SHORT, and INT columns still widen one step on the way back
        in; the Arrow ingest API has no override that pins them.

        The claim is honoured on every shape a ``to_pandas`` backend
        produces, so plain ``to_pandas()``,
        ``dtype_backend='pyarrow'`` and ``dtype_backend='numpy_nullable'``
        round-trip the same way. Besides the Arrow-backed and plain NumPy
        columns, that covers the object columns the other backends leave
        behind: Python ints, which is what a pandas masked column becomes
        and what plain ``to_pandas()`` returns a LONG256 as, and
        ``bytes``, which is how ``dtype_backend='numpy_nullable'``
        returns UUID and LONG256. A frame whose columns were retyped
        past that set — which a custom ``types_mapper`` can do — keeps
        the claim in ``attrs`` and cannot use it: a claimed column that
        arrives as a float or a string dtype lands as that dtype
        implies, with nothing said. An
        object column states no width or range of its own, so a claimed
        value the type cannot hold — an integer past ``2**32-1`` under
        ``ipv4``, a cell that is not exactly 16 or 32 bytes under
        ``uuid`` or ``long256`` — is refused rather than written into a
        column of the claimed type. A GEOHASH value wider than the
        precision it is written at is refused whichever route named the
        type — ``schema_overrides``, a ``df.attrs['questdb']`` claim, or
        ``questdb.column_type=geohash`` field metadata — and on every
        input shape, pandas, polars, ``pa.Table``, ``pa.RecordBatch``
        and a one-shot ``RecordBatchReader`` alike: a GEOHASH column
        keeps only the claimed low bits, and the high bits are the
        coarse position, so a wider value would reach the database as a
        valid geohash for somewhere else entirely. The values are
        checked batch by batch on their way out, so none that does not
        fit reaches the wire.

        A column of nothing but nulls names no type of its own, and
        without a claim it is left out of the write and out of the
        table the write auto-creates. A claim names one, so a claimed
        all-null column is written as the claimed type — which is what
        keeps a batch that happens to hold no value for one column from
        deciding the shape of the table.

        The claim a query result carries reads as an ordinary mapping
        but cannot be edited in place, because every copy of the frame
        shares it; assign a new mapping to ``df.attrs['questdb']`` to
        change it. A plain hand-written ``dict`` is read here just the
        same, in this shape:

        .. code-block:: python

            df.attrs['questdb'] = {
                'version': 1,
                'columns': {
                    'src_ip': {'kind': 'ipv4'},
                    'pos': {'kind': 'geohash', 'precision_bits': 20},
                },
            }

        ``version`` is required and must be ``1``: the claim's
        vocabulary can gain kinds, and a client applying a version it
        does not understand would write the wrong column type, which is
        the outcome the claim exists to prevent. A mapping without it,
        or carrying any other version, is not a claim this client can
        read and every column in it is ignored. ``kind`` is one of
        ``'uuid'``, ``'long256'``, ``'ipv4'``, ``'char'``, or
        ``'geohash'`` — the types whose claim cannot ride on a pandas
        dtype — and ``precision_bits`` accompanies ``'geohash'`` alone.
        Naming a column that is not in the frame is not an error; the
        entry is skipped.

        ``max_rows_per_batch`` sets the pipelining granularity, not a
        safety limit: any batch exceeding the negotiated per-batch byte
        cap is split regardless of it, and a single row is never bounded
        by it. Each batch is one unit of client memory and server-side
        apply. Sliceable Arrow inputs checkpoint about every 100 batches,
        so ``max_rows_per_batch * 100`` rows approximates their periodic
        replay window. The NumPy planner normally commits once after the
        whole frame. Either path may checkpoint earlier when deferred
        capacity fills. Raise ``max_rows_per_batch`` for narrow numeric
        rows; lower it for very wide rows or tight memory. Streaming Arrow
        input (``pa.RecordBatchReader``) is not re-batched — the producer's
        batch size governs its checkpoint window.
        """
        cdef qdb_pystr_buf* b = NULL
        cdef dataframe_plan_t plan = dataframe_plan_blank()
        cdef questdb_db* db = NULL
        cdef bint db_use = False
        cdef direct_conn_source_t src
        db = self._begin_db_use('dataframe')
        db_use = True
        try:
            # Claimed inside the `try`, and after `_begin_db_use`,
            # which raises on a closed handle: an allocation made
            # ahead of that raise has nothing to free it, so a retry
            # loop against a closed handle leaks one per attempt.
            b = qdb_pystr_buf_new()
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
            if b != NULL:
                qdb_pystr_buf_free(b)
            if db_use:
                self._end_db_use()

    def execute(self, str sql, object binds=None):
        """
        Run a statement and discard whatever it returns.

        Executes ``sql`` like :meth:`query`, drains the result to its
        clean end and returns the pooled connection — the pattern DDL
        and DML otherwise need spelled out as
        ``with db.query(sql) as r: r.to_pandas()``. Statement output
        (a ``COPY`` status row, admin-function rows, a stray
        ``SELECT``) is discarded; use :meth:`query` when you want the
        result. The connection's SYMBOL dictionary is left untouched.

        ``binds`` behaves exactly as on :meth:`query`. Returns
        ``None``: the protocol carries no rows-affected count.
        """
        self._begin_db_use('execute')
        try:
            result = self.query(sql, binds, reset_symbol_dict=False)
            try:
                result._drain()
            finally:
                result.close()
        finally:
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

    @property
    def error_events_delivered(self) -> int:
        """
        Total server rejections delivered to the ``error_handler``
        (or to the default logging handler when none was registered).
        """
        cdef questdb_db* db = self._begin_db_use('error_events_delivered')
        try:
            return questdb_db_rejection_events_delivered(db)
        finally:
            self._end_db_use()

    @property
    def error_events_dropped(self) -> int:
        """
        Total server rejections discarded by the handler inbox's
        drop-oldest policy.
        """
        cdef questdb_db* db = self._begin_db_use('error_events_dropped')
        try:
            return questdb_db_rejection_events_dropped(db)
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

        This method is idempotent. When called from inside one of this
        handle's own ``error_handler`` / ``connection_listener``
        callbacks, it does not wait for a concurrent ``close()`` on
        another thread to finish; the in-flight callback completes after
        that close returns.
        """
        cdef questdb_db* db = NULL
        cdef PyThreadState* gs = NULL
        cdef bint closed = False
        with self._state_cond:
            db = self._db
            if db == NULL:
                # A caller dispatching for this handle must not wait here:
                # the in-flight closer joins this very thread.
                if not _on_dispatch_thread_for(
                        self._error_handler, self._connection_listener):
                    while self._closing:
                        if (not self._state_cond.wait(timeout=5.0)
                                and self._closing):
                            warnings.warn(
                                'QuestDB.close() is still waiting for a '
                                'concurrent close() on another thread to '
                                'finish.',
                                UserWarning)
                return
            self._db = NULL
            self._closing = True
        try:
            with self._state_cond:
                while self._active_uses != 0:
                    if (not self._state_cond.wait(timeout=5.0)
                            and self._active_uses != 0):
                        warnings.warn(
                            'QuestDB.close() is waiting for '
                            f'{self._active_uses} outstanding lease(s) to be '
                            'released.',
                            UserWarning)
            _ensure_doesnt_have_gil(&gs)
            # `questdb_db_close` drains both the writer and reader free
            # lists in one shot (see `db.rs::DbInner::Drop`).
            questdb_db_close(db)
            closed = True
        finally:
            _ensure_has_gil(&gs)
            if closed:
                _release_callback_refs(self._cb_refs_key)
                self._cb_refs_key = 0
            with self._state_cond:
                if closed:
                    self._conf_str = None
                else:
                    self._db = db
                self._closing = False
                self._state_cond.notify_all()

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
        _release_callback_refs(self._cb_refs_key)
        self._cb_refs_key = 0


@cython.no_gc_clear
cdef class Sender:
    """
    Ingest data into QuestDB over a single connection.

    This is the connection-level API: one sender drives exactly one
    connection (ILP/HTTP, ILP/TCP, QWP/UDP, or a single QWP/WebSocket
    connection) and carries the point-to-point capabilities the
    deployment-level handle does not: HTTP transactions, UDP datagrams,
    and manual ws progress and buffer control. For pooled ingestion and
    queries, prefer :func:`questdb.connect`.

    See the :ref:`sender` documentation for more information.
    """

    # We need the Buffer held by a Sender can hold a weakref to its Sender.
    # This avoids a circular reference that requires the GC to clean up.
    cdef object __weakref__

    cdef line_sender_protocol _c_protocol
    cdef line_sender_opts* _opts
    cdef line_sender* _impl
    cdef Buffer _buffer
    cdef object _error_handler
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
    cdef size_t _cb_refs_key

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
            object error_handler,
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

        if error_handler is not None and not callable(error_handler):
            raise TypeError(
                '"error_handler" must be callable or None, '
                f'not {_fqn(type(error_handler))}')
        if error_handler is not None and not _is_qwp_ws_protocol(self._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'error_handler is only supported for QWP/WebSocket senders.')
        if _is_qwp_ws_protocol(self._c_protocol):
            if error_handler is None:
                error_handler = _default_error_handler
            self._error_handler = error_handler
            if not line_sender_opts_qwpws_error_handler(
                    self._opts,
                    _sender_error_trampoline,
                    <void*>self._error_handler,
                    &err):
                self._error_handler = None
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
                    _connection_event_trampoline,
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
            try:
                c_tls_ca = TlsCa.parse(tls_ca).c_value
            except ValueError:
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    f'"tls_ca" has invalid value: {tls_ca!r}')
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
        self._error_handler = None
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
            object tls_ca=None,  # TLS default: TlsCa.WebpkiAndOsRoots
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
            object error_handler=None,
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
            if protocol is None:
                raise QuestDBError(
                    QuestDBErrorCode.ConfigError,
                    '"protocol" is required and cannot be None.')
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
                error_handler,
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
            object tls_ca=None,  # TLS default: TlsCa.WebpkiAndOsRoots
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
            object error_handler=None,
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
                error_handler,
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
            object tls_ca=None,  # TLS default: TlsCa.WebpkiAndOsRoots
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
            object error_handler=None,
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
            error_handler=error_handler,
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

        self._cb_refs_key = _retain_callback_refs(
            self, self._error_handler, self._connection_listener)

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
            symbols: Optional[Dict[str, Optional[str]]]=None,
            columns: Optional[Dict[
                str,
                Union[None, bool, int, float, str, TimestampMicros,
                      TimestampNanos, datetime.datetime, numpy.ndarray,
                      Decimal, uuid.UUID, ipaddress.IPv4Address, bytes,
                      bytearray, memoryview, Char, DateMillis, Long256,
                      Geohash]]]=None,
            at: Union[TimestampNanos, datetime.datetime, ServerTimestampType]):
        """
        Write a row to the internal buffer.

        This may be sent automatically depending on the ``auto_flush`` setting
        in the constructor.

        See :func:`Buffer.row <questdb.ingress.Buffer.row>` for supported
        column types, protocol restrictions, server requirements, and
        ``NULL``-sentinel behavior.
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

        ``table_name_col`` follows the same split. The row-serializing
        protocols accept it, because they can change table between rows.
        QWP/WebSocket writes one table per call and rejects it with
        :class:`UnsupportedDataFrameShapeError
        <questdb.UnsupportedDataFrameShapeError>`; split the frame and make
        one call per table, or send those rows with :meth:`row`.

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
        # The QWP/WebSocket branch below goes straight to its own
        # connection and never reaches `_dataframe`, where the same
        # check stands for every other route. Without it here, a frame
        # re-entered from a half-written row publishes ahead of the row
        # it interrupted and the order of the two silently inverts.
        if self._buffer is not None:
            self._buffer._check_not_in_row('dataframe')
        if _is_qwp_ws_protocol(self._c_protocol):
            if self._qwp_ws_opts == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    "dataframe() can't be called: Sender is closed.")
            src.db = NULL
            src.opts = self._qwp_ws_opts
            ws_b = qdb_pystr_buf_new()
            ws_plan = dataframe_plan_blank()
            # Counted as a row in progress for as long as the run
            # lasts. `src.opts` is the sender's own options struct, and
            # the plan build below runs caller Python -- reading
            # `attrs`, sniffing object cells, pulling an Arrow stream --
            # any of which can call `close()`, which frees that struct
            # while `_direct_conn_open` is still to read it. The
            # row-serializing route gets this from `_dataframe`; this
            # one has to say it itself.
            if self._buffer is not None:
                self._buffer._row_depth += 1
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
                if self._buffer is not None:
                    self._buffer._row_depth -= 1
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
            self._buffer,
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
        :attr:`QuestDBError.sender_error`. Server diagnostics are also
        available through :func:`Sender.poll_error`.

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

        self._check_not_in_own_callback('flush')

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
        # Refused before the flush is attempted, and before the GIL is
        # released: a flush of a half-written row cannot succeed, and
        # the failure path clears the internal buffer, which would take
        # every finished row already in it with the part-written one.
        (buffer if buffer is not None else self._buffer)._check_not_in_row(
            'flush')
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

    cdef inline void_int _check_not_in_own_callback(self, str method) except -1:
        # The QWP/WebSocket error handler runs synchronously on the flushing
        # thread while the native sender is borrowed; reentering it from the
        # handler would alias or free the live sender and abort the process.
        if _on_dispatch_thread_for(
                self._error_handler, self._connection_listener):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'{method}() cannot be called from within this sender\'s own '
                'error_handler or connection_listener callback.')

    cdef inline void_int _check_qwp_ws(self, str method) except -1:
        if self._impl == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'{method}() can\'t be called: Sender is closed.')
        if not _is_qwp_ws_protocol(self._c_protocol):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'{method}() is only supported for QWP/WebSocket senders.')
        self._check_not_in_own_callback(method)

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
        (buffer if buffer is not None else self._buffer)._check_not_in_row(
            'flush_and_get_fsn')

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
        (buffer if buffer is not None else self._buffer)._check_not_in_row(
            'flush_and_keep_and_get_fsn')

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

    def await_acked_fsn(self, fsn, timeout_millis=0):
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
            if line_sender_error_get_code(err) != \
                    line_sender_error_failover_retry:
                raise c_err_to_py(err)
            line_sender_error_free(err)
            err = NULL

        # Re-read the watermark: the wait either drained ``fsn`` or expired
        # with no progress; the current ack level is the answer.
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

    def poll_error(self):
        """
        Poll the next structured QWP/WebSocket diagnostic.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_qwpws_error* qwp_err = NULL
        cdef line_sender_qwpws_error_view view

        self._check_qwp_ws('poll_error')
        if not line_sender_qwpws_poll_error(self._impl, &qwp_err, &err):
            raise c_err_to_py(err)
        if qwp_err == NULL:
            return None
        try:
            view = line_sender_qwpws_error_get_view(qwp_err)
            return _sender_error_from_raw(c_sender_error_view_to_raw(view))
        finally:
            line_sender_qwpws_error_free(qwp_err)

    def error_events_dropped(self):
        """
        Number of QWP/WebSocket diagnostics dropped from the bounded ring.
        """
        cdef line_sender_error* err = NULL
        cdef uint64_t dropped = 0

        self._check_qwp_ws('error_events_dropped')
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
        _release_callback_refs(self._cb_refs_key)
        self._cb_refs_key = 0
        self._buffer = None
        self._error_handler = None
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
        self._check_not_in_own_callback('close')
        # Refused outright while a row is part-way through, not merely
        # by way of the flush below: `_close()` runs from the `finally`
        # whatever the flush does, so a refusal there would still close
        # the sender and take the already-buffered rows with it. With
        # `flush=False` there is no flush to refuse at all.
        if self._buffer is not None:
            self._buffer._check_not_in_row('close')
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


@cython.no_gc_clear
cdef class PooledSender:
    """
    A row-building sender borrowed from a :class:`QuestDB` pool.

    Obtain a lease with :meth:`QuestDB.sender`; ``close()`` returns the
    native sender to the pool. Rows publish into an ordered,
    store-and-forward-covered QWP stream over one pooled connection.

    Rows go through ``row()``, ``dataframe()``, ``flush()``, ``wait()``
    and ``close()``; ``len(sender)`` is the number of buffered rows.
    Frame-level delivery tracking is available through
    :meth:`flush_and_get_fsn`, :meth:`flush_and_keep_and_get_fsn`,
    :meth:`published_fsn`, :meth:`acked_fsn` and
    :meth:`await_acked_fsn`, and per-lease server diagnostics through
    :meth:`poll_error` and :meth:`error_events_dropped`. Prefer
    :meth:`QuestDB.dataframe` for bulk loads; the lease's own
    ``dataframe()`` is a convenience that routes to the same direct
    columnar path. Row auto-flush is enabled by default at 1,000 rows, 100
    milliseconds, or a cap-derived byte threshold, and can be configured
    through the parent handle's connection settings.
    """
    cdef qwp_sender* _qwp
    cdef questdb_db* _db
    cdef QuestDB _handle
    cdef Buffer _buffer
    cdef object _lock
    cdef int64_t _batch_started_ms

    def __cinit__(self):
        self._qwp = NULL
        self._db = NULL
        self._handle = None
        self._buffer = None
        self._lock = threading.RLock()
        self._batch_started_ms = 0

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
        self._batch_started_ms = 0

    cdef void_int _check_open(self, str method) except -1:
        if self._qwp == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f"{method}() can't be called: Sender is closed.")

    cdef void_int _check_not_in_row(self, str method) except -1:
        """
        Refuse a call that arrives while `row()` is part-way through
        assembling a row on this lease's buffer.

        A column value whose conversion runs Python code re-enters here on
        the same thread, and `self._lock` is re-entrant, so the lock alone
        lets it through. Flushing a half-written row cannot succeed, and
        returning the lease to the pool would free the buffer the row is
        still writing into.
        """
        if self._buffer is not None and self._buffer._row_depth != 0:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f"{method}() can't be called while a row is being "
                "written. `row()` is part-way through assembling a row "
                "and something it called has come back into this sender, "
                "most likely a column value whose conversion runs Python "
                "code.")

    cdef bint _should_auto_flush_locked(self) except -1:
        cdef auto_flush_mode_t* mode = &self._handle._auto_flush_mode
        cdef size_t row_count
        cdef size_t buffer_size
        cdef size_t hard_cap = 0
        cdef size_t soft_cap = 0
        cdef size_t byte_limit = 0
        cdef cbool server_cap_known = False
        cdef line_sender_error* err = NULL

        if not mode.enabled:
            return False

        row_count = line_sender_buffer_row_count(self._buffer._impl)
        if mode.row_count != -1 and row_count >= <size_t>mode.row_count:
            return True

        if mode.byte_count != -1:
            if not qwp_sender_effective_frame_cap(
                    self._qwp, &hard_cap, &server_cap_known, &err):
                raise c_err_to_py(err)

            # floor(hard_cap * 0.9) without overflowing size_t.
            soft_cap = ((hard_cap // 10) * 9
                        + ((hard_cap % 10) * 9) // 10)
            if self._handle._auto_flush_bytes_dynamic:
                byte_limit = soft_cap
                if not server_cap_known and byte_limit > 8 * 1024 * 1024:
                    byte_limit = 8 * 1024 * 1024
            else:
                byte_limit = <size_t>mode.byte_count
                if soft_cap < byte_limit:
                    byte_limit = soft_cap

            buffer_size = line_sender_buffer_size(self._buffer._impl)
            if buffer_size >= byte_limit:
                return True

        if mode.interval != -1 and (
                (line_sender_now_micros() // 1000)
                - self._batch_started_ms) >= mode.interval:
            return True

        return False

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
        self._check_not_in_row('flush')
        if line_sender_buffer_row_count(self._buffer._impl) == 0:
            self._batch_started_ms = 0
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
        self._batch_started_ms = 0

    cdef void _release_locked(self) except *:
        cdef qwp_sender* sender = self._qwp
        cdef questdb_db* db = self._db
        cdef QuestDB handle = self._handle
        cdef PyThreadState* gs = NULL
        if sender == NULL:
            return
        self._check_not_in_row('close')
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
                      Decimal, uuid.UUID, ipaddress.IPv4Address, bytes,
                      bytearray, memoryview, Char, DateMillis, Long256,
                      Geohash]]] = None,
            at: Union[ServerTimestampType, TimestampNanos,
                      datetime.datetime]):
        """
        Append one row to this sender's QWP buffer.

        When pooled auto-flush is enabled on the :class:`QuestDB` handle,
        completing a row that breaches a configured row, byte, or interval
        threshold publishes the buffer without waiting for an acknowledgement.
        Any error raised by that publish propagates from this method.
        If a single row cannot fit in a QWP frame, that row is removed before
        :class:`QuestDBErrorCode.BatchTooLarge` is raised. If a multi-row
        batch exceeds the exact encoded limit despite the byte-size estimate,
        the complete batch remains buffered.

        See :func:`Buffer.row <questdb.ingress.Buffer.row>` for supported
        column types, protocol restrictions, server requirements, and
        ``NULL``-sentinel behavior.
        """
        cdef bint starts_batch
        cdef bint auto_flush
        cdef Buffer buf
        if at is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidTimestamp,
                "`at` must be of type TimestampNanos, datetime, or "
                "ServerTimestamp")
        with self._lock:
            self._check_open('row')
            # The lease holds the only reference to the buffer, and a
            # column value whose conversion runs Python code can reach
            # back in here. A strong local reference keeps the buffer
            # alive for the whole row, including its cleanup.
            buf = self._buffer
            starts_batch = (
                line_sender_buffer_row_count(buf._impl) == 0)
            buf._row(
                False, table_name, symbols, columns, at, True)
            if starts_batch:
                self._batch_started_ms = line_sender_now_micros() // 1000
            try:
                auto_flush = self._should_auto_flush_locked()
            except:
                buf._clear_marker()
                raise
            if not auto_flush:
                buf._clear_marker()
                return self
            try:
                self._flush_locked(False)
            except QuestDBError as exc:
                if (starts_batch and
                        exc.code == QuestDBErrorCode.BatchTooLarge):
                    buf._rewind_to_marker()
                    self._batch_started_ms = 0
                else:
                    buf._clear_marker()
                raise
            except:
                buf._clear_marker()
                raise
            buf._clear_marker()
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

        Prefer :meth:`QuestDB.dataframe` — this convenience forwards to the
        same path and is **not** part of this sender's row stream: the frame
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
            self._check_not_in_row('dataframe')
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
        Pass ``wait=True`` to wait for the server's OK acknowledgement of
        everything published through this lease. The wait is a pure ack
        barrier: only a terminal connection failure raises. Server
        rejections are delivered to the pool's ``error_handler``
        (default: the ``questdb`` logger) instead; retriable ones are
        replayed by the store-and-forward queue.
        """
        with self._lock:
            self._flush_locked(wait)
        return self

    def wait(self, timeout_millis=0):
        """
        Wait for everything published through this lease to receive an OK
        ack; returns immediately if this lease published nothing.

        The wait is a pure ack barrier: only a terminal connection failure
        raises. Server rejections are delivered to the pool's
        ``error_handler`` (default: the ``questdb`` logger) instead.

        ``timeout_millis`` is a no-progress timeout; ``0`` waits indefinitely.
        """
        if not isinstance(timeout_millis, int) or isinstance(timeout_millis, bool):
            raise TypeError('"timeout_millis" must be a non-negative int.')
        if timeout_millis < 0:
            raise ValueError('timeout_millis must be non-negative.')
        with self._lock:
            self._check_open('wait')
            self._wait_locked(<uint64_t>timeout_millis)
        return self

    def flush_and_get_fsn(self):
        """
        Publish and clear buffered rows, returning the frame sequence
        number (FSN) of the published frame, or ``None`` if the buffer was
        empty.

        FSNs are watermarks of the lease's pooled connection: use them for
        progress tracking while this lease is held (see
        :meth:`await_acked_fsn`); they are not portable receipts across
        leases, which may borrow different connections. Configure
        ``auto_flush=off`` when this call must publish and identify one whole
        application batch; auto-flush may already have published and cleared
        some or all buffered rows.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef line_sender_qwpws_fsn fsn
        cdef bint ok = False
        with self._lock:
            self._check_open('flush_and_get_fsn')
            self._check_not_in_row('flush_and_get_fsn')
            _ensure_doesnt_have_gil(&gs)
            ok = qwp_sender_flush_buffer_and_get_fsn(
                self._qwp, self._buffer._impl, &fsn, &err)
            _ensure_has_gil(&gs)
            if not ok:
                raise c_err_to_py(err)
            qdb_pystr_buf_clear(self._buffer._b)
            self._batch_started_ms = 0
        return fsn.value if fsn.has_value else None

    def flush_and_keep_and_get_fsn(self):
        """
        Publish buffered rows without clearing the buffer, returning the
        published frame's FSN, or ``None`` if the buffer was empty.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef line_sender_qwpws_fsn fsn
        cdef bint ok = False
        with self._lock:
            self._check_open('flush_and_keep_and_get_fsn')
            self._check_not_in_row('flush_and_keep_and_get_fsn')
            _ensure_doesnt_have_gil(&gs)
            ok = qwp_sender_flush_buffer_and_keep_and_get_fsn(
                self._qwp, self._buffer._impl, &fsn, &err)
            _ensure_has_gil(&gs)
            if not ok:
                raise c_err_to_py(err)
            if fsn.has_value:
                self._batch_started_ms = line_sender_now_micros() // 1000
        return fsn.value if fsn.has_value else None

    def poll_error(self):
        """
        Poll the next server-rejection diagnostic recorded on the lease's
        connection since this lease was borrowed, as a
        :class:`SenderError`, or ``None`` when none is pending.

        The pool's ``error_handler`` independently receives every rejection
        the moment it is recorded; polling here is a per-lease pull
        alternative for code that wants diagnostics inline.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_qwpws_error* c_error = NULL
        cdef line_sender_qwpws_error_view view
        with self._lock:
            self._check_open('poll_error')
            if not qwp_sender_poll_error(self._qwp, &c_error, &err):
                raise c_err_to_py(err)
            if c_error == NULL:
                return None
            try:
                view = line_sender_qwpws_error_get_view(c_error)
                return _sender_error_from_raw(
                    c_sender_error_view_to_raw(view))
            finally:
                line_sender_qwpws_error_free(c_error)

    def error_events_dropped(self):
        """
        Diagnostics dropped from the lease's connection ring
        (``error_inbox_capacity``).
        """
        cdef line_sender_error* err = NULL
        cdef uint64_t dropped = 0
        with self._lock:
            self._check_open('error_events_dropped')
            if not qwp_sender_error_events_dropped(
                    self._qwp, &dropped, &err):
                raise c_err_to_py(err)
        return dropped

    def published_fsn(self):
        """
        Highest FSN published locally on the lease's pooled connection, or
        ``None`` if nothing has been published on it yet.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_qwpws_fsn fsn
        with self._lock:
            self._check_open('published_fsn')
            if not qwp_sender_published_fsn(self._qwp, &fsn, &err):
                raise c_err_to_py(err)
        return fsn.value if fsn.has_value else None

    def acked_fsn(self):
        """
        Highest FSN completed on the lease's pooled connection by ACK or
        drop-and-continue rejection, or ``None`` if no frame has completed
        yet. A completed frame is not necessarily applied: a rejected frame
        the queue drops to make progress also advances this watermark.
        """
        cdef line_sender_error* err = NULL
        cdef line_sender_qwpws_fsn fsn
        with self._lock:
            self._check_open('acked_fsn')
            if not qwp_sender_acked_fsn(self._qwp, &fsn, &err):
                raise c_err_to_py(err)
        return fsn.value if fsn.has_value else None

    def await_acked_fsn(self, fsn, timeout_millis=0):
        """
        Wait until the acknowledgement watermark reaches ``fsn`` (as
        returned by :meth:`flush_and_get_fsn` on this lease).

        Returns ``True`` once ``fsn`` is acknowledged, or ``False`` if the
        no-progress timeout elapsed before ``fsn`` was acknowledged (``0``
        waits indefinitely). Only a terminal connection failure raises.
        """
        cdef line_sender_error* err = NULL
        cdef PyThreadState* gs = NULL
        cdef uint64_t c_fsn
        cdef uint64_t c_timeout_millis
        cdef line_sender_qwpws_fsn acked
        cdef bint ok = False
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
        with self._lock:
            self._check_open('await_acked_fsn')
            if not qwp_sender_acked_fsn(self._qwp, &acked, &err):
                raise c_err_to_py(err)
            if acked.has_value and acked.value >= c_fsn:
                return True
            # The barrier drains every frame published so far (a superset
            # of ``fsn``); a no-progress expiry surfaces as FailoverRetry.
            _ensure_doesnt_have_gil(&gs)
            ok = qwp_sender_wait(
                self._qwp,
                qwpws_ack_level.qwpws_ack_level_ok,
                c_timeout_millis,
                &err)
            _ensure_has_gil(&gs)
            if not ok:
                if line_sender_error_get_code(err) != \
                        line_sender_error_failover_retry:
                    raise c_err_to_py(err)
                line_sender_error_free(err)
                err = NULL
            # Re-read the watermark: the wait either drained ``fsn`` or
            # expired with no progress; the current ack level is the answer.
            if not qwp_sender_acked_fsn(self._qwp, &acked, &err):
                raise c_err_to_py(err)
            return bool(acked.has_value and acked.value >= c_fsn)

    def close(self, flush: bool=True, wait: bool=False):
        """
        Return this sender to its pool. Idempotent.

        Pending rows are published by default; delivery is owned by the
        store-and-forward queue, which keeps delivering after the sender
        is returned. ``wait=True`` additionally waits for an OK ack before
        returning the sender to the pool; without it, a later server
        rejection of this lease's rows is reported through the pool's
        ``error_handler`` (default: the ``questdb`` logger).
        """
        with self._lock:
            self._check_not_in_row('close')
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


@cython.no_gc_clear
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
    result is still open raises ``QuestDBError``. Closing an undrained
    result terminates the lease; call ``result.cancel()`` before
    ``result.close()`` to preserve it.

    Because every query shares one connection, passing
    ``reset_symbol_dict=False`` to follow-up queries keeps the
    connection's SYMBOL dictionary warm across them instead of
    resetting it per query.

    The lease counts as an active use of the :class:`QuestDB` handle
    (``QuestDB.close()`` waits for it) and has thread affinity: use one
    lease per thread, on the thread that created it. A
    :class:`QueryResult` returned by the lease may be handed to a worker
    under that result's thread hand-off rules; wait for it to finish
    before using the lease again.
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
                if last._is_live():
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
                self._reader, sql, binds, reset_symbol_dict, False)
            self._last_cursor = cursor_handle
        return QueryResult(cursor_handle)

    def execute(self, str sql, object binds=None):
        """
        Run a statement on the lease's connection and discard whatever
        it returns.

        Mirrors :meth:`QuestDB.execute`. The result is drained to its
        clean end, so the lease stays usable for the next call, and the
        connection's SYMBOL dictionary is left untouched — an
        interleaved statement does not invalidate a warm dictionary
        built with ``reset_symbol_dict=False``.
        """
        with self._lock:
            self._check_open('execute')
        result = self.query(sql, binds, reset_symbol_dict=False)
        try:
            result._drain()
        finally:
            result.close()

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
