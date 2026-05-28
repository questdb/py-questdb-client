# Egress (QWP/WebSocket reader) Cython glue.
#
# Bridges the Rust `line_reader_*` FFI (gated behind `sync-reader-ws`
# + `arrow` features in c-questdb-client) onto a Python `QueryResult`
# that exposes `__arrow_c_stream__()` via a pyarrow.RecordBatchReader.
# pandas 3.0 / pyarrow / polars / duckdb consume the dunder directly.


cdef inline object _reader_err_code_to_py(line_reader_error_code code):
    if code == line_reader_error_could_not_resolve_addr:
        return IngressErrorCode.CouldNotResolveAddr
    if code == line_reader_error_config_error:
        return IngressErrorCode.ConfigError
    if code == line_reader_error_invalid_api_call:
        return IngressErrorCode.InvalidApiCall
    if code == line_reader_error_socket_error:
        return IngressErrorCode.SocketError
    if code == line_reader_error_tls_error:
        return IngressErrorCode.TlsError
    if code == line_reader_error_auth_error:
        return IngressErrorCode.AuthError
    if code == line_reader_error_invalid_utf8:
        return IngressErrorCode.InvalidUtf8
    # Map every other reader-specific code to ServerFlushError as a
    # broad bucket. Refine later if users need finer-grained handling.
    return IngressErrorCode.ServerFlushError


cdef inline object _reader_err_to_py(line_reader_error* err):
    """Construct an ``IngressError`` from a ``line_reader_error*`` and free it."""
    cdef line_reader_error_code code = line_reader_error_get_code(err)
    cdef size_t c_len = 0
    cdef const char* c_msg = line_reader_error_msg(err, &c_len)
    cdef object py_code
    cdef object py_msg
    try:
        py_code = _reader_err_code_to_py(code)
        py_msg = PyUnicode_FromStringAndSize(c_msg, <Py_ssize_t>c_len)
        return IngressError(py_code, py_msg)
    finally:
        line_reader_error_free(err)


cdef class _ReaderHandle:
    """Owns a ``line_reader*``. Closed on dealloc."""
    cdef line_reader* _reader

    def __cinit__(self):
        self._reader = NULL

    cdef _attach(self, line_reader* reader):
        self._reader = reader

    cdef void _close(self) noexcept:
        if self._reader != NULL:
            line_reader_close(self._reader)
            self._reader = NULL

    def __dealloc__(self):
        self._close()


cdef class _CursorHandle:
    """Owns a ``line_reader_cursor*`` + back-ref to its reader. Freed on dealloc."""
    cdef line_reader_cursor* _cursor
    cdef _ReaderHandle _reader_ref

    def __cinit__(self):
        self._cursor = NULL
        self._reader_ref = None

    cdef _attach(self, line_reader_cursor* cursor, _ReaderHandle reader_ref):
        self._cursor = cursor
        self._reader_ref = reader_ref

    cdef void _free(self) noexcept:
        if self._cursor != NULL:
            line_reader_cursor_free(self._cursor)
            self._cursor = NULL

    def __dealloc__(self):
        self._free()


cdef object _fetch_one_batch(_CursorHandle handle, object pa_module):
    """Pull one batch via line_reader_cursor_next_arrow_batch.

    Returns:
      - None on clean end-of-stream.
      - A pyarrow.RecordBatch on success.
    Raises IngressError on FFI error.
    """
    cdef ArrowArray array
    cdef ArrowSchema schema
    cdef line_reader_error* err = NULL
    cdef line_reader_arrow_batch_result result
    cdef line_reader_cursor* cursor = handle._cursor

    if cursor == NULL:
        raise IngressError(
            IngressErrorCode.InvalidApiCall,
            'cursor is closed')

    with nogil:
        result = line_reader_cursor_next_arrow_batch(
            cursor, &array, &schema, &err)

    if result == line_reader_arrow_batch_ok:
        # Hand ownership of the array + schema buffers to pyarrow.
        # _import_from_c moves the structs and nulls their release
        # callbacks; pyarrow's RecordBatch owns the buffers from here.
        return pa_module.RecordBatch._import_from_c(
            <uintptr_t>&array, <uintptr_t>&schema)

    if result == line_reader_arrow_batch_end:
        return None

    # Error path.
    if err == NULL:
        raise IngressError(
            IngressErrorCode.ServerFlushError,
            'line_reader_cursor_next_arrow_batch returned error '
            'without setting err_out')
    raise _reader_err_to_py(err)


def _build_record_batch_reader(_CursorHandle cursor_handle):
    """Construct a pyarrow.RecordBatchReader over the cursor.

    Peeks the first batch to capture the stream schema, then yields
    the remaining batches lazily. The cursor is explicitly freed when
    the underlying generator completes (exhaustion, exception, or
    close), so the owning reader can be closed without leaking a live
    cursor.
    """
    import pyarrow as pa

    first = _fetch_one_batch(cursor_handle, pa)
    if first is None:
        # Empty result: no schema to anchor a RecordBatchReader.
        cursor_handle._free()
        empty = pa.table({})
        return empty.to_reader()

    schema = first.schema

    def _gen():
        try:
            yield first
            while True:
                nxt = _fetch_one_batch(cursor_handle, pa)
                if nxt is None:
                    return
                yield nxt
        finally:
            cursor_handle._free()

    return pa.RecordBatchReader.from_batches(schema, _gen())


cdef object _open_reader_from_conf(str conf_str):
    """Open a line_reader from a `ws::`-prefixed conf-string."""
    cdef bytes conf_bytes = conf_str.encode('utf-8')
    cdef line_sender_error* utf8_err = NULL
    cdef line_sender_utf8 conf_utf8
    cdef line_reader_error* err = NULL
    cdef line_reader* reader

    if not line_sender_utf8_init(
            &conf_utf8,
            <size_t>len(conf_bytes),
            <const char*><char*>conf_bytes,
            &utf8_err):
        raise c_err_to_py(utf8_err)

    with nogil:
        reader = line_reader_from_conf(conf_utf8, &err)

    if reader == NULL:
        if err == NULL:
            raise IngressError(
                IngressErrorCode.ConfigError,
                'line_reader_from_conf returned NULL without setting err')
        raise _reader_err_to_py(err)

    cdef _ReaderHandle handle = _ReaderHandle()
    handle._attach(reader)
    return handle


cdef object _execute_query(_ReaderHandle reader_handle, str sql):
    """Execute a SQL query and return a _CursorHandle."""
    cdef bytes sql_bytes = sql.encode('utf-8')
    cdef line_sender_error* utf8_err = NULL
    cdef line_sender_utf8 sql_utf8
    cdef line_reader_error* err = NULL
    cdef line_reader_cursor* cursor

    if reader_handle._reader == NULL:
        raise IngressError(
            IngressErrorCode.InvalidApiCall,
            'reader is closed')

    if not line_sender_utf8_init(
            &sql_utf8,
            <size_t>len(sql_bytes),
            <const char*><char*>sql_bytes,
            &utf8_err):
        raise c_err_to_py(utf8_err)

    with nogil:
        cursor = line_reader_execute(reader_handle._reader, sql_utf8, &err)

    if cursor == NULL:
        if err == NULL:
            raise IngressError(
                IngressErrorCode.ServerFlushError,
                'line_reader_execute returned NULL without setting err')
        raise _reader_err_to_py(err)

    cdef _CursorHandle handle = _CursorHandle()
    handle._attach(cursor, reader_handle)
    return handle


def _derive_reader_conf(str ingress_conf):
    """Convert an ingress conf-string (`qwpws::...` / `qwpwss::...`) into
    an egress reader conf-string (`ws::...` / `wss::...`).

    Only the service prefix changes; all key=value parameters are
    forwarded verbatim. Users with mixed endpoints can pass an
    explicit reader conf to ``Client.from_conf`` instead.
    """
    if ingress_conf.startswith('qwpws::'):
        return 'ws::' + ingress_conf[len('qwpws::'):]
    if ingress_conf.startswith('qwpwss::'):
        return 'wss::' + ingress_conf[len('qwpwss::'):]
    raise IngressError(
        IngressErrorCode.ConfigError,
        'Client.query requires a qwpws:: or qwpwss:: client conf-string; '
        f'got {ingress_conf!r}')


cdef object _ensure_pyarrow():
    try:
        import pyarrow
    except ImportError as e:
        raise IngressError(
            IngressErrorCode.InvalidApiCall,
            'pyarrow is required for Client.query(); install pyarrow >= 14')
    return pyarrow


class QueryResult:
    """Result of ``Client.query(sql)``. Produces pandas / pyarrow / any
    `__arrow_c_stream__` consumer.

    Single-use: each materialisation method (``to_pandas``, ``to_arrow``,
    ``iter_arrow``, ``iter_pandas``, ``__arrow_c_stream__``) consumes
    the underlying cursor. Calling more than one of them, or any of
    them twice, raises ``IngressError``.
    """

    def __init__(self, _CursorHandle cursor_handle):
        self._cursor_handle = cursor_handle
        self._consumed = False
        self._reader = None  # lazy-built pa.RecordBatchReader

    def _take_reader(self):
        if self._consumed:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'QueryResult already consumed')
        self._consumed = True
        if self._reader is None:
            self._reader = _build_record_batch_reader(self._cursor_handle)
        return self._reader

    def __arrow_c_stream__(self, requested_schema=None):
        reader = self._take_reader()
        return reader.__arrow_c_stream__(requested_schema=requested_schema)

    def to_arrow(self):
        """Read the full result into a ``pyarrow.Table``."""
        reader = self._take_reader()
        return reader.read_all()

    def to_pandas(self, *, dtype_backend=None, types_mapper=None):
        """Read the full result into a ``pandas.DataFrame``.

        ``dtype_backend`` / ``types_mapper`` follow the pandas core
        convention (matching ``pd.read_sql`` / ``pd.read_parquet``).
        Mutually exclusive; passing both raises ``ValueError``.
        """
        if dtype_backend is not None and types_mapper is not None:
            raise ValueError(
                'pass at most one of dtype_backend, types_mapper')
        table = self.to_arrow()
        kwargs = {}
        if types_mapper is not None:
            kwargs['types_mapper'] = types_mapper
        if dtype_backend is not None:
            if dtype_backend == 'pyarrow':
                import pandas as pd
                kwargs['types_mapper'] = pd.ArrowDtype
            elif dtype_backend == 'numpy_nullable':
                # pandas .to_pandas() doesn't accept dtype_backend; the
                # closest knob is types_mapper to the masked variants.
                # Punt: surface the limitation as a clear error.
                raise NotImplementedError(
                    'dtype_backend="numpy_nullable" is not yet '
                    'implemented for Client.query.to_pandas()')
            else:
                raise ValueError(
                    f'dtype_backend must be "pyarrow" or '
                    f'"numpy_nullable", got {dtype_backend!r}')
        return table.to_pandas(**kwargs)

    def iter_arrow(self):
        """Iterate over result batches as ``pyarrow.RecordBatch``."""
        reader = self._take_reader()
        for batch in reader:
            yield batch

    def iter_pandas(self, **to_pandas_kwargs):
        """Iterate over result batches as ``pandas.DataFrame``."""
        for batch in self.iter_arrow():
            yield batch.to_pandas(**to_pandas_kwargs)

    def cancel(self):
        """Cancel the underlying cursor. Idempotent."""
        cdef _CursorHandle handle = self._cursor_handle
        cdef line_reader_error* err = NULL
        cdef bint ok
        if handle is None or handle._cursor == NULL:
            return
        with nogil:
            ok = line_reader_cursor_cancel(handle._cursor, &err)
        if not ok and err != NULL:
            raise _reader_err_to_py(err)

    def close(self):
        """Release the cursor + reader. Idempotent."""
        if self._cursor_handle is not None:
            self._cursor_handle._free()
        self._cursor_handle = None
        self._reader = None
        self._consumed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
