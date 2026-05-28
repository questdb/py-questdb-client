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
    if code == line_reader_error_cancelled:
        return IngressErrorCode.Cancelled
    # Map every other reader-specific code (handshake, role mismatch,
    # protocol, invalid bind, schema drift, no schema, server-side
    # errors, etc.) to ServerFlushError as a broad bucket. Refine
    # later as users surface concrete distinctions.
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
    """Owns a ``line_reader*``.

    On dealloc the reader either returns to its pool or is dropped,
    depending on the ``line_reader``'s own ownership tag (set when it
    was constructed — see ``ReaderOwnership`` in the Rust FFI):

    - Pool-borrowed readers go back to the pool unless
      ``_must_close`` was set, in which case the pool drops them.
    - Standalone readers (from ``line_reader_from_conf``) are always
      dropped.

    The Python side carries only one extra bit of state —
    ``_must_close`` — which it forwards to the FFI via
    ``line_reader_mark_must_close`` before calling close. We never
    hold a raw ``questdb_db*`` pointer here: the line_reader struct
    holds an ``Arc<DbInner>`` internally, so the pool stays alive
    even if the user's ``Client.close()`` ran after ``query()``
    returned but before the reader dealloced.

    ``_must_close`` defaults to ``True``: only the generator's
    clean-drain path (or code that explicitly knows the cursor
    reached terminal) clears it. Any error path or abandon-without-
    consume path forces the reader to drop, since the Rust
    Cursor::Drop closes the transport whenever ``cursor_active`` is
    still set at drop time — recycling such a reader would hand the
    next borrower a broken pipe.
    """
    cdef line_reader* _reader
    cdef bint _must_close

    def __cinit__(self):
        self._reader = NULL
        self._must_close = True

    cdef _attach(self, line_reader* reader):
        self._reader = reader

    cdef void _close(self) noexcept:
        if self._reader == NULL:
            return
        if self._must_close:
            line_reader_mark_must_close(self._reader)
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


cdef object _build_record_batch_reader(_CursorHandle cursor_handle):
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
        # Empty result: cursor already reached terminal cleanly.
        # Safe to return the reader to its pool.
        _mark_reader_drained(cursor_handle)
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
                    # Reached terminal cleanly; reader is reusable.
                    _mark_reader_drained(cursor_handle)
                    return
                yield nxt
        finally:
            cursor_handle._free()

    return pa.RecordBatchReader.from_batches(schema, _gen())


cdef void _mark_reader_drained(_CursorHandle cursor_handle) noexcept:
    """Tell the reader handle it's safe to return to its pool on dealloc.

    The Rust Cursor::Drop closes the underlying transport whenever
    ``cursor_active`` is still set. Only call this once the cursor has
    reached its terminal frame (``_end``) — otherwise the next pool
    borrower would see a broken pipe.
    """
    if cursor_handle is None:
        return
    cdef _ReaderHandle reader = cursor_handle._reader_ref
    if reader is not None:
        reader._must_close = False


cdef _ReaderHandle _borrow_reader_from_pool(questdb_db* db):
    """Borrow a reader from the Rust-side ``questdb_db`` pool.

    Wraps ``questdb_db_borrow_reader`` and packs the result into a
    :class:`_ReaderHandle` that knows it came from this pool, so
    its dealloc returns/drops via the matching FFI.
    """
    cdef line_reader_error* err = NULL
    cdef line_reader* reader = NULL
    with nogil:
        reader = questdb_db_borrow_reader(db, &err)
    if reader == NULL:
        if err == NULL:
            raise IngressError(
                IngressErrorCode.ServerFlushError,
                'questdb_db_borrow_reader returned NULL without setting err')
        raise _reader_err_to_py(err)
    cdef _ReaderHandle handle = _ReaderHandle()
    handle._attach(reader)
    return handle


cdef _CursorHandle _execute_query(_ReaderHandle reader_handle, str sql):
    """Execute a SQL query and return a _CursorHandle."""
    cdef bytes sql_bytes = sql.encode('utf-8')
    cdef line_sender_error* utf8_err = NULL
    cdef line_sender_utf8 sql_utf8
    cdef line_reader_error* err = NULL
    cdef line_reader_cursor* cursor

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


cdef object _ensure_pyarrow():
    try:
        import pyarrow
    except ImportError:
        raise IngressError(
            IngressErrorCode.InvalidApiCall,
            'pyarrow is required for Client.query(); install pyarrow >= 14')
    return pyarrow


_NUMPY_NULLABLE_CACHE = None


cdef object _numpy_nullable_mapping():
    """Return a ``types_mapper`` callable that maps Arrow primitives to
    pandas nullable-extension dtypes (Int64Dtype, Float64Dtype, etc.).

    Mirrors ``pandas.io._util._arrow_dtype_mapping``'s coverage so that
    ``to_pandas(dtype_backend="numpy_nullable")`` here matches what
    ``pd.read_parquet(..., dtype_backend="numpy_nullable")`` produces.
    Non-primitive Arrow types fall through (mapper returns None) and
    pyarrow.Table.to_pandas applies its default conversion.
    """
    global _NUMPY_NULLABLE_CACHE
    if _NUMPY_NULLABLE_CACHE is None:
        import pyarrow as pa
        import pandas as pd
        _NUMPY_NULLABLE_CACHE = {
            pa.int8(): pd.Int8Dtype(),
            pa.int16(): pd.Int16Dtype(),
            pa.int32(): pd.Int32Dtype(),
            pa.int64(): pd.Int64Dtype(),
            pa.uint8(): pd.UInt8Dtype(),
            pa.uint16(): pd.UInt16Dtype(),
            pa.uint32(): pd.UInt32Dtype(),
            pa.uint64(): pd.UInt64Dtype(),
            pa.float32(): pd.Float32Dtype(),
            pa.float64(): pd.Float64Dtype(),
            pa.bool_(): pd.BooleanDtype(),
            pa.string(): pd.StringDtype(),
            pa.large_string(): pd.StringDtype(),
        }.get
    return _NUMPY_NULLABLE_CACHE


def _debug_egress_pool_stats(client):
    """Return ``(in_use, idle)`` from the client's reader pool.

    The Rust pool doesn't track "opened" / "reused" as counters — they
    fall out of ``in_use + idle`` plus the lazy-init pattern (first
    borrow opens a connection; the idle list grows on returns; reuse
    is implicit). Tests assert reuse by checking that ``idle == 1``
    after sequential queries that each borrowed and returned. Returns
    ``None`` if the Client is closed.

    Not part of the public API.
    """
    cdef Client c = client
    cdef questdb_db* db = c._db
    if db == NULL:
        return None
    # FFI exposes the counts via the Rust QuestDb methods; we surface
    # them through the column_sender_chunk debug accessors below.
    return (
        questdb_db_reader_in_use_count(db),
        questdb_db_reader_free_count(db))


class QueryResult:
    """Result of ``Client.query(sql)``.

    Streams query rows as Arrow RecordBatches. The result is **single-use**:
    each materialisation method (``to_pandas``, ``to_arrow``, ``iter_arrow``,
    ``iter_pandas``, or the ``__arrow_c_stream__`` PyCapsule protocol)
    consumes the underlying cursor. Calling any of them twice — or calling
    one after another — raises ``IngressError``.

    Example::

        with client.query('SELECT * FROM trades WHERE ts > $1') as result:
            df = result.to_pandas()

    The class is also a valid PyCapsule producer
    (``pd.DataFrame.from_arrow(result)`` / ``pa.Table.from_arrow(result)``
    / ``pl.DataFrame(result)`` / ``duckdb.from_arrow(result)``).
    """

    def __init__(self, _CursorHandle cursor_handle):
        self._cursor_handle = cursor_handle
        self._consumed = False

    def _take_reader(self):
        if self._consumed:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'QueryResult already consumed')
        self._consumed = True
        return _build_record_batch_reader(self._cursor_handle)

    def __arrow_c_stream__(self, requested_schema=None):
        reader = self._take_reader()
        return reader.__arrow_c_stream__(requested_schema=requested_schema)

    def to_arrow(self):
        """Read the full result into a ``pyarrow.Table``."""
        return self._take_reader().read_all()

    def to_pandas(self, *, dtype_backend=None, types_mapper=None):
        """Read the full result into a ``pandas.DataFrame``.

        ``dtype_backend`` / ``types_mapper`` follow the pandas core
        convention (matching ``pd.read_sql`` / ``pd.read_parquet``).
        Mutually exclusive; passing both raises ``ValueError``.

        ``dtype_backend="pyarrow"`` wraps every column in
        ``pd.ArrowDtype``. ``dtype_backend="numpy_nullable"`` maps
        primitives to pandas nullable extension dtypes
        (``Int64Dtype`` / ``Float64Dtype`` / ``BooleanDtype`` /
        ``StringDtype``); other types fall back to pyarrow's defaults.
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
                kwargs['types_mapper'] = _numpy_nullable_mapping()
            else:
                raise ValueError(
                    f'dtype_backend={dtype_backend!r} is invalid, '
                    'only "numpy_nullable" and "pyarrow" are allowed')
        return table.to_pandas(**kwargs)

    def iter_arrow(self):
        """Iterate result batches as ``pyarrow.RecordBatch``.

        If the iterator is abandoned partway, cleanup runs at the next
        garbage-collection cycle; call :meth:`close` (or use the context-
        manager) for deterministic release.
        """
        reader = self._take_reader()
        for batch in reader:
            yield batch

    def iter_pandas(self, **to_pandas_kwargs):
        """Iterate result batches as ``pandas.DataFrame``.

        Keyword arguments are forwarded to ``pa.RecordBatch.to_pandas``.
        """
        for batch in self.iter_arrow():
            yield batch.to_pandas(**to_pandas_kwargs)

    def cancel(self):
        """Ask the server to stop streaming. Idempotent.

        Distinct from :meth:`close`: ``cancel`` sends a cancellation
        frame to QuestDB so the server can drop in-flight work;
        ``close`` only releases local resources. A subsequent batch
        pull after ``cancel`` typically surfaces
        ``IngressErrorCode.Cancelled``.
        """
        cdef _CursorHandle handle = self._cursor_handle
        cdef line_reader_error* err = NULL
        cdef bint ok
        if handle is None or handle._cursor == NULL:
            return
        with nogil:
            ok = line_reader_cursor_cancel(handle._cursor, &err)
        if not ok:
            if err != NULL:
                raise _reader_err_to_py(err)
            raise IngressError(
                IngressErrorCode.ServerFlushError,
                'line_reader_cursor_cancel returned false '
                'without setting err_out')

    def close(self):
        """Release the cursor + reader. Idempotent.

        Does not send a cancellation frame; use :meth:`cancel` first if
        you need the server to stop work. After ``close``, any
        previously-returned iterator that hasn't been exhausted will
        fail on its next pump with
        ``IngressErrorCode.InvalidApiCall``.
        """
        if self._cursor_handle is not None:
            self._cursor_handle._free()
        self._cursor_handle = None
        self._consumed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
