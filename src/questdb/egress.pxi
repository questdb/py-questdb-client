# Egress (QWP/WebSocket reader) Cython glue.
#
# `QueryResult` exposes the Arrow PyCapsule Interface
# (`__arrow_c_stream__`) directly off the Rust cursor, so polars /
# duckdb / pandas 3.0 / any Arrow-native consumer can read query
# results without pyarrow. `to_arrow`, `to_pandas`, `iter_arrow`,
# `iter_pandas` are convenience wrappers that lazy-import pyarrow.


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


cdef size_t _arrow_metadata_byte_len(const char* md) noexcept:
    cdef int32_t n
    cdef int32_t klen
    cdef int32_t vlen
    cdef size_t pos
    cdef int32_t i
    memcpy(&n, md, sizeof(int32_t))
    pos = sizeof(int32_t)
    for i in range(n):
        memcpy(&klen, md + pos, sizeof(int32_t))
        pos += sizeof(int32_t) + <size_t>klen
        memcpy(&vlen, md + pos, sizeof(int32_t))
        pos += sizeof(int32_t) + <size_t>vlen
    return pos


cdef void _arrow_schema_clone_release(ArrowSchema* schema) noexcept:
    cdef int64_t i
    if schema.format != NULL:
        free(<void*>schema.format)
        schema.format = NULL
    if schema.name != NULL:
        free(<void*>schema.name)
        schema.name = NULL
    if schema.metadata != NULL:
        free(<void*>schema.metadata)
        schema.metadata = NULL
    if schema.children != NULL:
        for i in range(schema.n_children):
            if schema.children[i] != NULL:
                if schema.children[i].release != NULL:
                    schema.children[i].release(schema.children[i])
                free(schema.children[i])
        free(schema.children)
        schema.children = NULL
    if schema.dictionary != NULL:
        if schema.dictionary.release != NULL:
            schema.dictionary.release(schema.dictionary)
        free(schema.dictionary)
        schema.dictionary = NULL
    schema.release = NULL


cdef int _arrow_schema_deep_clone(const ArrowSchema* src, ArrowSchema* dst) noexcept:
    cdef size_t format_len
    cdef size_t name_len
    cdef size_t metadata_len
    cdef int64_t i
    cdef ArrowSchema* child
    memset(dst, 0, sizeof(ArrowSchema))
    dst.flags = src.flags
    dst.n_children = src.n_children
    if src.format != NULL:
        format_len = strlen(src.format)
        dst.format = <const char*>malloc(format_len + 1)
        if dst.format == NULL:
            _arrow_schema_clone_release(dst)
            return -1
        memcpy(<void*>dst.format, src.format, format_len + 1)
    if src.name != NULL:
        name_len = strlen(src.name)
        dst.name = <const char*>malloc(name_len + 1)
        if dst.name == NULL:
            _arrow_schema_clone_release(dst)
            return -1
        memcpy(<void*>dst.name, src.name, name_len + 1)
    if src.metadata != NULL:
        metadata_len = _arrow_metadata_byte_len(src.metadata)
        dst.metadata = <const char*>malloc(metadata_len)
        if dst.metadata == NULL:
            _arrow_schema_clone_release(dst)
            return -1
        memcpy(<void*>dst.metadata, src.metadata, metadata_len)
    if src.n_children > 0:
        dst.children = <ArrowSchema**>calloc(
            <size_t>src.n_children, sizeof(ArrowSchema*))
        if dst.children == NULL:
            _arrow_schema_clone_release(dst)
            return -1
        for i in range(src.n_children):
            child = <ArrowSchema*>malloc(sizeof(ArrowSchema))
            if child == NULL:
                _arrow_schema_clone_release(dst)
                return -1
            dst.children[i] = child
            if _arrow_schema_deep_clone(src.children[i], child) != 0:
                _arrow_schema_clone_release(dst)
                return -1
    if src.dictionary != NULL:
        dst.dictionary = <ArrowSchema*>malloc(sizeof(ArrowSchema))
        if dst.dictionary == NULL:
            _arrow_schema_clone_release(dst)
            return -1
        if _arrow_schema_deep_clone(src.dictionary, dst.dictionary) != 0:
            _arrow_schema_clone_release(dst)
            return -1
    dst.release = _arrow_schema_clone_release
    return 0


cdef class _QueryStreamProducer:
    """Holder for the Rust-cursor-backed ArrowArrayStream.

    The Arrow stream struct itself is owned by the enclosing PyCapsule;
    this object owns just the cached `(schema, array)` and the
    `_CursorHandle` keep-alive. Refcount is bumped on capsule creation
    and dropped by the stream's release callback so the consumer's
    capsule lifetime governs everything downstream.
    """
    cdef _CursorHandle cursor_handle
    cdef ArrowSchema cached_schema
    cdef ArrowArray cached_array
    cdef bint has_cached_schema
    cdef bint has_cached_array
    cdef bint exhausted
    cdef char* last_error

    def __cinit__(self):
        self.cursor_handle = None
        self.has_cached_schema = False
        self.has_cached_array = False
        self.exhausted = False
        self.last_error = NULL
        memset(&self.cached_schema, 0, sizeof(ArrowSchema))
        memset(&self.cached_array, 0, sizeof(ArrowArray))

    cdef void _free_cached(self) noexcept:
        if self.has_cached_schema:
            if self.cached_schema.release != NULL:
                self.cached_schema.release(&self.cached_schema)
            self.has_cached_schema = False
        if self.has_cached_array:
            if self.cached_array.release != NULL:
                self.cached_array.release(&self.cached_array)
            self.has_cached_array = False

    def __dealloc__(self):
        self._free_cached()
        if self.last_error != NULL:
            free(self.last_error)
            self.last_error = NULL


cdef void _qs_set_error(_QueryStreamProducer prod, const char* msg, size_t msg_len) noexcept:
    if prod.last_error != NULL:
        free(prod.last_error)
        prod.last_error = NULL
    prod.last_error = <char*>malloc(msg_len + 1)
    if prod.last_error == NULL:
        return
    memcpy(prod.last_error, msg, msg_len)
    prod.last_error[msg_len] = 0


cdef int _qs_pull(_QueryStreamProducer prod) noexcept:
    cdef line_reader_cursor* cursor
    cdef ArrowArray local_array
    cdef ArrowSchema local_schema
    cdef line_reader_error* err = NULL
    cdef line_reader_arrow_batch_result result
    cdef const char* err_msg = NULL
    cdef size_t err_len = 0
    if prod.exhausted:
        return 0
    if prod.cursor_handle is None or prod.cursor_handle._cursor == NULL:
        _qs_set_error(prod, b'cursor is closed', 16)
        prod.exhausted = True
        return -1
    cursor = prod.cursor_handle._cursor
    memset(&local_array, 0, sizeof(ArrowArray))
    memset(&local_schema, 0, sizeof(ArrowSchema))
    with nogil:
        result = line_reader_cursor_next_arrow_batch(
            cursor, &local_array, &local_schema, &err)
    if result == line_reader_arrow_batch_ok:
        if not prod.has_cached_schema:
            memcpy(&prod.cached_schema, &local_schema, sizeof(ArrowSchema))
            prod.has_cached_schema = True
        else:
            if local_schema.release != NULL:
                local_schema.release(&local_schema)
        memcpy(&prod.cached_array, &local_array, sizeof(ArrowArray))
        prod.has_cached_array = True
        return 0
    if result == line_reader_arrow_batch_end:
        prod.exhausted = True
        if prod.cursor_handle._reader_ref is not None:
            prod.cursor_handle._reader_ref._must_close = False
        return 0
    if err != NULL:
        err_msg = line_reader_error_msg(err, &err_len)
        if err_msg != NULL:
            _qs_set_error(prod, err_msg, err_len)
        else:
            _qs_set_error(prod, b'arrow batch fetch failed', 24)
        line_reader_error_free(err)
    else:
        _qs_set_error(
            prod,
            b'arrow batch fetch error without err_out', 39)
    prod.exhausted = True
    return -1


cdef int _qs_get_schema(ArrowArrayStream* stream, ArrowSchema* out) noexcept with gil:
    cdef _QueryStreamProducer prod
    if stream == NULL or stream.private_data == NULL:
        return 22  # EINVAL
    prod = <_QueryStreamProducer>stream.private_data
    if not prod.has_cached_schema:
        if _qs_pull(prod) != 0:
            return 5  # EIO
    if not prod.has_cached_schema:
        if _qs_install_empty_struct_schema(prod) != 0:
            return 12  # ENOMEM
    if _arrow_schema_deep_clone(&prod.cached_schema, out) != 0:
        _qs_set_error(prod, b'failed to clone ArrowSchema', 27)
        return 12  # ENOMEM
    return 0


cdef int _qs_install_empty_struct_schema(_QueryStreamProducer prod) noexcept:
    """For an empty result set, fabricate a zero-column struct schema
    so consumers (polars / pyarrow) iterate to a clean end-of-stream
    instead of erroring on missing schema."""
    cdef char* fmt = <char*>malloc(3)
    if fmt == NULL:
        return -1
    fmt[0] = b'+'
    fmt[1] = b's'
    fmt[2] = 0
    memset(&prod.cached_schema, 0, sizeof(ArrowSchema))
    prod.cached_schema.format = fmt
    prod.cached_schema.release = _arrow_schema_clone_release
    prod.has_cached_schema = True
    return 0


cdef int _qs_get_next(ArrowArrayStream* stream, ArrowArray* out) noexcept with gil:
    cdef _QueryStreamProducer prod
    memset(out, 0, sizeof(ArrowArray))
    if stream == NULL or stream.private_data == NULL:
        return 22  # EINVAL
    prod = <_QueryStreamProducer>stream.private_data
    if not prod.has_cached_array:
        if _qs_pull(prod) != 0:
            return 5  # EIO
    if prod.has_cached_array:
        memcpy(out, &prod.cached_array, sizeof(ArrowArray))
        memset(&prod.cached_array, 0, sizeof(ArrowArray))
        prod.has_cached_array = False
        return 0
    return 0


cdef const char* _qs_get_last_error(ArrowArrayStream* stream) noexcept:
    cdef _QueryStreamProducer prod
    if stream == NULL or stream.private_data == NULL:
        return NULL
    prod = <_QueryStreamProducer>stream.private_data
    return <const char*>prod.last_error


cdef void _qs_release(ArrowArrayStream* stream) noexcept with gil:
    cdef _QueryStreamProducer prod
    if stream == NULL or stream.private_data == NULL:
        return
    prod = <_QueryStreamProducer>stream.private_data
    stream.private_data = NULL
    stream.release = NULL
    Py_DECREF(prod)


cdef void _qs_capsule_destructor(object capsule) noexcept:
    cdef ArrowArrayStream* stream
    if not PyCapsule_IsValid(capsule, b'arrow_array_stream'):
        return
    stream = <ArrowArrayStream*>PyCapsule_GetPointer(
        capsule, b'arrow_array_stream')
    if stream == NULL:
        return
    if stream.release != NULL:
        stream.release(stream)
    free(stream)


cdef object _make_query_stream_capsule(_CursorHandle handle):
    cdef _QueryStreamProducer prod
    cdef ArrowArrayStream* stream
    prod = _QueryStreamProducer()
    prod.cursor_handle = handle
    stream = <ArrowArrayStream*>calloc(1, sizeof(ArrowArrayStream))
    if stream == NULL:
        raise MemoryError()
    stream.get_schema = _qs_get_schema
    stream.get_next = _qs_get_next
    stream.get_last_error = _qs_get_last_error
    stream.release = _qs_release
    Py_INCREF(prod)
    stream.private_data = <void*>prod
    return PyCapsule_New(
        <void*>stream, b'arrow_array_stream', _qs_capsule_destructor)


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

    Streams query rows as Arrow record batches. **Single-use**: each
    materialisation method (``to_pandas``, ``to_arrow``, ``iter_arrow``,
    ``iter_pandas``, or the ``__arrow_c_stream__`` PyCapsule protocol)
    consumes the underlying cursor; the second consumption raises
    ``IngressError``.

    ``__arrow_c_stream__`` is native — the cursor's record batches are
    exposed directly through the Arrow C Data Interface, so polars /
    duckdb / pandas 3.0 / any Arrow-native consumer can read query
    results without pyarrow installed. ``to_arrow`` / ``to_pandas`` /
    ``iter_arrow`` / ``iter_pandas`` are convenience wrappers that
    do require pyarrow.

    Example::

        with client.query('SELECT * FROM trades WHERE ts > $1') as result:
            df = polars.from_arrow(result)              # no pyarrow
            # df = result.to_pandas()                   # pyarrow required
            # table = pa.table(result)                  # pyarrow required
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

    def _take_cursor_handle(self):
        if self._consumed:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'QueryResult already consumed')
        if self._cursor_handle is None:
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                'QueryResult cursor was closed')
        self._consumed = True
        handle = self._cursor_handle
        self._cursor_handle = None
        return handle

    def __arrow_c_stream__(self, requested_schema=None):
        if requested_schema is not None:
            raise NotImplementedError(
                'requested_schema is not supported; consume the stream '
                'and project on the consumer side.')
        return _make_query_stream_capsule(self._take_cursor_handle())

    def to_arrow(self):
        """Read the full result into a ``pyarrow.Table``. Requires pyarrow.

        Pyarrow-free alternative: ``polars.from_arrow(result)`` /
        ``duckdb.from_arrow(result)`` / ``pa.table(result)`` consume
        the ``__arrow_c_stream__`` capsule directly.
        """
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
