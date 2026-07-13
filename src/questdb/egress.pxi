# Egress (QWP/WebSocket reader) Cython glue.
#
# `QueryResult` exposes the Arrow PyCapsule Interface
# (`__arrow_c_stream__`) directly off the Rust cursor, so polars /
# duckdb / pandas 3.0 / any Arrow-native consumer can read query
# results without pyarrow. `to_arrow`, `to_pandas`, `iter_arrow`,
# `iter_pandas` are convenience wrappers that lazy-import pyarrow.

cimport numpy as cnp


cdef inline object _reader_err_code_to_py(questdb_error_code code):
    # The error model is unified: `questdb_error_code` is an alias of
    # `line_sender_error_code`, so reader errors decode through the single
    # shared map. Every reader category now has its own `QuestDBErrorCode`
    # member (handshake, protocol, invalid bind, schema drift, the
    # server-side errors, ...) instead of the former `ServerFlushError`
    # bucket.
    return c_err_code_to_py(code)


cdef inline object _reader_err_to_py(questdb_error* err):
    """Construct a ``QuestDBError`` from a ``questdb_error*`` and free it."""
    return c_err_to_py(err)


cdef class _ReaderHandle:
    """Owns a ``reader*``.

    On dealloc the reader either returns to its pool or is dropped,
    depending on the ``reader``'s own ownership tag (set when it
    was constructed — see ``ReaderOwnership`` in the Rust FFI):

    - Pool-borrowed readers go back to the pool unless
      ``_must_close`` was set, in which case the pool drops them.
    - Standalone readers (from ``reader_from_conf``) are always
      dropped.

    The Python side carries only one extra bit of state —
    ``_must_close`` — which it forwards to the FFI via
    ``reader_drop_on_return`` before calling close. We never
    hold a raw ``questdb_db*`` pointer here: the reader struct
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
    cdef reader* _reader
    cdef bint _must_close

    def __cinit__(self):
        self._reader = NULL
        self._must_close = True

    cdef _attach(self, reader* reader):
        self._reader = reader

    cdef void _close(self) noexcept:
        cdef PyThreadState* gs = NULL
        if self._reader == NULL:
            return
        if self._must_close:
            reader_drop_on_return(self._reader)
        _ensure_doesnt_have_gil(&gs)
        reader_close(self._reader)
        _ensure_has_gil(&gs)
        self._reader = NULL

    def __dealloc__(self):
        self._close()


cdef class _CursorHandle:
    """Owns a ``reader_cursor*`` + back-ref to its reader. Freed on dealloc.

    ``_reset_seq`` counts mid-query failover resets. The
    ``_failover_reset_trampoline`` installed on the materialise-whole
    query path bumps it (a plain C-field write, no GIL, no FFI, no
    exception — honouring the reader's reentrancy contract) when the
    cursor re-executes the query on a new endpoint. The accumulating
    reader (``_numpy_frame_from_cursor`` etc.) compares it against the
    sequence it observed at start-of-stream and, when it advanced,
    discards every batch buffered so far so the replay-from-batch-0
    yields a correct whole result.
    """
    cdef reader_cursor* _cursor
    cdef _ReaderHandle _reader_ref
    cdef object _lock
    cdef int _reset_seq

    def __cinit__(self):
        self._cursor = NULL
        self._reader_ref = None
        self._lock = threading.Lock()
        self._reset_seq = 0

    cdef _attach(self, reader_cursor* cursor, _ReaderHandle reader_ref):
        self._cursor = cursor
        self._reader_ref = reader_ref

    cdef void _free(self) noexcept:
        cdef PyThreadState* gs = NULL
        with self._lock:
            if self._cursor != NULL:
                _ensure_doesnt_have_gil(&gs)
                reader_cursor_free(self._cursor)
                _ensure_has_gil(&gs)
                self._cursor = NULL

    def __dealloc__(self):
        self._free()


cdef object _fetch_one_batch(
        _CursorHandle handle, object pa_module, bint compact=False):
    """Pull one batch via reader_cursor_next_arrow_batch.

    Returns:
      - None on clean end-of-stream.
      - A pyarrow.RecordBatch on success.
    Raises QuestDBError on FFI error.
    """
    cdef ArrowArray array
    cdef ArrowSchema schema
    cdef questdb_error* err = NULL
    cdef reader_arrow_batch_result result
    cdef reader_cursor* cursor

    with handle._lock:
        cursor = handle._cursor
        if cursor == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'cursor is closed')
        with nogil:
            if compact:
                result = reader_cursor_next_arrow_batch_compact(
                    cursor, &array, &schema, &err)
            else:
                result = reader_cursor_next_arrow_batch(
                    cursor, &array, &schema, &err)

    if result == reader_arrow_batch_ok:
        # Hand ownership of the array + schema buffers to pyarrow.
        # _import_from_c moves the structs and nulls their release
        # callbacks; pyarrow's RecordBatch owns the buffers from here.
        try:
            return pa_module.RecordBatch._import_from_c(
                <uintptr_t>&array, <uintptr_t>&schema)
        except:
            if array.release != NULL:
                array.release(&array)
            if schema.release != NULL:
                schema.release(&schema)
            raise

    if result == reader_arrow_batch_end:
        return None

    # Error path.
    if err == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'reader_cursor_next_arrow_batch returned error '
            'without setting err_out')
    raise _reader_err_to_py(err)


cdef tuple _fetch_all_record_batches(
        _CursorHandle handle, object pa_module, bint compact=False):
    """Drain the cursor into a list of ``pyarrow.RecordBatch`` we own.

    The materialise-whole entry points install the failover-reset
    trampoline, which bumps ``handle._reset_seq`` when a mid-query
    failover re-executes the query on a new endpoint. Because we own the
    accumulator here, on a reset we discard every batch buffered so far
    and restart from the replayed batch-0 — yielding a correct,
    duplicate-free whole result. Returns ``(schema_or_None, batches)``;
    the cursor is freed and the reader marked drained on clean
    end-of-stream.
    """
    cdef int seen_seq = handle._reset_seq
    cdef object schema = None
    cdef list batches = []
    cdef object batch
    try:
        while True:
            batch = _fetch_one_batch(handle, pa_module, compact)
            if handle._reset_seq != seen_seq:
                # Mid-query failover replayed from batch-0: drop the
                # pre-failover accumulation and re-pin the schema.
                seen_seq = handle._reset_seq
                batches = []
                schema = None
            if batch is None:
                break
            if schema is None:
                schema = batch.schema
            batches.append(batch)
    except:
        handle._free()
        raise
    _mark_reader_drained(handle)
    handle._free()
    return (schema, batches)


cdef object _build_record_batch_reader(
        _CursorHandle cursor_handle, bint compact=False):
    """Construct a pyarrow.RecordBatchReader over the cursor.

    Peeks the first batch to capture the stream schema, then yields
    the remaining batches lazily. The cursor is explicitly freed when
    the underlying generator completes (exhaustion, exception, or
    close), so the owning reader can be closed without leaking a live
    cursor.
    """
    import pyarrow as pa

    first = _fetch_one_batch(cursor_handle, pa, compact)
    if first is None:
        # No result set (a non-SELECT statement ends with EXEC_DONE and
        # ships no RESULT_BATCH), so there is no schema to surface; an
        # empty SELECT still ships a zero-row batch carrying the schema.
        _mark_reader_drained(cursor_handle)
        cursor_handle._free()
        empty = pa.table({})
        return empty.to_reader()

    # Pin the sequence after the first batch: a pre-delivery failover
    # replays batch-0 transparently into ``first``, so only resets that
    # happen once batches are flowing can duplicate already-yielded data.
    cdef int seen_seq = cursor_handle._reset_seq
    schema = first.schema

    def _gen(compact):
        try:
            yield first
            while True:
                nxt = _fetch_one_batch(cursor_handle, pa, compact)
                if cursor_handle._reset_seq != seen_seq:
                    # Mid-query failover after batches were already
                    # yielded: the replayed batch-0 would duplicate what
                    # the consumer holds. Streaming can't discard it, so
                    # surface a clean, catchable error.
                    raise QuestDBError(
                        QuestDBErrorCode.FailoverWouldDuplicate,
                        'mid-query failover would duplicate already-'
                        'delivered batches; re-issue the query')
                if nxt is None:
                    # Reached terminal cleanly; reader is reusable.
                    _mark_reader_drained(cursor_handle)
                    return
                yield nxt
        finally:
            cursor_handle._free()

    return pa.RecordBatchReader.from_batches(schema, _gen(compact))


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
    cdef questdb_error* err = NULL
    cdef reader* reader = NULL
    with nogil:
        reader = questdb_db_borrow_reader(db, &err)
    if reader == NULL:
        if err == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'questdb_db_borrow_reader returned NULL without setting err')
        raise _reader_err_to_py(err)
    cdef _ReaderHandle handle
    try:
        handle = _ReaderHandle()
    except:
        reader_close(reader)
        raise
    handle._attach(reader)
    return handle


cdef object _snapshot_server_info(_ReaderHandle handle):
    """Copy the reader's last-seen ``SERVER_INFO`` into a :class:`ServerInfo`.

    The FFI pointer is borrowed and invalidated by any reader operation
    that may reconnect, so every field is copied out before returning.
    """
    cdef const reader_server_info* si = \
        reader_current_server_info(handle._reader)
    cdef const char* buf = NULL
    cdef size_t buf_len = 0
    if si == NULL:
        raise QuestDBError(
            QuestDBErrorCode.SocketError,
            'SERVER_INFO unavailable: the connection is mid-reconnect.')
    cdef int role_int = <int>reader_server_info_role(si)
    role = ServerRole.Other
    for entry in ServerRole:
        if entry.c_value == role_int:
            role = entry
            break
    cdef uint8_t role_byte = reader_server_info_role_byte(si)
    cdef uint64_t epoch = reader_server_info_epoch(si)
    cdef uint32_t capabilities = reader_server_info_capabilities(si)
    cdef int64_t server_wall_ns = reader_server_info_server_wall_ns(si)
    reader_server_info_cluster_id(si, &buf, &buf_len)
    cluster_id = PyUnicode_FromStringAndSize(buf, <Py_ssize_t>buf_len) \
        if buf != NULL else ''
    buf = NULL
    buf_len = 0
    reader_server_info_node_id(si, &buf, &buf_len)
    node_id = PyUnicode_FromStringAndSize(buf, <Py_ssize_t>buf_len) \
        if buf != NULL else ''
    buf = NULL
    buf_len = 0
    zone_id = None
    if reader_server_info_zone_id(si, &buf, &buf_len):
        zone_id = PyUnicode_FromStringAndSize(buf, <Py_ssize_t>buf_len) \
            if buf != NULL else ''
    return ServerInfo(
        role=role,
        role_byte=role_byte,
        epoch=epoch,
        capabilities=capabilities,
        server_wall_ns=server_wall_ns,
        cluster_id=cluster_id,
        node_id=node_id,
        zone_id=zone_id)


cdef void _failover_reset_trampoline(
        const reader_failover_reset_event* event,
        void* user_data) noexcept nogil:
    # Fires synchronously inside reader_cursor_next_batch while the
    # reader re-executes on a new endpoint, before the replayed batch-0
    # arrives. Honour the C reentrancy contract: no reentrant FFI on the
    # reader/query/cursor, no exception escapes, non-blocking. user_data is
    # a raw int* at the cursor's _reset_seq counter; bumping it is a plain
    # pointer write (no GIL, no Python object touched), which the
    # materialise-whole accumulator polls to discard its pre-failover batches.
    if user_data == NULL:
        return
    (<int*>user_data)[0] += 1


cdef _CursorHandle _execute_query(
        _ReaderHandle reader_handle, str sql, bint reset_symbol_dict=True):
    """Execute a SQL query and return a _CursorHandle.

    The query is prepared with an ``on_failover_reset`` trampoline that
    bumps the cursor's ``_reset_seq`` on a mid-query failover. The
    materialise-whole entry points poll it to discard their partial
    accumulation and replay-from-batch-0 transparently; the streaming
    entry points poll it to surface a clean ``FailoverWouldDuplicate``
    (the already-yielded batches can't be discarded). Installing the
    callback also clears the C-side silent-duplicate guard, so a
    post-delivery failover re-executes rather than aborting outright.
    """
    cdef bytes sql_bytes = sql.encode('utf-8')
    cdef line_sender_error* utf8_err = NULL
    cdef line_sender_utf8 sql_utf8
    cdef questdb_error* err = NULL
    cdef reader_query* query
    cdef reader_cursor* cursor

    if not line_sender_utf8_init(
            &sql_utf8,
            <size_t>len(sql_bytes),
            <const char*><char*>sql_bytes,
            &utf8_err):
        raise c_err_to_py(utf8_err)

    cdef _CursorHandle handle = _CursorHandle()

    with nogil:
        query = reader_prepare(reader_handle._reader, sql_utf8, &err)

    if query == NULL:
        if err == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'reader_prepare returned NULL without setting err')
        raise _reader_err_to_py(err)

    reader_query_set_reset_symbol_dict(query, reset_symbol_dict)

    reader_query_on_failover_reset(
        query, _failover_reset_trampoline, <void*>&handle._reset_seq)

    with nogil:
        cursor = reader_query_execute(&query, &err)

    if cursor == NULL:
        # _query_execute consumes the query (nulls *query_inout); the
        # defensive free is a no-op on the consumed handle.
        reader_query_free(query)
        if err == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'reader_query_execute returned NULL without setting err')
        raise _reader_err_to_py(err)

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
    if n <= 0:
        return pos
    for i in range(n):
        memcpy(&klen, md + pos, sizeof(int32_t))
        if klen < 0:
            return pos
        pos += sizeof(int32_t) + <size_t>klen
        memcpy(&vlen, md + pos, sizeof(int32_t))
        if vlen < 0:
            return pos
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
    cdef bint delivered
    cdef char* last_error
    cdef int seen_seq

    def __cinit__(self):
        self.cursor_handle = None
        self.has_cached_schema = False
        self.has_cached_array = False
        self.exhausted = False
        self.delivered = False
        self.last_error = NULL
        self.seen_seq = 0
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


cdef int _qs_pull(_QueryStreamProducer prod) noexcept with gil:
    cdef reader_cursor* cursor
    cdef ArrowArray local_array
    cdef ArrowSchema local_schema
    cdef questdb_error* err = NULL
    cdef reader_arrow_batch_result result
    cdef const char* err_msg = NULL
    cdef size_t err_len = 0
    cdef questdb_error_code code
    cdef object py_msg
    cdef bytes full
    if prod.exhausted:
        # A prior error left a diagnostic pinned; keep surfacing it rather
        # than reporting a clean end-of-stream to a consumer that pulls again.
        if prod.last_error != NULL:
            return -1
        return 0
    if prod.cursor_handle is None:
        _qs_set_error(prod, b'cursor is closed', 16)
        prod.exhausted = True
        return -1
    memset(&local_array, 0, sizeof(ArrowArray))
    memset(&local_schema, 0, sizeof(ArrowSchema))
    with prod.cursor_handle._lock:
        cursor = prod.cursor_handle._cursor
        if cursor == NULL:
            _qs_set_error(prod, b'cursor is closed', 16)
            prod.exhausted = True
            return -1
        with nogil:
            result = reader_cursor_next_arrow_batch_compact(
                cursor, &local_array, &local_schema, &err)
    if result == reader_arrow_batch_ok:
        if prod.cursor_handle._reset_seq != prod.seen_seq:
            if prod.delivered:
                # Mid-query failover replayed from batch-0 after batches
                # were already handed to the consumer; this one would
                # duplicate them. Streaming can't discard it — surface a
                # clean error (tagged like other capsule errors) and stop.
                if local_array.release != NULL:
                    local_array.release(&local_array)
                if local_schema.release != NULL:
                    local_schema.release(&local_schema)
                try:
                    full = (
                        '[' + QuestDBErrorCode.FailoverWouldDuplicate.name + '] '
                        'mid-query failover would duplicate already-delivered '
                        'batches; re-issue the query').encode('utf-8')
                    _qs_set_error(prod, full, <size_t>len(full))
                except:
                    fallback = (
                        b'[FailoverWouldDuplicate] mid-query failover would '
                        b'duplicate already-delivered batches; re-issue the query')
                    _qs_set_error(prod, fallback, <size_t>len(fallback))
                prod.exhausted = True
                return -1
            # Pre-delivery failover: nothing reached the consumer yet, so
            # re-pin to the replayed stream and restart from this batch-0.
            prod.seen_seq = prod.cursor_handle._reset_seq
            prod._free_cached()
        if not prod.has_cached_schema:
            memcpy(&prod.cached_schema, &local_schema, sizeof(ArrowSchema))
            prod.has_cached_schema = True
        else:
            if local_schema.release != NULL:
                local_schema.release(&local_schema)
        if prod.has_cached_array and prod.cached_array.release != NULL:
            prod.cached_array.release(&prod.cached_array)
        memcpy(&prod.cached_array, &local_array, sizeof(ArrowArray))
        prod.has_cached_array = True
        return 0
    if result == reader_arrow_batch_end:
        prod.exhausted = True
        if prod.cursor_handle._reader_ref is not None:
            prod.cursor_handle._reader_ref._must_close = False
        return 0
    if err != NULL:
        code = questdb_error_get_code(err)
        err_msg = questdb_error_msg(err, &err_len)
        try:
            if err_msg != NULL:
                py_msg = PyUnicode_FromStringAndSize(err_msg, <Py_ssize_t>err_len)
            else:
                py_msg = 'arrow batch fetch failed'
            full = (
                '[' + _reader_err_code_to_py(code).name + '] ' + py_msg
            ).encode('utf-8')
            _qs_set_error(prod, full, <size_t>len(full))
        except:
            if err_msg != NULL:
                _qs_set_error(prod, err_msg, err_len)
            else:
                _qs_set_error(prod, b'arrow batch fetch failed', 24)
        questdb_error_free(err)
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
        prod.delivered = True
        return 0
    return 0


cdef const char* _qs_get_last_error(ArrowArrayStream* stream) noexcept with gil:
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
    prod.seen_seq = handle._reset_seq
    stream = <ArrowArrayStream*>calloc(1, sizeof(ArrowArrayStream))
    if stream == NULL:
        raise MemoryError()
    stream.get_schema = _qs_get_schema
    stream.get_next = _qs_get_next
    stream.get_last_error = _qs_get_last_error
    stream.release = _qs_release
    Py_INCREF(prod)
    stream.private_data = <void*>prod
    try:
        return PyCapsule_New(
            <void*>stream, b'arrow_array_stream', _qs_capsule_destructor)
    except:
        Py_DECREF(prod)
        free(stream)
        raise


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


cdef object _table_shared_symbol_dict(object table):
    """Collapse every dictionary (SYMBOL) column so all chunks share ONE
    dictionary, and recast its index to signed int32.

    QuestDB SYMBOL egresses as ``dictionary(uint32, utf8)``, one chunk per wire
    batch, each carrying the full append-only connection dict with global,
    query-stable codes — so the last chunk's dictionary is the largest and
    covers every code (same invariant as ``_polars_dict_codes_cats``). A generic
    consumer (``Table.to_pandas`` / a foreign Arrow reader) otherwise re-unifies
    the per-batch dictionaries — ``O(batches × cardinality)``, which dominates
    high-cardinality SYMBOL egress. Re-pointing every chunk at that one shared
    dictionary makes the unify a no-op (codes are already valid against the
    superset). The index is also recast to int32 because pandas rejects unsigned
    dictionary indices (symbol cardinality fits int32). Returns the table
    unchanged when no column is a dictionary.
    """
    import pyarrow as pa
    cdef object schema = table.schema
    cdef bint changed = False
    cdef list cols = []
    cdef list fields = []
    cdef object field, ty, col, shared, signed_ty
    cdef Py_ssize_t i, j, n
    for i in range(table.num_columns):
        field = schema.field(i)
        ty = field.type
        if not pa.types.is_dictionary(ty):
            cols.append(table.column(i))
            fields.append(field)
            continue
        changed = True
        col = table.column(i)
        signed_ty = pa.dictionary(pa.int32(), ty.value_type, ty.ordered)
        n = col.num_chunks
        if n == 0:
            cols.append(col.cast(signed_ty))
        else:
            # Append-only => the last chunk's dict is the superset; rebase every
            # chunk's (int32-recast) codes onto it so they share one dictionary.
            shared = col.chunk(n - 1).dictionary
            cols.append(pa.chunked_array(
                [pa.DictionaryArray.from_arrays(
                    col.chunk(j).indices.cast(pa.int32()), shared)
                 for j in range(n)],
                type=signed_ty))
        fields.append(field.with_type(signed_ty))
    if not changed:
        return table
    return pa.Table.from_arrays(
        cols, schema=pa.schema(fields, metadata=schema.metadata))


cdef dict _KIND_NAMES = {
    <int>reader_column_kind_boolean: 'boolean',
    <int>reader_column_kind_byte: 'byte',
    <int>reader_column_kind_short: 'short',
    <int>reader_column_kind_int: 'int',
    <int>reader_column_kind_long: 'long',
    <int>reader_column_kind_float: 'float',
    <int>reader_column_kind_double: 'double',
    <int>reader_column_kind_char: 'char',
    <int>reader_column_kind_ipv4: 'ipv4',
    <int>reader_column_kind_timestamp: 'timestamp',
    <int>reader_column_kind_timestamp_nanos: 'timestamp_ns',
    <int>reader_column_kind_date: 'date',
    <int>reader_column_kind_uuid: 'uuid',
    <int>reader_column_kind_long256: 'long256',
    <int>reader_column_kind_geohash: 'geohash',
    <int>reader_column_kind_varchar: 'varchar',
    <int>reader_column_kind_binary: 'binary',
    <int>reader_column_kind_symbol: 'symbol',
    <int>reader_column_kind_double_array: 'double_array',
    <int>reader_column_kind_long_array: 'long_array',
    <int>reader_column_kind_decimal64: 'decimal',
    <int>reader_column_kind_decimal128: 'decimal',
    <int>reader_column_kind_decimal256: 'decimal',
}


cdef object _UUID_MODULE = None
cdef object _DECIMAL_TYPE = None


cdef object _uuid_module():
    global _UUID_MODULE
    if _UUID_MODULE is None:
        import uuid
        _UUID_MODULE = uuid
    return _UUID_MODULE


cdef object _decimal_type():
    global _DECIMAL_TYPE
    if _DECIMAL_TYPE is None:
        from decimal import Decimal
        _DECIMAL_TYPE = Decimal
    return _DECIMAL_TYPE


cdef int _reader_check(bint ok, questdb_error* err, str what) except -1:
    if ok:
        return 0
    if err != NULL:
        raise _reader_err_to_py(err)
    raise QuestDBError(
        QuestDBErrorCode.ServerFlushError,
        what + ' returned false without err_out')


cdef object _numpy_dtype_for_kind(reader_column_kind kind, object np):
    if kind == reader_column_kind_boolean:
        return np.dtype(np.bool_)
    if kind == reader_column_kind_byte:
        return np.dtype(np.int8)
    if kind == reader_column_kind_short:
        return np.dtype(np.int16)
    if kind == reader_column_kind_int:
        return np.dtype(np.int32)
    if kind == reader_column_kind_long:
        return np.dtype(np.int64)
    if kind == reader_column_kind_float:
        return np.dtype(np.float32)
    if kind == reader_column_kind_double:
        return np.dtype(np.float64)
    if kind == reader_column_kind_char:
        return np.dtype(np.uint16)
    if kind == reader_column_kind_ipv4:
        return np.dtype(np.uint32)
    if kind == reader_column_kind_timestamp:
        return np.dtype('datetime64[us]')
    if kind == reader_column_kind_timestamp_nanos:
        return np.dtype('datetime64[ns]')
    if kind == reader_column_kind_date:
        return np.dtype('datetime64[ms]')
    return None


cdef object _numpy_fixed_chunk(
        const reader_batch* batch,
        size_t col_idx,
        reader_column_kind kind,
        size_t row_count,
        object np):
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef object dtype = _numpy_dtype_for_kind(kind, np)
    cdef size_t itemsize
    cdef Py_ssize_t nbytes
    cdef unsigned char* src
    if dtype is None:
        raise QuestDBError(
            QuestDBErrorCode.InvalidApiCall,
            'numpy egress does not support column kind 0x{:02X} yet'.format(
                <int>kind))
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    itemsize = dtype.itemsize
    if cd.value_stride != itemsize:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'column kind 0x{:02X} wire stride {} != numpy itemsize {}'.format(
                <int>kind, cd.value_stride, itemsize))
    if row_count == 0:
        return np.empty(0, dtype=dtype)
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'column kind 0x{:02X} has {} rows but no values buffer'.format(
                <int>kind, row_count))
    nbytes = <Py_ssize_t>(row_count * cd.value_stride)
    src = <unsigned char*>cd.values
    if kind == reader_column_kind_boolean:
        return np.frombuffer((<unsigned char[:nbytes]>src), dtype=np.uint8) != 0
    return np.frombuffer((<unsigned char[:nbytes]>src), dtype=dtype).copy()


cdef inline void_int _obj_chunk_set(
        cnp.ndarray out, size_t r, object v) except -1:
    # np.empty(dtype=object) initialises every slot to None, so null rows are
    # left untouched and a mid-loop raise leaves the array safe to release.
    cnp.PyArray_SETITEM(out, cnp.PyArray_GETPTR1(out, <cnp.npy_intp>r), v)


cdef object _numpy_varlen_chunk(
        const reader_batch* batch,
        size_t col_idx,
        reader_column_kind kind,
        size_t row_count,
        object np):
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint32_t* offsets
    cdef const uint8_t* data
    cdef const uint8_t* validity
    cdef size_t r
    cdef uint32_t start
    cdef uint32_t end
    cdef cnp.ndarray out
    cdef bint is_binary = kind == reader_column_kind_binary
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if cd.var_offsets == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'column kind 0x{:02X} has {} rows but no offset table'.format(
                <int>kind, row_count))
    offsets = cd.var_offsets
    data = cd.var_data
    validity = cd.validity
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        start = offsets[r]
        end = offsets[r + 1]
        if end < start or end > cd.var_data_len:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'corrupt varlen offsets in column kind 0x{:02X}'.format(
                    <int>kind))
        if end > start:
            if is_binary:
                _obj_chunk_set(out, r, PyBytes_FromStringAndSize(
                    <const char*>(data + start), <Py_ssize_t>(end - start)))
            else:
                _obj_chunk_set(out, r, PyUnicode_FromStringAndSize(
                    <const char*>(data + start), <Py_ssize_t>(end - start)))
        else:
            _obj_chunk_set(out, r, b'' if is_binary else u'')
    return out


cdef object _numpy_symbol_codes_chunk(
        const reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint32_t* codes
    cdef const uint8_t* validity
    cdef size_t r
    cdef int64_t[::1] mv
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    out = np.empty(row_count, dtype=np.int64)
    if row_count == 0:
        return out
    if cd.symbol_codes == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'symbol column has {} rows but no codes buffer'.format(row_count))
    codes = cd.symbol_codes
    validity = cd.validity
    mv = out
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            mv[r] = -1
        else:
            mv[r] = <int64_t>codes[r]
    return out


cdef void _symbol_categories_extend(
        list cats, const reader_symbol_dict* sd, size_t start):
    cdef size_t i
    cdef const reader_symbol_entry* e
    for i in range(start, sd.entry_count):
        e = &sd.entries[i]
        if <uint64_t>e.offset + <uint64_t>e.length > <uint64_t>sd.heap_len:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'corrupt symbol dictionary heap offsets')
        cats.append(
            PyUnicode_FromStringAndSize(
                <const char*>(sd.heap + e.offset), <Py_ssize_t>e.length))


cdef object _numpy_geohash_chunk(
        const reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef object dtype
    cdef size_t stride
    cdef size_t target
    cdef Py_ssize_t nbytes
    cdef unsigned char* src
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    stride = cd.value_stride
    if stride == 1:
        dtype = np.dtype(np.uint8)
        target = 1
    elif stride == 2:
        dtype = np.dtype(np.uint16)
        target = 2
    elif stride == 3 or stride == 4:
        dtype = np.dtype(np.uint32)
        target = 4
    elif stride >= 5 and stride <= 8:
        dtype = np.dtype(np.uint64)
        target = 8
    else:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'unexpected geohash byte width {}'.format(stride))
    if row_count == 0:
        return np.empty(0, dtype=dtype)
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'geohash column has {} rows but no values buffer'.format(row_count))
    nbytes = <Py_ssize_t>(row_count * stride)
    src = <unsigned char*>cd.values
    if stride == target:
        return np.frombuffer((<unsigned char[:nbytes]>src), dtype=dtype).copy()
    raw = np.frombuffer(
        (<unsigned char[:nbytes]>src), dtype=np.uint8).reshape(
            <Py_ssize_t>row_count, <Py_ssize_t>stride)
    wide = np.zeros((<Py_ssize_t>row_count, <Py_ssize_t>target), dtype=np.uint8)
    wide[:, :stride] = raw
    return wide.view(dtype).reshape(<Py_ssize_t>row_count)


cdef object _numpy_uuid_chunk(
        const reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef object _uuid = _uuid_module()
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef size_t r
    cdef uint64_t lo
    cdef uint64_t hi
    cdef cnp.ndarray out
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'uuid column has {} rows but no values buffer'.format(row_count))
    validity = cd.validity
    values = <const uint8_t*>cd.values
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        memcpy(&lo, values + r * 16, 8)
        memcpy(&hi, values + r * 16 + 8, 8)
        _obj_chunk_set(out, r, _uuid.UUID(int=((<object>hi) << 64) | (<object>lo)))
    return out


cdef object _numpy_long256_chunk(
        const reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef size_t r
    cdef cnp.ndarray out
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'long256 column has {} rows but no values buffer'.format(row_count))
    validity = cd.validity
    values = <const uint8_t*>cd.values
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        _obj_chunk_set(out, r, int.from_bytes(
            PyBytes_FromStringAndSize(<const char*>(values + r * 32), 32),
            'little', signed=False))
    return out


cdef object _numpy_decimal_chunk(
        const reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef object Decimal = _decimal_type()
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef size_t r
    cdef size_t width
    cdef int scale
    cdef cnp.ndarray out
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'decimal column has {} rows but no values buffer'.format(row_count))
    validity = cd.validity
    values = <const uint8_t*>cd.values
    width = cd.value_stride
    scale = cd.decimal_scale
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        unscaled = int.from_bytes(
            PyBytes_FromStringAndSize(
                <const char*>(values + r * width), <Py_ssize_t>width),
            'little', signed=True)
        _obj_chunk_set(out, r, Decimal(f'{unscaled}e{-scale}'))
    return out


cdef object _numpy_array_chunk(
        const reader_batch* batch,
        size_t col_idx,
        reader_column_kind kind,
        size_t row_count,
        object np):
    cdef reader_array_data ad
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* data
    cdef const uint32_t* data_offsets
    cdef const uint32_t* shapes
    cdef const uint32_t* shape_offsets
    cdef size_t r
    cdef size_t k
    cdef uint32_t dstart
    cdef uint32_t dend
    cdef uint32_t sstart
    cdef uint32_t send
    cdef Py_ssize_t blen
    cdef cnp.ndarray out
    if kind != reader_column_kind_double_array:
        raise QuestDBError(
            QuestDBErrorCode.InvalidApiCall,
            'numpy egress supports only double arrays (kind 0x{:02X})'.format(
                <int>kind))
    _reader_check(
        reader_batch_array_column_data(batch, col_idx, &ad, &err), err,
        'reader_batch_array_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if ad.data_offsets == NULL or ad.shape_offsets == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'array column has {} rows but no offset tables'.format(row_count))
    validity = ad.validity
    data = ad.data
    data_offsets = ad.data_offsets
    shapes = ad.shapes
    shape_offsets = ad.shape_offsets
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        dstart = data_offsets[r]
        dend = data_offsets[r + 1]
        if dend < dstart or <uint64_t>dend > <uint64_t>ad.data_len:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'corrupt array data offsets in column kind 0x{:02X}'.format(
                    <int>kind))
        blen = <Py_ssize_t>(dend - dstart)
        if blen > 0:
            flat = np.frombuffer(
                (<unsigned char[:blen]>(<unsigned char*>(data + dstart))),
                dtype=np.float64).copy()
        else:
            flat = np.empty(0, dtype=np.float64)
        sstart = shape_offsets[r]
        send = shape_offsets[r + 1]
        if send < sstart or <uint64_t>send > <uint64_t>ad.shapes_len:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'corrupt array shape offsets in column kind 0x{:02X}'.format(
                    <int>kind))
        if send > sstart:
            _obj_chunk_set(out, r, flat.reshape(
                tuple(shapes[sstart + k] for k in range(send - sstart))))
        else:
            _obj_chunk_set(out, r, flat)
    return out


cdef object _numpy_column_chunk(
        const reader_batch* batch,
        size_t col_idx,
        reader_column_kind kind,
        size_t row_count,
        object np):
    if kind == reader_column_kind_symbol:
        return _numpy_symbol_codes_chunk(batch, col_idx, row_count, np)
    if (kind == reader_column_kind_varchar
            or kind == reader_column_kind_binary):
        return _numpy_varlen_chunk(batch, col_idx, kind, row_count, np)
    if kind == reader_column_kind_geohash:
        return _numpy_geohash_chunk(batch, col_idx, row_count, np)
    if kind == reader_column_kind_uuid:
        return _numpy_uuid_chunk(batch, col_idx, row_count, np)
    if kind == reader_column_kind_long256:
        return _numpy_long256_chunk(batch, col_idx, row_count, np)
    if (kind == reader_column_kind_decimal64
            or kind == reader_column_kind_decimal128
            or kind == reader_column_kind_decimal256):
        return _numpy_decimal_chunk(batch, col_idx, row_count, np)
    if (kind == reader_column_kind_double_array
            or kind == reader_column_kind_long_array):
        return _numpy_array_chunk(batch, col_idx, kind, row_count, np)
    return _numpy_fixed_chunk(batch, col_idx, kind, row_count, np)


cdef bint _is_hybrid_int(reader_column_kind kind):
    return (kind == reader_column_kind_int
            or kind == reader_column_kind_long
            or kind == reader_column_kind_ipv4
            or kind == reader_column_kind_geohash)


cdef object _numpy_validity_mask(
        const reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef reader_column_data cd
    cdef questdb_error* err = NULL
    cdef Py_ssize_t vbytes
    cdef unsigned char* vsrc
    _reader_check(
        reader_batch_column_data(batch, col_idx, &cd, &err), err,
        'reader_batch_column_data')
    if row_count == 0 or cd.validity == NULL:
        return None
    vbytes = <Py_ssize_t>((row_count + 7) // 8)
    vsrc = <unsigned char*>cd.validity
    return np.unpackbits(
        np.frombuffer((<unsigned char[:vbytes]>vsrc), dtype=np.uint8),
        count=<Py_ssize_t>row_count, bitorder='little').astype(bool)


cdef object _build_nullable_array(
        values, mask, reader_column_kind kind, object pd):
    if (kind == reader_column_kind_float
            or kind == reader_column_kind_double):
        return pd.arrays.FloatingArray(values, mask)
    if kind == reader_column_kind_boolean:
        return pd.arrays.BooleanArray(values, mask)
    return pd.arrays.IntegerArray(values, mask)


cdef object _combine_hybrid_mask(list value_chunks, list mask_chunks, object np):
    cdef size_t n = <size_t>len(mask_chunks)
    cdef size_t i
    cdef bint any_null = False
    for i in range(n):
        if mask_chunks[i] is not None:
            any_null = True
            break
    if not any_null:
        return None
    parts = []
    for i in range(n):
        if mask_chunks[i] is None:
            parts.append(np.zeros(len(value_chunks[i]), dtype=bool))
        else:
            parts.append(mask_chunks[i])
    if len(parts) == 1:
        return parts[0]
    return np.concatenate(parts)


cdef tuple _numpy_extract_meta(const reader_batch* batch):
    cdef size_t n_cols = reader_batch_column_count(batch)
    cdef size_t col_idx
    cdef reader_column_kind kind = reader_column_kind_unknown
    cdef const char* name_buf = NULL
    cdef size_t name_len = 0
    cdef questdb_error* err = NULL
    cdef reader_column_data cd_meta
    cdef bint has_symbol = False
    col_names = []
    col_kinds = []
    col_scales = []
    col_precision = []
    for col_idx in range(n_cols):
        _reader_check(
            reader_batch_column_name(
                batch, col_idx, &name_buf, &name_len, &err),
            err, 'reader_batch_column_name')
        col_names.append(
            PyUnicode_FromStringAndSize(name_buf, <Py_ssize_t>name_len))
        _reader_check(
            reader_batch_column_kind(batch, col_idx, &kind, &err),
            err, 'reader_batch_column_kind')
        col_kinds.append(<int>kind)
        col_scales.append(None)
        col_precision.append(None)
        if kind == reader_column_kind_symbol:
            has_symbol = True
        elif (kind == reader_column_kind_geohash
                or kind == reader_column_kind_decimal64
                or kind == reader_column_kind_decimal128
                or kind == reader_column_kind_decimal256):
            if reader_batch_column_data(batch, col_idx, &cd_meta, &err):
                if kind == reader_column_kind_geohash:
                    col_precision[col_idx] = cd_meta.geohash_precision_bits
                else:
                    col_scales[col_idx] = cd_meta.decimal_scale
            elif err != NULL:
                questdb_error_free(err)
                err = NULL
    return (col_names, col_kinds, col_scales, col_precision, has_symbol)


cdef object _FROM_CODES_HAS_VALIDATE = None


cdef object _symbol_from_codes(object pd, object arr, object dtype):
    # Bounds-check the wire codes against the dict once (a vectorised C-level
    # reduction), then skip pandas' O(rows) Python re-validation. `validate=`
    # exists since pandas 1.1; older pandas keeps the checked path. `dtype=`
    # reuses one cached category Index across columns and batches (vs
    # `categories=`, which rebuilds it every call).
    cdef Py_ssize_t n_cats = len(dtype.categories)
    if arr.size and <Py_ssize_t>int(arr.max()) >= n_cats:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'corrupt symbol codes: code out of dictionary range')
    global _FROM_CODES_HAS_VALIDATE
    if _FROM_CODES_HAS_VALIDATE is None:
        import inspect
        _FROM_CODES_HAS_VALIDATE = (
            'validate' in inspect.signature(
                pd.Categorical.from_codes).parameters)
    if _FROM_CODES_HAS_VALIDATE:
        return pd.Categorical.from_codes(arr, dtype=dtype, validate=False)
    return pd.Categorical.from_codes(arr, dtype=dtype)


cdef object _numpy_assemble_frame(
        list col_names, list col_kinds, list col_scales,
        list col_precision, list col_chunks, list symbol_categories,
        object np, object pd, list col_masks, object symbol_dtype=None):
    cdef size_t n_cols = <size_t>len(col_names)
    cdef size_t col_idx
    cdef reader_column_kind kind
    arrays = []
    for col_idx in range(n_cols):
        kind = <reader_column_kind><int>col_kinds[col_idx]
        chunks = col_chunks[col_idx]
        if len(chunks) == 1:
            arr = chunks[0]
        else:
            arr = np.concatenate(chunks)
        if kind == reader_column_kind_symbol:
            # Build the category Index once (here for fetch-all, or supplied
            # pre-built by `_NumpyBatchIter` across batches) and reuse it via
            # `dtype=` for every SYMBOL column, then build from the codes with no
            # bounds re-validation. Avoids the O(columns/batches x cardinality)
            # Index rebuild + validation the `categories=`/validate path costs.
            if symbol_dtype is None:
                symbol_dtype = pd.CategoricalDtype(symbol_categories)
            arr = _symbol_from_codes(pd, arr, symbol_dtype)
        elif _is_hybrid_int(kind):
            mask = _combine_hybrid_mask(chunks, col_masks[col_idx], np)
            if mask is not None:
                arr = _build_nullable_array(arr, mask, kind, pd)
        arrays.append(arr)
    frame = pd.DataFrame(dict(enumerate(arrays)), copy=False)
    frame.columns = col_names
    columns_meta = {}
    for col_idx in range(n_cols):
        entry = {'kind': _KIND_NAMES.get(col_kinds[col_idx], 'unknown')}
        if col_scales[col_idx] is not None:
            entry['scale'] = col_scales[col_idx]
        if col_precision[col_idx] is not None:
            entry['precision_bits'] = col_precision[col_idx]
        columns_meta[col_names[col_idx]] = entry
    frame.attrs['questdb'] = {'version': 1, 'columns': columns_meta}
    return frame


cdef tuple _numpy_batch_columns(
        const reader_batch* batch, list col_kinds,
        size_t n_cols, size_t row_count, object np):
    cdef size_t col_idx
    cdef reader_column_kind kind
    chunks = []
    masks = []
    for col_idx in range(n_cols):
        kind = <reader_column_kind><int>col_kinds[col_idx]
        chunks.append(_numpy_column_chunk(batch, col_idx, kind, row_count, np))
        if _is_hybrid_int(kind):
            masks.append(_numpy_validity_mask(batch, col_idx, row_count, np))
        else:
            masks.append(None)
    return (chunks, masks)


cdef object _numpy_frame_from_cursor(_CursorHandle handle):
    import numpy as np
    import pandas as pd
    cdef reader_cursor* cursor
    cdef questdb_error* err = NULL
    cdef const reader_batch* batch
    cdef reader_symbol_dict sd
    cdef size_t n_cols = 0
    cdef size_t row_count = 0
    cdef size_t col_idx
    cdef size_t prev_dict_n = 0
    cdef bint first = True
    cdef bint has_symbol = False
    cdef int seen_seq

    if handle is None or handle._cursor == NULL:
        raise QuestDBError(QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
    seen_seq = handle._reset_seq

    col_names = []
    col_kinds = []
    col_scales = []
    col_precision = []
    col_chunks = []
    col_masks = []
    symbol_categories = []

    try:
        while True:
            with handle._lock:
                cursor = handle._cursor
                if cursor == NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
                with nogil:
                    batch = reader_cursor_next_batch(cursor, &err)
                if handle._reset_seq != seen_seq:
                    # Mid-query failover replayed from batch-0: discard the
                    # pre-failover accumulation and re-derive the schema.
                    seen_seq = handle._reset_seq
                    first = True
                    prev_dict_n = 0
                    has_symbol = False
                    col_chunks = []
                    col_masks = []
                    symbol_categories = []
                if batch == NULL:
                    if err != NULL:
                        raise _reader_err_to_py(err)
                    break
                row_count = reader_batch_row_count(batch)
                if first:
                    (col_names, col_kinds, col_scales, col_precision,
                     has_symbol) = _numpy_extract_meta(batch)
                    n_cols = <size_t>len(col_names)
                    col_chunks = [[] for _ in range(n_cols)]
                    col_masks = [[] for _ in range(n_cols)]
                    first = False
                if has_symbol:
                    _reader_check(
                        reader_batch_symbol_dict(batch, &sd, &err), err,
                        'reader_batch_symbol_dict')
                    if sd.entry_count > prev_dict_n:
                        _symbol_categories_extend(
                            symbol_categories, &sd, prev_dict_n)
                        prev_dict_n = sd.entry_count
                batch_chunks, batch_masks = _numpy_batch_columns(
                    batch, col_kinds, n_cols, row_count, np)
            for col_idx in range(n_cols):
                col_chunks[col_idx].append(batch_chunks[col_idx])
                col_masks[col_idx].append(batch_masks[col_idx])
    except:
        handle._free()
        raise

    _mark_reader_drained(handle)
    handle._free()

    if first:
        return pd.DataFrame()
    return _numpy_assemble_frame(
        col_names, col_kinds, col_scales, col_precision,
        col_chunks, symbol_categories, np, pd, col_masks)


cdef class _PolarsSymbolRegistry:
    """A polars ``Categories`` shared by every SYMBOL column on the same
    (append-only) connection dictionary — interned once and grown as the dict
    grows, so a QWP code is its own physical categorical code and casts straight
    into a ``Categorical`` with no per-row remap (the Rust ``SymbolRegistry``
    analog). The interned dictionary is pinned in ``base`` to stop polars'
    auto-GC mapping from dropping it between calls."""
    cdef object pl
    cdef object cats
    cdef object base
    cdef object pinned
    cdef Py_ssize_t n

    def __cinit__(self, object pl):
        self.pl = pl
        self.cats = pl.Categories.random('questdb_symbol', physical=pl.UInt32)
        self.base = None
        self.pinned = None
        self.n = 0

    def accepts(self, object cats_arrow):
        # True if this registry's Categories maps `cats_arrow`'s codes
        # correctly: the smaller of (pinned, cats_arrow) must be a prefix of the
        # larger. The connection dict is append-only, so columns sharing it only
        # ever differ by a growth suffix; a column-local dict fails the check and
        # gets its own registry. `equals` short-circuits on the shared buffer.
        cdef Py_ssize_t m
        if self.pinned is None:
            return True
        m = len(cats_arrow)
        if m <= self.n:
            return self.pinned.slice(0, m).equals(cats_arrow)
        return cats_arrow.slice(0, self.n).equals(self.pinned)

    def column(self, object name, object codes, object cats_arrow):
        cdef object pl = self.pl
        if cats_arrow is not None and len(cats_arrow) > self.n:
            self.base = pl.Series(
                pl.from_arrow(cats_arrow), dtype=pl.Categorical(self.cats))
            self.pinned = cats_arrow
            self.n = len(cats_arrow)
        return codes.cast(pl.UInt32).cast(pl.Categorical(self.cats)).alias(name)


cdef tuple _polars_dict_codes_cats(object col, object pl):
    # col: a pyarrow ChunkedArray of dictionary type, one chunk per wire batch.
    # The dict is append-only and shared across the query — the Rust egress
    # attaches the full active connection dict to every batch and only ever grows
    # it (see `SymbolValuesCache` in c-questdb-client), and `Table.from_batches`
    # keeps the chunks in emission order — so the last chunk's dictionary is the
    # largest and covers every (global, stable) code. Returns (polars Series of
    # the dict indices with nulls preserved via Arrow validity, the full
    # dictionary values array). The indices flow straight from Arrow to polars
    # (no numpy round-trip, no -1 sentinel).
    cdef list chunks = col.chunks
    cdef list parts = []
    cdef object ch
    if not chunks:
        return (pl.Series([], dtype=pl.UInt32), None)
    for ch in chunks:
        parts.append(pl.from_arrow(ch.indices))
    return (pl.concat(parts) if len(parts) > 1 else parts[0],
            chunks[-1].dictionary)


cdef object _cast_to_string_view(object col, object svt, object pa, object pc):
    # Cast a Utf8 / LargeUtf8 ChunkedArray to Arrow ``string_view``, repairing the
    # null trailing variadic data buffer that pyarrow's cast leaves behind for a
    # chunk whose every value is inline (<= 12 bytes). The null buffer validates
    # fine in-process, but the Arrow C-Data-Interface exporter dereferences it
    # unconditionally — so it crashes (SIGSEGV) the moment polars re-exports the
    # column across the C-ABI. Swapping the null for a shared 0-length buffer is
    # zero-copy and makes the export safe. A natively built string_view already
    # uses an empty (non-null) buffer here, so only the cast output needs fixing.
    cdef list chunks = []
    cdef object empty = None
    cdef object ch, view, bufs
    for ch in col.chunks:
        view = pc.cast(ch, svt)
        bufs = view.buffers()
        if bufs and bufs[-1] is None:
            if empty is None:
                empty = pa.allocate_buffer(0)
            view = pa.Array.from_buffers(
                svt, len(view), bufs[:-1] + [empty],
                null_count=view.null_count, offset=view.offset)
        chunks.append(view)
    return pa.chunked_array(chunks, type=svt)


cdef object _polars_nonsymbol_frame(
        object table, list nd_idx, object pl, object pa):
    # `pl.from_arrow` for the non-SYMBOL columns. Utf8 / LargeUtf8 columns are
    # first cast to Arrow `string_view` (when pyarrow exposes it) so polars
    # adopts the byte/view buffers zero-copy — its `String` dtype *is* the view
    # ("German strings") layout — instead of rebuilding the view from the offset
    # layout. Fixed-width columns are already adopted zero-copy.
    if not nd_idx:
        return None
    cdef object tbl = table.select(nd_idx)
    cdef object types = tbl.schema.types
    cdef object sv = getattr(pa, 'string_view', None)
    cdef object svt
    cdef Py_ssize_t j
    if sv is not None and any(
            pa.types.is_string(t) or pa.types.is_large_string(t) for t in types):
        import pyarrow.compute as pc
        svt = sv()
        for j in range(len(types)):
            if pa.types.is_string(types[j]) or pa.types.is_large_string(types[j]):
                tbl = tbl.set_column(
                    j, tbl.schema.field(j).with_type(svt),
                    _cast_to_string_view(tbl.column(j), svt, pa, pc))
    return pl.from_arrow(tbl)


cdef object _polars_dataframe_hybrid(
        object table, object pl, object pa, dict registries):
    # SYMBOL (dictionary) columns are built from codes + dict via a `Categories`
    # registry (low CPU, no per-row remap); every other column keeps its exact
    # `pl.from_arrow` dtype. One shared registry (key -1) serves every column on
    # the connection dict — interned once, not per column — and falls back to a
    # per-column registry for a column-local dict. `registries` persists across
    # batches so a streaming `iter_polars` stitches via one `Categories`.
    cdef list types = table.schema.types
    cdef list is_dict = [pa.types.is_dictionary(t) for t in types]
    if not any(is_dict):
        return pl.from_arrow(table)
    cdef list names = table.column_names
    cdef list nd_idx = [i for i in range(len(types)) if not is_dict[i]]
    nd = _polars_nonsymbol_frame(table, nd_idx, pl, pa)
    cdef list cols = []
    cdef Py_ssize_t i
    cdef object codes, cats, reg
    cdef object shared = registries.get(-1)
    for i in range(len(types)):
        if is_dict[i]:
            codes, cats = _polars_dict_codes_cats(table.column(i), pl)
            if shared is None:
                shared = _PolarsSymbolRegistry(pl)
                registries[-1] = shared
            if shared.accepts(cats):
                reg = shared
            else:
                reg = registries.get(i)
                if reg is None:
                    reg = _PolarsSymbolRegistry(pl)
                    registries[i] = reg
            cols.append(reg.column(names[i], codes, cats))
        else:
            cols.append(nd.get_column(names[i]))
    return pl.DataFrame(cols)


cdef class _PolarsBatchIter:
    """Streaming `polars.DataFrame` per result batch. Holds the per-symbol
    `Categories` registries so every batch's Categoricals share one identity
    and `pl.concat` stitches cleanly."""
    cdef object reader
    cdef object pl
    cdef object pa
    cdef dict registries
    cdef bint use_hybrid

    def __cinit__(self, _CursorHandle handle, object pl, object pa):
        self.reader = _build_record_batch_reader(handle)
        self.pl = pl
        self.pa = pa
        self.registries = {}
        self.use_hybrid = getattr(pl, 'Categories', None) is not None

    def __iter__(self):
        return self

    def __next__(self):
        batch = next(self.reader)
        table = self.pa.Table.from_batches([batch])
        if not self.use_hybrid:
            return self.pl.from_arrow(table)
        try:
            return _polars_dataframe_hybrid(
                table, self.pl, self.pa, self.registries)
        except Exception:
            return self.pl.from_arrow(table)


cdef class _NumpyBatchIter:
    cdef _CursorHandle handle
    cdef object np
    cdef object pd
    cdef list col_names
    cdef list col_kinds
    cdef list col_scales
    cdef list col_precision
    cdef bint first
    cdef bint has_symbol
    cdef bint done
    cdef size_t prev_dict_n
    cdef list symbol_categories
    cdef object symbol_dtype
    cdef int seen_seq
    cdef bint delivered

    def __cinit__(self, _CursorHandle handle):
        import numpy as np
        import pandas as pd
        self.handle = handle
        self.np = np
        self.pd = pd
        self.col_names = []
        self.col_kinds = []
        self.col_scales = []
        self.col_precision = []
        self.first = True
        self.has_symbol = False
        self.done = False
        self.prev_dict_n = 0
        self.symbol_categories = []
        self.symbol_dtype = None
        self.seen_seq = handle._reset_seq if handle is not None else 0
        self.delivered = False

    def __iter__(self):
        return self

    def __next__(self):
        cdef reader_cursor* cursor
        cdef questdb_error* err = NULL
        cdef const reader_batch* batch
        cdef reader_symbol_dict sd
        cdef size_t row_count
        cdef size_t n_cols
        if self.done or self.handle is None or self.handle._cursor == NULL:
            raise StopIteration
        try:
            with self.handle._lock:
                cursor = self.handle._cursor
                if cursor == NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
                with nogil:
                    batch = reader_cursor_next_batch(cursor, &err)
                if self.handle._reset_seq != self.seen_seq:
                    if self.delivered:
                        # Mid-query failover after batches were already yielded:
                        # the replayed batch-0 would duplicate them. Streaming
                        # can't discard it, so surface a clean, catchable error.
                        self.done = True
                        raise QuestDBError(
                            QuestDBErrorCode.FailoverWouldDuplicate,
                            'mid-query failover would duplicate already-'
                            'delivered batches; re-issue the query')
                    # Pre-delivery failover replays batch-0 transparently;
                    # re-pin to the new stream and keep going.
                    self.seen_seq = self.handle._reset_seq
                if batch == NULL:
                    self.done = True
                    if err != NULL:
                        raise _reader_err_to_py(err)
                    _mark_reader_drained(self.handle)
                    raise StopIteration
                row_count = reader_batch_row_count(batch)
                if self.first:
                    (self.col_names, self.col_kinds, self.col_scales,
                     self.col_precision, self.has_symbol) = \
                        _numpy_extract_meta(batch)
                    self.first = False
                n_cols = <size_t>len(self.col_names)
                if self.has_symbol:
                    _reader_check(
                        reader_batch_symbol_dict(batch, &sd, &err), err,
                        'reader_batch_symbol_dict')
                    if sd.entry_count > self.prev_dict_n:
                        _symbol_categories_extend(
                            self.symbol_categories, &sd, self.prev_dict_n)
                        self.symbol_dtype = self.pd.CategoricalDtype(
                            self.symbol_categories)
                        self.prev_dict_n = sd.entry_count
                batch_chunks, batch_masks = _numpy_batch_columns(
                    batch, self.col_kinds, n_cols, row_count, self.np)
        except:
            self.done = True
            self.handle._free()
            raise
        col_chunks = [[c] for c in batch_chunks]
        col_masks = [[m] for m in batch_masks]
        frame = _numpy_assemble_frame(
            self.col_names, self.col_kinds, self.col_scales,
            self.col_precision, col_chunks, self.symbol_categories,
            self.np, self.pd, col_masks, symbol_dtype=self.symbol_dtype)
        self.delivered = True
        return frame

    def __dealloc__(self):
        if not self.done and self.handle is not None:
            self.handle._free()


cdef object _resolve_arrow_to_pandas_kwargs(dtype_backend, types_mapper):
    kwargs = {}
    if types_mapper is not None:
        kwargs['types_mapper'] = types_mapper
    elif dtype_backend == 'pyarrow':
        import pandas as pd
        kwargs['types_mapper'] = pd.ArrowDtype
    elif dtype_backend == 'numpy_nullable':
        kwargs['types_mapper'] = _numpy_nullable_mapping()
    elif dtype_backend is not None:
        raise ValueError(
            f'dtype_backend={dtype_backend!r} is invalid, '
            'only "pyarrow" and "numpy_nullable" are allowed')
    return kwargs


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
    cdef questdb_db* db
    try:
        db = c._begin_db_use('_debug_egress_pool_stats')
    except QuestDBError:
        return None
    try:
        return (
            questdb_db_dbg_reader_in_use_count(db),
            questdb_db_dbg_reader_free_count(db))
    finally:
        c._end_db_use()


class QueryResult:
    """Result of ``Client.query(sql)``.

    Streams query rows as Arrow record batches. **Single-use**: each
    materialisation method (``to_pandas``, ``to_arrow``, ``iter_arrow``,
    ``iter_pandas``, or the ``__arrow_c_stream__`` PyCapsule protocol)
    consumes the underlying cursor; the second consumption raises
    ``QuestDBError``.

    **Thread affinity**: the underlying cursor is bound to the thread that
    created it (via ``Client.query``). Create, consume, ``cancel``,
    ``close``, and drop the ``QueryResult`` on that same thread; handing it
    to another thread is undefined behaviour even with external
    synchronisation.

    ``__arrow_c_stream__`` is native — the cursor's record batches are
    exposed directly through the Arrow C Data Interface, so polars /
    duckdb / pandas 3.0 / any Arrow-native consumer can read query
    results without pyarrow installed. ``to_arrow`` / ``to_pandas`` /
    ``iter_arrow`` / ``iter_pandas`` are convenience wrappers that
    do require pyarrow.

    **SYMBOL columns**: ``to_polars`` / ``to_pandas`` build the Categorical
    directly, interning the connection dictionary once (no per-row remap).
    ``to_arrow`` / ``iter_arrow`` / ``__arrow_c_stream__`` emit a generic Arrow
    form whose per-batch SYMBOL dictionary is compacted to the values each
    batch uses, which a generic consumer reconciles. So when the target is a
    polars / pandas frame, the dedicated methods avoid the re-reconciliation
    that ``polars.from_arrow(result)`` / ``to_arrow().to_pandas()`` pay on
    SYMBOL-heavy results.

    Example::

        with client.query('SELECT * FROM trades WHERE ts > $1') as result:
            df = polars.from_arrow(result)              # no pyarrow
            # df = result.to_pandas()                   # pyarrow required
            # table = pa.table(result)                  # pyarrow required
    """

    def __init__(self, _CursorHandle cursor_handle):
        self._cursor_handle = cursor_handle
        self._cancel_handle = cursor_handle
        self._consumed = False

    def _take_cursor_handle(self):
        if self._consumed:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'QueryResult already consumed')
        if self._cursor_handle is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'QueryResult cursor was closed')
        self._consumed = True
        handle = self._cursor_handle
        self._cursor_handle = None
        return handle

    def __arrow_c_stream__(self, requested_schema=None):
        """Arrow C stream PyCapsule protocol (no pyarrow needed). SYMBOL
        columns arrive compact — each batch's dictionary holds only the values
        it references — so a consumer that unifies per-batch dictionaries
        (e.g. ``polars.from_arrow``) reconciles them.
        """
        if requested_schema is not None:
            raise NotImplementedError(
                'requested_schema is not supported; consume the stream '
                'and project on the consumer side.')
        # Streaming: hand batches out incrementally. A post-delivery
        # failover bumps the cursor's _reset_seq; the capsule producer
        # surfaces FailoverWouldDuplicate rather than feeding the
        # replayed batch-0 as a duplicate (see _qs_pull).
        return _make_query_stream_capsule(self._take_cursor_handle())

    def to_arrow(self):
        """Read the full result into a ``pyarrow.Table``. Requires pyarrow.

        Materialise-whole: a mid-query failover replays the result
        transparently — the partial accumulation we hold is discarded
        from batch-0. The pyarrow-free streaming path
        (``__arrow_c_stream__`` consumed by ``polars.from_arrow(result)``
        / ``pa.table(result)``) instead surfaces ``FailoverWouldDuplicate``
        on a post-delivery failover.
        """
        import pyarrow as pa
        handle = self._take_cursor_handle()
        schema, batches = _fetch_all_record_batches(handle, pa, True)
        if schema is None:
            return pa.table({})
        return pa.Table.from_batches(batches, schema)

    def to_pandas(self, *, dtype_backend=None, types_mapper=None):
        """Read the full result into a ``pandas.DataFrame``.

        The default is a native (no pyarrow) hybrid built straight from
        the QWP column buffers: a nullable integer column
        with nulls becomes a pandas nullable ``Int*`` (``pd.NA``); without
        nulls it stays plain numpy. ``double``/``float`` stay numpy with
        ``NaN``; ``SYMBOL`` → ``Categorical``; ``TIMESTAMP`` →
        ``datetime64`` (``NaT``); strings/decimal/uuid/binary → ``object``.
        Analysis-safe (aggregations skip ``pd.NA``/``NaN``), and feeds back
        into :meth:`Client.dataframe` for a type round-trip — the column
        kinds are carried in ``df.attrs['questdb']``.

        ``dtype_backend="pyarrow"`` / ``"numpy_nullable"`` / ``types_mapper``
        select the pyarrow-backed path instead (``pd.ArrowDtype``, pandas
        nullable extension dtypes, or a custom mapper) — matching the
        ``pd.read_sql`` / ``pd.read_parquet`` convention.
        """
        if dtype_backend is not None and types_mapper is not None:
            raise ValueError(
                'pass at most one of dtype_backend, types_mapper')
        if dtype_backend is None and types_mapper is None:
            return self._to_pandas_numpy()
        import pyarrow as pa
        handle = self._take_cursor_handle()
        schema, batches = _fetch_all_record_batches(handle, pa)
        table = (pa.table({}) if schema is None
                 else _table_shared_symbol_dict(
                     pa.Table.from_batches(batches, schema)))
        return table.to_pandas(
            **_resolve_arrow_to_pandas_kwargs(dtype_backend, types_mapper))

    def to_polars(self):
        """Read the full result into a ``polars.DataFrame``. Requires polars
        and pyarrow.

        Non-``SYMBOL`` columns keep their exact ``polars.from_arrow`` dtypes
        (tz-aware ``Datetime``, ``Decimal``, ``Binary``, ``List``/``Array``,
        …). ``SYMBOL`` columns are built into a polars ``Categorical`` directly
        from their codes + dictionary through a persistent ``Categories``
        registry — the wire code is its own physical categorical code, so
        there is no per-row ``Dictionary -> Categorical`` remap. Falls back to
        ``polars.from_arrow`` when polars' (unstable) ``Categories`` API is
        unavailable.

        Materialise-whole: a mid-query failover replays the result
        transparently. This accumulates batches in-library (via pyarrow)
        so the partial result can be discarded on failover; for the
        pyarrow-free streaming path consume ``__arrow_c_stream__``
        directly (``polars.from_arrow(result)``), which surfaces
        ``FailoverWouldDuplicate`` on a post-delivery failover.
        """
        try:
            import polars as pl
        except ImportError as ie:
            raise ImportError(
                '`polars` is required for `to_polars()`. '
                'Install with `pip install polars`.') from ie
        try:
            import pyarrow as pa
        except ImportError as ie:
            raise ImportError(
                '`pyarrow` is required for `to_polars()`. '
                'Install with `pip install pyarrow`.') from ie
        handle = self._take_cursor_handle()
        schema, batches = _fetch_all_record_batches(handle, pa)
        if schema is None:
            return pl.from_arrow(pa.table({}))
        table = pa.Table.from_batches(batches, schema)
        if getattr(pl, 'Categories', None) is None:
            return pl.from_arrow(table)
        try:
            return _polars_dataframe_hybrid(table, pl, pa, {})
        except Exception:
            return pl.from_arrow(table)

    def _to_pandas_numpy(self):
        return _numpy_frame_from_cursor(self._take_cursor_handle())

    def iter_arrow(self):
        """Iterate result batches as ``pyarrow.RecordBatch``.

        Streaming: a mid-query failover after the first batch has been
        yielded surfaces ``QuestDBErrorCode.FailoverWouldDuplicate`` (the
        already-yielded batches cannot be discarded); re-issue the query.
        If the iterator is abandoned partway, cleanup runs at the next
        garbage-collection cycle; call :meth:`close` (or use the context-
        manager) for deterministic release.
        """
        reader = _build_record_batch_reader(self._take_cursor_handle(), True)
        for batch in reader:
            yield batch

    def iter_pandas(self, *, dtype_backend=None, types_mapper=None):
        """Iterate result batches as ``pandas.DataFrame``.

        Mirrors :meth:`to_pandas`: with no arguments each batch is
        materialised straight into numpy (no pyarrow, sentinel-preserving,
        ``df.attrs['questdb']`` per batch). ``dtype_backend`` /
        ``types_mapper`` select the pyarrow-backed path instead.
        """
        if dtype_backend is not None and types_mapper is not None:
            raise ValueError(
                'pass at most one of dtype_backend, types_mapper')
        if dtype_backend is None and types_mapper is None:
            return _NumpyBatchIter(self._take_cursor_handle())
        return self._iter_pandas_arrow(dtype_backend, types_mapper)

    def _iter_pandas_arrow(self, dtype_backend, types_mapper):
        import pyarrow as pa
        kwargs = _resolve_arrow_to_pandas_kwargs(dtype_backend, types_mapper)
        reader = _build_record_batch_reader(self._take_cursor_handle())
        for batch in reader:
            table = _table_shared_symbol_dict(pa.Table.from_batches([batch]))
            yield table.to_pandas(**kwargs)

    def iter_polars(self):
        """Iterate result batches as ``polars.DataFrame``.

        Mirrors :meth:`to_polars` per batch (same ``Categorical`` SYMBOL
        handling) for streaming / low-peak-memory consumption. Every batch's
        SYMBOL Categoricals share one persistent ``Categories`` identity, so
        ``polars.concat`` over the yielded frames stitches without a
        categories-mismatch error.

        Streaming: a mid-query failover after the first batch has been yielded
        surfaces ``QuestDBErrorCode.FailoverWouldDuplicate``; re-issue the
        query. Requires polars and pyarrow.
        """
        try:
            import polars as pl
        except ImportError as ie:
            raise ImportError(
                '`polars` is required for `iter_polars()`. '
                'Install with `pip install polars`.') from ie
        try:
            import pyarrow as pa
        except ImportError as ie:
            raise ImportError(
                '`pyarrow` is required for `iter_polars()`. '
                'Install with `pip install pyarrow`.') from ie
        return _PolarsBatchIter(self._take_cursor_handle(), pl, pa)

    def cancel(self):
        """Ask the server to stop streaming. Idempotent.

        Distinct from :meth:`close`: ``cancel`` sends a cancellation
        frame to QuestDB so the server can drop in-flight work;
        ``close`` only releases local resources. A subsequent batch
        pull after ``cancel`` typically surfaces
        ``QuestDBErrorCode.Cancelled``.
        """
        cdef _CursorHandle handle = self._cancel_handle
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef reader_cursor* cursor
        if handle is None:
            return
        with handle._lock:
            cursor = handle._cursor
            if cursor == NULL:
                return
            with nogil:
                ok = reader_cursor_cancel(cursor, &err)
        if not ok:
            if err != NULL:
                raise _reader_err_to_py(err)
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'reader_cursor_cancel returned false '
                'without setting err_out')

    def close(self):
        """Release the cursor + reader. Idempotent.

        Does not send a cancellation frame; use :meth:`cancel` first if
        you need the server to stop work. After ``close``, any
        previously-returned iterator that hasn't been exhausted will
        fail on its next pump with
        ``QuestDBErrorCode.InvalidApiCall``.
        """
        cdef _CursorHandle handle = self._cancel_handle
        self._cursor_handle = None
        self._cancel_handle = None
        self._consumed = True
        if handle is not None:
            handle._free()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
