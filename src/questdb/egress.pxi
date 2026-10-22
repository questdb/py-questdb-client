# Egress (QWP/WebSocket reader) Cython glue.
#
# `QueryResult` exposes the Arrow PyCapsule Interface
# (`__arrow_c_stream__`) directly off the Rust cursor, so polars /
# duckdb / other Arrow-native consumers can read query results
# without pyarrow. `to_pandas` / `iter_pandas` default to a
# pyarrow-free native numpy path; `to_arrow` / `iter_arrow` /
# `to_polars` / `iter_polars` lazy-import pyarrow.

cimport numpy as cnp

import _thread
import collections


cdef inline object _reader_err_to_py(questdb_error* err):
    """Construct a ``QuestDBError`` from a ``questdb_error*`` and free it."""
    return c_err_to_py(err)


cdef class _ReaderHandle:
    """Owns a ``qwp_reader*``.

    ``_close()`` runs eagerly when the owning ``_CursorHandle`` is
    freed (clean drain, ``QueryResult.close()``, or consumer
    teardown); ``__dealloc__`` is the idempotent backstop. On close
    the reader either returns to its pool or is dropped,
    depending on the ``reader``'s own ownership tag (set when it
    was constructed — see ``ReaderOwnership`` in the Rust FFI):

    - Pool-borrowed readers go back to the pool unless
      ``_must_close`` was set, in which case the pool drops them.
    - Standalone readers (from ``reader_from_conf``) are always
      dropped.

    The Python side carries only one extra bit of state —
    ``_must_close`` — which it forwards to the FFI via
    ``qwp_reader_drop_on_return`` before calling close. We never
    hold a raw ``questdb_db*`` pointer here: the reader struct
    holds an ``Arc<DbInner>`` internally, so the pool stays alive
    even if the user's ``QuestDB.close()`` ran after ``query()``
    returned but before the reader closed.

    ``_oidc_auth`` and ``_bearer_token_provider`` retain their Python callback
    owners while this reader may reconnect, including when a result/stream
    outlives its QuestDB handle.

    ``_must_close`` defaults to ``True`` and is cleared only once the
    cursor is known to sit at a terminal frame on a live transport:
    by the clean-drain paths, and by ``_CursorHandle._free()`` itself
    whenever the native cursor reports its connection reusable (a
    clean end, a successful cancel, or a server-side error such as a
    parse error or a query timeout). An abandoned-mid-stream cursor
    never qualifies: the Rust Cursor::Drop closes the transport
    whenever ``cursor_active`` is still set at drop time, so recycling
    such a reader would hand the next borrower a broken pipe.
    """
    cdef qwp_reader* _reader
    cdef bint _must_close
    # Keep the Python callback target alive for as long as a reader can
    # silently refresh its native auth clone (including after QuestDB.close).
    cdef object _oidc_auth
    # Match the pool/sender holder's GC-visible ownership on PyPy.
    cdef dict __dict__

    def __cinit__(self):
        self._reader = NULL
        self._must_close = True
        self._oidc_auth = None
        self._bearer_token_provider = None

    cdef _attach(self, qwp_reader* reader):
        self._reader = reader

    cdef void _close(self, bint dealloc=False) noexcept:
        cdef PyThreadState* gs = NULL
        if self._reader == NULL:
            return
        if self._must_close:
            qwp_reader_drop_on_return(self._reader)
        _ensure_doesnt_have_gil(&gs)
        qwp_reader_close(self._reader)
        _ensure_has_gil(&gs)
        self._reader = NULL
        self._oidc_auth = None
        # Leave dictionary cleanup to Cython/PyPy during __dealloc__: normal
        # attribute access would recreate the dying PyPy wrapper.
        if not dealloc:
            self._bearer_token_provider = None

    def __dealloc__(self):
        self._close(dealloc=True)


@cython.no_gc_clear
cdef class _CursorHandle:
    """Owns a ``qwp_reader_cursor*`` + back-ref to its reader.

    The re-entrant lock is the publication boundary for every native
    cursor field. It serialises cursor operations, close, Arrow consumer
    callbacks, and cross-thread GC finalisation, and supplies the
    happens-before edge required when those operations move between
    threads. Concurrent public operations are still unsupported.

    Being re-entrant, the lock does not protect a decoder from code
    running on its *own* thread: a cyclic-GC finaliser (a
    ``PooledReader`` reachable only through a cycle, collected while a
    per-cell constructor runs) or a signal handler calling ``close()``
    re-acquires it freely. The decoders therefore ``_pin()`` the handle
    while they hold pointers into the current batch; a ``_free()`` that
    arrives while pinned is deferred to ``_unpin()``, and ``cancel()``
    refuses, instead of pulling the batch out from under the decoder.
    Every locked network call is pinned too, so a close whose lock wait
    was interrupted (Ctrl-C while another thread holds the lock) is
    finished by that thread once its current call returns.

    ``_pending_batch`` is the row stream's first batch, left undecoded on
    the cursor between ``_open_row_stream`` and the iterator's first step.
    Every call that invalidates it (free, cancel) clears it under the lock.

    ``_close()`` releases both: the cursor is freed and the reader closed
    (returned to its pool or dropped) in the same locked section;
    ``_free()`` is its non-raising form. The
    drain / close / consumer-teardown paths call it eagerly;
    ``__dealloc__`` is the backstop. Before freeing, it asks the native
    cursor whether its connection is reusable — at a terminal frame on
    a live transport — and clears ``_reader_ref._must_close`` when so.
    That covers clean drains, successful cancels and server-side error
    terminals alike; a cursor abandoned mid-stream stays unrecyclable.

    ``_owns_reader`` selects who releases the reader. The one-shot
    ``QuestDB.query(sql)`` path leaves it ``True``: the cursor is the
    reader's sole owner, so ``_free()`` closes the reader eagerly. A
    ``PooledReader`` lease sets it ``False``: the lease keeps its
    reader across queries, so ``_free()`` releases only the cursor and
    drops the back-reference. A lease released while its cursor's free
    is deferred hands the reader to that cursor.

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
    cdef qwp_reader_cursor* _cursor
    cdef const qwp_reader_batch* _pending_batch
    cdef _ReaderHandle _reader_ref
    cdef bint _owns_reader
    cdef object _lock
    cdef int _reset_seq
    # The RLock does not prevent same-thread re-entry, e.g. from a finalizer
    # or callback that runs on this thread mid-operation.
    cdef bint _native_in_flight
    cdef int _pinned
    cdef bint _free_pending
    cdef bint _cancelled
    cdef bint _rows_cut_short
    cdef bint _exec_done_valid
    cdef uint8_t _exec_done_op_type
    cdef uint64_t _exec_done_rows

    def __cinit__(self):
        self._cursor = NULL
        self._pending_batch = NULL
        self._reader_ref = None
        self._owns_reader = True
        self._lock = threading.RLock()
        self._reset_seq = 0
        self._native_in_flight = False
        self._pinned = 0
        self._free_pending = False
        self._cancelled = False
        self._rows_cut_short = False
        self._exec_done_valid = False
        self._exec_done_op_type = 0
        self._exec_done_rows = 0

    cdef void_int _check_not_driving(
            self, str method, bint check_lock=False) except -1:
        # At entry, reject a lock already owned by this thread: decoding can
        # run Python code while it still borrows the current native batch.
        # Under-lock checks must leave check_lock false.
        if self._native_in_flight or (check_lock and self._lock._is_owned()):
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'{method}() cannot be called during a cursor operation.')

    cdef void _attach(
            self,
            qwp_reader_cursor* cursor,
            _ReaderHandle reader_ref,
            bint owns_reader) noexcept:
        # The handle is new and has not been published yet, so adopting the
        # cursor needs no lock and must not introduce a failure point.
        self._cursor = cursor
        self._reader_ref = reader_ref
        self._owns_reader = owns_reader

    cdef bint _is_live(self):
        with self._lock:
            return self._cursor != NULL

    cdef int _reset_sequence(self):
        with self._lock:
            return self._reset_seq

    cdef void _free_locked(self) noexcept:
        """The body of `_close`. Caller must hold `_lock` and have checked
        that the handle is not pinned."""
        cdef PyThreadState* gs = NULL
        cdef qwp_reader_cursor* cursor
        if self._native_in_flight:
            return
        self._native_in_flight = True
        try:
            cursor = self._cursor
            if cursor != NULL:
                if qwp_reader_cursor_connection_reusable(cursor):
                    self._mark_drained()
                self._cursor = NULL
                self._pending_batch = NULL
                _ensure_doesnt_have_gil(&gs)
                qwp_reader_cursor_free(cursor)
                _ensure_has_gil(&gs)
            if self._reader_ref is not None:
                if self._owns_reader:
                    self._reader_ref._close()
                self._reader_ref = None
        finally:
            self._native_in_flight = False

    cdef void _mark_drained(self) noexcept:
        # Only valid once the cursor sits at its terminal frame: the Rust
        # `Cursor::Drop` tears the transport down whenever `cursor_active`
        # is still set, so recycling earlier would hand the next borrower a
        # broken pipe. The EXEC_DONE report is latched here because every
        # drain path frees the cursor immediately afterwards.
        cdef uint8_t op_type = 0
        cdef uint64_t rows_affected = 0
        if self._reader_ref is not None:
            self._reader_ref._must_close = False
        if self._cursor != NULL and not self._exec_done_valid:
            if qwp_reader_cursor_terminal_exec_done(
                    self._cursor, &op_type, &rows_affected):
                self._exec_done_op_type = op_type
                self._exec_done_rows = (
                    0 if rows_affected == QWP_READER_ROWS_AFFECTED_UNKNOWN
                    else rows_affected)
                self._exec_done_valid = True

    cdef void _pin(self) noexcept:
        # Called under `_lock` around a native call or a batch decode; until
        # the matching `_unpin`, a free is deferred (see the class
        # docstring).
        self._pinned += 1

    cdef void _unpin(self) noexcept:
        self._pinned -= 1
        if self._pinned == 0 and self._free_pending:
            self._free_pending = False
            self._free()

    cdef int _close(self) except -1:
        # An interrupted acquire (Ctrl-C while another thread holds the lock
        # in a network read) leaves the free to that thread's next `_unpin`.
        try:
            self._lock.acquire()
        except BaseException:
            self._free_pending = True
            raise
        try:
            if self._pinned > 0:
                # Same-thread re-entry mid-decode; `_unpin` finishes it.
                self._free_pending = True
            else:
                self._free_locked()
        finally:
            self._lock.release()
        return 0

    cdef void _free(self) noexcept:
        cdef PyObject* exc_type = NULL
        cdef PyObject* exc_value = NULL
        cdef PyObject* exc_tb = NULL
        # Capsule teardown can run with an exception pending; clear it
        # while acquiring the lock, then restore the caller's exception.
        # `_reclaim_cursor_no_wait` does the same for the finalizers.
        PyErr_Fetch(&exc_type, &exc_value, &exc_tb)
        try:
            self._close()
        except KeyboardInterrupt:
            if exc_type == NULL:
                _rearm_interrupt()
        except BaseException:
            if exc_type == NULL:
                raise
        PyErr_Restore(exc_type, exc_value, exc_tb)

    cdef int _try_reclaim(self):
        """Free from a finalizer without ever blocking.

        `_free` holds `_lock` across `qwp_reader_cursor_free` and
        `_ReaderHandle._close`, both of which release the GIL. A worker parked
        in one of those at interpreter finalization is frozen there holding the
        lock, and a blocking acquire from the shutdown GC -- which runs
        finalizers with the GIL held -- would then never return, hanging the
        interpreter. Cursor owners can be handed to another thread, so every
        finalizer and GC-triggered release path must use this non-blocking form.

        Returns 1 when it freed an un-drained cursor (the caller should warn),
        0 when it freed a drained one or there was nothing to free, and -1 when
        the lock was busy and it did nothing. The lock holder need not be a
        freeing operation; ``__dealloc__`` remains the eventual fallback.

        "Busy" includes a lock held by the calling thread itself. The lock is
        re-entrant, so a plain non-blocking acquire succeeds there -- and the
        holder may be a decoder on this very thread reading the cursor's
        borrowed batch buffers (``_NumpyBatchIter.__next__``,
        ``_numpy_frame_from_cursor``) when the cyclic GC collects the lease
        that owns this cursor. Freeing the cursor then left the decoder
        reading freed memory.

        A busy handle that a decoder or native call has pinned does not
        drop the free, though: it is left to that holder's ``_unpin()``, as
        ``_close()`` leaves it, so a lease collected mid-decode still ends
        the stream once the decoder lets go of the batch. The return value
        is -1 all the same; the caller must not touch the cursor.
        """
        cdef bint undrained
        if (self._native_in_flight
                or self._lock._is_owned()
                or not self._lock.acquire(False)):
            if self._pinned > 0:
                self._free_pending = True
            return -1
        try:
            if self._native_in_flight:
                return -1
            if self._cursor == NULL:
                return 0
            undrained = (
                self._reader_ref is None or self._reader_ref._must_close)
            self._free_locked()
        finally:
            self._lock.release()
        return 1 if undrained else 0

    def __dealloc__(self):
        self._free()


cdef void _rearm_interrupt() noexcept:
    try:
        _thread.interrupt_main()
    except BaseException:
        pass


cdef void _reclaim_cursor_no_wait(_CursorHandle handle) noexcept:
    """Best-effort cursor cleanup for GC and foreign release callbacks.

    A busy handle necessarily remains owned by the operation holding its lock;
    that owner performs the eventual `_CursorHandle.__dealloc__` fallback after
    releasing the lock. Finalizers must not wait for it, especially while the
    interpreter is stopping and worker threads can no longer make progress.
    """
    cdef PyObject* exc_type = NULL
    cdef PyObject* exc_value = NULL
    cdef PyObject* exc_tb = NULL
    if handle is None:
        return
    # A stream release can run while its caller unwinds an exception
    # (pyarrow drops its reader, Cython drops a capsule local). Set that
    # exception aside: the lock calls in `_try_reclaim` fail while one is
    # pending, which would lose the exception and could leave the lock held.
    PyErr_Fetch(&exc_type, &exc_value, &exc_tb)
    try:
        handle._try_reclaim()
    except BaseException:
        # Finalizers and Arrow release callbacks cannot propagate. The handle's
        # owning operation still retains it and supplies the eventual fallback.
        pass
    PyErr_Restore(exc_type, exc_value, exc_tb)


def _debug_new_cursor_handle():
    """Internal test seam: an empty handle is sufficient to test lock policy."""
    return _CursorHandle()


def _debug_hold_cursor_handle_lock(
        _CursorHandle handle, object entered, object release):
    with handle._lock:
        entered.set()
        release.wait()


def _debug_try_reclaim_cursor_handle(_CursorHandle handle):
    return handle._try_reclaim()


cdef object _fetch_one_batch(
        _CursorHandle handle, object pa_module, bint compact=False):
    """Pull one batch via qwp_reader_cursor_next_arrow_batch.

    Returns:
      - None on clean end-of-stream.
      - A pyarrow.RecordBatch on success.
    Raises QuestDBError on FFI error.
    """
    cdef ArrowArray array
    cdef ArrowSchema schema
    cdef questdb_error* err = NULL
    cdef qwp_reader_arrow_batch_result result
    cdef qwp_reader_cursor* cursor

    # The native Arrow export leaves both outputs untouched on failure. Mark
    # them released up front so any future shared cleanup path can safely use
    # the Arrow C Data convention (release == NULL) on every return path.
    array.release = NULL
    schema.release = NULL

    handle._check_not_driving('next_arrow_batch', check_lock=True)
    with handle._lock:
        handle._check_not_driving('next_arrow_batch')
        cursor = handle._cursor
        if cursor == NULL:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'cursor is closed')
        handle._native_in_flight = True
        handle._pin()
        try:
            with nogil:
                if compact:
                    result = qwp_reader_cursor_next_arrow_batch_compact(
                        cursor, &array, &schema, &err)
                else:
                    result = qwp_reader_cursor_next_arrow_batch(
                        cursor, &array, &schema, &err)
        finally:
            # Cleared before `_unpin`, which may run a deferred free.
            handle._native_in_flight = False
            handle._unpin()

    if result == qwp_reader_arrow_batch_ok:
        # Hand ownership of the array + schema buffers to pyarrow.
        # _import_from_c moves the structs and nulls their release
        # callbacks; pyarrow's RecordBatch owns the buffers from here.
        try:
            batch = pa_module.RecordBatch._import_from_c(
                <uintptr_t>&array, <uintptr_t>&schema)
            _oidc_raise_unscheduled_main()
            return batch
        except:
            if array.release != NULL:
                array.release(&array)
            if schema.release != NULL:
                schema.release(&schema)
            raise

    if result == qwp_reader_arrow_batch_end:
        _oidc_raise_unscheduled_main()
        return None

    # Error path.
    if err == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'qwp_reader_cursor_next_arrow_batch returned error '
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
    cdef int seen_seq = handle._reset_sequence()
    cdef int current_seq
    cdef object schema = None
    cdef list batches = []
    cdef object batch
    try:
        while True:
            batch = _fetch_one_batch(handle, pa_module, compact)
            current_seq = handle._reset_sequence()
            if current_seq != seen_seq:
                # Mid-query failover replayed from batch-0: drop the
                # pre-failover accumulation and re-pin the schema.
                seen_seq = current_seq
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
    try:
        _mark_reader_drained(handle)
    finally:
        handle._free()
    return (schema, batches)


def _record_batch_generator(
        object first,
        _CursorHandle cursor_handle,
        object pa,
        bint compact,
        int seen_seq):
    """Yield Arrow batches and never block when generator finalization runs."""
    try:
        yield first
        while True:
            nxt = _fetch_one_batch(cursor_handle, pa, compact)
            if cursor_handle._reset_sequence() != seen_seq:
                # Mid-query failover after batches were already yielded: the
                # replayed batch-0 would duplicate what the consumer holds.
                # Streaming cannot discard it, so surface a catchable error.
                raise QuestDBError(
                    QuestDBErrorCode.FailoverWouldDuplicate,
                    'mid-query failover would duplicate already-delivered '
                    'batches; re-issue the query')
            if nxt is None:
                # Reached terminal cleanly; reader is reusable.
                _mark_reader_drained(cursor_handle)
                return
            yield nxt
    finally:
        _reclaim_cursor_no_wait(cursor_handle)


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

    try:
        first = _fetch_one_batch(cursor_handle, pa, compact)
    except:
        cursor_handle._free()
        raise
    if first is None:
        # No result set (a non-SELECT statement ends with EXEC_DONE and
        # ships no RESULT_BATCH), so there is no schema to surface; an
        # empty SELECT still ships a zero-row batch carrying the schema.
        try:
            _mark_reader_drained(cursor_handle)
        finally:
            _reclaim_cursor_no_wait(cursor_handle)
        empty = pa.table({})
        return empty.to_reader()

    # Pin the sequence after the first batch: a pre-delivery failover
    # replays batch-0 transparently into ``first``, so only resets that
    # happen once batches are flowing can duplicate already-yielded data.
    cdef int seen_seq = cursor_handle._reset_sequence()
    schema = first.schema
    return pa.RecordBatchReader.from_batches(
        schema,
        _record_batch_generator(
            first, cursor_handle, pa, compact, seen_seq))


cdef void_int _mark_reader_drained(
        _CursorHandle cursor_handle) except -1:
    """Recycle the reader: call only once the cursor reached its terminal."""
    if cursor_handle is None:
        return 0
    with cursor_handle._lock:
        cursor_handle._mark_drained()
    return 0


cdef void_int _drain_cursor(_CursorHandle handle) except -1:
    cdef qwp_reader_cursor* cursor
    cdef questdb_error* err = NULL
    cdef const qwp_reader_batch* batch
    handle._check_not_driving('drain', check_lock=True)
    while True:
        with handle._lock:
            cursor = handle._cursor
            if cursor == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
            handle._check_not_driving('drain')
            handle._native_in_flight = True
            handle._pin()
            try:
                with nogil:
                    batch = qwp_reader_cursor_next_batch(cursor, &err)
            finally:
                # Cleared before `_unpin`, which may run a deferred free.
                handle._native_in_flight = False
                handle._unpin()
        if batch == NULL:
            if err != NULL:
                raise _reader_err_to_py(err)
            _oidc_raise_unscheduled_main()
            break
        _oidc_raise_unscheduled_main()
    try:
        _mark_reader_drained(handle)
    finally:
        handle._free()


cdef _ReaderHandle _borrow_reader_from_pool(questdb_db* db):
    """Borrow a reader from the Rust-side ``questdb_db`` pool.

    Wraps ``questdb_db_borrow_reader`` and packs the result into a
    :class:`_ReaderHandle` that knows it came from this pool, so
    its dealloc returns/drops via the matching FFI.
    """
    cdef questdb_error* err = NULL
    cdef qwp_reader* reader = NULL
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
        with nogil:
            qwp_reader_close(reader)
        raise
    handle._attach(reader)
    # A freshly borrowed reader is pristine: recycle it unless a query
    # arms _must_close at the network boundary (_execute_query).
    handle._must_close = False
    _oidc_raise_unscheduled_main()
    return handle


cdef object _snapshot_server_info(_ReaderHandle handle):
    """Copy the reader's last-seen ``SERVER_INFO`` into a :class:`ServerInfo`.

    The FFI pointer is borrowed and invalidated by any reader operation
    that may reconnect, so every field is copied out before returning.
    """
    cdef const qwp_reader_server_info* si = \
        qwp_reader_current_server_info(handle._reader)
    cdef const char* buf = NULL
    cdef size_t buf_len = 0
    if si == NULL:
        raise QuestDBError(
            QuestDBErrorCode.SocketError,
            'SERVER_INFO unavailable: the connection is mid-reconnect.')
    cdef int role_int = <int>qwp_reader_server_info_role(si)
    role = ServerRole.Other
    for entry in ServerRole:
        if entry.c_value == role_int:
            role = entry
            break
    cdef uint8_t role_byte = qwp_reader_server_info_role_byte(si)
    cdef uint64_t epoch = qwp_reader_server_info_epoch(si)
    cdef uint32_t capabilities = qwp_reader_server_info_capabilities(si)
    cdef int64_t server_wall_ns = qwp_reader_server_info_server_wall_ns(si)
    qwp_reader_server_info_cluster_id(si, &buf, &buf_len)
    cluster_id = PyUnicode_FromStringAndSize(buf, <Py_ssize_t>buf_len) \
        if buf != NULL else ''
    buf = NULL
    buf_len = 0
    qwp_reader_server_info_node_id(si, &buf, &buf_len)
    node_id = PyUnicode_FromStringAndSize(buf, <Py_ssize_t>buf_len) \
        if buf != NULL else ''
    buf = NULL
    buf_len = 0
    zone_id = None
    if qwp_reader_server_info_zone_id(si, &buf, &buf_len):
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
        const qwp_reader_failover_reset_event* event,
        void* user_data) noexcept nogil:
    # Fires synchronously inside qwp_reader_cursor_next_batch while the
    # reader re-executes on a new endpoint, before the replayed batch-0
    # arrives. Honour the C reentrancy contract: no reentrant FFI on the
    # reader/query/cursor, no exception escapes, non-blocking. user_data is
    # a raw int* at the cursor's _reset_seq counter; bumping it is a plain
    # pointer write (no GIL, no Python object touched). Once the handle is
    # published, every cursor drive call holds its RLock, so this write is
    # covered by the same cross-thread hand-off boundary as the native call.
    # The materialise-whole accumulator polls it to discard its pre-failover
    # batches.
    if user_data == NULL:
        return
    (<int*>user_data)[0] += 1


cdef int64_t _EPOCH_ORDINAL = 719163  # datetime.date(1970, 1, 1).toordinal()


cdef void_int _bind_decimal(
        qwp_reader_query* query, object value, Py_ssize_t idx) except -1:
    """Bind a ``decimal.Decimal`` as the narrowest DECIMAL that holds it:
    DECIMAL64 up to 18 digits, DECIMAL128 up to 38, DECIMAL256 up to 76,
    with the fractional digits as the scale. Read off ``as_tuple()`` so the
    decimal context's precision never rounds the value.
    """
    cdef bytes le_bytes
    if not decimal.Decimal.is_finite(value):
        raise ValueError(
            f'query bind ${idx}: DECIMAL has no NaN or infinity: {value!r}')
    sign, digits, exponent = decimal.Decimal.as_tuple(value)
    if digits == (0,):
        exponent = max(min(exponent, 0), -76)
    scale = max(-exponent, 0)
    ndigits = len(digits) + max(exponent, 0)
    if scale > 76:
        raise ValueError(
            f'query bind ${idx}: {value!r} has scale {scale}; the widest '
            f'QuestDB DECIMAL holds scale 76')
    if ndigits > 76:
        raise ValueError(
            f'query bind ${idx}: {value!r} has {ndigits} digits; the widest '
            f'QuestDB DECIMAL holds 76 digits')
    mantissa = int(''.join(map(str, digits))) * 10 ** max(exponent, 0)
    if sign:
        mantissa = -mantissa
    if ndigits <= 18 and scale <= 18:
        qwp_reader_query_bind_decimal64(query, mantissa, scale)
    elif ndigits <= 38 and scale <= 38:
        qwp_reader_query_bind_decimal128(
            query, mantissa & 0xFFFFFFFFFFFFFFFF, mantissa >> 64, scale)
    else:
        le_bytes = mantissa.to_bytes(32, 'little', signed=True)
        qwp_reader_query_bind_decimal256(
            query, <const uint8_t*>PyBytes_AsString(le_bytes), scale)
    return 0


cdef void_int _bind_query_params(qwp_reader_query* query, object binds) except -1:
    """Append positional binds matching the SQL's ``$1``..``$N`` placeholders.

    Bind calls follow the C ABI's deferred-error contract (they return
    void; the only native-side failure surfaces from execute), so the only
    errors raised here are Python-side type rejections.
    """
    cdef bytes utf8
    cdef bytes uuid_bytes
    cdef line_sender_utf8 c_utf8
    cdef line_sender_error* utf8_err = NULL
    cdef Py_ssize_t idx = 0
    for value in binds:
        idx += 1
        if value is None:
            qwp_reader_query_bind_null_varchar(query)
        elif value is True or value is False:
            qwp_reader_query_bind_bool(query, value)
        elif isinstance(value, int):
            if value < INT64_MIN or value > INT64_MAX:
                raise OverflowError(
                    f'query bind ${idx}: int out of the signed 64-bit '
                    f'range QuestDB LONG supports: {value}')
            qwp_reader_query_bind_i64(query, value)
        elif isinstance(value, float):
            qwp_reader_query_bind_f64(query, value)
        elif isinstance(value, str):
            utf8 = value.encode('utf-8')
            if not line_sender_utf8_init(
                    &c_utf8,
                    <size_t>len(utf8),
                    <const char*>utf8,
                    &utf8_err):
                raise c_err_to_py(utf8_err)
            qwp_reader_query_bind_varchar(query, c_utf8)
        elif isinstance(value, TimestampNanos):
            qwp_reader_query_bind_timestamp_nanos(query, value.value)
        elif isinstance(value, TimestampMicros):
            qwp_reader_query_bind_timestamp_micros(query, value.value)
        elif isinstance(value, datetime.datetime):
            qwp_reader_query_bind_timestamp_micros(
                query, datetime_to_micros(value))
        elif isinstance(value, uuid.UUID):
            # The bind takes canonical RFC 4122 big-endian bytes, which is
            # exactly `UUID.bytes`; the native client byte-swaps them into
            # QWP wire order (lo half LE, then hi half LE).
            uuid_bytes = value.bytes
            # `UUID.bytes` is a property a subclass can override, and the
            # bind reads exactly 16 bytes from the pointer, so the length
            # is checked before the buffer is handed over.
            if len(uuid_bytes) != 16:
                raise ValueError(
                    f'query bind ${idx}: uuid.UUID.bytes returned '
                    f'{len(uuid_bytes)} bytes, expected 16.')
            qwp_reader_query_bind_uuid(
                query, <const uint8_t*>PyBytes_AsString(uuid_bytes))
        elif isinstance(value, datetime.date):
            # datetime.datetime matched above; a plain date is a DATE.
            qwp_reader_query_bind_date_millis(
                query, (value.toordinal() - _EPOCH_ORDINAL) * 86_400_000)
        elif isinstance(value, decimal.Decimal):
            _bind_decimal(query, value, idx)
        else:
            raise TypeError(
                f'query bind ${idx}: unsupported type '
                f'{_fqn(type(value))}. Supported: None, bool, int, float, '
                f'str, datetime.datetime, datetime.date, decimal.Decimal, '
                f'TimestampMicros, TimestampNanos, uuid.UUID.')
    return 0


cdef int64_t _query_timeout_to_millis(object timeout) except -2:
    """``None`` → ``-1`` (keep the connect-string default); ``0`` clears it;
    a positive sub-millisecond ``timedelta`` rounds up to 1 ms."""
    cdef object millis = None
    if timeout is None:
        return -1
    if isinstance(timeout, datetime.timedelta):
        if timeout < datetime.timedelta(0):
            raise ValueError('timeout must not be negative')
        return <int64_t>_timedelta_to_millis(timeout)
    if not isinstance(timeout, (bool, numpy.bool_)):
        try:
            millis = operator.index(timeout)
        except TypeError:
            pass
    if millis is None:
        raise TypeError(
            'timeout must be an int (milliseconds), a '
            f'datetime.timedelta, or None, not {_fqn(type(timeout))}')
    if millis < 0:
        raise ValueError('timeout must not be negative')
    if millis > INT64_MAX:
        raise ValueError(
            f'timeout of {millis} ms exceeds the largest supported value '
            f'({INT64_MAX} ms)')
    return <int64_t>millis


def _debug_query_timeout_to_millis(timeout):
    return _query_timeout_to_millis(timeout)


cdef inline bint _execute_failed_before_send(questdb_error* err) noexcept:
    """Whether an execute failure happened before the request was written:
    `qwp_reader_query_execute` returns these codes only from its pre-write
    checks; a failed write is always `SocketError`."""
    cdef questdb_error_code code = questdb_error_get_code(err)
    return (
        code == line_sender_error_unsupported_server
        or code == line_sender_error_invalid_api_call
        or code == line_sender_error_invalid_bind
        or code == line_sender_error_invalid_utf8)


cdef _CursorHandle _execute_query(
        _ReaderHandle reader_handle, str sql, object binds,
        bint reset_symbol_dict=True, bint owns_reader=True,
        int64_t timeout_ms=-1):
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
    cdef qwp_reader_query* query
    cdef qwp_reader_cursor* cursor

    if not line_sender_utf8_init(
            &sql_utf8,
            <size_t>len(sql_bytes),
            <const char*><char*>sql_bytes,
            &utf8_err):
        raise c_err_to_py(utf8_err)

    cdef _CursorHandle handle = _CursorHandle()

    with nogil:
        query = qwp_reader_prepare(reader_handle._reader, sql_utf8, &err)

    if query == NULL:
        if err == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'qwp_reader_prepare returned NULL without setting err')
        raise _reader_err_to_py(err)

    if binds is not None:
        try:
            _bind_query_params(query, binds)
        except:
            qwp_reader_query_free(query)
            raise

    qwp_reader_query_set_reset_symbol_dict(query, reset_symbol_dict)

    if timeout_ms >= 0:
        qwp_reader_query_set_timeout_ms(query, <uint64_t>timeout_ms)

    qwp_reader_query_on_failover_reset(
        query, _failover_reset_trampoline, <void*>&handle._reset_seq)

    # Mark the reader unrecyclable only once we cross the network boundary:
    # a client-side failure above (bad SQL/bind) leaves the reader clean, so
    # a pooled lease must not drop a healthy connection over it.
    cdef bint was_armed = reader_handle._must_close
    reader_handle._must_close = True

    with nogil:
        cursor = qwp_reader_query_execute(&query, &err)

    if cursor == NULL:
        # _query_execute consumes the query (nulls *query_inout); the
        # defensive free is a no-op on the consumed handle.
        qwp_reader_query_free(query)
        if err == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'qwp_reader_query_execute returned NULL without setting err')
        if _execute_failed_before_send(err):
            # Nothing reached the socket: the connection is exactly as
            # healthy as before this call, so hand back whatever state the
            # previous query left rather than dropping it.
            reader_handle._must_close = was_armed
        raise _reader_err_to_py(err)

    handle._attach(cursor, reader_handle, owns_reader)
    _oidc_raise_unscheduled_main()
    return handle


cdef enum:
    _ARROW_METADATA_CLONE_CAP = 1 << 20


cdef size_t _arrow_metadata_byte_len(
        const char* md, size_t max_len, int32_t* kept_n) noexcept:
    cdef int32_t n
    cdef int32_t klen
    cdef int32_t vlen
    cdef size_t pos
    cdef int32_t i
    kept_n[0] = 0
    if max_len < sizeof(int32_t):
        return 0
    memcpy(&n, md, sizeof(int32_t))
    pos = sizeof(int32_t)
    if n <= 0:
        return pos
    for i in range(n):
        if max_len - pos < sizeof(int32_t):
            return pos
        memcpy(&klen, md + pos, sizeof(int32_t))
        if klen < 0 or <size_t>klen > max_len - pos - sizeof(int32_t):
            return pos
        pos += sizeof(int32_t) + <size_t>klen
        if max_len - pos < sizeof(int32_t):
            return pos
        memcpy(&vlen, md + pos, sizeof(int32_t))
        if vlen < 0 or <size_t>vlen > max_len - pos - sizeof(int32_t):
            return pos
        pos += sizeof(int32_t) + <size_t>vlen
        kept_n[0] = i + 1
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
    cdef int32_t kept_n
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
        metadata_len = _arrow_metadata_byte_len(
            src.metadata, _ARROW_METADATA_CLONE_CAP, &kept_n)
        dst.metadata = <const char*>malloc(metadata_len)
        if dst.metadata == NULL:
            _arrow_schema_clone_release(dst)
            return -1
        memcpy(<void*>dst.metadata, src.metadata, metadata_len)
        # Keep the cloned block self-consistent: its leading key/value count
        # must never exceed the entries actually copied, or a downstream
        # consumer would read past the allocation.
        memcpy(<void*>dst.metadata, &kept_n, sizeof(int32_t))
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
    and dropped by the stream's release callback. Before that, a
    producer from `QueryResult.__arrow_c_schema__` is owned by the
    result. The cursor is freed when the stream reaches its end, or at
    release.
    """
    cdef _CursorHandle cursor_handle
    cdef ArrowSchema cached_schema
    cdef ArrowArray cached_array
    cdef bint has_cached_schema
    cdef bint has_cached_array
    cdef bint exhausted
    cdef bint delivered
    cdef char* last_error
    # The exception behind `last_error`, if there is one, so that a
    # Python-level caller (`QueryResult.__arrow_c_schema__`) can raise it
    # with its type and code.
    cdef object last_exc
    cdef int seen_seq

    def __cinit__(self):
        self.cursor_handle = None
        self.has_cached_schema = False
        self.has_cached_array = False
        self.exhausted = False
        self.delivered = False
        self.last_error = NULL
        self.last_exc = None
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


cdef void _qs_set_error(
        _QueryStreamProducer prod, const char* msg, size_t msg_len,
        object exc=None) noexcept:
    if exc is not None:
        # A raised `exc` can reach the consumer's frames through its
        # traceback and context. If a frame holds the stream, that is a
        # cycle through the stream's C reference to `prod`, which the GC
        # cannot see: the cursor and its connection would never be freed.
        exc.__traceback__ = None
        exc.__context__ = None
        exc.__cause__ = None
    prod.last_exc = exc
    if prod.last_error != NULL:
        free(prod.last_error)
        prod.last_error = NULL
    prod.last_error = <char*>malloc(msg_len + 1)
    if prod.last_error == NULL:
        return
    memcpy(prod.last_error, msg, msg_len)
    prod.last_error[msg_len] = 0


cdef int _qs_pull(_QueryStreamProducer prod) noexcept with gil:
    cdef bytes full
    cdef int status
    try:
        status = _qs_pull_impl(prod)
        # Native outputs/errors now have cleanup ownership, including on end
        # and error returns. A failed pending-call enqueue must not leave an
        # intercepted signal parked until an unrelated client operation.
        _oidc_raise_unscheduled_main()
        return status
    except BaseException as e:
        # A successful native pull may already have cached a batch. Discard it
        # so a subsequent get_next cannot deliver data after this stream error.
        prod._free_cached()
        try:
            full = ('arrow batch pull failed: ' + repr(e)).encode('utf-8')
            _qs_set_error(prod, full, <size_t>len(full), e)
        except:
            _qs_set_error(prod, b'arrow batch pull failed', 23)
        prod.exhausted = True
        return -1


cdef int _qs_pull_impl(_QueryStreamProducer prod):
    cdef qwp_reader_cursor* cursor
    cdef ArrowArray local_array
    cdef ArrowSchema local_schema
    cdef questdb_error* err = NULL
    cdef qwp_reader_arrow_batch_result result
    cdef bytes full
    cdef object exc
    cdef int current_seq
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
        prod.cursor_handle._check_not_driving('arrow_stream')
        cursor = prod.cursor_handle._cursor
        if cursor == NULL:
            _qs_set_error(prod, b'cursor is closed', 16)
            prod.exhausted = True
            return -1
        prod.cursor_handle._native_in_flight = True
        prod.cursor_handle._pin()
        try:
            with nogil:
                result = qwp_reader_cursor_next_arrow_batch_compact(
                    cursor, &local_array, &local_schema, &err)
        finally:
            # Cleared before `_unpin`, which may run a deferred free.
            prod.cursor_handle._native_in_flight = False
            prod.cursor_handle._unpin()
    if result == qwp_reader_arrow_batch_ok:
        current_seq = prod.cursor_handle._reset_sequence()
        if current_seq != prod.seen_seq:
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
                    exc = QuestDBError(
                        QuestDBErrorCode.FailoverWouldDuplicate,
                        'mid-query failover would duplicate already-delivered '
                        'batches; re-issue the query')
                    full = _qs_error_text(exc)
                    _qs_set_error(prod, full, <size_t>len(full), exc)
                except:
                    fallback = (
                        b'[FailoverWouldDuplicate] mid-query failover would '
                        b'duplicate already-delivered batches; re-issue the query')
                    _qs_set_error(prod, fallback, <size_t>len(fallback))
                prod.exhausted = True
                return -1
            # Pre-delivery failover: nothing reached the consumer yet, so
            # re-pin to the replayed stream and restart from this batch-0.
            prod.seen_seq = current_seq
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
    if result == qwp_reader_arrow_batch_end:
        prod.exhausted = True
        try:
            _mark_reader_drained(prod.cursor_handle)
        finally:
            # Free the cursor now, not at stream release: a pending
            # `__arrow_c_schema__` producer of a non-SELECT must not keep
            # a `PooledReader` lease busy.
            prod.cursor_handle._free()
        return 0
    if err != NULL:
        # `_reader_err_to_py` frees `err`, also when it raises.
        exc = _reader_err_to_py(err)
        err = NULL
        full = _qs_error_text(exc)
        _qs_set_error(prod, full, <size_t>len(full), exc)
    else:
        _qs_set_error(
            prod,
            b'arrow batch fetch error without err_out', 39)
    prod.exhausted = True
    return -1


cdef bytes _qs_error_text(object exc):
    # The C text of a stream error: "[CodeName] message".
    return ('[' + exc.code.name + '] ' + str(exc)).encode('utf-8')


cdef int _qs_ensure_schema(_QueryStreamProducer prod) noexcept with gil:
    # Pull the first batch if there is no schema yet. A result without a
    # batch (a non-SELECT) gets an empty struct schema.
    if not prod.has_cached_schema:
        if _qs_pull(prod) != 0:
            return 5  # EIO
    if not prod.has_cached_schema:
        if _qs_install_empty_struct_schema(prod) != 0:
            return 12  # ENOMEM
    return 0


cdef int _qs_get_schema(ArrowArrayStream* stream, ArrowSchema* out) noexcept with gil:
    cdef _QueryStreamProducer prod
    cdef int rc
    # duckdb before 1.5 reads `out` even when this call fails.
    memset(out, 0, sizeof(ArrowSchema))
    if stream == NULL or stream.private_data == NULL:
        return 22  # EINVAL
    prod = <_QueryStreamProducer>stream.private_data
    try:
        rc = _qs_ensure_schema(prod)
        if rc != 0:
            return rc
        if _arrow_schema_deep_clone(&prod.cached_schema, out) != 0:
            _qs_set_error(prod, b'failed to clone ArrowSchema', 27)
            return 12  # ENOMEM
        return 0
    except BaseException:
        _qs_set_error(prod, b'arrow schema fetch failed', 25)
        return 5  # EIO


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
    try:
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
    except BaseException:
        _qs_set_error(prod, b'arrow batch fetch failed', 24)
        return 5  # EIO


cdef const char* _qs_get_last_error(ArrowArrayStream* stream) noexcept with gil:
    cdef _QueryStreamProducer prod
    if stream == NULL or stream.private_data == NULL:
        return NULL
    try:
        prod = <_QueryStreamProducer>stream.private_data
        return <const char*>prod.last_error
    except BaseException:
        return NULL


cdef void _qs_release(ArrowArrayStream* stream) noexcept with gil:
    cdef _QueryStreamProducer prod
    cdef PyObject* exc_type
    cdef PyObject* exc_value
    cdef PyObject* exc_tb
    if stream == NULL or stream.private_data == NULL:
        return
    # A dropped capsule can be released while an exception propagates.
    PyErr_Fetch(&exc_type, &exc_value, &exc_tb)
    prod = <_QueryStreamProducer>stream.private_data
    stream.private_data = NULL
    stream.release = NULL
    if prod.cursor_handle is not None:
        _reclaim_cursor_no_wait(prod.cursor_handle)
    try:
        Py_DECREF(prod)
    except BaseException:
        pass
    PyErr_Restore(exc_type, exc_value, exc_tb)


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


cdef _QueryStreamProducer _new_query_stream_producer(_CursorHandle handle):
    cdef _QueryStreamProducer prod = _QueryStreamProducer()
    prod.cursor_handle = handle
    prod.seen_seq = handle._reset_sequence()
    return prod


cdef object _query_stream_capsule(_QueryStreamProducer prod):
    cdef ArrowArrayStream* stream
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


cdef class _ArrowSchemaBox:
    """Owns one deep clone of a result's ArrowSchema. Each
    ``QueryResult.__arrow_c_schema__`` call exports a new clone of it, so
    the call also works after the stream was handed to a consumer."""
    cdef ArrowSchema schema
    cdef bint has_schema

    def __cinit__(self):
        memset(&self.schema, 0, sizeof(ArrowSchema))
        self.has_schema = False

    def __dealloc__(self):
        if self.has_schema and self.schema.release != NULL:
            self.schema.release(&self.schema)
        self.has_schema = False


cdef _ArrowSchemaBox _arrow_schema_box(const ArrowSchema* src):
    cdef _ArrowSchemaBox box = _ArrowSchemaBox()
    if _arrow_schema_deep_clone(src, &box.schema) != 0:
        raise MemoryError()
    box.has_schema = True
    return box


cdef void _arrow_schema_capsule_destructor(object capsule) noexcept:
    cdef ArrowSchema* schema
    if not PyCapsule_IsValid(capsule, b'arrow_schema'):
        return
    schema = <ArrowSchema*>PyCapsule_GetPointer(capsule, b'arrow_schema')
    if schema == NULL:
        return
    # A consumer that imported the schema has moved it and cleared
    # `release`.
    if schema.release != NULL:
        schema.release(schema)
    free(schema)


cdef object _arrow_schema_capsule(_ArrowSchemaBox box):
    cdef ArrowSchema* schema = <ArrowSchema*>malloc(sizeof(ArrowSchema))
    if schema == NULL:
        raise MemoryError()
    # On failure, the clone releases what it allocated.
    if _arrow_schema_deep_clone(&box.schema, schema) != 0:
        free(schema)
        raise MemoryError()
    try:
        return PyCapsule_New(
            <void*>schema, b'arrow_schema', _arrow_schema_capsule_destructor)
    except:
        schema.release(schema)
        free(schema)
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
    <int>qwp_reader_column_kind_boolean: 'boolean',
    <int>qwp_reader_column_kind_byte: 'byte',
    <int>qwp_reader_column_kind_short: 'short',
    <int>qwp_reader_column_kind_int: 'int',
    <int>qwp_reader_column_kind_long: 'long',
    <int>qwp_reader_column_kind_float: 'float',
    <int>qwp_reader_column_kind_double: 'double',
    <int>qwp_reader_column_kind_char: 'char',
    <int>qwp_reader_column_kind_ipv4: 'ipv4',
    <int>qwp_reader_column_kind_timestamp: 'timestamp',
    <int>qwp_reader_column_kind_timestamp_nanos: 'timestamp_ns',
    <int>qwp_reader_column_kind_date: 'date',
    <int>qwp_reader_column_kind_uuid: 'uuid',
    <int>qwp_reader_column_kind_long256: 'long256',
    <int>qwp_reader_column_kind_geohash: 'geohash',
    <int>qwp_reader_column_kind_varchar: 'varchar',
    <int>qwp_reader_column_kind_binary: 'binary',
    <int>qwp_reader_column_kind_symbol: 'symbol',
    <int>qwp_reader_column_kind_double_array: 'double_array',
    <int>qwp_reader_column_kind_long_array: 'long_array',
    <int>qwp_reader_column_kind_decimal64: 'decimal',
    <int>qwp_reader_column_kind_decimal128: 'decimal',
    <int>qwp_reader_column_kind_decimal256: 'decimal',
}


cdef int _reader_check(bint ok, questdb_error** err, str what) except -1:
    # By address, not value: reading `err` in the same argument list as the
    # FFI call that writes it is indeterminately sequenced (C11 6.5.2.2p10).
    if ok:
        return 0
    if err[0] != NULL:
        taken = err[0]
        err[0] = NULL
        raise _reader_err_to_py(taken)
    raise QuestDBError(
        QuestDBErrorCode.ServerFlushError,
        what + ' returned false without err_out')


cdef object _numpy_dtype_for_kind(qwp_reader_column_kind kind, object np):
    if kind == qwp_reader_column_kind_boolean:
        return np.dtype(np.bool_)
    if kind == qwp_reader_column_kind_byte:
        return np.dtype(np.int8)
    if kind == qwp_reader_column_kind_short:
        return np.dtype(np.int16)
    if kind == qwp_reader_column_kind_int:
        return np.dtype(np.int32)
    if kind == qwp_reader_column_kind_long:
        return np.dtype(np.int64)
    if kind == qwp_reader_column_kind_float:
        return np.dtype(np.float32)
    if kind == qwp_reader_column_kind_double:
        return np.dtype(np.float64)
    if kind == qwp_reader_column_kind_char:
        return np.dtype(np.uint16)
    if kind == qwp_reader_column_kind_ipv4:
        return np.dtype(np.uint32)
    if kind == qwp_reader_column_kind_timestamp:
        return np.dtype('datetime64[us]')
    if kind == qwp_reader_column_kind_timestamp_nanos:
        return np.dtype('datetime64[ns]')
    if kind == qwp_reader_column_kind_date:
        return np.dtype('datetime64[ms]')
    return None


cdef object _numpy_fixed_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        qwp_reader_column_kind kind,
        size_t row_count,
        object np):
    cdef qwp_reader_column_data cd
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
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
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
    if kind == qwp_reader_column_kind_boolean:
        return np.frombuffer((<unsigned char[:nbytes]>src), dtype=np.uint8) != 0
    return np.frombuffer((<unsigned char[:nbytes]>src), dtype=dtype).copy()


cdef inline void_int _obj_chunk_set(
        cnp.ndarray out, size_t r, object v) except -1:
    # np.empty(dtype=object) initialises every slot to None, so null rows are
    # left untouched and a mid-loop raise leaves the array safe to release.
    cnp.PyArray_SETITEM(out, cnp.PyArray_GETPTR1(out, <cnp.npy_intp>r), v)


cdef object _numpy_varlen_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        qwp_reader_column_kind kind,
        size_t row_count,
        object np):
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint32_t* offsets
    cdef const uint8_t* data
    cdef const uint8_t* validity
    cdef size_t r
    cdef uint32_t start
    cdef uint32_t end
    cdef cnp.ndarray out
    cdef bint is_binary = kind == qwp_reader_column_kind_binary
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
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
    if data == NULL and cd.var_data_len > 0:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'column kind 0x{:02X} has {} rows but no data buffer'.format(
                <int>kind, row_count))
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
        const qwp_reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint32_t* codes
    cdef const uint8_t* validity
    cdef size_t r
    cdef int64_t[::1] mv
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
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
        list cats, const qwp_reader_symbol_dict* sd, size_t start):
    cdef size_t i
    cdef const qwp_reader_symbol_entry* e
    for i in range(start, sd.entry_count):
        e = &sd.entries[i]
        if <uint64_t>e.offset + <uint64_t>e.length > <uint64_t>sd.heap_len:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'corrupt symbol dictionary heap offsets')
        cats.append(
            PyUnicode_FromStringAndSize(
                <const char*>(sd.heap + e.offset), <Py_ssize_t>e.length))


cdef class _SymbolStrings:
    """A row result's SYMBOL strings by dictionary code, decoded on first use.

    A lease that keeps the connection dictionary warm
    (``reset_symbol_dict=False``) can carry far more entries than a result
    references, so nothing is decoded up front. Within one query the
    dictionary only grows, so a decoded string stays valid for the whole
    result. Each slot named in ``_decoded`` holds its own reference.
    """
    cdef PyObject** _slots
    cdef size_t _cap
    cdef uint32_t* _decoded
    cdef size_t _n_decoded
    cdef size_t _decoded_cap

    def __cinit__(self):
        self._slots = NULL
        self._cap = 0
        self._decoded = NULL
        self._n_decoded = 0
        self._decoded_cap = 0

    def __dealloc__(self):
        cdef size_t i
        for i in range(self._n_decoded):
            Py_XDECREF(self._slots[self._decoded[i]])
        free(self._decoded)
        free(self._slots)

    cdef int _reserve(self, size_t entry_count) except -1:
        cdef size_t cap
        cdef PyObject** slots
        if entry_count <= self._cap:
            return 0
        cap = max(entry_count, 2 * self._cap)
        if self._slots == NULL:
            slots = <PyObject**>calloc(cap, sizeof(PyObject*))
        else:
            slots = <PyObject**>realloc(self._slots, cap * sizeof(PyObject*))
            if slots != NULL:
                memset(slots + self._cap, 0,
                       (cap - self._cap) * sizeof(PyObject*))
        if slots == NULL:
            raise MemoryError()
        self._slots = slots
        self._cap = cap
        return 0

    cdef str _decode(self, const qwp_reader_symbol_dict* sd, uint32_t code):
        cdef const qwp_reader_symbol_entry* e = &sd.entries[code]
        cdef size_t cap
        cdef uint32_t* decoded
        cdef str s
        if <uint64_t>e.offset + <uint64_t>e.length > <uint64_t>sd.heap_len:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'corrupt symbol dictionary heap offsets')
        if self._n_decoded == self._decoded_cap:
            cap = max(<size_t>16, 2 * self._decoded_cap)
            decoded = <uint32_t*>realloc(
                self._decoded, cap * sizeof(uint32_t))
            if decoded == NULL:
                raise MemoryError()
            self._decoded = decoded
            self._decoded_cap = cap
        s = PyUnicode_FromStringAndSize(
            <const char*>(sd.heap + e.offset), <Py_ssize_t>e.length)
        Py_INCREF(s)
        self._slots[code] = <PyObject*>s
        self._decoded[self._n_decoded] = code
        self._n_decoded += 1
        return s


cdef inline uint64_t _geohash_wire_value(
        const unsigned char* src, size_t row, size_t stride, size_t nbytes,
        uint64_t mask) noexcept nogil:
    """Row `row` of a GEOHASH column `stride` bytes wide, zero-extended.

    Wherever 8 bytes remain, it is one unaligned load masked to the
    value's own bytes; the last few rows assemble theirs byte by byte.
    The load reads the wire's little-endian bytes as a host integer,
    which `_numpy_uuid_chunk` also relies on."""
    cdef size_t off = row * stride
    cdef uint64_t v = 0
    cdef size_t b
    if off + 8 <= nbytes:
        memcpy(&v, src + off, 8)
        return v & mask
    for b in range(stride):
        v |= (<uint64_t>src[off + b]) << (8 * b)
    return v


cdef object _numpy_geohash_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef object dtype
    cdef size_t stride
    cdef size_t target
    cdef unsigned int precision
    cdef Py_ssize_t nbytes
    cdef unsigned char* src
    cdef uint64_t mask
    cdef void* dst
    cdef size_t r
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
    stride = cd.value_stride
    precision = cd.geohash_precision_bits
    # The width comes from the precision rather than from the wire
    # stride, and is signed, so that it matches what the Arrow egress
    # gives the same column. A signed width is also what every write
    # route takes as it stands: the native Arrow importer carries a
    # `geohash` claim only on a signed Int8/16/32/64, and an unsigned
    # column reaches it only once `_dataframe_reinterpret_unsigned_geohash`
    # has given it the signed type of the same width.
    #
    # Each width carries a bit of headroom because a precision fills its
    # bits: an 8-bit geohash holds 0..255, which is every bit of an
    # unsigned byte and more than a signed one can express, so it takes
    # the 16-bit slot. That keeps every value positive in its container,
    # which is what the range check on the way back in expects.
    if precision < 1 or precision > 60:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'unexpected geohash precision {} bits'.format(precision))
    if precision <= 7:
        dtype = np.dtype(np.int8)
        target = 1
    elif precision <= 15:
        dtype = np.dtype(np.int16)
        target = 2
    elif precision <= 31:
        dtype = np.dtype(np.int32)
        target = 4
    else:
        dtype = np.dtype(np.int64)
        target = 8
    if stride == 0 or stride > target:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'geohash column of {} bits arrived {} bytes wide'.format(
                precision, stride))
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
    if stride == 1 or stride == 2 or stride == 4:
        # A narrower power-of-two width widens in one vectorized cast.
        return np.frombuffer(
            (<unsigned char[:nbytes]>src),
            dtype=np.dtype('<u{}'.format(stride))).astype(dtype)
    # Strides 3, 5, 6 and 7 have no NumPy integer type to cast from, so
    # each value is assembled from its bytes.
    mask = (<uint64_t>1 << (8 * stride)) - 1
    out = np.empty(<Py_ssize_t>row_count, dtype=dtype)
    dst = cnp.PyArray_DATA(<cnp.ndarray>out)
    with nogil:
        if target == 4:
            for r in range(row_count):
                (<int32_t*>dst)[r] = <int32_t>_geohash_wire_value(
                    src, r, stride, <size_t>nbytes, mask)
        else:
            for r in range(row_count):
                (<int64_t*>dst)[r] = <int64_t>_geohash_wire_value(
                    src, r, stride, <size_t>nbytes, mask)
    return out


cdef object _numpy_uuid_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef object uuid_cls = _uuid.UUID
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef size_t r
    cdef size_t stride
    cdef uint64_t lo
    cdef uint64_t hi
    cdef cnp.ndarray out
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'uuid column has {} rows but no values buffer'.format(row_count))
    validity = cd.validity
    values = <const uint8_t*>cd.values
    stride = cd.value_stride if cd.value_stride != 0 else 16
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        # The reader hands out canonical RFC 4122 big-endian rows,
        # having already reversed them out of QWP wire order: the first
        # eight bytes are the high half and the second eight the low
        # half, each most-significant byte first. `UUID(int=...)` takes
        # the two halves as host-order integers, so each one is byte
        # swapped on the way in. `UUID(bytes=...)` would accept the row
        # verbatim but costs about 70 ns more per row, because CPython
        # builds the same integer from the buffer anyway on top of
        # allocating a `bytes` object for every row.
        memcpy(&hi, values + r * stride, 8)
        memcpy(&lo, values + r * stride + 8, 8)
        hi = bswap64(hi)
        lo = bswap64(lo)
        _obj_chunk_set(
            out, r, uuid_cls(int=((<object>hi) << 64) | (<object>lo)))
    return out


cdef object _numpy_long256_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef size_t r
    cdef size_t stride
    cdef cnp.ndarray out
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'long256 column has {} rows but no values buffer'.format(row_count))
    validity = cd.validity
    values = <const uint8_t*>cd.values
    stride = cd.value_stride if cd.value_stride != 0 else 32
    for r in range(row_count):
        if validity != NULL and ((validity[r >> 3] >> (r & 7)) & 1):
            continue
        _obj_chunk_set(out, r, int.from_bytes(
            PyBytes_FromStringAndSize(<const char*>(values + r * stride), 32),
            'little', signed=False))
    return out


cdef object _numpy_decimal_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef object decimal_cls = Decimal
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef size_t r
    cdef size_t width
    cdef int scale
    cdef cnp.ndarray out
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
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
        _obj_chunk_set(out, r, decimal_cls(f'{unscaled}e{-scale}'))
    return out


cdef object _numpy_array_chunk(
        const qwp_reader_batch* batch,
        size_t col_idx,
        qwp_reader_column_kind kind,
        size_t row_count,
        object np):
    cdef qwp_reader_array_data ad
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
    if kind != qwp_reader_column_kind_double_array:
        raise QuestDBError(
            QuestDBErrorCode.InvalidApiCall,
            'numpy egress supports only double arrays (kind 0x{:02X})'.format(
                <int>kind))
    _reader_check(
        qwp_reader_batch_array_column_data(batch, col_idx, &ad, &err), &err,
        'qwp_reader_batch_array_column_data')
    out = np.empty(row_count, dtype=object)
    if row_count == 0:
        return out
    if ad.data_offsets == NULL or ad.shape_offsets == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'array column has {} rows but no offset tables'.format(row_count))
    if ad.data == NULL and ad.data_len > 0:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'array column has {} data bytes but no data buffer'.format(
                ad.data_len))
    if ad.shapes == NULL and ad.shapes_len > 0:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'array column has {} shape entries but no shapes buffer'.format(
                ad.shapes_len))
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
        const qwp_reader_batch* batch,
        size_t col_idx,
        qwp_reader_column_kind kind,
        size_t row_count,
        object np):
    if kind == qwp_reader_column_kind_symbol:
        return _numpy_symbol_codes_chunk(batch, col_idx, row_count, np)
    if (kind == qwp_reader_column_kind_varchar
            or kind == qwp_reader_column_kind_binary):
        return _numpy_varlen_chunk(batch, col_idx, kind, row_count, np)
    if kind == qwp_reader_column_kind_geohash:
        return _numpy_geohash_chunk(batch, col_idx, row_count, np)
    if kind == qwp_reader_column_kind_uuid:
        return _numpy_uuid_chunk(batch, col_idx, row_count, np)
    if kind == qwp_reader_column_kind_long256:
        return _numpy_long256_chunk(batch, col_idx, row_count, np)
    if (kind == qwp_reader_column_kind_decimal64
            or kind == qwp_reader_column_kind_decimal128
            or kind == qwp_reader_column_kind_decimal256):
        return _numpy_decimal_chunk(batch, col_idx, row_count, np)
    if (kind == qwp_reader_column_kind_double_array
            or kind == qwp_reader_column_kind_long_array):
        return _numpy_array_chunk(batch, col_idx, kind, row_count, np)
    return _numpy_fixed_chunk(batch, col_idx, kind, row_count, np)


cdef bint _is_hybrid_int(qwp_reader_column_kind kind):
    return (kind == qwp_reader_column_kind_int
            or kind == qwp_reader_column_kind_long
            or kind == qwp_reader_column_kind_ipv4
            or kind == qwp_reader_column_kind_geohash)


cdef object _numpy_validity_mask(
        const qwp_reader_batch* batch,
        size_t col_idx,
        size_t row_count,
        object np):
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef Py_ssize_t vbytes
    cdef unsigned char* vsrc
    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
    if row_count == 0 or cd.validity == NULL:
        return None
    vbytes = <Py_ssize_t>((row_count + 7) // 8)
    vsrc = <unsigned char*>cd.validity
    return np.unpackbits(
        np.frombuffer((<unsigned char[:vbytes]>vsrc), dtype=np.uint8),
        count=<Py_ssize_t>row_count, bitorder='little').astype(bool)


cdef object _build_nullable_array(
        values, mask, qwp_reader_column_kind kind, object pd):
    if (kind == qwp_reader_column_kind_float
            or kind == qwp_reader_column_kind_double):
        return pd.arrays.FloatingArray(values, mask)
    if kind == qwp_reader_column_kind_boolean:
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


cdef list _numpy_pinned_name_bytes(tuple pinned):
    """The pinned column names as UTF-8, encoded once per result.

    The per-batch check compares them against the borrowed name buffer
    the reader hands back, so no `PyUnicode` is built per column per
    batch.
    """
    return [(<str>name).encode('utf-8') for name in <list>pinned[0]]


cdef _numpy_check_meta_pinned(
        tuple pinned, list pinned_names, const qwp_reader_batch* batch):
    """Refuse a batch whose schema disagrees with the first one's.

    The NumPy path reads the column names, kinds, decimal scales and
    geohash precisions once and decodes every later batch against them,
    and the round-trip claim it hands back names them too. A batch that
    disagrees would be decoded with the wrong dtype and described by a
    claim that does not match its own values, so it stops here instead.

    The count, the names, the kinds and the two numbers the decoders
    read per batch are all compared. The names are compared as raw
    bytes against the buffer the reader borrows out, because a
    `PyUnicode` per column per batch is the cost this path had removed
    -- and a rename is the one alteration that moves neither the count
    nor the kinds, which is exactly what the reference client checks
    first.
    """
    cdef list pinned_kinds = <list>pinned[1]
    cdef list pinned_scales = <list>pinned[2]
    cdef list pinned_precision = <list>pinned[3]
    cdef size_t n_cols = qwp_reader_batch_column_count(batch)
    cdef size_t col_idx
    cdef qwp_reader_column_kind kind = qwp_reader_column_kind_unknown
    cdef questdb_error* err = NULL
    cdef qwp_reader_column_data cd_meta
    cdef object want
    cdef const char* name_buf = NULL
    cdef size_t name_len = 0
    cdef bytes want_name
    if n_cols != <size_t>len(pinned_kinds):
        _numpy_meta_drift()
    for col_idx in range(n_cols):
        _reader_check(
            qwp_reader_batch_column_name(
                batch, col_idx, &name_buf, &name_len, &err),
            &err, 'qwp_reader_batch_column_name')
        want_name = <bytes>pinned_names[col_idx]
        if (name_len != <size_t>len(want_name)
                or memcmp(name_buf, <const char*>want_name,
                          name_len) != 0):
            _numpy_meta_drift()
        _reader_check(
            qwp_reader_batch_column_kind(batch, col_idx, &kind, &err),
            &err, 'qwp_reader_batch_column_kind')
        if <int>kind != <int>(<object>pinned_kinds[col_idx]):
            _numpy_meta_drift()
        if kind == qwp_reader_column_kind_geohash:
            want = pinned_precision[col_idx]
        elif (kind == qwp_reader_column_kind_decimal64
                or kind == qwp_reader_column_kind_decimal128
                or kind == qwp_reader_column_kind_decimal256):
            want = pinned_scales[col_idx]
        else:
            continue
        if qwp_reader_batch_column_data(batch, col_idx, &cd_meta, &err):
            if kind == qwp_reader_column_kind_geohash:
                if want is not None and cd_meta.geohash_precision_bits != want:
                    _numpy_meta_drift()
            elif want is not None and cd_meta.decimal_scale != want:
                _numpy_meta_drift()
        elif err != NULL:
            questdb_error_free(err)
            err = NULL


def _debug_numpy_pinned_meta(_NumpyBatchIter it):
    """The schema a NumPy-backed iterator pinned from its first batch.

    Not part of the public API. The drift check compares a later batch
    against this, and a result whose schema really changes mid-stream
    cannot be arranged from a test, so the pin is what a test perturbs
    instead. Comparing a good batch against a doctored pin exercises
    the same comparison as a doctored batch against a good pin.
    """
    return (it.pinned_meta, list(it.pinned_names_b))


def _debug_numpy_force_pin(_NumpyBatchIter it, tuple pinned_meta,
                           list pinned_names_b):
    """Seed an iterator's pinned schema so its *first* batch is checked
    against it rather than pinning one of its own.

    Not part of the public API.
    """
    it.pinned_meta = pinned_meta
    it.pinned_names_b = pinned_names_b
    (it.col_names, it.col_kinds, it.col_scales,
     it.col_precision, it.has_symbol) = pinned_meta
    it.claim = _numpy_roundtrip_claim(
        it.col_names, it.col_kinds, it.col_scales, it.col_precision)
    it.first = False


cdef _numpy_meta_drift():
    # `SchemaDrift` is the code the header gives a mid-stream schema
    # change, and the one `to_arrow()` and `iter_polars()` already raise
    # for the same event through the Arrow reader. `ServerSchemaMismatch`
    # is a server-reported ingest status and means something else.
    raise QuestDBError(
        QuestDBErrorCode.SchemaDrift,
        'The result schema changed between batches: this reader '
        'decodes every batch against the first one\'s columns, so a '
        'later batch that disagrees cannot be read.')


cdef tuple _numpy_extract_meta(const qwp_reader_batch* batch):
    cdef size_t n_cols = qwp_reader_batch_column_count(batch)
    cdef size_t col_idx
    cdef qwp_reader_column_kind kind = qwp_reader_column_kind_unknown
    cdef const char* name_buf = NULL
    cdef size_t name_len = 0
    cdef questdb_error* err = NULL
    cdef qwp_reader_column_data cd_meta
    cdef bint has_symbol = False
    col_names = []
    col_kinds = []
    col_scales = []
    col_precision = []
    for col_idx in range(n_cols):
        _reader_check(
            qwp_reader_batch_column_name(
                batch, col_idx, &name_buf, &name_len, &err),
            &err, 'qwp_reader_batch_column_name')
        col_names.append(
            PyUnicode_FromStringAndSize(name_buf, <Py_ssize_t>name_len))
        _reader_check(
            qwp_reader_batch_column_kind(batch, col_idx, &kind, &err),
            &err, 'qwp_reader_batch_column_kind')
        col_kinds.append(<int>kind)
        col_scales.append(None)
        col_precision.append(None)
        if kind == qwp_reader_column_kind_symbol:
            has_symbol = True
        elif (kind == qwp_reader_column_kind_geohash
                or kind == qwp_reader_column_kind_decimal64
                or kind == qwp_reader_column_kind_decimal128
                or kind == qwp_reader_column_kind_decimal256):
            if qwp_reader_batch_column_data(batch, col_idx, &cd_meta, &err):
                if kind == qwp_reader_column_kind_geohash:
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
    # exists since pandas 2.1; older pandas keeps the checked path. `dtype=`
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


cdef object _numpy_roundtrip_claim(
        list col_names, list col_kinds, list col_scales, list col_precision):
    """The ``df.attrs['questdb']`` claim for a native (no pyarrow) result.

    Built from the schema the first batch reported, which the whole
    result shares, so a streaming read builds it once and hands the same
    claim to every batch. That is sound for the same reason pandas can
    share it between copies of one frame: the claim declines to be
    copied and cannot be edited.
    """
    cdef size_t col_idx
    cdef size_t n_cols = <size_t>len(col_names)
    cdef dict columns_meta = {}
    cdef dict entry
    for col_idx in range(n_cols):
        entry = {'kind': _KIND_NAMES.get(col_kinds[col_idx], 'unknown')}
        if col_scales[col_idx] is not None:
            entry['scale'] = col_scales[col_idx]
        if col_precision[col_idx] is not None:
            entry['precision_bits'] = col_precision[col_idx]
        columns_meta[col_names[col_idx]] = entry
    return _RoundtripClaim(
        {'version': _ROUNDTRIP_META_VERSION, 'columns': columns_meta})


cdef object _numpy_assemble_frame(
        list col_names, list col_kinds, list col_scales,
        list col_precision, list col_chunks, list symbol_categories,
        object np, object pd, list col_masks, object symbol_dtype=None,
        object claim=None):
    cdef size_t n_cols = <size_t>len(col_names)
    cdef size_t col_idx
    cdef qwp_reader_column_kind kind
    arrays = []
    for col_idx in range(n_cols):
        kind = <qwp_reader_column_kind><int>col_kinds[col_idx]
        chunks = col_chunks[col_idx]
        if len(chunks) == 1:
            arr = chunks[0]
        else:
            arr = np.concatenate(chunks)
        if kind == qwp_reader_column_kind_symbol:
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
    # Keep pandas 3 from inferring a pyarrow-backed StringDtype Index.
    frame.columns = pd.Index(col_names, dtype=object)
    frame.attrs['questdb'] = (
        _numpy_roundtrip_claim(
            col_names, col_kinds, col_scales, col_precision)
        if claim is None else claim)
    return frame


cdef tuple _numpy_batch_columns(
        const qwp_reader_batch* batch, list col_kinds,
        size_t n_cols, size_t row_count, object np):
    cdef size_t col_idx
    cdef qwp_reader_column_kind kind
    chunks = []
    masks = []
    for col_idx in range(n_cols):
        kind = <qwp_reader_column_kind><int>col_kinds[col_idx]
        chunks.append(_numpy_column_chunk(batch, col_idx, kind, row_count, np))
        if _is_hybrid_int(kind):
            masks.append(_numpy_validity_mask(batch, col_idx, row_count, np))
        else:
            masks.append(None)
    return (chunks, masks)


# ---------------------------------------------------------------------------
# Row egress: `QueryResult.columns()` / `QueryResult.iter_rows()`.
#
# Built on the same raw-batch C API as the numpy path
# (`qwp_reader_cursor_next_batch` + the `qwp_reader_column_data` view), not on
# Arrow: a row consumer wants Python objects, and going through Arrow would
# add a conversion and a hard `pyarrow` dependency to get them. Serves
# DB-API-shaped callers — dbt, a PEP 249 shim, plain scripting — where result
# sets are small (metadata lookups, test rows, `LIMIT 0` schema probes) and
# exact QuestDB type names matter more than throughput. Large analytical
# results still belong on `to_pandas()` / `to_arrow()`, which stay columnar.
# ---------------------------------------------------------------------------

# `datetime.min` / `datetime.max` as microseconds since the epoch.
cdef int64_t _DATETIME_MIN_MICROS = -62_135_596_800_000_000
cdef int64_t _DATETIME_MAX_MICROS = 253_402_300_799_999_999
cdef int64_t _MICROS_PER_DAY = 86_400_000_000

# Geohash base-32 alphabet, matching `GeoHashes.base32` on the server.
cdef const char* _GEOHASH_BASE32 = b'0123456789bcdefghjkmnpqrstuvwxyz'


# Fixed part of the QuestDB DDL spelling per column kind. GEOHASH and the
# DECIMALs carry parameters and are rendered by `_questdb_type_name`.
# Spellings follow `ColumnType.typeNameMap` on the server, including the
# mixed-case `IPv4`, so a caller can compare them against `tables()` /
# `information_schema.questdb_columns()` output without normalising.
_QDB_TYPE_NAMES = {
    <int>qwp_reader_column_kind_boolean: 'BOOLEAN',
    <int>qwp_reader_column_kind_byte: 'BYTE',
    <int>qwp_reader_column_kind_short: 'SHORT',
    <int>qwp_reader_column_kind_int: 'INT',
    <int>qwp_reader_column_kind_long: 'LONG',
    <int>qwp_reader_column_kind_float: 'FLOAT',
    <int>qwp_reader_column_kind_double: 'DOUBLE',
    <int>qwp_reader_column_kind_symbol: 'SYMBOL',
    <int>qwp_reader_column_kind_timestamp: 'TIMESTAMP',
    <int>qwp_reader_column_kind_date: 'DATE',
    <int>qwp_reader_column_kind_uuid: 'UUID',
    <int>qwp_reader_column_kind_long256: 'LONG256',
    # STRING columns reach the client as VARCHAR: QWP has no STRING column
    # kind, so a legacy STRING column is reported as VARCHAR here.
    <int>qwp_reader_column_kind_varchar: 'VARCHAR',
    <int>qwp_reader_column_kind_timestamp_nanos: 'TIMESTAMP_NS',
    # The wire schema carries no dimension count, so every array column is
    # reported in its 1-D spelling; the server writes a 2-D one `DOUBLE[][]`.
    <int>qwp_reader_column_kind_double_array: 'DOUBLE[]',
    <int>qwp_reader_column_kind_long_array: 'LONG[]',
    <int>qwp_reader_column_kind_char: 'CHAR',
    <int>qwp_reader_column_kind_binary: 'BINARY',
    <int>qwp_reader_column_kind_ipv4: 'IPv4',
}

# Storage-width precision ceilings for the three DECIMAL widths. The wire
# carries only `decimal_scale`, so a column declared `DECIMAL(10,2)` is
# reported as `DECIMAL(18,2)` — the declared precision is not recoverable
# from a result set.
_DECIMAL_PRECISION = {
    <int>qwp_reader_column_kind_decimal64: 18,
    <int>qwp_reader_column_kind_decimal128: 38,
    <int>qwp_reader_column_kind_decimal256: 76,
}


cdef str _questdb_type_name(int kind, object scale, object precision_bits):
    """QuestDB DDL type name for a column kind plus its parameters."""
    cdef int bits
    cdef int dec_precision
    if kind == <int>qwp_reader_column_kind_geohash:
        # `GeoHashes`/`ColumnType`: character form when the width is a whole
        # number of base-32 characters, bit form otherwise.
        bits = 0 if precision_bits is None else <int>precision_bits
        if bits > 0 and bits % 5 == 0:
            return 'GEOHASH({}c)'.format(bits // 5)
        return 'GEOHASH({}b)'.format(bits)
    if kind in _DECIMAL_PRECISION:
        dec_precision = <int>_DECIMAL_PRECISION[kind]
        return 'DECIMAL({},{})'.format(
            dec_precision, 0 if scale is None else <int>scale)
    name = _QDB_TYPE_NAMES.get(kind)
    if name is None:
        return 'UNKNOWN(0x{:02X})'.format(kind)
    return name


cdef str _geohash_text(uint64_t value, int bits):
    """Render a geohash the way the server's text protocols do.

    `GeoHashes.appendCharsUnsafe` for a whole number of base-32 characters,
    `appendBinaryStringUnsafe` (a run of '0'/'1') otherwise. Returning the
    raw integer instead would be unusable against a `where geohash = '...'`
    predicate.
    """
    # `bits` is 1..60 (checked by the caller), so at most 60 characters.
    cdef char[64] buf
    cdef int i
    cdef int n
    if bits > 0 and bits % 5 == 0:
        n = bits // 5
        for i in range(n):
            buf[i] = _GEOHASH_BASE32[(value >> ((n - 1 - i) * 5)) & 0x1F]
    else:
        n = bits
        for i in range(n):
            buf[i] = c'1' if (value >> (n - 1 - i)) & 1 else c'0'
    return PyUnicode_FromStringAndSize(buf, n)


cdef inline bint _cell_is_null(const uint8_t* validity, size_t row) noexcept:
    # Per QWP the null bitmap is LSB-first within each byte and a set bit
    # means NULL; a column with no nulls carries no bitmap at all.
    return validity != NULL and ((validity[row >> 3] >> (row & 7)) & 1)


cdef void_int _raise_temporal_out_of_range(
        str col_name, size_t row, int64_t value) except -1:
    raise QuestDBError(
        QuestDBErrorCode.InvalidTimestamp,
        f'column {col_name!r} row {row}: value {value} is outside the range '
        'datetime can represent (years 1..9999); use to_pandas() / '
        'to_arrow() for this result')


cdef object _utc_datetime(int64_t days, int secs, int micros):
    cdef int z = <int>days + 719468
    cdef int era = z // 146097
    cdef int doe = z - era * 146097
    cdef int yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    cdef int doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    cdef int mp = (5 * doy + 2) // 153
    cdef int month = mp + 3 if mp < 10 else mp - 9
    return datetime_new(
        era * 400 + yoe + (month <= 2), month, doy - (153 * mp + 2) // 5 + 1,
        secs // 3600, secs // 60 % 60, secs % 60, micros, _UTC)


cdef list _row_column_values(
        const qwp_reader_batch* batch,
        size_t col_idx,
        str col_name,
        int kind,
        size_t row_count,
        const qwp_reader_symbol_dict* sd,
        _SymbolStrings symbols,
        object np):
    """Decode one column of one batch into a list of Python values.

    Dispatch happens once per column rather than once per cell, which is
    where a naive row reader spends most of its time. `None` marks a null.
    """
    cdef qwp_reader_column_data cd
    cdef questdb_error* err = NULL
    cdef const uint8_t* validity
    cdef const uint8_t* values
    cdef const uint32_t* offsets
    cdef const uint8_t* var_data
    cdef const uint32_t* codes
    cdef PyObject* slot
    cdef size_t r
    cdef size_t stride
    cdef uint32_t start
    cdef uint32_t end
    cdef uint32_t code
    cdef uint64_t lo
    cdef uint64_t hi
    cdef uint64_t u64
    cdef int64_t i64
    cdef int32_t i32
    cdef int16_t i16
    cdef uint16_t u16
    cdef uint32_t u32
    cdef float f32
    cdef double f64
    cdef int scale
    cdef int bits
    cdef int64_t micros
    cdef int64_t days
    cdef int64_t rem
    cdef int64_t secs
    cdef list out = []
    cdef object ipv4_cls
    cdef object decimal_cls
    cdef object uuid_cls

    # Arrays have their own accessor: `qwp_reader_batch_column_data` fails
    # outright on an array column, so this branch must come before it.
    # `_numpy_array_chunk` already walks the shape table and yields
    # ndarrays, so reuse it rather than re-deriving that here.
    if (kind == <int>qwp_reader_column_kind_double_array
            or kind == <int>qwp_reader_column_kind_long_array):
        if row_count == 0:
            return out
        if kind == <int>qwp_reader_column_kind_long_array:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'column {col_name!r}: row egress does not support LONG[] '
                'columns yet')
        chunk = _numpy_array_chunk(
            batch, col_idx, <qwp_reader_column_kind>kind, row_count, np)
        for r in range(row_count):
            out.append(chunk[r])
        return out

    _reader_check(
        qwp_reader_batch_column_data(batch, col_idx, &cd, &err), &err,
        'qwp_reader_batch_column_data')
    if <int>cd.kind != kind:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            f'column {col_name!r} changed kind mid-query: expected '
            '0x{:02X}, batch carries 0x{:02X}'.format(kind, <int>cd.kind))
    if row_count == 0:
        return out
    validity = cd.validity

    if kind == <int>qwp_reader_column_kind_symbol:
        codes = cd.symbol_codes
        if codes == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'symbol column has {} rows but no code array'.format(
                    row_count))
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            code = codes[r]
            if <size_t>code >= sd.entry_count:
                raise QuestDBError(
                    QuestDBErrorCode.ServerFlushError,
                    'corrupt symbol codes: code out of dictionary range')
            slot = symbols._slots[code]
            if slot == NULL:
                out.append(symbols._decode(sd, code))
            else:
                out.append(<object>slot)
        return out

    # VARCHAR / BINARY share the offsets + heap layout.
    if (kind == <int>qwp_reader_column_kind_varchar
            or kind == <int>qwp_reader_column_kind_binary):
        offsets = cd.var_offsets
        if offsets == NULL:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'column kind 0x{:02X} has {} rows but no offset table'.format(
                    kind, row_count))
        var_data = cd.var_data
        if var_data == NULL and cd.var_data_len > 0:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                'column kind 0x{:02X} has {} rows but no data buffer'.format(
                    kind, row_count))
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            start = offsets[r]
            end = offsets[r + 1]
            if end < start or <size_t>end > cd.var_data_len:
                raise QuestDBError(
                    QuestDBErrorCode.ServerFlushError,
                    'corrupt varlen offsets in column kind '
                    '0x{:02X}'.format(kind))
            if kind == <int>qwp_reader_column_kind_binary:
                out.append(PyBytes_FromStringAndSize(
                    <const char*>(var_data + start),
                    <Py_ssize_t>(end - start)))
            else:
                out.append(PyUnicode_FromStringAndSize(
                    <const char*>(var_data + start),
                    <Py_ssize_t>(end - start)))
        return out

    # Everything below is fixed-width and needs a values buffer.
    if cd.values == NULL:
        raise QuestDBError(
            QuestDBErrorCode.ServerFlushError,
            'column kind 0x{:02X} has {} rows but no values buffer'.format(
                kind, row_count))
    values = <const uint8_t*>cd.values
    stride = cd.value_stride

    if kind == <int>qwp_reader_column_kind_boolean:
        # BOOLEAN is dense one byte per row on the C side; honour the stride
        # the decoder reports rather than assuming it.
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
            else:
                out.append(values[r * stride] != 0)
        return out

    if kind == <int>qwp_reader_column_kind_byte:
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
            else:
                out.append(<int>(<const int8_t*>values)[r])
        return out

    if kind == <int>qwp_reader_column_kind_short:
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&i16, values + r * stride, 2)
            out.append(<int>i16)
        return out

    if kind == <int>qwp_reader_column_kind_char:
        # QuestDB has no separate null for CHAR: the wire marks the row
        # non-null and carries code point 0. PGWire's `outColChar` renders
        # that as NULL, so match it rather than handing back '\x00'.
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&u16, values + r * stride, 2)
            out.append(None if u16 == 0 else chr(u16))
        return out

    if kind == <int>qwp_reader_column_kind_int:
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&i32, values + r * stride, 4)
            out.append(<int>i32)
        return out

    if kind == <int>qwp_reader_column_kind_ipv4:
        # The server already turns its IPv4 null sentinel into a validity
        # bit (`appendIPv4OrNull`), so there is no zero to special-case.
        ipv4_cls = ipaddress.IPv4Address
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&u32, values + r * stride, 4)
            out.append(ipv4_cls(<unsigned long>u32))
        return out

    if kind == <int>qwp_reader_column_kind_long:
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&i64, values + r * stride, 8)
            out.append(<long long>i64)
        return out

    if kind == <int>qwp_reader_column_kind_float:
        # NaN never reaches the values buffer: the server converts it to a
        # validity bit (`appendFloatOrNull`), so null and NaN are one thing.
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&f32, values + r * stride, 4)
            out.append(<double>f32)
        return out

    if kind == <int>qwp_reader_column_kind_double:
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&f64, values + r * stride, 8)
            out.append(f64)
        return out

    if (kind == <int>qwp_reader_column_kind_timestamp
            or kind == <int>qwp_reader_column_kind_timestamp_nanos
            or kind == <int>qwp_reader_column_kind_date):
        # Timezone-aware UTC, matching the Arrow path, which tags all three
        # temporal kinds `Timestamp(unit, "UTC")`. A naive datetime would be
        # read as local time by most downstream code. TIMESTAMP_NS is
        # rounded down to microseconds: `datetime` has no nanosecond field.
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&i64, values + r * stride, 8)
            if kind == <int>qwp_reader_column_kind_timestamp_nanos:
                micros = i64 // 1000
            elif kind == <int>qwp_reader_column_kind_date:
                if (i64 < _DATETIME_MIN_MICROS // 1000
                        or i64 > _DATETIME_MAX_MICROS // 1000):
                    _raise_temporal_out_of_range(col_name, r, i64)
                micros = i64 * 1000
            else:
                micros = i64
            if micros < _DATETIME_MIN_MICROS or micros > _DATETIME_MAX_MICROS:
                _raise_temporal_out_of_range(col_name, r, i64)
            days = micros // _MICROS_PER_DAY
            rem = micros - days * _MICROS_PER_DAY
            secs = rem // 1_000_000
            out.append(_utc_datetime(
                days, <int>secs, <int>(rem - secs * 1_000_000)))
        return out

    if kind == <int>qwp_reader_column_kind_uuid:
        # Canonical RFC-4122 bytes, decoded the same way as
        # `_numpy_uuid_chunk`.
        uuid_cls = _uuid.UUID
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            memcpy(&hi, values + r * stride, 8)
            memcpy(&lo, values + r * stride + 8, 8)
            hi = bswap64(hi)
            lo = bswap64(lo)
            out.append(uuid_cls(int=((<object>hi) << 64) | (<object>lo)))
        return out

    if kind == <int>qwp_reader_column_kind_long256:
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            out.append(int.from_bytes(
                PyBytes_FromStringAndSize(
                    <const char*>(values + r * stride), 32),
                'little'))
        return out

    if kind in _DECIMAL_PRECISION:
        # Build from a string so the scale (and its trailing zeros) is
        # preserved: `Decimal(unscaled) / 10**scale` would normalise it away.
        decimal_cls = Decimal
        scale = cd.decimal_scale
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            unscaled = int.from_bytes(
                PyBytes_FromStringAndSize(
                    <const char*>(values + r * stride), <Py_ssize_t>stride),
                'little', signed=True)
            out.append(decimal_cls(f'{unscaled}e{-scale}'))
        return out

    if kind == <int>qwp_reader_column_kind_geohash:
        bits = cd.geohash_precision_bits
        if stride < 1 or stride > 8 or bits < 1 or bits > 60:
            raise QuestDBError(
                QuestDBErrorCode.ServerFlushError,
                f'column {col_name!r}: corrupt geohash column (stride '
                f'{stride}, precision {bits} bits)')
        for r in range(row_count):
            if _cell_is_null(validity, r):
                out.append(None)
                continue
            u64 = 0
            memcpy(&u64, values + r * stride, stride)
            out.append(_geohash_text(u64, bits))
        return out

    raise QuestDBError(
        QuestDBErrorCode.InvalidApiCall,
        'row egress does not support column kind 0x{:02X} yet; use '
        'to_arrow() / to_pandas() for this result'.format(kind))


cdef list _row_batch_decode(
        const qwp_reader_batch* batch,
        list col_names,
        list col_kinds,
        _SymbolStrings symbols,
        object np):
    """Decode a whole batch into a list of row tuples.

    Must run while `batch` is still the cursor's current batch, i.e. under
    the caller's lock and pin, before the next
    `qwp_reader_cursor_next_batch`.
    """
    cdef questdb_error* err = NULL
    cdef qwp_reader_symbol_dict sd
    cdef const qwp_reader_symbol_dict* sd_ptr = NULL
    cdef size_t row_count = qwp_reader_batch_row_count(batch)
    cdef size_t n_cols = <size_t>len(col_names)
    cdef size_t col_idx

    if row_count == 0:
        return []
    if n_cols == 0:
        # A batch with rows but no columns cannot be expressed as tuples of
        # column values; emit empty tuples so the row count still shows.
        return [() for _ in range(row_count)]

    for col_idx in range(n_cols):
        if col_kinds[col_idx] == <int>qwp_reader_column_kind_symbol:
            _reader_check(
                qwp_reader_batch_symbol_dict(batch, &sd, &err), &err,
                'qwp_reader_batch_symbol_dict')
            symbols._reserve(sd.entry_count)
            sd_ptr = &sd
            break

    columns = [
        _row_column_values(
            batch, col_idx, col_names[col_idx], <int>col_kinds[col_idx],
            row_count, sd_ptr, symbols, np)
        for col_idx in range(n_cols)
    ]
    # `zip` builds the tuples in C; building them row-by-row in Python is
    # measurably slower for the same result.
    return list(zip(*columns))


cdef tuple _open_row_stream(_CursorHandle handle):
    """Start a row stream: returns ``(columns, row_iterator)``.

    Batches are pulled up to the first one carrying rows, so the column list
    is known before any row is consumed — a DB-API caller needs
    `description` before `fetchall`. That batch is left undecoded on the
    cursor as the handle's `_pending_batch` until the iterator's first step,
    so a schema probe neither pays for the rows nor fails on their values.
    A result with no rows at all (a `LIMIT 0` probe, an empty SELECT) is
    read through to its terminal, so the connection goes back to the pool
    even if the caller never iterates.
    """
    cdef qwp_reader_cursor* cursor
    cdef questdb_error* err = NULL
    cdef const qwp_reader_batch* batch
    cdef int seen_seq
    cdef list col_names = []
    cdef list col_kinds = []
    cdef list col_scales = []
    cdef list col_precision = []
    cdef bint have_schema = False
    cdef bint terminal = False
    import numpy as np

    if handle is None or not handle._is_live():
        raise QuestDBError(QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
    seen_seq = handle._reset_sequence()

    try:
        while True:
            with handle._lock:
                cursor = handle._cursor
                if cursor == NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
                with nogil:
                    batch = qwp_reader_cursor_next_batch(cursor, &err)
                if err == NULL:
                    _oidc_raise_unscheduled_main()
                handle._pin()
                try:
                    if handle._reset_seq != seen_seq:
                        # Mid-query failover before any row was handed out:
                        # the replay restarts at batch 0.
                        seen_seq = handle._reset_seq
                        have_schema = False
                        col_names = []
                        col_kinds = []
                        col_scales = []
                        col_precision = []
                        if batch == NULL and err == NULL:
                            continue
                    if batch == NULL:
                        if err != NULL:
                            raise _reader_err_to_py(err)
                        # A non-SELECT ships no RESULT_BATCH: no schema, no
                        # rows; `exec_done` carries the outcome.
                        terminal = True
                        break
                    if not have_schema:
                        col_names, col_kinds, col_scales, col_precision, _ = (
                            _numpy_extract_meta(batch))
                        have_schema = True
                    if qwp_reader_batch_row_count(batch) == 0:
                        continue
                    handle._pending_batch = batch
                finally:
                    handle._unpin()
            break

        columns = [
            (col_names[i],
             _questdb_type_name(
                 <int>col_kinds[i], col_scales[i], col_precision[i]))
            for i in range(len(col_names))
        ]

        if terminal:
            _mark_reader_drained(handle)
            handle._free()
            return (columns, iter(()))

        rows_iter = _row_stream_iter(
            handle, col_names, col_kinds, seen_seq, np)
        # Prime it into its `try`, so dropping an iterator that was never
        # advanced still runs its `finally` and frees the cursor.
        next(rows_iter)
        return (columns, rows_iter)
    except:
        handle._free()
        raise


def _row_stream_iter(
        _CursorHandle handle not None,
        list col_names,
        list col_kinds,
        int seen_seq,
        object np):
    """Yield row tuples, starting with the handle's `_pending_batch`.

    Only the batch being handed out is held as Python rows: each decoded
    batch is dropped as soon as its rows are out, so the peak is one batch
    of row tuples plus the one being decoded.
    """
    cdef qwp_reader_cursor* cursor
    cdef questdb_error* err = NULL
    cdef const qwp_reader_batch* batch
    cdef _SymbolStrings symbols = _SymbolStrings()
    cdef list rows
    try:
        yield None
        with handle._lock:
            batch = handle._pending_batch
            handle._pending_batch = NULL
            if handle._cursor == NULL:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
            if handle._cancelled:
                return
            handle._pin()
            try:
                rows = _row_batch_decode(
                    batch, col_names, col_kinds, symbols, np)
            finally:
                handle._unpin()
        while True:
            # Read without the lock: a stale value delays the verdict by one
            # row. A cancelled cursor already sits at its terminal.
            for row in rows:
                if handle._cursor == NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
                if handle._cancelled:
                    return
                yield row
            rows = None
            with handle._lock:
                cursor = handle._cursor
                if cursor == NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
                if handle._cancelled:
                    return
                with nogil:
                    batch = qwp_reader_cursor_next_batch(cursor, &err)
                if err == NULL:
                    _oidc_raise_unscheduled_main()
                if handle._reset_seq != seen_seq:
                    # Rows may already be out the door, so a replay from
                    # batch 0 would duplicate them. Streaming cannot take
                    # them back: surface a clean, catchable error (the same
                    # contract as the Arrow stream).
                    if err != NULL:
                        questdb_error_free(err)
                        err = NULL
                    raise QuestDBError(
                        QuestDBErrorCode.FailoverWouldDuplicate,
                        'mid-query failover would duplicate already-'
                        'delivered rows; re-issue the query')
                if batch == NULL:
                    if err != NULL:
                        raise _reader_err_to_py(err)
                    _mark_reader_drained(handle)
                    return
                handle._pin()
                try:
                    rows = _row_batch_decode(
                        batch, col_names, col_kinds, symbols, np)
                finally:
                    handle._unpin()
    except BaseException:
        handle._rows_cut_short = True
        raise
    finally:
        handle._free()


cdef object _numpy_frame_from_cursor(_CursorHandle handle):
    import numpy as np
    import pandas as pd
    cdef qwp_reader_cursor* cursor
    cdef questdb_error* err = NULL
    cdef const qwp_reader_batch* batch
    cdef qwp_reader_symbol_dict sd
    cdef size_t n_cols = 0
    cdef size_t row_count = 0
    cdef size_t col_idx
    cdef size_t prev_dict_n = 0
    cdef bint first = True
    cdef tuple pinned_meta = None
    cdef list pinned_names_b = None
    cdef bint has_symbol = False
    cdef int seen_seq

    if handle is None:
        raise QuestDBError(QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
    handle._check_not_driving('to_numpy', check_lock=True)
    if not handle._is_live():
        raise QuestDBError(QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
    seen_seq = handle._reset_sequence()

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
                handle._check_not_driving('to_numpy')
                handle._pin()
                try:
                    handle._native_in_flight = True
                    try:
                        with nogil:
                            batch = qwp_reader_cursor_next_batch(
                                cursor, &err)
                    finally:
                        handle._native_in_flight = False
                    if err == NULL:
                        _oidc_raise_unscheduled_main()
                    if handle._reset_seq != seen_seq:
                        # Mid-query failover replayed from batch-0: discard
                        # the pre-failover accumulation and the schema.
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
                    row_count = qwp_reader_batch_row_count(batch)
                    if first:
                        pinned_meta = _numpy_extract_meta(batch)
                        pinned_names_b = _numpy_pinned_name_bytes(pinned_meta)
                        (col_names, col_kinds, col_scales, col_precision,
                         has_symbol) = pinned_meta
                        n_cols = <size_t>len(col_names)
                        col_chunks = [[] for _ in range(n_cols)]
                        col_masks = [[] for _ in range(n_cols)]
                        first = False
                    else:
                        _numpy_check_meta_pinned(
                            pinned_meta, pinned_names_b, batch)
                    if has_symbol:
                        _reader_check(
                            qwp_reader_batch_symbol_dict(batch, &sd, &err),
                            &err, 'qwp_reader_batch_symbol_dict')
                        if sd.entry_count > prev_dict_n:
                            _symbol_categories_extend(
                                symbol_categories, &sd, prev_dict_n)
                            prev_dict_n = sd.entry_count
                    batch_chunks, batch_masks = _numpy_batch_columns(
                        batch, col_kinds, n_cols, row_count, np)
                finally:
                    handle._unpin()
            for col_idx in range(n_cols):
                col_chunks[col_idx].append(batch_chunks[col_idx])
                col_masks[col_idx].append(batch_masks[col_idx])
    except:
        handle._free()
        raise

    try:
        _mark_reader_drained(handle)
    finally:
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
    cdef bint cat_to

    def __cinit__(self, object pl):
        self.pl = pl
        self.cats = pl.Categories.random('questdb_symbol', physical=pl.UInt32)
        self.base = None
        self.pinned = None
        self.n = 0
        # polars >= 1.43 deprecates the physical-code `.cast(Categorical)` in
        # favour of `.cat.to(Categorical)`, which older polars lacks.
        self.cat_to = hasattr(pl.Series([], dtype=pl.UInt32).cat, 'to')

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
        codes = codes.cast(pl.UInt32)
        if self.cat_to:
            return codes.cat.to(pl.Categorical(self.cats)).alias(name)
        return codes.cast(pl.Categorical(self.cats)).alias(name)


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


cdef str _polars_fallback_warning(object exc):
    return (
        'The polars Categorical fast path failed '
        f'({type(exc).__name__}: {exc}); the frame was built with '
        '`polars.from_arrow` instead, which is slower. Please report this '
        'at https://github.com/questdb/py-questdb-client/issues.')


cdef object _polars_hybrid_fallback(object table, object pl, object exc):
    # polars marks the `Categories` API as unstable, so a new release can
    # break the hybrid path. `pl.from_arrow` gives the same values, only
    # slower, so fall back, but do not hide the failure.
    # cdef frames and this module's def entry points are invisible to the
    # warnings stack walker, so level 1 is already the user's call.
    frame = pl.from_arrow(table)
    warnings.warn(_polars_fallback_warning(exc), RuntimeWarning, stacklevel=1)
    return frame


cdef tuple _polars_frame_with_registry_cats(
        object frame, object table, object pl, object pa, dict registries):
    # Give a `pl.from_arrow` frame the registry `Categories` of the hybrid
    # frames that `iter_polars` yielded before it, so that `pl.concat` still
    # stitches. Without a registry, every frame is a plain Categorical.
    # Returns `(frame, exc)`: `exc` is the cast failure, or None.
    cdef list types
    cdef list names
    cdef list exprs = []
    cdef Py_ssize_t i
    cdef _PolarsSymbolRegistry reg
    if not registries:
        return (frame, None)
    types = table.schema.types
    names = table.column_names
    try:
        for i in range(len(types)):
            if pa.types.is_dictionary(types[i]):
                reg = registries.get(i) or registries.get(-1)
                if reg is not None:
                    exprs.append(
                        pl.col(names[i]).cast(pl.Categorical(reg.cats)))
        if exprs:
            frame = frame.with_columns(exprs)
    except Exception as exc:
        # The cast uses the `Categories` API that the fallback works
        # around, so it can fail the same way. Keep the values, stop
        # casting, and let the caller warn: `pl.concat` with the earlier
        # frames fails.
        registries.clear()
        return (frame, exc)
    return (frame, None)


cdef class _PolarsBatchIter:
    """Streaming `polars.DataFrame` per result batch. Holds the per-symbol
    `Categories` registries so every batch's Categoricals share one identity
    and `pl.concat` stitches cleanly."""
    cdef object reader
    cdef object pl
    cdef object pa
    cdef dict registries
    cdef bint use_hybrid
    cdef bint hybrid_yielded
    # A built frame whose warning raised (`-W error`): the next call
    # returns it, so that no batch is lost.
    cdef object pending

    def __cinit__(self, _CursorHandle handle, object pl, object pa):
        self.reader = _build_record_batch_reader(handle)
        self.pl = pl
        self.pa = pa
        self.registries = {}
        self.use_hybrid = getattr(pl, 'Categories', None) is not None
        self.hybrid_yielded = False
        self.pending = None

    def __iter__(self):
        return self

    def __next__(self):
        cdef list msgs = []
        frame = self.pending
        if frame is not None:
            self.pending = None
            return frame
        batch = next(self.reader)
        table = self.pa.Table.from_batches([batch])
        if self.use_hybrid:
            try:
                frame = _polars_dataframe_hybrid(
                    table, self.pl, self.pa, self.registries)
            except Exception as exc:
                # One warning per iterator: the remaining batches take the
                # fallback too, cast to the registries' `Categories`, so
                # every yielded frame has the same dtypes.
                self.use_hybrid = False
                if not self.hybrid_yielded:
                    # No yielded frame uses the registries: plain frames
                    # stitch, and the cast would only cost a string remap.
                    self.registries.clear()
                msgs.append(_polars_fallback_warning(exc))
                frame = self.pl.from_arrow(table)
            else:
                self.hybrid_yielded = True
                return frame
        else:
            frame = self.pl.from_arrow(table)
        frame, cast_exc = _polars_frame_with_registry_cats(
            frame, table, self.pl, self.pa, self.registries)
        if cast_exc is not None:
            msgs.append(
                'The cast to the Categories of the frames before failed '
                f'({type(cast_exc).__name__}: {cast_exc}); this frame and '
                'the following ones have a plain Categorical dtype, so '
                '`polars.concat` with the frames before fails. Please '
                'report this at '
                'https://github.com/questdb/py-questdb-client/issues.')
        # One warning, after the frame is built: under `-W error` the
        # warning raises, and the frame must not be lost with it.
        if msgs:
            try:
                warnings.warn(' '.join(msgs), RuntimeWarning, stacklevel=1)
            except BaseException:
                self.pending = frame
                raise
        return frame


cdef class _NumpyBatchIter:
    cdef _CursorHandle handle
    cdef object np
    cdef object pd
    cdef list col_names
    cdef list col_kinds
    cdef list col_scales
    cdef list col_precision
    cdef object claim
    cdef tuple pinned_meta
    cdef list pinned_names_b
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
        self.claim = None
        self.first = True
        self.has_symbol = False
        self.done = False
        self.prev_dict_n = 0
        self.symbol_categories = []
        self.symbol_dtype = None
        self.seen_seq = (
            handle._reset_sequence() if handle is not None else 0)
        self.delivered = False

    def __iter__(self):
        return self

    def __next__(self):
        cdef qwp_reader_cursor* cursor
        cdef questdb_error* err = NULL
        cdef const qwp_reader_batch* batch
        cdef qwp_reader_symbol_dict sd
        cdef size_t row_count
        cdef size_t n_cols
        if self.done or self.handle is None:
            raise StopIteration
        # A rejected nested call must neither wait on the outer pull's lock
        # nor enter cleanup that marks this shared iterator done.
        self.handle._check_not_driving('iter_numpy', check_lock=True)
        try:
            with self.handle._lock:
                cursor = self.handle._cursor
                if cursor == NULL:
                    raise QuestDBError(
                        QuestDBErrorCode.InvalidApiCall, 'cursor is closed')
                self.handle._check_not_driving('iter_numpy')
                self.handle._pin()
                try:
                    self.handle._native_in_flight = True
                    try:
                        with nogil:
                            batch = qwp_reader_cursor_next_batch(
                                cursor, &err)
                    finally:
                        self.handle._native_in_flight = False
                    if err == NULL:
                        _oidc_raise_unscheduled_main()
                    if self.handle._reset_seq != self.seen_seq:
                        if self.delivered:
                            # The replayed batch-0 would duplicate batches
                            # already yielded.
                            self.done = True
                            if err != NULL:
                                questdb_error_free(err)
                                err = NULL
                            raise QuestDBError(
                                QuestDBErrorCode.FailoverWouldDuplicate,
                                'mid-query failover would duplicate already-'
                                'delivered batches; re-issue the query')
                        # Pre-delivery failover replays batch-0: the replayed
                        # stream restarts its dictionary at entry 0, so drop
                        # the accumulated schema and categories.
                        self.seen_seq = self.handle._reset_seq
                        self.first = True
                        self.claim = None
                        self.has_symbol = False
                        self.prev_dict_n = 0
                        self.symbol_categories = []
                        self.symbol_dtype = None
                    if batch == NULL:
                        self.done = True
                        if err != NULL:
                            raise _reader_err_to_py(err)
                        _mark_reader_drained(self.handle)
                        raise StopIteration
                    row_count = qwp_reader_batch_row_count(batch)
                    if self.first:
                        self.pinned_meta = _numpy_extract_meta(batch)
                        self.pinned_names_b = _numpy_pinned_name_bytes(
                            self.pinned_meta)
                        (self.col_names, self.col_kinds, self.col_scales,
                         self.col_precision, self.has_symbol) = \
                            self.pinned_meta
                        self.claim = _numpy_roundtrip_claim(
                            self.col_names, self.col_kinds,
                            self.col_scales, self.col_precision)
                        self.first = False
                    else:
                        _numpy_check_meta_pinned(
                            self.pinned_meta, self.pinned_names_b, batch)
                    n_cols = <size_t>len(self.col_names)
                    if self.has_symbol:
                        _reader_check(
                            qwp_reader_batch_symbol_dict(batch, &sd, &err),
                            &err, 'qwp_reader_batch_symbol_dict')
                        if sd.entry_count > self.prev_dict_n:
                            _symbol_categories_extend(
                                self.symbol_categories, &sd,
                                self.prev_dict_n)
                            self.symbol_dtype = self.pd.CategoricalDtype(
                                self.symbol_categories)
                            self.prev_dict_n = sd.entry_count
                    batch_chunks, batch_masks = _numpy_batch_columns(
                        batch, self.col_kinds, n_cols, row_count, self.np)
                finally:
                    self.handle._unpin()
            # Assembly needs no cursor state, so it runs off the lock —
            # but inside the try, so a failure here ends the iterator and
            # frees the cursor like every other error path. Resuming
            # after it would step onto a consumed batch carrying a
            # half-built schema.
            col_chunks = [[c] for c in batch_chunks]
            col_masks = [[m] for m in batch_masks]
            frame = _numpy_assemble_frame(
                self.col_names, self.col_kinds, self.col_scales,
                self.col_precision, col_chunks, self.symbol_categories,
                self.np, self.pd, col_masks, symbol_dtype=self.symbol_dtype,
                claim=self.claim)
        except:
            self.done = True
            self.handle._free()
            raise
        self.delivered = True
        return frame

    def __dealloc__(self):
        if not self.done and self.handle is not None:
            _reclaim_cursor_no_wait(self.handle)


def _debug_new_cursor_finalizer_owner(
        str kind, _CursorHandle handle):
    """Construct a real cursor owner for non-blocking finalizer tests."""
    if kind == 'numpy':
        return _NumpyBatchIter(handle)
    if kind == 'generator':
        owner = _record_batch_generator(None, handle, None, False, 0)
        next(owner)  # Enter its try/finally and park at the first yielded batch.
        return owner
    if kind == 'capsule':
        return _query_stream_capsule(_new_query_stream_producer(handle))
    if kind == 'query_result':
        return QueryResult(handle)
    raise ValueError(f'unknown cursor finalizer owner {kind!r}')


cdef bytes _ARROW_MD_COLUMN_TYPE = b'questdb.column_type'
cdef bytes _ARROW_MD_GEOHASH_BITS = b'questdb.geohash_bits'


# The Rust egress spells a few kinds differently on the Arrow field than
# the QWP reader reports them, so map them onto the `_KIND_NAMES`
# vocabulary: `df.attrs['questdb']` then means the same thing whichever
# backend built the frame.
cdef dict _ARROW_KIND_ALIASES = {
    'timestamp_nanos': 'timestamp_ns',
    'decimal64': 'decimal',
    'decimal128': 'decimal',
    'decimal256': 'decimal',
}


cdef object _arrow_field_roundtrip_meta(object field):
    """One `df.attrs['questdb']['columns']` entry built from an Arrow
    field's QuestDB metadata, or None where the field claims no kind."""
    cdef object md = field.metadata
    cdef object raw
    cdef object bits
    cdef object scale
    cdef dict entry
    if not md:
        return None
    raw = md.get(_ARROW_MD_COLUMN_TYPE)
    if raw is None:
        return None
    kind = raw.decode('utf-8', 'replace')
    kind = _ARROW_KIND_ALIASES.get(kind, kind)
    entry = {'kind': kind}
    if kind == 'geohash':
        bits = md.get(_ARROW_MD_GEOHASH_BITS)
        if bits is not None:
            try:
                entry['precision_bits'] = int(bits)
            except ValueError:
                pass
    elif kind == 'decimal':
        scale = getattr(field.type, 'scale', None)
        if scale is not None:
            entry['scale'] = scale
    return entry


cdef dict _arrow_roundtrip_columns_meta(object schema):
    """The `columns` map of `df.attrs['questdb']`, read off the Arrow
    schema's QuestDB field metadata."""
    cdef dict columns_meta = {}
    cdef Py_ssize_t i
    cdef object entry
    # `schema.names` rebuilds the list and decodes every field name on
    # each read, so it is read once. Every column of a QuestDB result
    # carries type metadata, which made the old per-column read
    # quadratic in the column count and `iter_pandas` paid it per batch.
    cdef list names = schema.names
    for i in range(len(names)):
        entry = _arrow_field_roundtrip_meta(schema.field(i))
        if entry is not None:
            columns_meta[names[i]] = entry
    return columns_meta


cdef object _arrow_roundtrip_claim(object schema):
    """The ``df.attrs['questdb']`` claim for an Arrow schema.

    One schema covers every batch of a stream, and the claim declines to
    be copied and cannot be edited, so a streaming read builds it once
    and hands the same claim to every batch it yields.
    """
    return _RoundtripClaim(
        {'version': _ROUNDTRIP_META_VERSION,
         'columns': _arrow_roundtrip_columns_meta(schema)})


cdef object _attach_arrow_roundtrip_attrs(object frame, object schema):
    """Carry the per-column QuestDB kinds into `df.attrs['questdb']`,
    matching what the native numpy path attaches.

    The kind claim rides on the Arrow *field*, while a pandas dtype holds
    only the *type*, so `Table.to_pandas` drops it. `attrs` is the
    pandas-level carrier `QuestDB.dataframe()` reads back to restore
    IPV4 / CHAR / GEOHASH / UUID / LONG256 columns, which are otherwise
    indistinguishable from their storage types on the way back in.
    """
    frame.attrs['questdb'] = _arrow_roundtrip_claim(schema)
    return frame


def _debug_arrow_roundtrip_columns_meta(schema):
    """Internal: the ``df.attrs['questdb']['columns']`` map that the
    pyarrow-backed ``to_pandas`` / ``iter_pandas`` attach for a
    ``pyarrow.Schema``. Lets the mapping be tested without a server.

    Not part of the public API.
    """
    return _arrow_roundtrip_columns_meta(schema)


cdef object _resolve_arrow_to_pandas_kwargs(dtype_backend, types_mapper):
    kwargs = {}
    if types_mapper is not None:
        kwargs['types_mapper'] = types_mapper
    elif dtype_backend == 'pyarrow':
        import pandas as pd
        if not hasattr(pd, 'ArrowDtype'):
            raise ImportError(_pandas_too_old_message(
                pd, "`dtype_backend='pyarrow'`"))
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
    ``None`` if the QuestDB handle is closed.

    Not part of the public API.
    """
    cdef QuestDB c = client
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


cdef bint _cursor_handle_is_live(_CursorHandle h):
    return h is not None and h._is_live()


ExecDone = collections.namedtuple(
    'ExecDone', ['op_type', 'rows_affected'], module=__name__)
ExecDone.__doc__ = """A non-SELECT statement's terminal ``EXEC_DONE`` report.

Returned by :meth:`QuestDB.execute` / :meth:`PooledReader.execute` and
carried on :attr:`QueryResult.exec_done`; both give ``None`` instead for a
statement that streamed a result set. A named tuple:
``done = db.execute(sql)`` reads as ``done.rows_affected``.
"""
ExecDone.op_type.__doc__ = (
    "The server's QWP operation-type byte, carried through as an opaque "
    "``int``; its values are the server's, not part of this client's API.")
ExecDone.rows_affected.__doc__ = (
    'The number of rows an ``INSERT`` or ``UPDATE`` touched; ``0`` for a '
    'statement that carries no count, such as one executed at parse time '
    '(``TRUNCATE``, ``RENAME TABLE``, ``SET``, ...).')


cdef bint _called_by_old_polars():
    # polars before 1.32 decodes every batch of a stream with the first
    # batch's dictionary, so SYMBOL values come out wrong without an error.
    # polars calls `__arrow_c_stream__` from its own Python code
    # (`pycapsule_to_frame`, `Series.__init__`), and a Cython function pushes
    # no frame, so frame 0 is the caller. duckdb 1.5 calls it from a worker
    # thread with no Python frame, where `sys._getframe` raises ValueError.
    cdef object pl = sys.modules.get('polars')
    if pl is None:
        return False
    try:
        major, minor = pl.__version__.split('.')[:2]
        if (int(major), int(minor)) >= (1, 32):
            return False
        return sys._getframe(0).f_globals.get('__name__', '').startswith(
            'polars.')
    except Exception:
        return False


cdef str _query_result_reuse_message(object consumed_by):
    # `consumed_by` is None when the result was closed before any read.
    if consumed_by is None:
        return 'QueryResult is closed. Run the query again to read the result.'
    return (
        f'QueryResult was already read by {consumed_by}. A result can be '
        'read only once. Run the query again to read the result again.')


class QueryResult:
    """Result of ``QuestDB.query(sql)``.

    Streams query rows as Arrow record batches. **Single-use**: each
    materialisation method (``to_pandas``, ``to_arrow``, ``iter_arrow``,
    ``iter_pandas``, or the ``__arrow_c_stream__`` PyCapsule protocol)
    consumes the underlying cursor; the second consumption raises
    ``QuestDBError``. For the same reason it cannot be copied.

    **Consumption rule**: fully drain the result, use it as a context
    manager (``with db.query(...) as result:``), or call :meth:`close`.
    Closing a partial result drops its connection. Call :meth:`cancel`
    before closing to preserve it.

    **Thread hand-off**: a ``QueryResult`` may be transferred to another
    thread and consumed, cancelled, closed, or dropped there. Use a normal
    synchronisation hand-off (for example, ``Thread.start`` / ``join`` or a
    thread-safe queue), and access it from only one thread at a time.
    Concurrent operations on one result are unsupported. Calling ``close``
    or ``cancel`` from code that runs on the reading thread while the cursor
    is being read (a finalizer, say) raises ``InvalidApiCall`` instead of
    releasing an in-use cursor.

    ``__arrow_c_stream__`` is native — the cursor's record batches are
    exposed directly through the Arrow C Data Interface, so polars
    (``polars.DataFrame(result)``, polars 1.32 or later), duckdb
    (``duckdb.from_arrow(result)``, duckdb 1.2 or later) and other
    Arrow-native consumers can read query results without pyarrow
    installed. duckdb reads when you fetch, so fetch before the result is
    closed.
    ``to_pandas`` / ``iter_pandas`` are also pyarrow-free by default (a
    native numpy path); they only need pyarrow when ``dtype_backend`` /
    ``types_mapper`` is passed.
    ``to_arrow`` / ``iter_arrow`` / ``to_polars`` / ``iter_polars``
    do require pyarrow.

    **SYMBOL columns**: ``to_polars`` / ``to_pandas`` build the Categorical
    directly, interning the connection dictionary once (no per-row remap).
    ``to_arrow`` / ``iter_arrow`` / ``__arrow_c_stream__`` emit a generic Arrow
    form whose per-batch SYMBOL dictionary is compacted to the values each
    batch uses, which a generic consumer reconciles. So when the target is a
    polars / pandas frame, the dedicated methods avoid the re-reconciliation
    that ``polars.DataFrame(result)`` / ``to_arrow().to_pandas()`` pay on
    SYMBOL-heavy results. ``to_arrow().to_pandas()`` needs pyarrow 23 or
    later for SYMBOL columns, which use ``uint32`` dictionary indices.

    Example::

        with db.query('SELECT * FROM trades WHERE ts > $1',
                      [datetime.datetime(2026, 7, 1)]) as result:
            df = polars.DataFrame(result)              # no pyarrow
            # df = result.to_pandas()                   # no pyarrow
            # rows = duckdb.from_arrow(result).fetchall()  # no pyarrow
            # table = pa.table(result)                  # pyarrow required
    """

    def __init__(self, _CursorHandle cursor_handle):
        self._cursor_handle = cursor_handle
        self._cancel_handle = cursor_handle
        self._exec_done_handle = cursor_handle
        self._consumed = False
        self._row_columns = None
        self._row_iter = None
        # The method that read the result, for the reuse error.
        self._consumed_by = None
        # Set by `__arrow_c_schema__`: the stream producer that holds the
        # cursor and the first batch until `__arrow_c_stream__` takes it.
        self._schema_producer = None
        # A saved copy of the schema for later `__arrow_c_schema__` calls.
        self._schema_box = None

    def _take_cursor_handle(self, str what):
        cdef _CursorHandle handle
        if self._consumed:
            if self._schema_producer is not None:
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'QueryResult.__arrow_c_schema__() read the first batch '
                    '(pa.schema(result), polars.Schema(result) and duckdb '
                    '1.5 call it). Read the result through the Arrow stream, '
                    'for example with pa.table(result) or '
                    'polars.DataFrame(result), or run the query again.')
            if (self._consumed_by is None
                    and _cursor_handle_is_live(self._cancel_handle)):
                # cancel() dropped the batch that `__arrow_c_schema__`
                # read, and the cursor stays open until close().
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'QueryResult was cancelled after __arrow_c_schema__() '
                    'read its first batch. Call close(), then run the query '
                    'again to read the result.')
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                _query_result_reuse_message(self._consumed_by))
        handle = self._cursor_handle
        if handle is None or self._cancel_handle is None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                _query_result_reuse_message(None))
        if handle._cancelled:
            # An empty result here would read as a non-SELECT.
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'QueryResult was cancelled before it was read')
        self._consumed = True
        self._consumed_by = what
        self._cursor_handle = None
        return handle

    def _drain(self):
        _drain_cursor(self._take_cursor_handle('execute()'))

    def __arrow_c_schema__(self):
        """Arrow schema PyCapsule protocol (no pyarrow needed).

        It reads the first batch to get the schema, and keeps that batch
        for ``__arrow_c_stream__``. duckdb 1.5 calls it, so that
        ``duckdb.from_arrow(result)`` opens the stream only once.
        ``pa.schema(result)`` and ``polars.Schema(result)`` call it too.
        After this call, read the result only through ``__arrow_c_stream__``
        (``pa.table(result)``, ``polars.DataFrame(result)``, duckdb): the
        other readers raise ``InvalidApiCall`` on their first read. When
        the schema was read first, later calls return the same schema, also
        after the stream was taken.
        """
        cdef _QueryStreamProducer prod
        cdef _CursorHandle handle
        cdef _ArrowSchemaBox box = self._schema_box
        cdef object exc
        cdef int rc
        if box is not None:
            return _arrow_schema_capsule(box)
        handle = self._take_cursor_handle('__arrow_c_schema__()')
        prod = _new_query_stream_producer(handle)
        self._schema_producer = prod
        rc = _qs_ensure_schema(prod)
        if rc != 0:
            # Release the connection now, and raise the error with its own
            # type and code (as `to_arrow()` would).
            exc = prod.last_exc
            prod.last_exc = None
            if exc is None and rc == 12:  # ENOMEM
                exc = MemoryError()
            elif exc is None:
                exc = QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'failed to read the result schema: ' + (
                        (<bytes>prod.last_error).decode('utf-8', 'replace')
                        if prod.last_error != NULL else 'out of memory'))
            # Nothing was read: a later read says "closed", not "read by".
            self._schema_producer = None
            self._consumed_by = None
            prod._free_cached()
            handle._free()
            raise exc
        box = _arrow_schema_box(&prod.cached_schema)
        self._schema_box = box
        return _arrow_schema_capsule(box)

    def __arrow_c_stream__(self, requested_schema=None):
        """Arrow C stream PyCapsule protocol (no pyarrow needed). SYMBOL
        columns arrive compact — each batch's dictionary holds only the values
        it references — so a consumer that unifies per-batch dictionaries
        (e.g. ``polars.DataFrame(result)`` on polars 1.32 or later)
        reconciles them. Older polars misreads such a stream, so when polars
        before 1.32 reads the result directly (``polars.DataFrame(result)``,
        ``polars.from_arrow(result)``), the call raises ``InvalidApiCall``
        and the result stays unread. A stream that reaches polars through
        another library is not checked.

        ``requested_schema`` is ignored, also when it does not match: the
        stream always has the result's own schema, so check the schema you
        get. ``pa.table(result, schema=...)`` and
        ``polars.DataFrame(result, schema=...)`` cast the result;
        ``pa.RecordBatchReader.from_stream(result, schema=...)`` (pyarrow
        15 or later) returns the result's own schema.

        The stream releases the result's connection when the consumer
        reads the last batch, not when it releases the stream. A reader
        lease can run its next query as soon as the stream is drained.

        The stream does not replay a result: after a batch has been
        delivered, a failover ends the stream with an error. The consumer
        raises its own exception type, and the message contains
        ``[FailoverWouldDuplicate]``; re-issue the query. polars raises a
        ``ComputeError``, but polars before 1.37.1 raises a
        ``PanicException``, which ``except Exception`` does not catch.

        The returned stream may be handed to a consumer worker thread.
        The cursor lock serialises the native cursor calls, not the
        stream's own state, so the Arrow C stream must be consumed by only
        one thread at a time; concurrent ``get_next`` / ``release`` calls
        are unsupported.

        Typed-``OidcError`` caveat (``oidc_auth`` transports only): a token
        failure that happens *mid-stream* — a failover reconnect between
        batches that needs a fresh token — reaches the consumer as a generic
        Arrow / ``OSError``, **not** a typed
        :class:`~questdb.auth.OidcError`, because the Arrow C-stream boundary
        carries only an error string, not a Python exception type. A failure
        acquiring the token *before* streaming begins still raises the typed
        ``OidcError``, and the Python-driven readers (:meth:`iter_arrow`,
        :meth:`to_pandas`, :meth:`to_arrow`, :meth:`to_polars`) surface it on
        every path. Call :meth:`~questdb.auth.OidcDeviceAuth.sign_in` up front
        so no mid-stream token acquisition is needed, or materialize with
        :meth:`to_pandas` / :meth:`to_arrow`, when the typed error must be
        caught.
        """
        cdef _QueryStreamProducer prod
        if _called_by_old_polars():
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                f'polars {sys.modules["polars"].__version__} misreads '
                'multi-batch query results. Upgrade to polars 1.32 or '
                'later, or ' + (
                    'run the query again and call to_polars().'
                    if self._consumed else 'call result.to_polars().'))
        # Read the field after the check, which runs Python code.
        prod = self._schema_producer
        # Streaming: hand batches out incrementally. A post-delivery
        # failover bumps the cursor's _reset_seq; the capsule producer
        # surfaces FailoverWouldDuplicate rather than feeding the
        # replayed batch-0 as a duplicate (see _qs_pull).
        if prod is not None:
            # `__arrow_c_schema__` already pulled the first batch: hand
            # out that producer. Clear the field only when the capsule
            # exists, so that close() still reaches the cursor on failure.
            capsule = _query_stream_capsule(prod)
            self._schema_producer = None
            self._consumed_by = '__arrow_c_stream__()'
            return capsule
        return _query_stream_capsule(_new_query_stream_producer(
            self._take_cursor_handle('__arrow_c_stream__()')))

    def to_arrow(self):
        """Read the full result into a ``pyarrow.Table``. Requires pyarrow.

        SYMBOL columns use ``uint32`` dictionary indices, which
        ``Table.to_pandas()`` accepts only from pyarrow 23. For a pandas
        frame, call :meth:`to_pandas` instead. DECIMAL columns with a
        precision of 18 or less arrive as ``decimal32`` / ``decimal64``,
        which need pyarrow 20 or later.

        Materialise-whole: a mid-query failover replays the result
        transparently — the partial accumulation we hold is discarded
        from batch-0. The streaming path (``__arrow_c_stream__``, as
        consumed by ``polars.DataFrame(result)`` or ``pa.table(result)``)
        does not replay: on a post-delivery failover, the consumer raises
        its own error, and the message contains
        ``[FailoverWouldDuplicate]``.
        """
        import pyarrow as pa
        handle = self._take_cursor_handle('to_arrow()')
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
        With no arguments or a ``dtype_backend``, a ``GEOHASH`` column is
        a signed integer sized by its precision: ``int8`` up to 7 bits,
        ``int16`` to 15, ``int32`` to 31, ``int64`` to 60, as
        :meth:`to_arrow` gives it.
        Analysis-safe (aggregations skip ``pd.NA``/``NaN``), and feeds back
        into :meth:`QuestDB.dataframe` for a type round-trip — the column
        kinds are carried in ``df.attrs['questdb']``.

        ``dtype_backend="pyarrow"`` / ``"numpy_nullable"`` / ``types_mapper``
        select the pyarrow-backed path instead (``pd.ArrowDtype``, pandas
        nullable extension dtypes, or a custom mapper) — matching the
        ``pd.read_sql`` / ``pd.read_parquet`` convention. Those paths
        attach ``df.attrs['questdb']`` too: a pandas dtype holds an Arrow
        type and no field, so ``attrs`` is what carries the IPV4 / CHAR /
        GEOHASH / UUID / LONG256 claims that live in Arrow field metadata.
        :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` reads the
        claim back from either built-in backend — including the masked
        and object-dtype columns ``"numpy_nullable"`` produces for those
        five — so writing the result out again gives the same column
        types, and for those five the same wire bytes.

        A custom ``types_mapper`` gets the same ``attrs``, but whether
        the claim can be applied depends on the dtype the mapper picked.
        It has to be a shape that still holds the value:
        an Arrow-backed column, a NumPy ``uint32`` / ``uint16`` / integer
        column, or an object column of Python ints or ``bytes``. Map one
        of those five columns to a float or a string dtype and the claim
        is ignored — the column lands as that dtype implies, and
        re-ingestion logs a ``questdb`` warning about the dropped claim.

        ``df.attrs['questdb']`` reads as an ordinary mapping but cannot
        be edited in place: pandas deep-copies the whole of ``attrs``
        once per column whenever it propagates it, so a per-column claim
        that copied would make slicing and exporting quadratic in the
        column count. Every copy of the frame shares the one claim
        instead. To change what a frame claims, assign a new mapping to
        ``df.attrs['questdb']``, or state the type outright with
        ``schema_overrides`` / ``symbols``, which outrank it.
        """
        if dtype_backend is not None and types_mapper is not None:
            raise ValueError(
                'pass at most one of dtype_backend, types_mapper')
        if dtype_backend is None and types_mapper is None:
            return self._to_pandas_numpy()
        import pyarrow as pa
        # Check the arguments before the result is read: a bad value must
        # not consume the cursor.
        kwargs = _resolve_arrow_to_pandas_kwargs(dtype_backend, types_mapper)
        handle = self._take_cursor_handle('to_pandas()')
        schema, batches = _fetch_all_record_batches(handle, pa)
        table = (pa.table({}) if schema is None
                 else _table_shared_symbol_dict(
                     pa.Table.from_batches(batches, schema)))
        frame = table.to_pandas(**kwargs)
        return _attach_arrow_roundtrip_attrs(frame, table.schema)

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
        unavailable. If that path fails, it also falls back and issues a
        ``RuntimeWarning``: the values stay correct, but the read is slower.
        Under ``-W error`` that warning raises after the result was read,
        so the frame is lost; run the query again.

        Materialise-whole: a mid-query failover replays the result
        transparently. This accumulates batches in-library (via pyarrow)
        so the partial result can be discarded on failover. The
        pyarrow-free ``polars.DataFrame(result)`` streams through
        ``__arrow_c_stream__`` and does not replay: on a post-delivery
        failover, polars raises its own error, and the message contains
        ``[FailoverWouldDuplicate]``. polars before 1.37.1 raises a
        ``PanicException``, which ``except Exception`` does not catch.
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
        handle = self._take_cursor_handle('to_polars()')
        schema, batches = _fetch_all_record_batches(handle, pa)
        if schema is None:
            return pl.from_arrow(pa.table({}))
        table = pa.Table.from_batches(batches, schema)
        if getattr(pl, 'Categories', None) is None:
            return pl.from_arrow(table)
        try:
            return _polars_dataframe_hybrid(table, pl, pa, {})
        except Exception as exc:
            return _polars_hybrid_fallback(table, pl, exc)

    def _to_pandas_numpy(self):
        return _numpy_frame_from_cursor(
            self._take_cursor_handle('to_pandas()'))

    def iter_arrow(self):
        """Iterate result batches as ``pyarrow.RecordBatch``.

        Streaming: a mid-query failover after the first batch has been
        yielded surfaces ``QuestDBErrorCode.FailoverWouldDuplicate`` (the
        already-yielded batches cannot be discarded); re-issue the query.
        If the iterator is abandoned partway, cleanup runs at the next
        garbage-collection cycle; call :meth:`close` (or use the context-
        manager) for deterministic release.
        """
        reader = _build_record_batch_reader(
            self._take_cursor_handle('iter_arrow()'), True)
        for batch in reader:
            yield batch

    def iter_pandas(self, *, dtype_backend=None, types_mapper=None):
        """Iterate result batches as ``pandas.DataFrame``.

        Mirrors :meth:`to_pandas`: with no arguments each batch is
        materialised straight into numpy (no pyarrow, sentinel-preserving).
        ``dtype_backend`` / ``types_mapper`` select the pyarrow-backed path
        instead. Either way every batch carries ``df.attrs['questdb']``,
        a ``GEOHASH`` column gets the same signed integer type, and what
        a custom ``types_mapper`` costs the round trip is the same as it
        is there.
        """
        if dtype_backend is not None and types_mapper is not None:
            raise ValueError(
                'pass at most one of dtype_backend, types_mapper')
        if dtype_backend is None and types_mapper is None:
            return _NumpyBatchIter(self._take_cursor_handle('iter_pandas()'))
        return self._iter_pandas_arrow(dtype_backend, types_mapper)

    def _iter_pandas_arrow(self, dtype_backend, types_mapper):
        import pyarrow as pa
        kwargs = _resolve_arrow_to_pandas_kwargs(dtype_backend, types_mapper)
        reader = _build_record_batch_reader(
            self._take_cursor_handle('iter_pandas()'))
        # `_table_shared_symbol_dict` re-types SYMBOL columns but keeps
        # every field's name and metadata, which is all the claim reads,
        # so the stream's own schema gives the same answer for every
        # batch.
        claim = _arrow_roundtrip_claim(reader.schema)
        for batch in reader:
            table = _table_shared_symbol_dict(pa.Table.from_batches([batch]))
            frame = table.to_pandas(**kwargs)
            frame.attrs['questdb'] = claim
            yield frame

    def iter_polars(self):
        """Iterate result batches as ``polars.DataFrame``.

        Mirrors :meth:`to_polars` per batch (same ``Categorical`` SYMBOL
        handling) for streaming / low-peak-memory consumption. Every batch's
        SYMBOL Categoricals share one persistent ``Categories`` identity, so
        ``polars.concat`` over the yielded frames stitches without a
        categories-mismatch error. If the ``Categorical`` fast path fails,
        it issues one ``RuntimeWarning`` and builds the remaining frames with
        ``polars.from_arrow``, cast to the same ``Categories``. If that cast
        fails too, the warning says so: those frames have a plain
        ``Categorical`` dtype, and ``polars.concat`` with the frames before
        fails.

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
        return _PolarsBatchIter(
            self._take_cursor_handle('iter_polars()'), pl, pa)

    def _open_rows(self, str what):
        """Start (or re-use) this result's row stream."""
        cdef _CursorHandle handle = self._exec_done_handle
        if self._row_iter is None:
            self._row_columns, self._row_iter = _open_row_stream(
                self._take_cursor_handle(what))
        elif handle._rows_cut_short:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'QueryResult row stream was cut short by an error or by '
                'closing its iterator; re-issue the query')
        return self._row_iter

    def columns(self):
        """Column names and QuestDB types as ``[(name, type_name), ...]``.

        ``type_name`` is the QuestDB DDL spelling — ``VARCHAR``,
        ``TIMESTAMP``, ``TIMESTAMP_NS``, ``SYMBOL``, ``IPv4``,
        ``GEOHASH(8c)``, ``DECIMAL(18,2)``, ``DOUBLE[]`` — matching
        ``ColumnType`` on the server, so it can be compared against
        ``tables()`` / ``information_schema.questdb_columns()`` without
        normalising. Use ``select ... limit 0`` to read a schema without
        fetching rows; such a probe is read to its end here, so its
        connection goes back to the pool.

        Does not consume the rows — :meth:`iter_rows` continues from the
        same stream — but does consume the result, so it rules out
        :meth:`to_pandas`, :meth:`to_arrow` and the other materialisation
        methods. It decodes no rows, is cheap to call repeatedly, and once
        read still answers after :meth:`close`.

        A result that returned rows is not at its terminal until they have
        been read, so closing it straight after ``columns()`` drops the
        connection (and on a :class:`PooledReader` lease makes the lease
        terminal). Iterate the rows, or call :meth:`cancel` before
        :meth:`close`, to keep it; a ``LIMIT 0`` probe needs neither.

        Empty for a statement that ships no result set at all (a
        non-SELECT); see :attr:`exec_done` for that outcome.

        Three limits are the protocol's, not this client's:

        - A ``DECIMAL`` column is reported with its storage width's maximum
          precision (18, 38 or 76) because the wire carries only the scale:
          a column declared ``DECIMAL(10,2)`` reads back as
          ``DECIMAL(18,2)``. Compare on scale when that matters.
        - A legacy ``STRING`` column is reported as ``VARCHAR``: QWP has no
          separate STRING column kind.
        - An array column is reported as ``DOUBLE[]`` / ``LONG[]`` whatever
          its dimensionality, because the wire schema carries no dimension
          count; the server spells a 2-D column ``DOUBLE[][]``.

        .. code-block:: python

            with db.query('select * from trades limit 0') as result:
                for name, type_name in result.columns():
                    print(name, type_name)
        """
        if self._row_columns is None:
            self._open_rows('columns()')
        return list(self._row_columns)

    def iter_rows(self):
        """Iterate the result as tuples of Python objects.

        For DB-API-shaped callers — a PEP 249 cursor, dbt, plain scripting —
        and for small result sets where exact QuestDB types matter more than
        throughput: metadata lookups, test rows, ``limit 0`` schema probes.
        Needs neither ``pyarrow`` nor ``pandas``. Large analytical results
        belong on :meth:`to_pandas` / :meth:`to_arrow`, which stay columnar;
        a row of Python objects costs an allocation per cell.

        Streams one batch at a time: the batch being decoded is the only one
        held in memory. ``None`` marks a SQL NULL in every column.

        Value mapping:

        ========================================  ==========================
        QuestDB                                   Python
        ========================================  ==========================
        ``BOOLEAN``                               ``bool``
        ``BYTE``, ``SHORT``, ``INT``, ``LONG``    ``int``
        ``FLOAT``, ``DOUBLE``                     ``float`` (never NaN: the
                                                  server sends NULL)
        ``VARCHAR``, ``SYMBOL``                   ``str``
        ``CHAR``                                  one-character ``str``;
                                                  code point 0 is ``None``
        ``TIMESTAMP``                             ``datetime`` (UTC-aware,
                                                  microseconds)
        ``TIMESTAMP_NS``                          ``datetime`` (UTC-aware),
                                                  rounded down to
                                                  microseconds
        ``DATE``                                  ``datetime`` (UTC-aware,
                                                  milliseconds)
        ``UUID``                                  ``uuid.UUID``
        ``LONG256``                               ``int`` (unsigned)
        ``DECIMAL(p,s)``                          ``decimal.Decimal``
                                                  (scale preserved)
        ``GEOHASH(nc)``                           ``str`` (base-32, or
                                                  '0'/'1' bits when n is
                                                  not a multiple of 5)
        ``IPv4``                                  ``ipaddress.IPv4Address``
        ``BINARY``                                ``bytes``
        ``DOUBLE[]``                              ``numpy.ndarray``
        ========================================  ==========================

        Datetimes are timezone-aware UTC, matching what
        :meth:`to_arrow` labels them. ``TIMESTAMP_NS`` is rounded down
        (towards the past) because ``datetime`` has no nanosecond field; use
        :meth:`to_pandas` or :meth:`to_arrow` for full precision. A temporal
        value outside ``datetime``'s years 1..9999 raises
        ``QuestDBErrorCode.InvalidTimestamp`` naming the column. Like any
        other error raised while decoding, it closes the result before its
        terminal, so the connection is dropped (on a :class:`PooledReader`
        lease, the lease becomes terminal). A server-side error — a parse
        error, a timeout — ends at a terminal frame instead and keeps the
        connection. A ``CHAR`` holding a UTF-16 surrogate code unit comes
        back as that lone surrogate, which cannot be encoded to UTF-8.
        ``LONG[]`` columns are not supported on this path.

        Every call returns the same iterator, so :meth:`columns` and
        ``iter_rows()`` share one stream and an exhausted iterator stays
        empty. The batch being handed out is the only one held in memory.
        After :meth:`cancel` no further rows are yielded, including any
        already decoded; after :meth:`close` the iterator raises. A fresh
        ``iter_rows()`` call raises too once the stream ended with an error
        or its iterator was closed. A mid-query failover after iteration
        has begun raises
        ``QuestDBErrorCode.FailoverWouldDuplicate`` rather than replaying
        rows the caller already holds.

        .. code-block:: python

            with db.query('select ts, symbol from trades limit 3') as result:
                for ts, symbol in result.iter_rows():
                    print(ts, symbol)
        """
        return self._open_rows('iter_rows()')

    @property
    def exec_done(self):
        """The statement's terminal ``EXEC_DONE`` report, or ``None``.

        An :class:`ExecDone` ``(op_type, rows_affected)`` once a non-SELECT
        has been drained to its clean end; ``None`` before that, and
        ``None`` for a statement that streamed a result set instead (a
        ``SELECT`` ends with ``END``, which carries no counts).

        ``rows_affected`` is meaningful for ``INSERT`` and ``UPDATE``, and
        ``0`` for a statement that carries no count — one executed at parse
        time (``TRUNCATE``, ``RENAME TABLE``, ``SET``,
        ``ALTER TABLE ... RESUME WAL``, ...). ``op_type`` is
        the server's QWP operation-type byte, carried through as an opaque
        ``int``: its values are the server's, not part of this client's
        API. Still available after :meth:`close`.

        .. code-block:: python

            with db.query('insert into t values (1)') as result:
                list(result.iter_rows())    # drains it
            print(result.exec_done.rows_affected)
        """
        cdef _CursorHandle handle = self._exec_done_handle
        if handle is None:
            return None
        with handle._lock:
            if not handle._exec_done_valid:
                return None
            return ExecDone(
                int(handle._exec_done_op_type),
                int(handle._exec_done_rows))

    def cancel(self):
        """Cancel the query and drain to terminal.

        The result remains open; call :meth:`close` before starting
        another query on the same reader. On failure, the result is
        closed. Idempotent after success. Rows already decoded but not
        yet handed out by :meth:`iter_rows` are not yielded afterwards, and
        a result not yet read cannot be read afterwards: :meth:`columns`,
        :meth:`iter_rows` and the materialisation methods raise
        ``QuestDBErrorCode.InvalidApiCall``.

        Cancel between rows, not from a signal handler that interrupts a
        batch being decoded on the same thread: that call is refused with
        ``QuestDBErrorCode.InvalidApiCall``, because it would invalidate
        the batch under the decoder.
        """
        cdef _CursorHandle handle = self._cancel_handle
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef bint reusable
        cdef qwp_reader_cursor* cursor
        cdef object exc = None
        # Without a handle there is no pending `__arrow_c_schema__` producer:
        # close() drops it when it detaches the handle.
        if handle is None:
            return
        handle._check_not_driving('cancel', check_lock=True)
        with handle._lock:
            handle._check_not_driving('cancel')
            if handle._pinned > 0:
                # Same-thread re-entry (a signal handler) while a decoder
                # holds pointers into the current batch: cancelling would
                # drop that batch under it.
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'cancel() called while this result is being decoded '
                    'on the same thread; cancel it between rows instead')
            # A pending `__arrow_c_schema__` producer holds the first batch. A
            # stream after the cancel would give that batch and then a clean
            # end: a truncated result without an error. Drop it, once the
            # cancel can no longer be refused.
            self._drop_schema_producer()
            cursor = handle._cursor
            if cursor == NULL:
                return
            handle._pending_batch = NULL
            handle._native_in_flight = True
            handle._pin()
            try:
                with nogil:
                    ok = qwp_reader_cursor_cancel(cursor, &err)
                    reusable = qwp_reader_cursor_connection_reusable(cursor)
                # Rows decoded ahead of the consumer stop flowing from here.
                handle._cancelled = True
            finally:
                # Cleared before `_unpin`, which may run a deferred free.
                handle._native_in_flight = False
                handle._unpin()
        if not ok:
            if err != NULL:
                exc = _reader_err_to_py(err)
                err = NULL
            else:
                exc = QuestDBError(
                    QuestDBErrorCode.ServerFlushError,
                    'qwp_reader_cursor_cancel returned false '
                    'without setting err_out')
        if reusable:
            try:
                _mark_reader_drained(handle)
            except BaseException as mark_exc:
                self.close()
                if exc is not None:
                    raise exc from mark_exc
                raise
        if exc is not None:
            self.close()
            raise exc

    def close(self):
        """Release the cursor and reader. Idempotent.

        Terminal connections return to the pool; others are dropped.
        This does not cancel the query. Outstanding iterators become
        invalid.
        """
        cdef _CursorHandle handle = self._cancel_handle
        if handle is not None:
            # A native next_batch/next_arrow_batch may still borrow this cursor
            # when Python code runs on this thread mid-operation (a finalizer,
            # say). Reject re-entry before detaching the only public references.
            handle._check_not_driving('close', check_lock=True)
        self._drop_schema_producer()
        self._cursor_handle = None
        self._consumed = True
        # Let go of a suspended row stream so its buffered batch can be
        # collected; a caller still holding the iterator sees 'cursor is
        # closed' on its next step. The column list stays answerable.
        self._row_iter = None
        if handle is not None:
            # Waits for another thread's call on the cursor to return. If
            # that wait is interrupted, the call frees the cursor instead.
            handle._close()
        self._cancel_handle = None

    def _drop_schema_producer(self):
        cdef _QueryStreamProducer prod = self._schema_producer
        if prod is not None:
            # Nothing outside this result has the producer yet, and nothing
            # was read: a later read says "closed", not "read by". Drop
            # the saved schema too, so that a schema read and a stream
            # read agree. After a stream was taken, keep it.
            self._schema_producer = None
            self._schema_box = None
            self._consumed_by = None
            prod._free_cached()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __copy__(self):
        raise TypeError('QueryResult cannot be copied: it is a single-use '
                        'stream over one cursor')

    def __del__(self):
        cdef _CursorHandle handle
        cdef _QueryStreamProducer prod
        cdef int outcome = 0
        try:
            handle = self._cursor_handle
            if handle is None:
                # After `__arrow_c_schema__`, a pending producer holds the
                # cursor. Do not use `_cancel_handle` for a read result: a
                # stream given to a consumer owns its cursor.
                prod = self._schema_producer
                if prod is not None:
                    handle = prod.cursor_handle
                elif self._consumed_by is None:
                    # cancel() dropped the pending producer, so only this
                    # result holds the cursor. (After close() or a failed
                    # schema read, the handle is gone or freed.)
                    handle = self._cancel_handle
            if handle is None:
                return
            # Reclaim FIRST, warn after. The warning used to come first inside
            # this same `except Exception: pass`, so under `-W error` (or
            # `simplefilter('error')`) the ResourceWarning raised, was
            # swallowed, and `close()` never ran -- the deterministic release
            # silently degraded to `_CursorHandle.__dealloc__`. Ordering the
            # free ahead of the warning makes that structurally impossible.
            outcome = handle._try_reclaim()
            if outcome < 0:
                # Another thread owns the lock. It may not be freeing; leave
                # the handle attached so its eventual __dealloc__ reclaims the
                # cursor without blocking this finalizer.
                return
            self._drop_schema_producer()
            self._cursor_handle = None
            self._cancel_handle = None
            self._consumed = True
        except Exception:
            return
        if outcome > 0:
            try:
                warnings.warn(
                    'QueryResult was neither drained nor closed; its '
                    'pooled connection is being released by the garbage '
                    'collector. Use `with db.query(...)` or call '
                    '`close()`.',
                    ResourceWarning,
                    stacklevel=2)
            except Exception:
                pass
