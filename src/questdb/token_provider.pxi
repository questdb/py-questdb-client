################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2025 QuestDB
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

# Rotating Bearer-token providers: the Python face of the native
# `questdb/token_provider.h` API.
#
# A `RefreshingTokenProvider` is a native, proactively refreshing cache in
# front of a Python token source. An application provider -- any callable
# returning a token, or an object with `token()` and an optional
# `on_rejected()` -- is bridged through the native function table. Either
# attaches to `Sender` and `questdb.connect` with `bearer_token_provider=`.
#
# Native code never holds a Python reference. `_TOKEN_CALLBACKS` maps native
# `user_data` ids to weak references to GC-visible handles. Each handle owns
# its source/provider, and clients retain the handle's wrapper for as long as
# native clones can call it. The registry must not root ownership cycles such
# as a bound source method whose owner also owns the provider. Dispatch retains
# the handle strongly until the callback returns. The native release callback
# is deliberately empty: it can run on any thread after interpreter finalization,
# so it must never enter Python (see `_oidc_user_data_release_trampoline`).
# Every callback goes through the process-wide callback gate that `oidc.pxi`
# drains before finalization. Once questdb's atexit hook has closed it, a
# provider callback the main thread makes from inside its own native call --
# an ILP/HTTP flush from `logging.shutdown()`, say -- still runs (see
# `_tp_main_thread_before_finalization`).

import weakref
from functools import partial as _token_partial
from types import MethodType as _TokenBoundMethod

cdef dict _TOKEN_CALLBACKS = {}
# Weak values do not root provider/client ownership cycles. Use identity keys:
# application providers need not be hashable or comparable. Publish a prebuilt
# weakref with dict.setdefault so GC/reentrant construction cannot replace an
# already-published holder.
cdef dict _APPLICATION_TOKEN_PROVIDERS = {}
# Reentrant: a wrapper collected while this thread holds the lock (a
# garbage collection triggered inside a locked section) unregisters itself.
cdef object _TOKEN_CALLBACKS_LOCK = threading.RLock()
cdef size_t _token_callback_last_id = 0


def _token_after_fork_in_child():
    # A parent thread may have held this lock when it disappeared. Inherited
    # providers remain unusable, but even cleanup must not take the old lock.
    global _TOKEN_CALLBACKS_LOCK, _APPLICATION_TOKEN_PROVIDERS
    _TOKEN_CALLBACKS_LOCK = threading.RLock()
    _APPLICATION_TOKEN_PROVIDERS = {}


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_token_after_fork_in_child)

# Longest token-source error message passed to native, which sanitizes and
# caps it at 256 characters anyway.
cdef Py_ssize_t _TOKEN_ERROR_MSG_MAX = 1024

# Token callbacks in progress on each thread, kept as the integer value of a
# native thread-specific slot: counting allocates nothing and cannot raise,
# and nothing is left to free when the thread ends. A callback runs inside the
# client call that needs the token -- for ILP/HTTP, the flush on the caller's
# own thread -- which may still hold the sender and its buffer, so Python
# running there is never the frame's own (see `Buffer._reentered_from_frame`).
cdef Py_tss_t _TOKEN_CALLBACK_DEPTH_KEY

if PyThread_tss_create(&_TOKEN_CALLBACK_DEPTH_KEY) != 0:
    raise RuntimeError('Could not create the QuestDB token-callback key.')


cdef inline bint _token_callback_enter() noexcept:
    """Count a token callback on this thread. The result goes to the matching
    `_token_callback_exit`: False when the count could not be recorded."""
    cdef uintptr_t depth = <uintptr_t>PyThread_tss_get(
        &_TOKEN_CALLBACK_DEPTH_KEY)
    return PyThread_tss_set(
        &_TOKEN_CALLBACK_DEPTH_KEY, <void*>(depth + 1)) == 0


cdef inline void _token_callback_exit(bint entered) noexcept:
    cdef uintptr_t depth
    if entered:
        depth = <uintptr_t>PyThread_tss_get(&_TOKEN_CALLBACK_DEPTH_KEY)
        PyThread_tss_set(&_TOKEN_CALLBACK_DEPTH_KEY, <void*>(depth - 1))


cdef inline bint _token_callback_on_this_thread() noexcept:
    return PyThread_tss_get(&_TOKEN_CALLBACK_DEPTH_KEY) != NULL


class ExpiringToken:
    """
    A token and when it expires: the result of one token-source fetch.

    ``expires_at`` and the optional ``refresh_at`` (the platform's suggested
    refresh time) are timezone-aware :class:`datetime.datetime` objects or
    epoch seconds, within Python's datetime range. Values outside that range
    are rejected so provider status snapshots always remain readable. If the
    platform reports a relative lifetime, such as
    OAuth's ``expires_in``, use :meth:`expires_in`, which computes the expiry
    from the local time of receipt.

    The ``repr`` never shows the token.
    """
    __slots__ = ('token', 'expires_at', 'refresh_at')

    def __init__(self, token, expires_at, refresh_at=None):
        if not isinstance(token, str):
            raise TypeError(
                f'"token" must be a str, not {_fqn(type(token))}')
        self.token = token
        self.expires_at = _tp_epoch_seconds(expires_at, 'expires_at')
        self.refresh_at = (
            None if refresh_at is None
            else _tp_epoch_seconds(refresh_at, 'refresh_at'))

    @classmethod
    def expires_in(cls, token, lifetime, refresh_at=None):
        """A token that expires ``lifetime`` (seconds or a
        :class:`datetime.timedelta`) from now."""
        return cls(
            token,
            time.time() + _tp_seconds(lifetime, 'lifetime'),
            refresh_at)

    def __repr__(self):
        return (
            f'ExpiringToken(<redacted, {len(self.token)} chars>, '
            f'expires_at={_tp_format_epoch(self.expires_at)})')


class TokenUnavailableError(QuestDBError):
    """
    A token source or provider could not supply a token.

    Raise it from a source or provider to classify the failure: ``retryable``
    says whether retrying can succeed -- network failures, timeouts, HTTP 429
    and 5xx are retryable; missing or wrong configuration is not -- and
    ``retry_after`` (seconds or a :class:`datetime.timedelta`) is the
    platform's suggested wait. Any other exception from a source counts as
    retryable.

    A connection the caller waits for -- an eager :func:`questdb.connect`, a
    sender's synchronous initial connect -- retries only a retryable failure.
    Background reconnects retry either kind. To stop them on a failure that
    retrying can never clear, an application provider raises
    :class:`~questdb.QuestDBError` with ``code``
    ``QuestDBErrorCode.InvalidApiCall`` instead; a
    :class:`RefreshingTokenProvider` is stopped by closing it.

    The client raises it too, when a provider failed to supply a token; then
    ``code`` is the failing operation's error code. Never put a token in the
    message.
    """
    def __init__(
            self, message, *, retryable=True, retry_after=None, code=None):
        if code is None:
            code = (QuestDBErrorCode.SocketError if retryable
                    else QuestDBErrorCode.AuthError)
        super().__init__(code, message)
        self._retryable = bool(retryable)
        self._retry_after = (
            None if retry_after is None
            else _tp_seconds(retry_after, 'retry_after'))

    @property
    def retryable(self) -> bool:
        """Whether retrying can succeed."""
        return self._retryable

    @property
    def retry_after(self):
        """The suggested wait before retrying, in seconds, or ``None``."""
        return self._retry_after


class FetchFailure:
    """A failed fetch of a :class:`RefreshingTokenProvider`. Never carries the
    token."""
    __slots__ = ('retryable', 'retry_after', 'message', 'at')

    def __init__(self, retryable, retry_after, message, at):
        #: Whether the source classified the failure as retryable.
        self.retryable = retryable
        #: The source's suggested wait in seconds, or ``None``.
        self.retry_after = retry_after
        #: The sanitized message, at most 256 characters.
        self.message = message
        #: When the fetch failed, a timezone-aware ``datetime``.
        self.at = at

    def __repr__(self):
        return (
            f'FetchFailure(retryable={self.retryable!r}, '
            f'retry_after={self.retry_after!r}, at={self.at!r}, '
            f'message={self.message!r})')


class TokenProviderStatus:
    """A snapshot of a :class:`RefreshingTokenProvider`. Never contains the
    token."""
    __slots__ = (
        'closed', 'token_expires_at', 'last_success_at',
        'consecutive_failures', 'last_failure')

    def __init__(
            self, closed, token_expires_at, last_success_at,
            consecutive_failures, last_failure):
        #: Whether the provider is closed.
        self.closed = closed
        #: When the held token expires, or ``None``.
        self.token_expires_at = token_expires_at
        #: When the last successful fetch completed, or ``None``.
        self.last_success_at = last_success_at
        #: Failed fetches since the last successful one.
        self.consecutive_failures = consecutive_failures
        #: The most recent :class:`FetchFailure`, kept after recovery.
        self.last_failure = last_failure

    def __repr__(self):
        return (
            f'TokenProviderStatus(closed={self.closed!r}, '
            f'token_expires_at={self.token_expires_at!r}, '
            f'last_success_at={self.last_success_at!r}, '
            f'consecutive_failures={self.consecutive_failures!r}, '
            f'last_failure={self.last_failure!r})')


cdef double _tp_seconds(object value, str name) except? -1.0:
    cdef double seconds
    if isinstance(value, datetime.timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value
    else:
        raise TypeError(
            f'"{name}" must be a number of seconds or a timedelta, '
            f'not {_fqn(type(value))}')
    if not (seconds >= 0.0) or seconds == float('inf'):
        raise ValueError(f'"{name}" must be a finite, non-negative duration')
    return seconds


cdef double _tp_epoch_seconds(object value, str name) except? -1.0:
    cdef double seconds
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            raise ValueError(
                f'"{name}" must be a timezone-aware datetime; a naive one '
                'is ambiguous')
        seconds = value.timestamp()
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        if value != value or value in (float('inf'), float('-inf')):
            raise ValueError(f'"{name}" must be finite')
        seconds = value
    else:
        raise TypeError(
            f'"{name}" must be a datetime or epoch seconds, '
            f'not {_fqn(type(value))}')
    # Every accepted expiry must also be representable in status snapshots.
    # Validate all source shapes, including mutated ExpiringToken instances.
    try:
        datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise ValueError(
            f'"{name}" must be representable as a Python datetime') from None
    return seconds


cdef object _tp_format_epoch(double seconds):
    try:
        return datetime.datetime.fromtimestamp(
            seconds, datetime.timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return repr(seconds)


cdef object _tp_datetime_from_ms(int64_t ms):
    if ms < 0:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000.0, datetime.timezone.utc)


# 2**64 as a double. Converting a double at or above it to `uint64_t` is
# undefined behaviour in C (C11 6.3.1.4): the result differs by compiler and
# architecture, and some x86-64 code sequences give 0, so a "wait forever"
# duration such as `sys.maxsize` could become no wait at all. Native caps
# every duration at 100 years anyway.
cdef double _TP_UINT64_LIMIT = 18446744073709551616.0


cdef uint64_t _tp_duration_ms(object value, str name) except? 0:
    cdef double ms = _tp_seconds(value, name) * 1000.0 + 0.5
    if ms >= _TP_UINT64_LIMIT:
        return <uint64_t>-1
    return <uint64_t>ms


cdef int64_t _tp_retry_after_ms(double seconds) noexcept:
    """A source's non-negative ``retry_after`` in milliseconds, clamped below
    the `int64_t` range: converting the Python float overflowed past it, and
    the source's failure was then reported unclassified. Native caps the wait
    at ``retry_after_max`` anyway."""
    if seconds * 1000.0 >= 9.2e18:
        return <int64_t>9200000000000000000
    return <int64_t>(seconds * 1000.0)


cdef int64_t _tp_epoch_ms(double seconds) except? -1:
    if seconds * 1000.0 >= 9.2e18:
        return <int64_t>9200000000000000000
    if seconds * 1000.0 <= -9.2e18:
        return <int64_t>-9200000000000000000
    return <int64_t>(seconds * 1000.0)


cdef tuple _tp_result_parts(object result):
    """``(token, expires_at_ms, refresh_at_ms)`` from what a source returned:
    an :class:`ExpiringToken`, a ``(token, expires_at[, refresh_at])`` tuple,
    or any object with ``token`` and ``expires_on`` (epoch seconds) plus an
    optional ``refresh_on``, such as Azure Core's ``AccessToken`` and
    ``AccessTokenInfo``."""
    cdef object token, expires_at, refresh_at = None
    if isinstance(result, ExpiringToken):
        token = result.token
        expires_at = _tp_epoch_seconds(result.expires_at, 'expires_at')
        if result.refresh_at is not None:
            refresh_at = _tp_epoch_seconds(result.refresh_at, 'refresh_at')
    elif isinstance(result, tuple) and len(result) in (2, 3):
        token = result[0]
        expires_at = _tp_epoch_seconds(result[1], 'expires_at')
        if len(result) == 3 and result[2] is not None:
            refresh_at = _tp_epoch_seconds(result[2], 'refresh_at')
    elif hasattr(result, 'token') and hasattr(result, 'expires_on'):
        token = result.token
        expires_at = _tp_epoch_seconds(result.expires_on, 'expires_on')
        refresh_on = getattr(result, 'refresh_on', None)
        if refresh_on is not None:
            refresh_at = _tp_epoch_seconds(refresh_on, 'refresh_on')
    else:
        raise TypeError(
            'a token source must return an ExpiringToken, a '
            '(token, expires_at) tuple, or an object with "token" and '
            f'"expires_on" attributes, not {_fqn(type(result))}')
    if not isinstance(token, str):
        raise TypeError(
            f'the token must be a str, not {_fqn(type(token))}')
    return (
        token,
        _tp_epoch_ms(expires_at),
        0 if refresh_at is None else _tp_epoch_ms(refresh_at))


cdef bint _tp_on_main_thread():
    # The native ident, like every other main-thread check: under gevent,
    # `threading.get_ident()` is a greenlet id.
    return questdb_thread_ident() == _OIDC_MAIN_THREAD_IDENT


cdef void _tp_sink_text_error(
        questdb_token_sink* sink,
        bint retryable,
        int64_t retry_after_ms,
        str message) noexcept:
    cdef bytes encoded
    try:
        encoded = message[:_TOKEN_ERROR_MSG_MAX].encode('utf-8', 'replace')
        questdb_token_sink_set_error(
            sink, retryable, retry_after_ms, encoded, len(encoded))
    except BaseException:
        questdb_token_sink_set_error(
            sink, True, -1, b'the token callback failed', 25)


cdef bint _tp_sink_terminal_error(
        questdb_token_sink* sink, str message) except -1:
    """Report a failure that retrying can never clear, which also stops
    background reconnects. False, recording nothing, when the sink refuses
    one: a refreshing provider's source cannot report it."""
    cdef bytes encoded = message[:_TOKEN_ERROR_MSG_MAX].encode(
        'utf-8', 'replace')
    return questdb_token_sink_set_terminal_error(
        sink, encoded, len(encoded))


cdef void _tp_sink_exception(
        questdb_token_sink* sink, object exc, object signal_codes) noexcept:
    """Report provider failures; preserve main-thread interruptions separately.

    Snapshot handler codes before invoking user code: a signal handler can
    unregister itself before raising any Exception subclass.
    """
    cdef int64_t retry_after_ms = -1
    try:
        if _tp_on_main_thread() and (
                not isinstance(exc, Exception)
                or _oidc_raised_in_signal_handler(exc, signal_codes)):
            _oidc_park_main(exc)
            _oidc_schedule_parked_main()
            _tp_sink_text_error(
                sink, True, -1, 'the token callback was interrupted')
            return
        if isinstance(exc, TokenUnavailableError):
            if exc.retry_after is not None:
                retry_after_ms = _tp_retry_after_ms(exc.retry_after)
            _tp_sink_text_error(sink, exc.retryable, retry_after_ms, str(exc))
            return
        # `InvalidApiCall` is the failure retrying can never clear, as for a
        # Rust provider or a C++ one throwing `invalid_api_call`: clients stop
        # reconnecting. A source's sink refuses it, and the exception then
        # counts as retryable like any other. A TokenUnavailableError, handled
        # above, keeps its classification whatever its code. The refusal of
        # a lease or pool wait this callback gave up on
        # (`_callback_wait_refused`) counts as retryable: that lease or pool
        # is busy, maybe waiting for this very token, not broken.
        if (isinstance(exc, QuestDBError)
                and getattr(exc, 'code', None)
                    is QuestDBErrorCode.InvalidApiCall
                and not getattr(exc, '_callback_wait_refused', False)
                and _tp_sink_terminal_error(sink, str(exc))):
            return
        _tp_sink_text_error(
            sink, True, -1, f'{type(exc).__qualname__}: {exc}')
    except BaseException:
        questdb_token_sink_set_error(
            sink, True, -1, b'the token callback failed', 25)


cdef void _tp_sink_unavailable(questdb_token_sink* sink) noexcept nogil:
    cdef const char* msg = (
        b'the Python interpreter is shutting down, so the Python token '
        b'callback cannot run')
    questdb_token_sink_set_error(sink, True, -1, msg, strlen(msg))


cdef inline bint _tp_main_thread_before_finalization() noexcept nogil:
    """Whether a provider callback the exit gate refused may run anyway.

    questdb's atexit hook (`_oidc_detach_diagnostics_at_exit`) closes the
    callback gate so that no native thread is entering Python when
    finalization begins. The main thread runs the atexit callbacks that come
    after that hook -- `logging.shutdown()`, and any registered before
    `import questdb` -- and only then finalizes the interpreter. A provider
    callback it makes from inside its own native call, such as an ILP/HTTP
    flush from a logging handler, therefore returns before finalization can
    start, and needs no drain. Refusing it failed that flush and lost its
    rows. Callbacks on any other thread -- a QWP/WebSocket connect, a
    refreshing provider's refresher -- stay refused.
    """
    return (questdb_thread_ident() == _OIDC_MAIN_THREAD_IDENT
            and not qdb_py_is_finalizing())


cdef void _token_source_dispatch(
        size_t callback_id, questdb_token_sink* sink) noexcept with gil:
    cdef object source
    cdef _TokenProviderHandle handle
    cdef bytes encoded
    cdef object result = None
    cdef object token = None
    cdef bint entered = False
    cdef bint counted = _token_callback_enter()
    try:
        handle = _token_callback_get(callback_id)
        if handle is None:
            _tp_sink_text_error(
                sink, True, -1, 'the token source was released')
            return
        # Native serializes source calls. Mark this one in progress BEFORE
        # checking `_source_closed`, which `close()` sets before it waits on
        # `_source_done`: either that wait sees the event cleared and waits
        # for this call, or this check sees the flag and the source never
        # runs. Checked first, a thread switch inside `clear()` -- Python
        # code -- let `close(wait=True)` return before the source ran.
        entered = True
        handle._source_done.clear()
        if handle._source_closed:
            _tp_sink_text_error(sink, False, -1, 'the token provider is closed')
            return
        source = handle._target
        if source is None:
            _tp_sink_text_error(
                sink, True, -1, 'the token source was released')
            return
        handle._source_thread = questdb_thread_ident()
        result = source()
        token, expires_ms, refresh_ms = _tp_result_parts(result)
        encoded = token.encode('utf-8')
        if not questdb_token_sink_set_token(
                sink, encoded, len(encoded), expires_ms, refresh_ms):
            _tp_sink_text_error(
                sink, True, -1, 'the token source returned an unusable token')
    except BaseException as exc:
        _tp_sink_exception(sink, exc, {})
    finally:
        if entered:
            # Drop user result/source locals before signalling the drain: their
            # destructors can run Python too.
            result = None
            token = None
            source = None
            handle._source_thread = 0
            handle._source_done.set()
        _token_callback_exit(counted)


cdef void _token_source_trampoline(
        void* user_data, questdb_token_sink* sink) noexcept nogil:
    cdef qdb_oidc_callback_gate* gate = qdb_oidc_callback_enter()
    if gate == NULL:
        _tp_sink_unavailable(sink)
        return
    try:
        if qdb_py_is_finalizing():
            _tp_sink_unavailable(sink)
            return
        qdb_oidc_callback_before_gil(gate)
        if qdb_oidc_callback_stopped(gate):
            _tp_sink_unavailable(sink)
            return
        _token_source_dispatch(<size_t>user_data, sink)
    finally:
        qdb_oidc_callback_leave(gate)


cdef void _token_get_dispatch(
        size_t callback_id, questdb_token_sink* sink) noexcept with gil:
    global _OIDC_MAIN_TOKEN_DEPTH
    cdef object provider
    cdef _TokenProviderHandle handle
    cdef bytes encoded
    # A mapping like `_oidc_signal_handler_codes()` returns, even before that
    # snapshot is taken: the error path below looks codes up in it.
    cdef object signal_codes = {}
    cdef bint on_main = questdb_thread_ident() == _OIDC_MAIN_THREAD_IDENT
    cdef bint counted = _token_callback_enter()
    if on_main:
        _OIDC_MAIN_TOKEN_DEPTH += 1
    try:
        try:
            PyErr_CheckSignals()
            if CYTHON_COMPILING_IN_PYPY:
                _oidc_signal_tick()
        except BaseException as exc:
            if on_main:
                # Use the native-return fallback on PyPy too. Re-arming SIGINT
                # here would deliver it into the next token retry again.
                _oidc_park_main(exc)
        handle = _token_callback_get(callback_id)
        if handle is None:
            _tp_sink_text_error(
                sink, True, -1, 'the token provider was released')
            return
        provider = handle._target
        if provider is None:
            _tp_sink_text_error(
                sink, True, -1, 'the token provider was released')
            return
        signal_codes = _oidc_signal_handler_codes() if on_main else {}
        token = provider.token()
        if not isinstance(token, str):
            raise TypeError(
                'a token provider must return a str, not '
                f'{_fqn(type(token))}; wrap a source that returns an expiry '
                'in RefreshingTokenProvider')
        encoded = token.encode('utf-8')
        if not questdb_token_sink_set_token(sink, encoded, len(encoded), 0, 0):
            _tp_sink_text_error(
                sink, True, -1, 'the token provider returned an unusable token')
    except BaseException as exc:
        _tp_sink_exception(sink, exc, signal_codes)
    finally:
        if on_main:
            _OIDC_MAIN_TOKEN_DEPTH -= 1
            if _OIDC_MAIN_TOKEN_DEPTH == 0:
                _oidc_schedule_parked_main()
        _token_callback_exit(counted)


cdef void _token_get_trampoline(
        void* user_data, questdb_token_sink* sink) noexcept nogil:
    cdef qdb_oidc_callback_gate* gate = qdb_oidc_callback_enter()
    if gate == NULL:
        if _tp_main_thread_before_finalization():
            _token_get_dispatch(<size_t>user_data, sink)
        else:
            _tp_sink_unavailable(sink)
        return
    try:
        if qdb_py_is_finalizing():
            _tp_sink_unavailable(sink)
            return
        qdb_oidc_callback_before_gil(gate)
        if (qdb_oidc_callback_stopped(gate)
                and not _tp_main_thread_before_finalization()):
            _tp_sink_unavailable(sink)
            return
        _token_get_dispatch(<size_t>user_data, sink)
    finally:
        qdb_oidc_callback_leave(gate)


cdef void _token_rejected_dispatch(
        size_t callback_id,
        const char* token,
        size_t token_len,
        uint16_t http_status) noexcept with gil:
    global _OIDC_MAIN_TOKEN_DEPTH
    cdef object provider
    cdef _TokenProviderHandle handle
    # A mapping even before the snapshot below: the error path looks codes
    # up in it, and `on_rejected` can raise before the snapshot is taken.
    cdef object signal_codes = {}
    cdef bint on_main = questdb_thread_ident() == _OIDC_MAIN_THREAD_IDENT
    cdef bint counted = _token_callback_enter()
    if on_main:
        _OIDC_MAIN_TOKEN_DEPTH += 1
    try:
        try:
            PyErr_CheckSignals()
            if CYTHON_COMPILING_IN_PYPY:
                _oidc_signal_tick()
        except BaseException as exc:
            if on_main:
                _oidc_park_main(exc)
        handle = _token_callback_get(callback_id)
        if handle is None:
            return
        provider = handle._target
        on_rejected = getattr(provider, 'on_rejected', None)
        if on_rejected is None:
            return
        signal_codes = _oidc_signal_handler_codes() if on_main else {}
        on_rejected(
            PyUnicode_FromStringAndSize(token, <Py_ssize_t>token_len),
            http_status)
    except BaseException as exc:
        if _tp_on_main_thread() and (
                not isinstance(exc, Exception)
                or _oidc_raised_in_signal_handler(exc, signal_codes)):
            _oidc_park_main(exc)
            _oidc_schedule_parked_main()
        # Otherwise ignored: the client pulls a token next either way.
    finally:
        if on_main:
            _OIDC_MAIN_TOKEN_DEPTH -= 1
            if _OIDC_MAIN_TOKEN_DEPTH == 0:
                _oidc_schedule_parked_main()
        _token_callback_exit(counted)


cdef void _token_rejected_trampoline(
        void* user_data,
        const char* token,
        size_t token_len,
        uint16_t http_status) noexcept nogil:
    cdef qdb_oidc_callback_gate* gate = qdb_oidc_callback_enter()
    if gate == NULL:
        if _tp_main_thread_before_finalization():
            _token_rejected_dispatch(
                <size_t>user_data, token, token_len, http_status)
        return
    try:
        if qdb_py_is_finalizing():
            return
        qdb_oidc_callback_before_gil(gate)
        if (not qdb_oidc_callback_stopped(gate)
                or _tp_main_thread_before_finalization()):
            _token_rejected_dispatch(
                <size_t>user_data, token, token_len, http_status)
    finally:
        qdb_oidc_callback_leave(gate)


cdef void _token_release_trampoline(void* user_data) noexcept nogil:
    """Deliberately empty: see the module comment. The Python wrapper drops
    the registry entry when it is collected."""
    pass


cdef size_t _token_callback_register(
        _TokenProviderHandle handle, object target) except 0:
    global _token_callback_last_id
    handle._target = target
    cdef object reference = PyWeakref_NewRef(handle, None)
    with _TOKEN_CALLBACKS_LOCK:
        _token_callback_last_id += 1
        _TOKEN_CALLBACKS[_token_callback_last_id] = reference
        return _token_callback_last_id


cdef _TokenProviderHandle _token_callback_get(size_t callback_id):
    cdef object reference
    with _TOKEN_CALLBACKS_LOCK:
        reference = _TOKEN_CALLBACKS.get(callback_id)
    return None if reference is None else reference()


cdef void _token_callback_unregister(size_t callback_id) noexcept:
    cdef object removed
    try:
        with _TOKEN_CALLBACKS_LOCK:
            removed = _TOKEN_CALLBACKS.pop(callback_id, None)
        # Drop references outside the lock, even if registry ownership changes.
        removed = None
    except BaseException:
        pass


def _debug_token_callbacks_size():
    with _TOKEN_CALLBACKS_LOCK:
        return len(_TOKEN_CALLBACKS)


def _debug_token_callbacks_ids():
    with _TOKEN_CALLBACKS_LOCK:
        return frozenset(_TOKEN_CALLBACKS)


def _debug_token_callbacks_lock():
    """Internal hook for a deterministic inherited-lock subprocess test."""
    return _TOKEN_CALLBACKS_LOCK


@cython.no_gc_clear
cdef class _TokenProviderHandle:
    """Owns one native provider handle and its registry entry."""
    cdef questdb_token_provider* _raw
    cdef size_t _callback_id
    # PyPy cannot trace ownership cycles through arbitrary cpyext fields.
    # Keep _target in the instance dictionary, which both collectors see.
    cdef dict __dict__
    cdef object __weakref__
    cdef bint _source_closed
    cdef unsigned long _source_thread
    cdef object _source_done

    def __cinit__(self):
        self._raw = NULL
        self._callback_id = 0
        self._target = None
        self._source_closed = False
        self._source_thread = 0
        self._source_done = threading.Event()
        self._source_done.set()

    def __dealloc__(self):
        cdef questdb_token_provider* raw = self._raw
        self._raw = NULL
        if raw != NULL:
            # Freeing an inherited handle is safe: native leaks it.
            with nogil:
                questdb_token_provider_free(raw)
        if self._callback_id != 0:
            _token_callback_unregister(self._callback_id)
            self._callback_id = 0


cdef object _tp_token(_TokenProviderHandle handle):
    cdef questdb_error* err = NULL
    cdef questdb_token* token
    with nogil:
        token = questdb_token_provider_token(handle._raw, &err)
    if token == NULL:
        raise c_err_to_py(err)
    try:
        return PyUnicode_FromStringAndSize(
            questdb_token_data(token), <Py_ssize_t>questdb_token_len(token))
    finally:
        questdb_token_free(token)


cdef void_int _tp_on_rejected(
        _TokenProviderHandle handle, str token, int http_status) except -1:
    if not 0 <= http_status <= 65535:
        raise ValueError(f'"http_status" is not an HTTP status: {http_status}')
    cdef bytes encoded = token.encode('utf-8')
    cdef const char* buf = encoded
    cdef size_t length = len(encoded)
    cdef uint16_t status = <uint16_t>http_status
    with nogil:
        questdb_token_provider_on_rejected(handle._raw, buf, length, status)
    return 0


# The only name `RefreshingTokenProvider` keeps in its instance dictionary.
cdef tuple _REFRESHING_PROVIDER_DICT_ATTRS = ('_handle',)


cdef class RefreshingTokenProvider:
    """
    A proactively refreshing token cache in front of a token source.

    ``source`` fetches a new token however you like -- Azure Identity, an
    OAuth2 client-credentials or PKCE flow, a device flow, a secrets vault. It
    is a callable, or an object with a ``fetch_token()`` method, returning an
    :class:`ExpiringToken`, a ``(token, expires_at)`` tuple, or any object with
    ``token`` and ``expires_on`` (epoch seconds) attributes -- Azure Core's
    ``AccessToken`` and ``AccessTokenInfo`` (with ``refresh_on``) qualify, so::

        from azure.identity import ManagedIdentityCredential  # your dependency
        credential = ManagedIdentityCredential()
        tokens = RefreshingTokenProvider(
            lambda: credential.get_token_info("api://<app-id>/.default"))
        with questdb.connect(
                "wss::addr=db.example.com:9000;",
                bearer_token_provider=tokens) as db:
            ...

    To classify a failure, raise :class:`TokenUnavailableError`; any other
    exception counts as retryable. A source cannot stop the provider's
    clients from reconnecting: :meth:`close` the provider instead.

    The provider calls ``source`` only on its own thread, never concurrently,
    starting at once. It refreshes the token in the background at about half
    its lifetime, so clients read a cached token and never wait for the
    identity platform; it retries failures with jittered backoff, and
    refreshes early when a server rejects the token with HTTP 401. A token is
    handed out only while more than ``handout_floor`` of its lifetime
    remains. Behaviour follows the cross-language dynamic-credential
    specification.

    The keyword parameters are durations, in seconds or as
    :class:`datetime.timedelta`, and default to the specification's values:
    ``refresh_margin`` (300), ``min_refresh_interval`` (30), ``handout_floor``
    (60), ``cold_wait`` (30: the longest a client waits when no usable token
    is held), ``backoff_initial`` (0.5), ``backoff_max`` (60),
    ``retry_after_max`` (300), ``forced_min_interval`` (30) and
    ``forced_wait`` (5).

    Clients keep the provider alive and never close it: close it, or use it as
    a context manager, once no client needs it. Closing it fails every client
    that still uses it. Providers and clients must not be used across
    ``fork()``. Use ``spawn``/``exec`` workers and construct providers there,
    or fork before constructing any providers or clients: a fresh provider
    alone does not repair a multithreaded parent's inherited native state.
    Its ``repr`` never shows the token.
    """
    # _handle must be visible to PyPy's collector too: sources may own this
    # provider, so a cpyext-only reference here would still root that cycle.
    # `__setattr__` keeps every other name out of it.
    cdef dict __dict__
    cdef str _name

    def __setattr__(self, name, value):
        _setattr_without_dict(
            self, RefreshingTokenProvider, _REFRESHING_PROVIDER_DICT_ATTRS,
            name, value)

    def __cinit__(self):
        self._handle = None

    def __init__(
            self,
            source,
            *,
            name=None,
            refresh_margin=None,
            min_refresh_interval=None,
            handout_floor=None,
            cold_wait=None,
            backoff_initial=None,
            backoff_max=None,
            retry_after_max=None,
            forced_min_interval=None,
            forced_wait=None):
        cdef questdb_refreshing_token_provider_options options
        cdef questdb_error* err = NULL
        cdef questdb_token_provider* raw
        cdef bytes name_bytes = None
        cdef _TokenProviderHandle handle
        cdef size_t callback_id
        if self._handle is not None:
            raise QuestDBError(
                QuestDBErrorCode.InvalidApiCall,
                'RefreshingTokenProvider is already initialized.')
        fetch = getattr(source, 'fetch_token', None)
        if fetch is None:
            if not callable(source):
                raise TypeError(
                    '"source" must be a callable or an object with a '
                    f'fetch_token() method, not {_fqn(type(source))}')
            fetch = source
        questdb_refreshing_token_provider_options_init(
            &options, sizeof(questdb_refreshing_token_provider_options))
        if name is not None:
            if not isinstance(name, str) or not name.strip():
                raise ValueError('"name" must be a non-empty str')
            name_bytes = name.encode('utf-8')
            options.name = name_bytes
            options.name_len = len(name_bytes)
        for field_name, value in (
                ('refresh_margin', refresh_margin),
                ('min_refresh_interval', min_refresh_interval),
                ('handout_floor', handout_floor),
                ('cold_wait', cold_wait),
                ('backoff_initial', backoff_initial),
                ('backoff_max', backoff_max),
                ('retry_after_max', retry_after_max),
                ('forced_min_interval', forced_min_interval),
                ('forced_wait', forced_wait)):
            if value is None:
                continue
            ms = _tp_duration_ms(value, field_name)
            if field_name == 'refresh_margin':
                options.refresh_margin_ms = ms
            elif field_name == 'min_refresh_interval':
                options.min_refresh_interval_ms = ms
            elif field_name == 'handout_floor':
                options.handout_floor_ms = ms
            elif field_name == 'cold_wait':
                options.cold_wait_ms = ms
            elif field_name == 'backoff_initial':
                options.backoff_initial_ms = ms
            elif field_name == 'backoff_max':
                options.backoff_max_ms = ms
            elif field_name == 'retry_after_max':
                options.retry_after_max_ms = ms
            elif field_name == 'forced_min_interval':
                options.forced_min_interval_ms = ms
            else:
                options.forced_wait_ms = ms
        handle = _TokenProviderHandle()
        callback_id = _token_callback_register(handle, fetch)
        handle._callback_id = callback_id
        with nogil:
            raw = questdb_refreshing_token_provider_new(
                _token_source_trampoline,
                <void*>callback_id,
                _token_release_trampoline,
                &options,
                &err)
        if raw == NULL:
            raise c_err_to_py(err)
        handle._raw = raw
        self._handle = handle
        self._name = name if name is not None else 'token-provider'

    cdef _TokenProviderHandle _require_handle(self):
        if self._handle is None:
            raise ValueError('RefreshingTokenProvider is not initialized')
        return self._handle

    def token(self) -> str:
        """
        A usable token: at once when one is held, otherwise after waiting up
        to ``cold_wait`` for a fetch. Releases the GIL while it waits.

        :raises TokenUnavailableError: no usable token arrived in time
            (classified like the most recent failed fetch), or the provider
            is closed.
        """
        return _tp_token(self._require_handle())

    def headers(self) -> dict:
        """``{"Authorization": "Bearer <token>"}``, for a client of your
        own."""
        return {'Authorization': 'Bearer ' + self.token()}

    def on_rejected(self, str token, int http_status=401):
        """
        Tell the provider that a server rejected ``token`` with
        ``http_status``, for a client of your own. A 401 for the current token
        refreshes it early -- at most once per ``forced_min_interval`` -- and
        waits up to ``forced_wait`` for the result.
        """
        _tp_on_rejected(self._require_handle(), token, http_status)

    def await_ready(self, timeout=30.0) -> bool:
        """Wait up to ``timeout`` (seconds or a ``timedelta``) for a usable
        token. Returns ``False`` on timeout or once closed."""
        cdef _TokenProviderHandle handle = self._require_handle()
        cdef uint64_t timeout_ms = _tp_duration_ms(timeout, 'timeout')
        cdef bint ready
        with nogil:
            ready = questdb_token_provider_await_ready(handle._raw, timeout_ms)
        return ready

    def close(self, *, bint wait=False, timeout=30.0):
        """Stop refreshing and fail every later token request. Idempotent.

        An already-running source cannot be forcibly cancelled. With
        ``wait=True``, wait up to ``timeout`` seconds (or a ``timedelta``) for
        it to return, raising ``TimeoutError`` if it has not. Source I/O must
        have its own deadline: interpreter shutdown must drain callbacks even
        after close, and cannot safely abandon a stuck Python source.
        """
        cdef double seconds = _tp_seconds(timeout, 'timeout')
        cdef _TokenProviderHandle handle = self._handle
        if handle is None or handle._raw == NULL:
            return
        handle._source_closed = True
        with nogil:
            questdb_token_provider_close(handle._raw)
        if wait:
            if handle._source_thread == questdb_thread_ident():
                raise QuestDBError(
                    QuestDBErrorCode.InvalidApiCall,
                    'close(wait=True) cannot wait for its own token source.')
            # In slices: a lock wait longer than `threading.TIMEOUT_MAX`
            # raises OverflowError instead of waiting, and a timedelta can
            # ask for one.
            deadline = time.monotonic() + seconds
            while not handle._source_done.wait(
                    min(seconds, threading.TIMEOUT_MAX)):
                seconds = deadline - time.monotonic()
                if seconds <= 0.0:
                    raise TimeoutError(
                        'The token source has not returned after close.')

    @property
    def name(self) -> str:
        """The non-secret label used in log lines and errors."""
        return self._name

    @property
    def closed(self) -> bool:
        """Whether the provider is closed."""
        cdef _TokenProviderHandle handle = self._require_handle()
        cdef questdb_token_provider_status status
        memset(&status, 0, sizeof(questdb_token_provider_status))
        status.struct_size = sizeof(questdb_token_provider_status)
        if not questdb_token_provider_get_status(handle._raw, &status):
            return True
        return bool(status.closed)

    @property
    def status(self):
        """A :class:`TokenProviderStatus` snapshot. Never contains the
        token."""
        cdef _TokenProviderHandle handle = self._require_handle()
        cdef questdb_token_provider_status status
        cdef char[1024] message
        memset(&status, 0, sizeof(questdb_token_provider_status))
        status.struct_size = sizeof(questdb_token_provider_status)
        status.last_failure_msg = message
        status.last_failure_msg_capacity = sizeof(message)
        if not questdb_token_provider_get_status(handle._raw, &status):
            return TokenProviderStatus(True, None, None, 0, None)
        last_failure = None
        if status.has_last_failure:
            last_failure = FetchFailure(
                bool(status.last_failure_retryable),
                (None if status.last_failure_retry_after_ms < 0
                 else status.last_failure_retry_after_ms / 1000.0),
                message[:min(status.last_failure_msg_len, sizeof(message))]
                    .decode('utf-8', 'replace'),
                _tp_datetime_from_ms(status.last_failure_at_ms))
        return TokenProviderStatus(
            bool(status.closed),
            _tp_datetime_from_ms(status.token_expires_at_ms),
            _tp_datetime_from_ms(status.last_success_at_ms),
            status.consecutive_failures,
            last_failure)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def __repr__(self):
        return f'RefreshingTokenProvider(name={self._name!r})'


cdef class _ApplicationTokenProvider:
    """Bridges a Python provider -- a callable returning a token, or an object
    with ``token()`` and an optional ``on_rejected()`` -- to the native
    function table."""
    cdef dict __dict__
    cdef object __weakref__

    def __cinit__(self):
        self._handle = None

    def __init__(self, target):
        cdef questdb_token_provider_vtable vtable
        cdef questdb_error* err = NULL
        cdef questdb_token_provider* raw
        cdef _TokenProviderHandle handle = _TokenProviderHandle()
        cdef size_t callback_id
        cdef object adapter = target
        if getattr(target, 'token', None) is None or not callable(target.token):
            if not callable(target):
                raise TypeError(
                    '"bearer_token_provider" must be a RefreshingTokenProvider, '
                    'a callable returning a token, or an object with a token() '
                    f'method, not {_fqn(type(target))}')
            adapter = _CallableTokenProvider(target)
        callback_id = _token_callback_register(handle, adapter)
        handle._callback_id = callback_id
        vtable.struct_size = sizeof(questdb_token_provider_vtable)
        vtable.get_token = _token_get_trampoline
        vtable.on_rejected = _token_rejected_trampoline
        vtable.release = _token_release_trampoline
        with nogil:
            raw = questdb_token_provider_new(&vtable, <void*>callback_id, &err)
        if raw == NULL:
            raise c_err_to_py(err)
        handle._raw = raw
        self._handle = handle


class _CallableTokenProvider:
    __slots__ = ('_fn',)

    def __init__(self, fn):
        self._fn = fn

    def token(self):
        return self._fn()


def _token_application_provider_collected(key, reference):
    try:
        with _TOKEN_CALLBACKS_LOCK:
            if _APPLICATION_TOKEN_PROVIDERS.get(key) is reference:
                _APPLICATION_TOKEN_PROVIDERS.pop(key, None)
    except BaseException:
        # Weakref callbacks must also tolerate module teardown, like the
        # native-handle registry's best-effort unregister path.
        pass


cdef object _tp_resolve_bearer_token_provider(object provider):
    """The native-backed holder for a ``bearer_token_provider=`` argument: a
    ``RefreshingTokenProvider`` as is, anything else wrapped in an
    ``_ApplicationTokenProvider``. The caller keeps the holder alive."""
    if isinstance(provider, RefreshingTokenProvider):
        (<RefreshingTokenProvider>provider)._require_handle()
        return provider
    if isinstance(provider, OidcDeviceAuth) or isinstance(
            getattr(provider, '__self__', None), OidcDeviceAuth):
        raise TypeError(
            'Pass an OidcDeviceAuth as "oidc_auth=", not as '
            '"bearer_token_provider=".')
    # Each Python bound-method lookup creates a new object. Normalize it to
    # its owner/function identity so attachments still share single-flight.
    key = ((id(provider.__self__), id(provider.__func__))
           if isinstance(provider, _TokenBoundMethod) else (id(provider),))
    with _TOKEN_CALLBACKS_LOCK:
        reference = _APPLICATION_TOKEN_PROVIDERS.get(key)
        holder = reference() if reference is not None else None
        if holder is not None:
            return holder
    # Introspection of an application target can execute arbitrary Python;
    # never do it while holding the callback registry lock. Concurrent misses
    # may construct unused candidates, but only the atomically published holder
    # is returned and attached to native clients.
    candidate = _ApplicationTokenProvider(provider)
    reference = weakref.ref(
        candidate, _token_partial(_token_application_provider_collected, key))
    with _TOKEN_CALLBACKS_LOCK:
        previous = _APPLICATION_TOKEN_PROVIDERS.setdefault(key, reference)
        holder = previous()
        if holder is None:
            _APPLICATION_TOKEN_PROVIDERS[key] = reference
            holder = candidate
    return holder


cdef const questdb_token_provider* _tp_holder_raw(object holder) except NULL:
    cdef _TokenProviderHandle handle
    if isinstance(holder, RefreshingTokenProvider):
        handle = (<RefreshingTokenProvider>holder)._require_handle()
    else:
        handle = (<_ApplicationTokenProvider>holder)._handle
    if handle is None or handle._raw == NULL:
        raise ValueError('the token provider is not initialized')
    return handle._raw


cdef object _tp_err_to_py(line_sender_error* err):
    """A ``TokenUnavailableError`` for a native error a token provider caused,
    keeping the transport's code and message; ``None`` for any other. Frees
    ``err`` only when it returns an exception."""
    cdef questdb_token_unavailable_view view
    memset(&view, 0, sizeof(questdb_token_unavailable_view))
    view.struct_size = sizeof(questdb_token_unavailable_view)
    if not questdb_error_token_unavailable_get_view(err, &view):
        return None
    cdef object tup = c_err_to_fields(err)
    exc = TokenUnavailableError(
        tup[1],
        retryable=bool(view.retryable),
        retry_after=(None if view.retry_after_ms < 0
                     else view.retry_after_ms / 1000.0),
        code=tup[0])
    exc._sender_error = tup[2]
    exc._in_doubt = bool(tup[3])
    return exc


# Whether the cleartext warning below was given. A race between two first
# attachments can give it twice, which is harmless.
_tp_cleartext_warned = False


def _tp_warn_if_cleartext(protocol):
    """Warn once per process that a token provider the application attached
    (``bearer_token_provider=`` or ``oidc_auth=``) is used over a protocol
    without TLS: every token then crosses the network in cleartext
    (design/qwp-token-provider-spec.md, section 9). The native core logs the
    same warning through Rust's ``log`` facade, which Python does not see."""
    global _tp_cleartext_warned
    if protocol.tls_enabled or _tp_cleartext_warned:
        return
    _tp_cleartext_warned = True
    logging.getLogger('questdb').warning(
        'a token provider is used over %s:: (no TLS): bearer tokens cross '
        'the network in cleartext; use %ss:: in production',
        protocol.tag, protocol.tag)


_TOKEN_PROVIDER_CONF_KEYS = (
    'token_provider', 'azure_resource', 'azure_client_id', 'azure_credential')


cdef void_int _tp_reject_conf_keys(dict params) except -1:
    """Reject the connect-string keys reserved for selecting a token provider.
    This client selects none from a connect string; native rejects them too,
    but the Python sender drops keys it does not forward on ILP schemas."""
    for key in _TOKEN_PROVIDER_CONF_KEYS:
        if key in params:
            raise QuestDBError(
                QuestDBErrorCode.ConfigError,
                f'"{key}" selects a connect-string token provider, which this '
                'client does not support. Pass a token provider instead: '
                '"bearer_token_provider=RefreshingTokenProvider(source)", '
                'whose source can call Azure Identity or any OAuth2 flow.')
    return 0
