################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2026 QuestDB
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

# Native OIDC device-flow glue. The Rust implementation in c-questdb-client is
# the single owner of token acquisition, refresh, caching, and persistence;
# Python retains presentation and adapter conveniences only.


# Live providers, keyed by the opaque integer handed to native as `user_data`.
#
# Native is given a key rather than a `PyObject*` so that the release callback
# -- which can run on an abandoned acquisition worker long after the owning
# handles are gone, potentially past the start of interpreter finalization --
# owns no Python reference and never needs the GIL.
#
# The value is a weakref: the registry must not keep a provider alive, or a
# `with` block's exit would never collect one.
#
# This is BOOKKEEPING ONLY. The native handle is owned by the provider itself
# (`OidcDeviceAuth._native`), not by `_OIDC_NATIVE_HANDLES`, so evicting or
# losing an entry here cannot free a handle a live provider still points at.
# It used to be the sole owner, and that was a use-after-free: the cyclic
# collector runs weakref callbacks BEFORE finalizers, so a provider in a cycle
# had its handle freed by this callback and was then handed to a `__del__` in
# the same cycle that called `close()` on the dangling pointer.
#
# `_OIDC_REGISTRY_LOCK` guards the id counter and every paired dict update. It
# is re-entrant because a weakref callback can run synchronously inside a
# collection triggered while the same thread already holds it; an ordinary lock
# would deadlock there. The RLock keeps the registry invariant independent of
# the GIL without changing a hot path -- it is taken only during provider
# construction, collection, and the one shutdown snapshot.
#
# A module-level `cdef object` is a C static rather than an entry in the module
# dict, so `_PyModule_Clear` never swaps it for None at interpreter shutdown:
# readers need no None guard.
cdef object _OIDC_PROVIDERS = {}
cdef object _OIDC_NATIVE_HANDLES = {}
cdef object _OIDC_REGISTRY_LOCK = threading.RLock()
cdef size_t _oidc_last_provider_id = 0
# Set before the atexit hook snapshots the registry. A provider whose native
# build completes after that snapshot observes the flag under the same lock and
# detaches its callbacks before construction returns, closing the new-provider
# race while the hook releases the GIL to drain an older callback.
cdef bint _oidc_callbacks_shutting_down = False


def _debug_oidc_registry_size():
    """Internal test hook: live entries in the provider registry.

    Native owns no Python reference. A provider weakref callback removes both
    registry entries; either one left behind would retain bookkeeping or the
    native auth handle for the life of the process.
    """
    if len(_OIDC_PROVIDERS) != len(_OIDC_NATIVE_HANDLES):
        raise AssertionError('OIDC provider/native registries are out of sync')
    return len(_OIDC_PROVIDERS)


def _debug_oidc_registry_snapshot():
    """Internal test hook: paired registry ids observed under their lock."""
    with _OIDC_REGISTRY_LOCK:
        return (frozenset(_OIDC_PROVIDERS),
                frozenset(_OIDC_NATIVE_HANDLES))


def _debug_oidc_last_provider_id():
    """Internal test hook: the most recently allocated provider id.

    Ids are monotonic and never reused, so every provider built after a reading
    has a larger id. That lets a test track exactly the providers it created,
    regardless of what earlier tests left for the collector to finalize.
    """
    with _OIDC_REGISTRY_LOCK:
        return _oidc_last_provider_id


def _debug_oidc_callbacks_shutting_down():
    """Internal test hook: has the callback-detach atexit phase started?"""
    with _OIDC_REGISTRY_LOCK:
        return bool(_oidc_callbacks_shutting_down)


def _debug_oidc_reset_callback_shutdown():
    """Internal test hook: undo a manually invoked atexit phase."""
    global _oidc_callbacks_shutting_down
    with _OIDC_REGISTRY_LOCK:
        _oidc_callbacks_shutting_down = False


def _debug_oidc_reset_errors_module():
    """Internal test hook: drop the cached ``questdb.auth._errors`` module."""
    global _OIDC_ERRORS_MOD
    _OIDC_ERRORS_MOD = None


def _debug_oidc_errors_module_ready():
    """Internal test hook: does the non-importing resolver find a usable module?"""
    return _oidc_errors_module_if_ready() is not None


def _debug_oidc_errors_module_resolved():
    """Internal test hook: does the importing resolver find a usable module?"""
    return _oidc_errors_module() is not None


def _oidc_provider_collected(size_t provider_id, object provider_ref):
    """Drop the registry bookkeeping for a collected provider.

    Ids come from a monotonic counter and are never reused, so both entries
    belong to this provider alone and are dropped unconditionally. The previous
    identity guard returned early when the `_OIDC_PROVIDERS` entry was already
    gone, without touching `_OIDC_NATIVE_HANDLES` -- so anything that removed
    one and not the other left the two permanently out of sync, retaining the
    stale entry for the life of the process.

    Frees nothing: the provider owns its native handle (`OidcDeviceAuth.
    _native`), so neither this callback nor a caller invoking it with a forged
    id can release a handle that is still in use.
    """
    with _OIDC_REGISTRY_LOCK:
        _OIDC_PROVIDERS.pop(provider_id, None)
        _OIDC_NATIVE_HANDLES.pop(provider_id, None)


cdef void _oidc_detach_handle_callbacks(
        _OidcNativeHandle handle, bint wait_for_events) noexcept:
    """Stop one native auth handle delivering managed-runtime callbacks.

    Both native detaches are idempotent and NULL-tolerant. At interpreter exit
    suppression must be published without waiting for arbitrary renderer or
    logging-handler code; construction-time self-detach may use the draining
    forms because the provider has not yet been exposed.
    """
    cdef PyThreadState* gs = NULL
    if handle is None or handle.raw == NULL:
        return
    _ensure_doesnt_have_gil(&gs)
    if wait_for_events:
        questdb_oidc_auth_detach_events(handle.raw)
        questdb_oidc_auth_detach_diagnostics(handle.raw)
    else:
        questdb_oidc_auth_detach_events_nowait(handle.raw)
        questdb_oidc_auth_detach_diagnostics_nowait(handle.raw)
    _ensure_has_gil(&gs)


def _oidc_detach_diagnostics_at_exit():
    """Silence every still-registered provider before finalization begins.

    Both native trampolines can run on non-main threads: diagnostics from an
    attached transport worker, and renderer events when the application put
    ``sign_in()`` on a daemon thread. In either case their
    ``qdb_py_is_finalizing()`` guard is a TOCTOU: finalization can begin between
    the test and the ``PyGILState_Ensure`` the ``with gil`` dispatch emits. No
    check placed in a trampoline can close that window.

    ``atexit`` runs while the interpreter is still fully alive, so detaching
    both managed-runtime callback targets here closes the window rather than
    merely re-testing it.

    A provider already dropped from the registry cannot be reached from here at
    all -- its weakref callback has removed both entries -- and is covered
    instead by ``_OidcNativeHandle.__dealloc__``, which detaches that provider's
    diagnostics before releasing the handle. Renderer events require a live
    ``sign_in`` frame, which itself retains the provider, so a collected
    provider needs no separate event detach. Between the two paths, no
    still-deliverable managed-runtime callback target survives finalization.

    Both detaches publish exact suppression but deliberately do not wait for
    callback code already running. Renderer callbacks are arbitrary user code,
    and a diagnostic callback can itself be parked in an arbitrary user
    ``logging.Handler``; either may be abandoned as the process exits.

    Detaches rather than closes. Every handle reachable here belongs to a
    provider the user still holds, which is exactly the set attached to live
    transports, and ``close()`` is terminal for all of them: a closed provider
    fails every later token pull with a non-retryable error, so a reconnect
    during the interpreter's remaining shutdown work terminalizes a QWP
    publication store and discards frames it had already accepted. That work is
    real and runs after this hook -- ``atexit`` precedes module clearing, so
    ``QuestDB.__dealloc__`` -> ``questdb_db_close`` -> the bounded
    ``close_flush_timeout`` drain happens later. Detaching gives the hook's
    stated guarantee (no new managed-runtime callback can start) without taking
    anything else away. It never waits on a renderer or on the acquisition lock
    behind an in-flight IdP request, so user code and the provider's 30s/120s
    HTTP timeout are not added to exit latency.

    Marks callback shutdown and snapshots under the registry lock, then
    detaches outside it: the detach is a native call made with the GIL released,
    and holding the lock across it would invert against concurrent provider
    registration. A constructor that finishes after the snapshot checks the
    marker under the same lock and detaches its own completed native handle
    before returning, so it cannot escape through that GIL-release window.
    Iterating the handle dict resurrects nothing -- it holds the handles, while
    `_OIDC_PROVIDERS` holds only weakrefs to the providers themselves.
    """
    global _oidc_callbacks_shutting_down
    try:
        with _OIDC_REGISTRY_LOCK:
            # Publish before the snapshot. A constructor can run while native
            # detach below has released the GIL; after its build it checks this
            # flag under the same lock and detaches itself if it missed the
            # snapshot. Holding the registry lock across a native drain would
            # instead create a lock inversion with provider registration.
            _oidc_callbacks_shutting_down = True
            handles = list(_OIDC_NATIVE_HANDLES.values())
        for handle in handles:
            _oidc_detach_handle_callbacks(handle, False)
    except BaseException:
        # An atexit hook that raises prints a traceback and buys nothing: the
        # process is going away regardless, and every provider this failed to
        # reach is left exactly where it would have been without the hook.
        pass


atexit.register(_oidc_detach_diagnostics_at_exit)


cdef inline object _oidc_text(const char* buf, size_t length):
    if buf == NULL:
        return None
    return PyUnicode_FromStringAndSize(buf, <Py_ssize_t>length)


cdef inline size_t _oidc_error_view_v1_size() noexcept nogil:
    # Mirrors native's `QUESTDB_OIDC_ERROR_VIEW_V1_SIZE`: the prefix through
    # `retry_after_seconds`. `acquisition_busy` was appended after v1, and
    # `questdb_error_oidc_get_view` accepts -- and reports -- any capacity from
    # this size up. Computed from the declared layout rather than hardcoded, so
    # it tracks the header on every ABI the extension is compiled for.
    cdef questdb_oidc_error_view layout
    return (<size_t>(<char*>&layout.retry_after_seconds - <char*>&layout)
            + sizeof(uint64_t))


cdef inline bint _oidc_error_view_has_v1(size_t struct_size) noexcept nogil:
    # kind, idp_error, idp_error_description, status and retry_after.
    return struct_size >= _oidc_error_view_v1_size()


cdef inline bint _oidc_error_view_has_acquisition_busy(
        size_t struct_size) noexcept nogil:
    # The appended tail. Gated separately: a library that predates it still
    # reports a complete, classifiable v1 prefix, and collapsing that to a bare
    # OidcError would drop the kind the foreground fail-fast gate and every
    # `except OidcInteractionRequired` / `except OidcTimeoutError` key on.
    return struct_size >= sizeof(questdb_oidc_error_view)


cdef inline bint _oidc_config_view_is_full(size_t struct_size):
    # Native's config-view v1 ends at `issuer_len`, which is also the last
    # declared field, so v1 and the full struct coincide today.
    return struct_size >= sizeof(questdb_oidc_config_view)


def _debug_oidc_view_prefix_support(size_t error_size, size_t config_size):
    """Internal test hook for config/error view write-back gates.

    Returns ``(error_has_v1, error_has_acquisition_busy, config_is_full)``.
    """
    return (_oidc_error_view_has_v1(error_size),
            _oidc_error_view_has_acquisition_busy(error_size),
            _oidc_config_view_is_full(config_size))


def _debug_oidc_view_sizes():
    """Internal test hook: ``(error_v1, error_full, config_full)`` sizes."""
    return (_oidc_error_view_v1_size(),
            sizeof(questdb_oidc_error_view),
            sizeof(questdb_oidc_config_view))


def _debug_oidc_error_from_view_prefix(
        size_t struct_size, int kind, bint acquisition_busy):
    """Internal test hook: convert a synthetic error view of a given prefix.

    Drives the same classification an older shared library's write-back would,
    so the per-tier gates are tested through the real conversion rather than
    only through the predicates.
    """
    cdef questdb_oidc_error_view view
    errors = _oidc_errors_module()
    if errors is None:
        raise RuntimeError('questdb.auth._errors is unavailable')
    memset(&view, 0, sizeof(questdb_oidc_error_view))
    view.struct_size = struct_size
    view.kind = <questdb_oidc_error_kind>kind
    view.has_status = True
    view.status = 400
    view.has_retry_after = True
    view.retry_after_seconds = 7
    view.acquisition_busy = acquisition_busy
    return _oidc_exc_from_view(
        errors, 'synthetic OIDC error', &view, False,
        QuestDBErrorCode.AuthError, None)


# Resolved on first use and cached. See `_oidc_errors_module`.
cdef object _OIDC_ERRORS_MOD = None


# Every class `_oidc_err_to_py_unowned` and `_is_oidc_terminal_for_foreground`
# resolve off the module. The readiness test must cover all of them, not just
# the first: a module is published in `sys.modules` before its body runs, so a
# sentinel on the first-defined class (`OidcError`, which `_errors.py` defines
# ahead of the other six) accepts a module on which the rest do not exist yet
# -- exactly the half-built window the test is there to reject.
cdef tuple _OIDC_ERROR_NAMES = (
    'OidcError', 'OidcConfigError', 'OidcNetworkError',
    'OidcInteractionRequired', 'OidcCancelledError',
    'OidcDeviceFlowError', 'OidcTimeoutError')


cdef bint _oidc_errors_module_complete(object mod):
    """Does ``mod`` carry every class the error paths resolve off it?

    Checked as a whole rather than one sentinel, so a module caught partway
    through its body is treated as absent by every caller.
    """
    for name in _OIDC_ERROR_NAMES:
        if getattr(mod, name, None) is None:
            return False
    return True


cdef object _oidc_errors_module_if_ready():
    """``questdb.auth._errors`` if it is already imported and usable, else None.

    Never imports, so a caller on the ordinary (non-OIDC) error path pays one
    dict lookup and nothing else.

    A module is published in ``sys.modules`` *before* its body runs, and
    ``_errors.py`` imports ``._render`` and ``questdb._client`` before defining
    a single class. A concurrent first import -- or a re-entrant one reached
    from this very error path -- therefore hands back a module object with none
    of the ``Oidc*`` attributes on it, and reading one raises ``AttributeError``
    *over* the failure being reported, defeating every ``except QuestDBError``
    handler. That is precisely the substitution the untyped fallback exists to
    prevent, so a half-built module counts as absent and is deliberately not
    cached: a later call gets the finished one.

    This is also the single source of class identity for the whole extension.
    Resolving the classes here but testing ``isinstance`` against a separate
    ``sys.modules`` lookup elsewhere would let the two disagree after any
    ``sys.modules`` swap, silently disabling the check that used the other.
    """
    global _OIDC_ERRORS_MOD
    if _OIDC_ERRORS_MOD is not None:
        return _OIDC_ERRORS_MOD
    mod = sys.modules.get('questdb.auth._errors')
    if mod is None or not _oidc_errors_module_complete(mod):
        return None
    _OIDC_ERRORS_MOD = mod
    return mod


cdef object _oidc_errors_module():
    """The ``questdb.auth._errors`` module, or None if it cannot be reached.

    Deliberately not imported eagerly from ``questdb/__init__.py``: that would
    pull ``unicodedata`` / ``re`` / ``urllib.parse`` into every process that
    imports questdb, for a module only an auth failure needs. Cached after the
    first success so a long-running sender pays the lookup once rather than
    re-entering the import machinery on every failure.

    Returns None instead of raising. This runs while a failure is already being
    reported, and it can be the *first* import of the package -- a caller who
    took ``from questdb._client import OidcDeviceAuth`` never touches
    ``questdb.auth``. If that first import lands during interpreter
    finalization, where ``sys.meta_path`` is None and any import raises
    ``ImportError``, letting it propagate would replace the flush or connect
    error the caller actually needs with a misleading one about the import.
    ``_oidc_err_to_py_unowned`` falls back to a plain ``QuestDBError`` carrying
    the same native message and code, which is strictly better than that.
    """
    global _OIDC_ERRORS_MOD
    mod = _oidc_errors_module_if_ready()
    if mod is not None:
        return mod
    if 'questdb.auth._errors' in sys.modules:
        # Present but half-built (see `_oidc_errors_module_if_ready`). Importing
        # again just returns the same partial object out of `sys.modules`, so
        # there is nothing to gain and a re-entrant import to risk.
        return None
    try:
        import questdb.auth._errors as mod
    except BaseException:
        return None
    if not _oidc_errors_module_complete(mod):
        return None
    _OIDC_ERRORS_MOD = mod
    return mod


cdef object _oidc_err_to_py_unowned(questdb_error* err):
    cdef const char* msg_buf = NULL
    cdef size_t msg_len = 0
    cdef questdb_oidc_error_view view
    cdef line_sender_qwpws_error_view qwp_ws_view
    cdef object message
    cdef object sender_error = None
    cdef bint in_doubt = False
    cdef object code
    cdef object exc

    errors = _oidc_errors_module()
    if errors is None:
        # The typed classes are unreachable (see `_oidc_errors_module`). Report
        # the native failure untyped rather than losing it: `except
        # QuestDBError` still catches this, only `except OidcError` does not.
        if err == NULL:
            return QuestDBError(
                QuestDBErrorCode.AuthError, 'Unknown native OIDC error.')
        msg_buf = questdb_error_msg(err, &msg_len)
        if line_sender_error_qwpws_get_view(err, &qwp_ws_view):
            sender_error = c_sender_error_view_to_raw(qwp_ws_view)
        return QuestDBError(
            c_err_code_to_py(questdb_error_get_code(err)),
            _oidc_text(msg_buf, msg_len) or 'Unknown native OIDC error.',
            sender_error,
            in_doubt=questdb_error_in_doubt(err))

    if err == NULL:
        return errors.OidcError('Unknown native OIDC error.')
    msg_buf = questdb_error_msg(err, &msg_len)
    message = _oidc_text(msg_buf, msg_len) or 'Unknown native OIDC error.'
    # Carry the native in-doubt flag through the OIDC error path the same way
    # the non-OIDC c_err_to_py path does (via c_err_to_fields). A token-provider
    # failure is classified before any write today, so this is False in
    # practice, but propagating it keeps an OidcError from silently under-
    # reporting delivery uncertainty should the native classification ever raise
    # an OIDC error after a partial write -- an except-QuestDBError retry or
    # dead-letter handler keys on .in_doubt to avoid replaying a landed write.
    in_doubt = questdb_error_in_doubt(err)
    # Carry the native classification too. The non-OIDC path does this via
    # c_err_to_fields; the OIDC path used to hardcode AuthError and drop it.
    # Native deliberately reclassifies a recoverable token-provider failure as a
    # retryable SocketError so failover polls it again, and QuestDB.dataframe()'s
    # reconnect loop gates on exactly that code -- stamping AuthError turned a
    # retryable reconnect into an immediate raise, so `oidc_auth=` silently lost
    # the retry that `token=` still had.
    code = c_err_code_to_py(questdb_error_get_code(err))
    # Native reports an OIDC cause anywhere in the causal chain, so a
    # QWP/WebSocket rejection whose root cause was a token refresh reaches this
    # branch with the transport's structured payload still attached. This is
    # the only path that builds it, since returning here skips `c_err_to_fields`
    # -- without this the payload, and `.sender_error` with it, was silently
    # dropped for exactly the errors an `oidc_auth` transport produces.
    if line_sender_error_qwpws_get_view(err, &qwp_ws_view):
        sender_error = c_sender_error_view_to_raw(qwp_ws_view)
    memset(&view, 0, sizeof(questdb_oidc_error_view))
    view.struct_size = sizeof(questdb_oidc_error_view)
    if questdb_error_oidc_get_view(err, &view):
        return _oidc_exc_from_view(
            errors, message, &view, in_doubt, code, sender_error)
    exc = errors.OidcError(
        message, in_doubt=in_doubt, code=code, sender_error=sender_error)
    exc._acquisition_busy = False
    return exc


cdef object _oidc_exc_from_view(
        object errors,
        object message,
        const questdb_oidc_error_view* view,
        bint in_doubt,
        object code,
        object sender_error):
    """Build the typed exception a populated error view describes.

    ``view.struct_size`` is the prefix the library wrote back, which an older
    shared library reports as its own (smaller) size. Each field is read only
    when that prefix covers it, in the same two tiers as the native contract:
    the v1 prefix (kind, IdP fields, status, retry-after) and the appended
    ``acquisition_busy`` tail. An unwritten tail keeps its zero default.
    """
    cdef object idp_error = None
    cdef object description = None
    cdef object status = None
    cdef object retry_after = None
    cdef bint acquisition_busy = False
    cdef object exc
    OidcConfigError = errors.OidcConfigError
    OidcCancelledError = errors.OidcCancelledError
    OidcDeviceFlowError = errors.OidcDeviceFlowError
    OidcError = errors.OidcError
    OidcInteractionRequired = errors.OidcInteractionRequired
    OidcNetworkError = errors.OidcNetworkError
    OidcTimeoutError = errors.OidcTimeoutError

    if _oidc_error_view_has_v1(view.struct_size):
        idp_error = _oidc_text(view.idp_error, view.idp_error_len)
        description = _oidc_text(
            view.idp_error_description, view.idp_error_description_len)
        status = view.status if view.has_status else None
        retry_after = (
            view.retry_after_seconds if view.has_retry_after else None)
        if _oidc_error_view_has_acquisition_busy(view.struct_size):
            acquisition_busy = view.acquisition_busy
        if view.kind == QUESTDB_OIDC_ERROR_CONFIG:
            exc = OidcConfigError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
        elif view.kind == QUESTDB_OIDC_ERROR_NETWORK:
            exc = OidcNetworkError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
        elif view.kind == QUESTDB_OIDC_ERROR_DEVICE_FLOW:
            exc = OidcDeviceFlowError(
                message,
                error=idp_error,
                error_description=description,
                status=status,
                retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
        elif view.kind == QUESTDB_OIDC_ERROR_TIMEOUT:
            # OidcTimeoutError is an OidcDeviceFlowError, and the native side
            # attaches the IdP error (e.g. "expired_token"); carry it through
            # like the DEVICE_FLOW branch instead of dropping it.
            exc = OidcTimeoutError(
                message,
                error=idp_error,
                error_description=description,
                status=status,
                retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
        elif view.kind == QUESTDB_OIDC_ERROR_INTERACTION_REQUIRED:
            exc = OidcInteractionRequired(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
        elif view.kind == QUESTDB_OIDC_ERROR_CANCELLED:
            exc = OidcCancelledError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
        else:
            exc = OidcError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code, sender_error=sender_error)
    else:
        exc = OidcError(
            message, in_doubt=in_doubt, code=code, sender_error=sender_error)
    # Internal transport-retry discriminator. Keeping it on the converted
    # exception makes the decision describe the native error that actually
    # occurred, rather than racing a later read of provider state.
    exc._acquisition_busy = bool(acquisition_busy)
    return exc


cdef object _oidc_err_to_py(questdb_error* err):
    if err == NULL:
        return _oidc_err_to_py_unowned(err)
    try:
        return _oidc_err_to_py_unowned(err)
    finally:
        questdb_error_free(err)


cdef inline bytes _oidc_required_utf8(object value, str name):
    from questdb.auth._errors import OidcConfigError
    if not isinstance(value, str) or not value:
        raise OidcConfigError(
            f'{name} is required and must be a non-empty string')
    try:
        return value.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise OidcConfigError(
            f'{name} must contain valid Unicode text') from exc


cdef inline bytes _oidc_optional_utf8(object value, str name):
    from questdb.auth._errors import OidcConfigError
    if value is None:
        return None
    if not isinstance(value, str):
        raise OidcConfigError(f'{name} must be a string or None')
    try:
        return value.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise OidcConfigError(
            f'{name} must contain valid Unicode text') from exc


cdef inline void _oidc_validate_bool(
        object value, str name, bint allow_none) except *:
    if (value is None and allow_none) or isinstance(value, bool):
        return
    from questdb.auth._errors import OidcConfigError
    if allow_none:
        raise OidcConfigError(f'{name} must be a bool or None')
    raise OidcConfigError(f'{name} must be a bool')


cdef void _oidc_cancel_sign_in_from_callback(
        OidcDeviceAuth provider) noexcept:
    """Cancel only this device flow from inside its event callback.

    Native's attempt-scoped signal wakes the poll wait without taking the
    authentication lock held around this callback. It does not close the shared
    provider, discard credentials, or disable attached transports. Errors are
    swallowed deliberately: this runs on the interrupt path, where the pending
    ``KeyboardInterrupt`` or ``SystemExit`` is the thing worth surfacing.

    The GIL is released around the native call for consistency with every other
    native call in this file, not because a deadlock is known here: native's
    ``cancel_sign_in`` takes only ``close_wait``, and ``CEventHandler::
    cancel_sign_in`` takes ``callback_gate``, which is free while a callback
    runs. Holding the GIL across a native lock acquisition is the shape this
    file otherwise treats as a rule, so it is not worth being the one exception
    should either of those lock disciplines change.
    """
    cdef questdb_error* err = NULL
    cdef bint ok
    cdef PyThreadState* gs = NULL
    if provider._raw == NULL:
        return
    _ensure_doesnt_have_gil(&gs)
    ok = questdb_oidc_auth_cancel_sign_in(provider._raw, &err)
    _ensure_has_gil(&gs)
    if not ok:
        if err != NULL:
            questdb_error_free(err)


cdef inline bint _oidc_event_has_browser_target(size_t struct_size):
    # browser_target_len immediately precedes the appended uint64 interval.
    # A struct from the ABI revision before interval therefore ends here.
    return struct_size >= (
        sizeof(questdb_oidc_event) - sizeof(uint64_t))


cdef inline bint _oidc_event_has_interval(size_t struct_size):
    return struct_size >= sizeof(questdb_oidc_event)


def _debug_oidc_event_tail_support(size_t struct_size):
    """Internal test hook for the event ABI's appended-field gates."""
    return (_oidc_event_has_browser_target(struct_size),
            _oidc_event_has_interval(struct_size))


def _debug_oidc_event_tail_sizes():
    """Internal test hook: minimum ABI sizes for browser URL and interval."""
    return (sizeof(questdb_oidc_event) - sizeof(uint64_t),
            sizeof(questdb_oidc_event))


cdef class _OidcForegroundCall:
    """One ``sign_in()`` / ``token()`` / ``clear()`` running on this thread.

    Those calls release the GIL for the whole native operation, so the only
    Python bytecode that runs on the caller's thread meanwhile is a callback
    native makes from inside it -- a renderer event, or a persistence
    diagnostic -- and CPython delivers a pending signal at the first bytecode
    it runs. A ``KeyboardInterrupt`` (or ``SystemExit``) raised there must not
    unwind through native, so the callback parks it here for the call to
    re-raise once native returns.
    """
    # The provider whose `sign_in()` this is, or None for token() / clear().
    # A sign-in parks on the provider's own `_interrupt` slot, which the event
    # dispatcher also reads, and is additionally cancelled.
    cdef object provider
    cdef object interrupt
    # The record this one shadows: a renderer callback may itself call token().
    cdef object previous


# Per-thread stack of `_OidcForegroundCall` records. The diagnostic callback's
# `user_data` is NULL -- it is shared by background token-provider workers,
# which must never be handed a Python object -- so the current thread is the
# only thing that can associate a diagnostic with the call it interrupted.
# `with gil` on a thread that released the GIL through `PyEval_SaveThread`
# resumes that thread's own state, so this sees the caller's record. A native
# worker thread gets a fresh state and correctly sees none.
cdef object _OIDC_FOREGROUND = threading.local()


cdef _OidcForegroundCall _oidc_foreground_enter(object provider):
    cdef _OidcForegroundCall call = _OidcForegroundCall.__new__(
        _OidcForegroundCall)
    call.provider = provider
    call.interrupt = None
    call.previous = getattr(_OIDC_FOREGROUND, 'call', None)
    _OIDC_FOREGROUND.call = call
    return call


cdef object _oidc_foreground_exit(_OidcForegroundCall call):
    """Pop ``call`` and return the interrupt a callback parked on it."""
    _OIDC_FOREGROUND.call = call.previous
    call.previous = None
    return call.interrupt


cdef void _oidc_park_foreground_interrupt(object exc) noexcept:
    """Hand an interrupt raised inside a diagnostic to the call it stopped.

    Must not raise: it runs on the error path of a ``noexcept`` callback.
    """
    cdef _OidcForegroundCall call
    cdef OidcDeviceAuth provider
    try:
        record = getattr(_OIDC_FOREGROUND, 'call', None)
        if record is not None:
            call = <_OidcForegroundCall>record
            if call.provider is not None:
                # A sign-in. Park where the renderer path parks, so sign_in()
                # has one slot to read, and cancel the device flow so the call
                # actually returns. A diagnostic can also fire before the flow
                # starts (a failed store read, or the save after a refresh that
                # did not yield a servable token), when there is nothing to
                # cancel yet; `_oidc_event_dispatch` then cancels at the first
                # prompt instead of painting it.
                provider = <OidcDeviceAuth>call.provider
                if provider._interrupt is None:
                    provider._interrupt = exc
                _oidc_cancel_sign_in_from_callback(provider)
            elif call.interrupt is None:
                call.interrupt = exc
            return
        if (isinstance(exc, KeyboardInterrupt)
                and threading.get_ident() == threading.main_thread().ident):
            # No foreground call to re-raise it, yet this is the main thread:
            # an attached transport pulled a token on the caller's thread with
            # the GIL released. Catching the exception cleared CPython's
            # tripped-signal flag, so re-arm it; the interpreter raises it at
            # the first bytecode after that native call returns, exactly where
            # an uncaught Ctrl-C during a blocking call would surface.
            PyErr_SetInterrupt()
        # Anywhere else this is a native worker thread, where signals are never
        # delivered and there is no caller to hand an exception to.
    except BaseException:
        pass


cdef void _oidc_event_dispatch(
        void* user_data,
        const questdb_oidc_event* event) noexcept with gil:
    cdef object browser_target = None
    cdef uint64_t interval_seconds = 0
    provider = _oidc_provider_from_user_data(user_data)
    if provider is None:
        return
    if ((<OidcDeviceAuth>provider)._interrupt is not None
            and event.kind in (
                QUESTDB_OIDC_EVENT_PROMPT, QUESTDB_OIDC_EVENT_WAITING)):
        # An interrupt was parked before the device flow could be cancelled --
        # by a persistence diagnostic ahead of the prompt, or by an earlier
        # event racing the cancellation. Stop instead of painting a prompt
        # nobody should answer.
        _oidc_cancel_sign_in_from_callback(<OidcDeviceAuth>provider)
        return
    renderer = (<OidcDeviceAuth>provider)._renderer
    if renderer is None:
        return
    try:
        if event.kind == QUESTDB_OIDC_EVENT_PROMPT:
            # These fields were appended to the public C struct. Static linkage
            # makes this build use matching layouts today, but the callback ABI
            # explicitly supports an older shared library: never read beyond
            # the size that producer reported.
            if _oidc_event_has_browser_target(event.struct_size):
                browser_target = _oidc_text(
                    event.browser_target, event.browser_target_len)
            if _oidc_event_has_interval(event.struct_size):
                interval_seconds = event.interval_seconds
            renderer.on_prompt({
                'user_code': _oidc_text(event.user_code, event.user_code_len),
                'verification_uri': _oidc_text(
                    event.verification_uri, event.verification_uri_len),
                'verification_uri_complete': _oidc_text(
                    event.verification_uri_complete,
                    event.verification_uri_complete_len),
                # The bounded values the native polling loop actually uses,
                # matching Java's complete device challenge. Each is passed with
                # the C type the event declares -- `expires_in` a double (as
                # `on_waiting` and `on_success` also pass it), `interval` a
                # uint64 -- rather than narrowed here: a C double->unsigned cast
                # is in range only because native happens to clamp the lifetime.
                'expires_in': event.expires_in_seconds,
                'interval': interval_seconds,
                # This is the only native-vetted actionable URL. Built-in
                # renderers prefer it for links and QR codes.
                'browser_target': browser_target,
            })
        elif event.kind == QUESTDB_OIDC_EVENT_WAITING:
            renderer.on_waiting(event.seconds_left)
        elif event.kind == QUESTDB_OIDC_EVENT_SUCCESS:
            renderer.on_success(
                _oidc_text(event.identity, event.identity_len),
                event.expires_in_seconds)
        elif event.kind == QUESTDB_OIDC_EVENT_FAILURE:
            renderer.on_failure(
                _oidc_text(event.message, event.message_len) or
                'OIDC sign-in failed.')
    except (KeyboardInterrupt, SystemExit) as exc:
        # sign_in() releases the GIL for the whole native flow, so this callback
        # -- and the persistence diagnostic, see `_OidcForegroundCall` -- is
        # where Python bytecode runs on the caller's thread, and therefore
        # where CPython delivers a pending SIGINT. Swallowing it here
        # made Ctrl-C print a traceback and change nothing, leaving sign_in()
        # polling until the device code expired, with every later Ctrl-C eaten
        # the same way. Stash it for sign_in() to re-raise, and cancel the flow
        # so it actually stops.
        (<OidcDeviceAuth>provider)._interrupt = exc
        if event.kind in (
                QUESTDB_OIDC_EVENT_PROMPT, QUESTDB_OIDC_EVENT_WAITING):
            # Only the waiting phase needs cancelling. On SUCCESS/FAILURE the
            # flow is already ending; in particular, SUCCESS has already
            # committed the token that the interrupt must not discard.
            _oidc_cancel_sign_in_from_callback(<OidcDeviceAuth>provider)
    except BaseException:
        logging.getLogger('questdb').exception('OIDC renderer callback failed')


cdef void _oidc_diagnostic_dispatch(
        const questdb_oidc_diagnostic* diagnostic) noexcept with gil:
    try:
        if diagnostic.kind == QUESTDB_OIDC_DIAGNOSTIC_PERSISTENCE_WARNING:
            logging.getLogger('questdb').warning(
                'OIDC %s',
                _oidc_text(diagnostic.message, diagnostic.message_len) or
                'token-store persistence operation failed')
    except (KeyboardInterrupt, SystemExit) as exc:
        # Not an ordinary handler failure. On the thread running sign_in(),
        # token() or clear() this is where CPython delivers a pending Ctrl-C
        # (warn_persistence runs inside those calls with the GIL released), and
        # catching it clears the tripped-signal flag: discarding it left
        # sign_in() polling to the device-code deadline with the interrupt
        # gone. Hand it to that call instead.
        _oidc_park_foreground_interrupt(exc)
    except BaseException:
        # Logging handlers are user code. Diagnostics are best-effort and must
        # never unwind through C/Rust or turn a usable token into a failure.
        pass


cdef void _oidc_diagnostic_trampoline(
        void* user_data,
        const questdb_oidc_diagnostic* diagnostic) noexcept nogil:
    # This dedicated callback, unlike renderer events, may originate on an
    # attached transport's provider thread. It invokes no user renderer and
    # drops diagnostics once interpreter finalization has begun.
    #
    # Being background-dispatched, it cannot borrow the foreground-only
    # argument that makes `_oidc_event_trampoline` safe. It is instead the
    # same shape `_connection_event_trampoline` and `_sender_error_trampoline`
    # in `_client.pyx` already use for genuinely background dispatch, and it
    # inherits their residual window: finalization can begin between this
    # check and the GIL acquisition below, and no check placed here can close
    # that.
    #
    # What closes it is stopping a diagnostic from arriving during
    # finalization at all, which two places between them do through the
    # non-waiting native diagnostic detach: `_oidc_detach_diagnostics_at_exit`,
    # registered with `atexit` over the provider registry, runs while the
    # interpreter is still whole and detaches every provider still registered;
    # and `_OidcNativeHandle.__dealloc__` detaches a provider that was already
    # collected, which the registry can no longer reach but whose abandoned
    # Rust worker may still be running. The native call publishes exact
    # suppression without waiting for arbitrary user logging-handler code, so
    # neither kind of provider leaves a newly deliverable target behind. The
    # check below remains for an embedder that tears the interpreter down
    # without running `atexit`.
    #
    # It is narrower than both in one respect: `user_data` is NULL, so nothing
    # here dereferences a Python object from a native thread.
    if qdb_py_is_finalizing():
        return
    _oidc_diagnostic_dispatch(diagnostic)


cdef void _oidc_event_trampoline(
        void* user_data,
        const questdb_oidc_event* event) noexcept nogil:
    """Enter Python to run the renderer, from whichever thread native uses.

    Unlike the release callback -- which owns nothing and never enters Python
    (see ``_oidc_user_data_release_trampoline``) -- this one must, because
    running the renderer *is* its job. That is safe for a reason worth stating,
    since it is not local to this function:

    Renderer events are emitted only by the device flow, which is reached only
    through ``sign_in()``; background refresh is silent. The provider is
    strongly referenced by that frame, but Python does not require callers to
    keep the sign-in on the main thread: a daemon sign-in can still be alive
    when interpreter shutdown starts.

    The ``qdb_py_is_finalizing`` check is a final fallback for embedders, not
    the synchronization: finalization can begin between it and the GIL
    acquisition. The registered atexit hook closes that window by detaching
    every live provider's event target while Python is still fully callable.
    """
    if qdb_py_is_finalizing():
        return
    _oidc_event_dispatch(user_data, event)


cdef void _oidc_user_data_release_trampoline(
        void* user_data) noexcept nogil:
    """Deliberately empty: the final release must never enter Python.

    ``user_data`` is an opaque integer key, not a Python object pointer, and
    nothing was allocated for it -- so there is nothing to free here and no
    reason to acquire the GIL.

    This has to hold because the release can run on a thread the interpreter
    does not manage and cannot join. A token provider's acquisition is isolated
    onto its own worker so shutdown can abandon it (``bearer_header_isolated_
    until``); that worker keeps the provider closure -- and therefore the
    callback state -- until its own HTTP call returns, which may be well after
    the owning handles are gone and the interpreter has begun finalizing.
    Dropping the last reference there used to run a ``Py_DECREF`` behind a
    ``Py_IsFinalizing`` check, and no such check can be made safe: finalization
    can begin between the test and ``PyGILState_Ensure``, which then hangs the
    thread or faults. Holding no Python reference at all removes the hazard
    rather than narrowing its window.

    The provider is instead reached through ``_OIDC_PROVIDERS`` under the GIL,
    and its weakref callback drops both registry entries and the native-handle
    owner on a managed thread.
    """
    pass


cdef void _oidc_builder_set_string(
        questdb_oidc_builder* builder,
        object value,
        str name,
        int setting) except *:
    cdef bytes encoded = _oidc_optional_utf8(value, name)
    cdef questdb_error* err = NULL
    cdef bint ok = True
    if encoded is None:
        return
    # All six, not just 0..3. `token_endpoint` (4) and
    # `device_authorization_endpoint` (5) were omitted, so
    # `from_questdb(url, token_endpoint='')` sailed past validation and only
    # failed later at parse time -- and an empty override still counts as
    # EXPLICIT on the native side, which suppresses the IdP-discovery fallback
    # for that endpoint. An endpoint is a URL; empty is never meaningful.
    if PyBytes_GET_SIZE(encoded) == 0:
        from questdb.auth._errors import OidcConfigError
        raise OidcConfigError(
            f'{name} must be a non-empty string or None')
    if setting == 0:
        ok = questdb_oidc_builder_client_id(
            builder, PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err)
    elif setting == 1:
        ok = questdb_oidc_builder_scope(
            builder, PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err)
    elif setting == 2:
        ok = questdb_oidc_builder_audience(
            builder, PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err)
    elif setting == 3:
        ok = questdb_oidc_builder_issuer(
            builder, PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err)
    elif setting == 4:
        ok = questdb_oidc_builder_token_endpoint(
            builder, PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err)
    elif setting == 5:
        ok = questdb_oidc_builder_device_authorization_endpoint(
            builder, PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err)
    else:
        raise AssertionError(
            f'_oidc_builder_set_string: unknown setting {setting!r}')
    if not ok:
        raise _oidc_err_to_py(err)


cdef class _OidcNativeHandle:
    """Registry-owned leaf that releases one native auth handle."""

    cdef questdb_oidc_auth* raw

    def __cinit__(self):
        self.raw = NULL

    def __dealloc__(self):
        cdef questdb_oidc_auth* raw = self.raw
        cdef PyThreadState* gs = NULL
        if raw == NULL:
            return
        self.raw = NULL
        # Reaching here means the provider that owned this handle has been
        # collected, so nothing in Python can close it any more -- but a
        # cancelled token-acquisition worker, which Rust starts detached and
        # never joins, can still hold a clone of the same shared state and
        # reach a token-store write. Its persistence diagnostic would enter
        # `_oidc_diagnostic_trampoline`, whose `qdb_py_is_finalizing()` test
        # cannot be atomic with the `with gil` acquisition that follows it: a
        # shutdown beginning in that window crashes or hangs the interpreter.
        # `_oidc_detach_diagnostics_at_exit` cannot reach this provider either,
        # since the weakref callback has already dropped it from the registry.
        #
        # Detaching is the half no check placed in the trampoline can perform:
        # it stops any later callback, so the window is closed rather than
        # merely re-tested.
        #
        # The non-waiting form, because this runs wherever a collection fired
        # and so cannot know what this thread already holds. The waiting form
        # blocks on the callback gate, and the callback -- once it has the
        # gate -- logs through `logging`, which takes the handler's lock. A
        # collection triggered by an allocation inside that handler's `emit`
        # runs this finalizer on a thread already owning that lock, so waiting
        # parks both threads permanently: this one for the gate, the callback
        # for the handler lock. Releasing the GIL below does not prevent it,
        # because the GIL is not the lock in contention -- which is exactly
        # what the waiting form's own contract now warns about.
        #
        # The GIL is still released: a diagnostic already inside the gate is
        # blocked acquiring it, and even the bounded drain should let that
        # callback finish rather than spin against it.
        #
        # Skipped once finalization has begun, where it can no longer achieve
        # anything: a callback that has not passed the trampoline's
        # `qdb_py_is_finalizing()` check returns at it, and one that has is
        # already parked in a `PyGILState_Ensure` that never returns for a
        # non-main thread afterwards.
        _ensure_doesnt_have_gil(&gs)
        if not qdb_py_is_finalizing():
            questdb_oidc_auth_detach_diagnostics_nowait(raw)
        questdb_oidc_auth_free(raw)
        _ensure_has_gil(&gs)


cdef class OidcDeviceAuth:
    """Native-backed OAuth 2.0 device-flow token provider for QuestDB."""

    cdef object __weakref__
    # The owner of the native handle. Holding it here -- rather than letting
    # `_OIDC_NATIVE_HANDLES` be the sole owner -- is what makes `_raw` safe:
    # `_raw` cannot outlive `self`, because the handle is released only when
    # this reference goes. That matters for a provider caught in a cycle, which
    # is the documented shape (a renderer holding its own provider, or a
    # subclass instance): the cyclic collector runs weakref callbacks BEFORE
    # finalizers, so a registry-owned handle was freed by the callback and then
    # dereferenced by a `__del__` in the same cycle -- a use-after-free that
    # segfaulted the interpreter with no traceback.
    #
    # Cycle collection is unaffected. `_OidcNativeHandle` has no object fields,
    # so Cython gives it no `tp_traverse` and CPython does not track it: it can
    # be referenced by a cycle but never part of one, and it is released by this
    # class's `tp_clear`, which runs after every finalizer. `OidcDeviceAuth`
    # deliberately carries no `@cython.no_gc_clear` for the same reason.
    cdef _OidcNativeHandle _native
    # Cached from `_native.raw` so the hot calls need no attribute lookup; the
    # reference above is the ownership.
    cdef questdb_oidc_auth* _raw
    cdef size_t _provider_id
    cdef object _renderer
    cdef bint _closed
    # A KeyboardInterrupt/SystemExit delivered inside a renderer callback,
    # parked for the sole active sign_in() to re-raise once native returns.
    cdef object _interrupt
    # Native serializes acquisition, but a second native call can regain the
    # GIL before the callback-owning call and steal `_interrupt`. Reject it
    # before releasing the GIL so callback exceptions remain invocation-owned.
    cdef object _sign_in_lock

    def __cinit__(self):
        self._native = None
        self._raw = NULL
        self._provider_id = 0
        self._renderer = None
        self._closed = False
        self._interrupt = None
        self._sign_in_lock = threading.Lock()

    cdef void _require_open(self) except *:
        if self._raw == NULL:
            # Never __init__'d (e.g. cls.__new__ without construction) -- not
            # the same state as closed, which is reported below.
            raise RuntimeError('OidcDeviceAuth is not initialized')
        if self._closed:
            from questdb.auth._errors import OidcCancelledError
            raise OidcCancelledError(
                'The OIDC authentication provider is closed.')

    def __init__(
            self,
            client_id,
            device_authorization_endpoint,
            token_endpoint,
            *,
            scope='openid',
            groups_in_token=False,
            audience=None,
            issuer=None,
            insecure=False,
            ca_bundle=None,
            open_browser=None,
            interactive=None,
            qr=False,
            renderer=None,
            default_interval=5,
            timeout=30,
            token_store=None):
        """Configure a provider from explicit IdP endpoints.

        ``client_id``, ``device_authorization_endpoint`` and ``token_endpoint``
        are required. Use :meth:`from_questdb` to discover them from a QuestDB
        server instead.

        ``groups_in_token`` selects the token kind: ``False`` (the default)
        returns the access token; ``True`` selects the ID token.
        It does not modify ``scope``; include ``openid`` explicitly when the
        identity provider requires it to issue an ID token. Unlike
        :meth:`from_questdb` there is no server-advertised default to inherit
        here, so the kind is always chosen explicitly and defaults to ``False``.

        Because this is an extension type, ``help()`` and the rendered API
        reference cannot introspect the signature, so every parameter is
        documented here:

        * ``scope`` — the non-empty OAuth scope string sent verbatim on the
          initial request. Refresh requests omit it so the identity provider
          preserves the scope originally granted, as required by RFC 6749
          section 6.
        * ``audience`` / ``issuer`` — optional, but must be non-empty when
          provided. ``issuer`` additionally pins the credential endpoints: one
          advertised by a QuestDB server must either sit under the issuer's
          origin and path, or be confirmed by the IdP's own discovery document.
        * ``insecure`` — permits plaintext HTTP for the **QuestDB discovery
          request only**. The identity provider is always held to HTTPS (or
          loopback); this flag never relaxes that.
        * ``ca_bundle`` — path to a PEM bundle used instead of the system roots
          when contacting QuestDB and the IdP.
        * ``open_browser`` — whether :meth:`sign_in` launches a browser at the
          verification URL. ``None`` (the default) opens one, except inside a
          Jupyter kernel, where the kernel may be on a different machine from
          the person reading the notebook and the browser would open where
          nobody is looking. Pass ``True`` to open one anyway — correct for a
          *local* ``jupyter lab``, where that guess is wrong — or ``False`` to
          never open one.
        * ``interactive`` — whether :meth:`sign_in` may prompt at all.
          ``False`` makes it fail immediately with
          :class:`~questdb.auth.OidcInteractionRequired` rather than print a
          device code nobody will read and poll until it expires, which is what
          a headless service or a CI job wants. ``None`` (the default) prompts,
          except in a notebook executed headlessly (papermill / ``nbclient`` /
          ``nbconvert --execute``), whose kernel reports that no human can
          answer. There is no terminal detection: a missing TTY is not evidence
          of a missing human.
        * ``qr`` — also render the verification URL as a QR code, for signing in
          from a phone. The terminal QR needs ``qrcode``; the notebook PNG QR
          additionally needs Pillow (install ``qrcode[pil]``). Ignored when a
          custom ``renderer`` is supplied.
        * ``renderer`` — a :class:`~questdb.auth.Renderer` presenting the prompt.
          Its callbacks receive native display-normalised but still untrusted
          identity-provider text; see that class for sink-encoding, actionable-
          URL and re-entrancy rules.
        * ``default_interval`` — seconds between device-code polls when the
          identity provider does not specify one (default 5, minimum 5,
          maximum 1800, the longest a device code may live). Native clamps the
          value to that range, so a smaller number would be accepted here and
          silently become 5; it is rejected instead. A server-supplied
          interval, and any ``Retry-After``, take precedence.
        * ``timeout`` — the per-HTTP-request timeout in seconds (default 30,
          maximum 120). This is **not** a deadline for the sign-in as a whole,
          which is bounded by the device code's own lifetime.
        * ``token_store`` — a :class:`~questdb.auth.FileTokenStore` enabling
          plaintext on-disk persistence. Credentials stay in memory when this is
          ``None``.
        """
        cdef questdb_oidc_builder* builder
        _oidc_validate_bool(groups_in_token, 'groups_in_token', False)
        _oidc_validate_bool(insecure, 'insecure', False)
        _oidc_validate_bool(open_browser, 'open_browser', True)
        _oidc_validate_bool(interactive, 'interactive', True)
        _oidc_validate_bool(qr, 'qr', False)
        builder = questdb_oidc_builder_new()
        if builder == NULL:
            raise MemoryError()
        try:
            # These three are required positionals, so validate them as such
            # here rather than letting the optional-string path drop a None and
            # leave the field unset. Native then reports the field as missing
            # from QuestDB's /settings and tells the caller to pass it
            # explicitly -- advice that makes no sense for a constructor that
            # never contacts /settings, and names a builder method this API does
            # not have. `from_questdb` keeps the optional path, where they are
            # genuine overrides.
            _oidc_required_utf8(client_id, 'client_id')
            _oidc_required_utf8(token_endpoint, 'token_endpoint')
            _oidc_required_utf8(
                device_authorization_endpoint, 'device_authorization_endpoint')
            _oidc_builder_set_string(builder, client_id, 'client_id', 0)
            _oidc_builder_set_string(builder, scope, 'scope', 1)
            _oidc_builder_set_string(builder, audience, 'audience', 2)
            _oidc_builder_set_string(builder, issuer, 'issuer', 3)
            _oidc_builder_set_string(builder, token_endpoint, 'token_endpoint', 4)
            _oidc_builder_set_string(
                builder, device_authorization_endpoint,
                'device_authorization_endpoint', 5)
            self._finish_builder(
                builder,
                groups_in_token=groups_in_token,
                insecure=insecure,
                ca_bundle=ca_bundle,
                open_browser=open_browser,
                interactive=interactive,
                qr=qr,
                renderer=renderer,
                default_interval=default_interval,
                timeout=timeout,
                token_store=token_store)
        finally:
            questdb_oidc_builder_free(builder)

    @classmethod
    def from_questdb(
            cls,
            url,
            *,
            client_id=None,
            scope=None,
            audience=None,
            groups_in_token=None,
            issuer=None,
            token_endpoint=None,
            device_authorization_endpoint=None,
            insecure=False,
            ca_bundle=None,
            open_browser=None,
            interactive=None,
            qr=False,
            renderer=None,
            default_interval=5,
            timeout=30,
            token_store=None):
        """Discover OIDC configuration from a QuestDB server's ``/settings``.

        Explicit keyword arguments override discovery. ``groups_in_token``
        defaults to ``None``, meaning "inherit whatever the server advertises"
        (``acl.oidc.groups.encoded.in.token``); pass ``True`` or ``False`` to
        force the ID-token or access-token kind regardless of the server. This
        differs from the direct constructor, whose ``groups_in_token`` defaults
        to ``False``.

        ``url`` is the QuestDB server to discover from; ``client_id``,
        ``scope``, ``audience``, ``issuer``, ``token_endpoint`` and
        ``device_authorization_endpoint`` default to ``None``, meaning "take
        the server's value", and any you pass override it. ``client_id``,
        ``scope``, ``audience``, ``issuer``, ``token_endpoint`` and
        ``device_authorization_endpoint`` must be non-empty when provided.
        The remaining parameters — ``insecure``, ``ca_bundle``, ``open_browser``,
        ``interactive``, ``qr``, ``renderer``, ``default_interval``,
        ``timeout`` and ``token_store`` — are not discovered at all and behave
        exactly as documented on :meth:`__init__`; see there for each.

        Note this call performs blocking network I/O: it fetches
        ``/settings``, and may follow up with the identity provider's own
        discovery document to confirm the advertised endpoints. Each request
        is bounded by ``timeout``.
        """
        cdef bytes encoded_url
        cdef questdb_error* err = NULL
        cdef questdb_oidc_builder* builder
        cdef OidcDeviceAuth auth
        cdef PyThreadState* gs = NULL
        cdef const char* encoded_url_ptr
        cdef size_t encoded_url_len

        _oidc_validate_bool(groups_in_token, 'groups_in_token', True)
        _oidc_validate_bool(insecure, 'insecure', False)
        _oidc_validate_bool(open_browser, 'open_browser', True)
        _oidc_validate_bool(interactive, 'interactive', True)
        _oidc_validate_bool(qr, 'qr', False)
        encoded_url = _oidc_required_utf8(url, 'url')
        auth = cls.__new__(cls)
        encoded_url_ptr = PyBytes_AsString(encoded_url)
        encoded_url_len = PyBytes_GET_SIZE(encoded_url)

        _ensure_doesnt_have_gil(&gs)
        builder = questdb_oidc_builder_from_questdb(
            encoded_url_ptr, encoded_url_len, &err)
        _ensure_has_gil(&gs)
        if builder == NULL:
            raise _oidc_err_to_py(err)
        try:
            _oidc_builder_set_string(builder, client_id, 'client_id', 0)
            _oidc_builder_set_string(builder, scope, 'scope', 1)
            _oidc_builder_set_string(builder, audience, 'audience', 2)
            _oidc_builder_set_string(builder, issuer, 'issuer', 3)
            _oidc_builder_set_string(builder, token_endpoint, 'token_endpoint', 4)
            _oidc_builder_set_string(
                builder, device_authorization_endpoint,
                'device_authorization_endpoint', 5)
            auth._finish_builder(
                builder,
                groups_in_token=groups_in_token,
                insecure=insecure,
                ca_bundle=ca_bundle,
                open_browser=open_browser,
                interactive=interactive,
                qr=qr,
                renderer=renderer,
                default_interval=default_interval,
                timeout=timeout,
                token_store=token_store)
            return auth
        finally:
            questdb_oidc_builder_free(builder)

    cdef void _finish_builder(
            self,
            questdb_oidc_builder* builder,
            object groups_in_token,
            object insecure,
            object ca_bundle,
            object open_browser,
            object interactive,
            object qr,
            object renderer,
            object default_interval,
            object timeout,
            object token_store) except *:
        cdef questdb_error* err = NULL
        cdef bytes encoded
        cdef uint64_t timeout_ms
        cdef PyThreadState* gs = NULL
        cdef _OidcNativeHandle native
        cdef questdb_oidc_auth* built_raw = NULL
        cdef size_t provider_id
        cdef bint detach_callbacks_after_build
        cdef object selected_renderer
        from questdb.auth._errors import OidcConfigError
        from questdb.auth._render import (
            detect_interactive, in_ipython_kernel, make_renderer)
        from questdb.auth._store import FileTokenStore

        if self._raw != NULL:
            raise OidcConfigError('OidcDeviceAuth is already initialized')
        if groups_in_token is not None and not questdb_oidc_builder_groups_in_token(
                builder, groups_in_token is True, &err):
            raise _oidc_err_to_py(err)
        if not questdb_oidc_builder_allow_insecure_transport(
                builder, insecure is True, &err):
            raise _oidc_err_to_py(err)
        if open_browser is None:
            # Auto: suppress inside a Jupyter/ZMQ kernel, where the kernel may
            # well be on a different machine from the person reading the
            # notebook, so the browser would open where nobody is looking.
            # That guess is wrong for a LOCAL `jupyter lab`, which is why an
            # explicit True now overrides it instead of being silently dropped
            # -- previously `open_browser=True` was a no-op in every kernel,
            # with no way to ask for the browser at all.
            open_browser = not in_ipython_kernel()
        if not questdb_oidc_builder_open_browser(
                builder,
                open_browser is True,
                &err):
            raise _oidc_err_to_py(err)
        if interactive is None:
            interactive = detect_interactive()
        if not questdb_oidc_builder_interactive(
                builder, interactive is True, &err):
            raise _oidc_err_to_py(err)

        # Bounded at the device code's own maximum lifetime, which is the
        # ceiling native clamps the interval to anyway. The old bound was the
        # full uint64 range, and native casts the value to i64 before clamping
        # -- so anything at or above 2**63 wrapped negative, floored to 0, and
        # came back out as the 5s MINIMUM. The largest value the validator
        # accepted therefore produced the fastest possible polling, which is
        # the opposite of what it asked for.
        if (not isinstance(default_interval, int)
                or isinstance(default_interval, bool)
                or default_interval < 5
                or default_interval > 1800):
            raise OidcConfigError(
                'default_interval must be an integer number of seconds '
                'between 5 and 1800 (the maximum device-code lifetime)')
        if not questdb_oidc_builder_default_interval_seconds(
                builder, <uint64_t>default_interval, &err):
            raise _oidc_err_to_py(err)

        if (not isinstance(timeout, (int, float))
                or isinstance(timeout, bool)
                or timeout <= 0
                or timeout > 120
                or (isinstance(timeout, float) and isnan(timeout))):
            raise OidcConfigError(
                'timeout must be a positive, finite number no greater than 120 seconds')
        timeout_ms = <uint64_t>round(timeout * 1000)
        if timeout_ms == 0 or not questdb_oidc_builder_timeout_ms(
                builder, timeout_ms, &err):
            if err != NULL:
                raise _oidc_err_to_py(err)
            raise OidcConfigError('timeout is too small')

        encoded = _oidc_optional_utf8(ca_bundle, 'ca_bundle')
        if encoded is not None and not questdb_oidc_builder_ca_bundle(
                builder,
                PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err):
            raise _oidc_err_to_py(err)

        if token_store is not None:
            if not isinstance(token_store, FileTokenStore):
                raise OidcConfigError(
                    'native OIDC supports FileTokenStore persistence only')
            # The native ABI accepts UTF-8 text, not arbitrary POSIX path
            # bytes. FileTokenStore uses os.fsdecode(), which intentionally
            # preserves undecodable bytes as surrogate characters; translate
            # that unsupported case into the public configuration error type.
            encoded = _oidc_required_utf8(
                os.fspath(token_store.directory), 'token_store directory')
            if not questdb_oidc_builder_file_token_store(
                    builder,
                    PyBytes_AsString(encoded), PyBytes_GET_SIZE(encoded), &err):
                raise _oidc_err_to_py(err)

        # Validate a local before publishing it on `self`. A renderer is allowed
        # to hold its provider, so assigning first made a validation failure a
        # provider <-> renderer cycle that PyPy cpyext cannot reclaim.
        selected_renderer = (
            renderer if renderer is not None else make_renderer(qr=qr is True))
        for callback_name in (
                'on_prompt', 'on_waiting', 'on_success', 'on_failure'):
            if not callable(getattr(selected_renderer, callback_name, None)):
                raise OidcConfigError(
                    f'renderer callback {callback_name} must be callable')
        # Hand native an opaque integer key, not a `PyObject*`. Native callback
        # state then owns no Python reference, so its release callback -- which
        # can run on an abandoned acquisition worker after the interpreter has
        # begun finalizing -- never needs the GIL. See
        # `_oidc_user_data_release_trampoline`.
        global _oidc_last_provider_id
        with _OIDC_REGISTRY_LOCK:
            _oidc_last_provider_id += 1
            provider_id = _oidc_last_provider_id
        self._provider_id = provider_id
        native = _OidcNativeHandle()
        # The provider owns the handle; take that reference before anything can
        # fail, so no path can build a handle the provider does not hold.
        self._native = native
        # The registry keeps only a weakref, so it never keeps a provider alive
        # and a `with` block's exit still collects one. The provider owns the
        # renderer. Cython exposes that edge to CPython's cyclic GC; PyPy's
        # cpyext cannot reclaim a cycle crossing the extension-object boundary
        # (PyPy issue #3848), so callers there should explicitly close or use a
        # weak renderer back-reference. Attached transports retain the provider
        # separately, and native holds its own cloned handle on top of that.
        try:
            self._renderer = selected_renderer
            from functools import partial
            provider_ref = PyWeakref_NewRef(
                self, partial(_oidc_provider_collected, provider_id))
            # The lock is released before the blocking native build below: it
            # guards the registry, not the construction.
            with _OIDC_REGISTRY_LOCK:
                _OIDC_PROVIDERS[provider_id] = provider_ref
                _OIDC_NATIVE_HANDLES[provider_id] = native
            # Registered <=> built. Every failure after registration must drop
            # both entries immediately, rather than waiting for the half-built
            # provider to be collected. That also lets a subclass retry
            # `__init__` without stranding a dead weakref or a registry entry.
            if not questdb_oidc_builder_event_handler(
                    builder,
                    _oidc_event_trampoline,
                    <void*>provider_id,
                    _oidc_user_data_release_trampoline,
                    &err):
                raise _oidc_err_to_py(err)
            # Persistence warnings use a separate stateless callback because
            # they can originate on background provider threads. They never
            # enter the user renderer or retain this Python object.
            #
            # Installed only alongside a token store. Every native
            # `warn_persistence` site sits behind a store gate, so a provider
            # without one emits no diagnostic and the handler would be nothing
            # but added interpreter-shutdown surface -- see the trampoline's
            # note on the finalization window it inherits. Native's default
            # stderr handler stays registered in that case and is equally
            # unreachable, so nothing escapes the `questdb` logger.
            if token_store is not None:
                if not questdb_oidc_builder_diagnostic_handler(
                        builder,
                        _oidc_diagnostic_trampoline,
                        NULL,
                        NULL,
                        &err):
                    raise _oidc_err_to_py(err)
            # Build into a thread-local C pointer while the GIL is released.
            # Publishing directly into `native.raw` here races the atexit hook:
            # it can acquire the GIL, snapshot this registered handle, and read
            # that field while native is writing it. Reacquiring the GIL before
            # publication makes the field obey the same synchronization as every
            # reader; the marker check below then self-detaches a build that the
            # shutdown snapshot observed while its raw field was still NULL.
            _ensure_doesnt_have_gil(&gs)
            built_raw = questdb_oidc_builder_build(builder, &err)
            _ensure_has_gil(&gs)
            if built_raw == NULL:
                raise _oidc_err_to_py(err)
            native.raw = built_raw
            with _OIDC_REGISTRY_LOCK:
                detach_callbacks_after_build = _oidc_callbacks_shutting_down
            if detach_callbacks_after_build:
                # This provider finished after the atexit snapshot while an
                # older callback drain had released the GIL. No Python caller
                # can use it before __init__ returns, so detach both callback
                # targets now, before exposing the completed provider.
                _oidc_detach_handle_callbacks(native, True)
            self._raw = native.raw
            # A failed build leaves `_raw` NULL and this function supports
            # re-initialisation from that state, but `close()` latches
            # `_closed` even on a NULL handle -- and `__cinit__`, the only
            # other place the flag is cleared, does not re-run. Without this a
            # retried `__init__` would build a perfectly usable provider that
            # every entry point then refused as "closed", with no way back.
            self._closed = False
        except:
            # Dropping `_native` releases the handle if the build got that far;
            # `_OidcNativeHandle.__dealloc__` is NULL-safe when it did not.
            self._raw = NULL
            self._native = None
            self._provider_id = 0
            # Match close(): break the supported renderer -> provider back-edge
            # on every failed native build. Validation failures never publish
            # the renderer at all.
            self._renderer = None
            with _OIDC_REGISTRY_LOCK:
                _OIDC_PROVIDERS.pop(provider_id, None)
                _OIDC_NATIVE_HANDLES.pop(provider_id, None)
            raise

    def sign_in(self):
        """Run interactive sign-in if no cached or refreshable token exists.

        ``Ctrl-C`` during the wait cancels only this sign-in attempt and raises
        ``KeyboardInterrupt``. The provider remains open, attached transports
        remain usable, and a later ``sign_in()`` on the same provider can retry.

        Only one ``sign_in()`` call may run on a provider at a time. A concurrent
        call raises :class:`~questdb.auth.OidcError` instead of waiting behind
        the interactive flow.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        cdef _OidcForegroundCall call
        self._require_open()
        if not self._sign_in_lock.acquire(False):
            from questdb.auth._errors import OidcError
            raise OidcError(
                'OIDC sign_in() is already in progress on this provider.')
        try:
            # A callback may park an interrupt only for this invocation: the
            # non-blocking lock above prevents another sign_in() from entering
            # native and winning the race to consume the provider field.
            self._interrupt = None
            # Lets a persistence diagnostic on this thread find this provider:
            # its callback carries no user_data.
            call = _oidc_foreground_enter(self)
            _ensure_doesnt_have_gil(&gs)
            ok = questdb_oidc_auth_sign_in(self._raw, &err)
            _ensure_has_gil(&gs)
            _oidc_foreground_exit(call)
            interrupt = self._interrupt
            self._interrupt = None
            if interrupt is not None:
                # The interrupt is what the user asked for; the native error is
                # just the cancellation it caused.
                if err != NULL:
                    questdb_error_free(err)
                raise interrupt
            if not ok:
                raise _oidc_err_to_py(err)
        finally:
            self._sign_in_lock.release()

    def cancel_sign_in(self):
        """Cancel the current interactive sign-in without closing the provider.

        The active :meth:`sign_in` raises
        :class:`~questdb.auth.OidcCancelledError`. Cached credentials are not
        discarded, attached senders, pools and readers remain usable, and a
        later ``sign_in()`` on this provider can succeed. If no device flow is
        running, this is an idempotent no-op that does not affect the next one.

        Safe from any thread, including a renderer callback. Use :meth:`close`
        instead only when the provider and every attached transport should be
        disabled permanently.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        if self._raw == NULL:
            return
        _ensure_doesnt_have_gil(&gs)
        ok = questdb_oidc_auth_cancel_sign_in(self._raw, &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise _oidc_err_to_py(err)

    @property
    def _sign_in_in_progress(self):
        """Whether this wrapper's local :meth:`sign_in` lock is held.

        Internal diagnostic/test property. Transport retry classification does
        not consult this mutable state; it uses the immutable
        ``_acquisition_busy`` bit captured in the native error instead.
        """
        return self._sign_in_lock.locked()

    def token(self):
        """Return a cached or silently refreshed token; never prompt.

        Raises :class:`~questdb.auth.OidcInteractionRequired` when no cached or
        silently refreshable credential is available -- notably before the first
        :meth:`sign_in`, and once a refresh token has expired. This method never
        displays a prompt, so that condition can only be cleared by calling
        :meth:`sign_in` on a thread that may interact with the user.
        """
        cdef questdb_error* err = NULL
        cdef questdb_oidc_token* token = NULL
        cdef const char* data = NULL
        cdef size_t length = 0
        cdef PyThreadState* gs = NULL
        cdef _OidcForegroundCall call
        self._require_open()
        call = _oidc_foreground_enter(None)
        _ensure_doesnt_have_gil(&gs)
        token = questdb_oidc_auth_token(self._raw, &err)
        _ensure_has_gil(&gs)
        interrupt = _oidc_foreground_exit(call)
        if interrupt is not None:
            # Ctrl-C landed in a persistence diagnostic during a refresh. The
            # user asked to stop; the outcome of the call is moot.
            if token != NULL:
                questdb_oidc_token_free(token)
            if err != NULL:
                questdb_error_free(err)
            raise interrupt
        if token == NULL:
            raise _oidc_err_to_py(err)
        try:
            data = questdb_oidc_token_data(token)
            length = questdb_oidc_token_len(token)
            # Guard NULL like every other native span in this file does, via
            # _oidc_text. The header promises non-NULL for a non-NULL token and
            # native returns a Rust String's pointer, so this is unreachable
            # today -- but PyUnicode_FromStringAndSize(NULL, n) returns an
            # *uninitialized* str on CPython 3.10/3.11 rather than raising, and
            # this is the one function that returns a credential.
            return _oidc_text(data, length) or ''
        finally:
            questdb_oidc_token_free(token)

    def headers(self):
        """Return an HTTP Authorization header containing the current token.

        A fresh ``{"Authorization": "Bearer ..."}`` dict on each call. Raises
        whatever :meth:`token` raises, including
        :class:`~questdb.auth.OidcInteractionRequired` before sign-in.
        """
        return {'Authorization': 'Bearer ' + self.token()}

    def clear(self):
        """Clear memory and persisted credentials without revoking at the IdP.

        Works after :meth:`close`. ``close`` drops the in-memory credential but
        deliberately leaves the persisted entry, so clearing has to stay
        available afterwards -- otherwise the ``with`` form, whose exit closes
        the provider, would leave a long-lived plaintext refresh token on disk
        with no supported way to remove it.

        Never waits behind an interactive :meth:`sign_in` running on another
        thread, which can hold the provider for the device code's whole
        lifetime (the GIL is released for the call, so it would not even see
        ``Ctrl-C``). It raises :class:`~questdb.QuestDBError` with code
        ``InvalidApiCall`` instead and clears **nothing**: call
        :meth:`cancel_sign_in` first, or retry once the sign-in completes.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        cdef _OidcForegroundCall call
        if self._raw == NULL:
            return
        call = _oidc_foreground_enter(None)
        _ensure_doesnt_have_gil(&gs)
        ok = questdb_oidc_auth_clear(self._raw, &err)
        _ensure_has_gil(&gs)
        interrupt = _oidc_foreground_exit(call)
        if interrupt is not None:
            if err != NULL:
                questdb_error_free(err)
            raise interrupt
        if not ok:
            raise _oidc_err_to_py(err)

    def close(self):
        """Permanently close this provider and cancel interruptible waits.

        A sign-in, silent-refresh coordination, or bundled file-store lock wait
        running on another thread is asked to stop, and the in-memory credential
        is dropped. Cloned native handles retained by attached transports share
        the closed state. Idempotent.

        The call ordinarily waits for the running operation to leave the native
        authentication critical section. While this provider's renderer callback
        is active, however, it publishes the close, drops the in-memory credential,
        and returns without waiting, regardless of which thread called it. A
        callback may delegate ``close()`` to another thread and join that thread,
        so waiting there could deadlock just as it would on the callback thread
        itself. A later ``close()`` after the callback returns performs the wait.

        The persisted entry is deliberately left behind so :meth:`clear` can
        still remove it after closing.

        .. warning::

           Closing is **terminal for every attached transport**, not merely a
           state they observe. Closing is monotonic, so every
           :class:`~questdb.Sender`, :func:`questdb.connect` pool and reader
           built from this provider fails its next token pull non-retryably:
           reconnect loops stop, a QWP/WebSocket publication store is
           terminalized with accepted frames still queued, and there is no way
           to attach a replacement provider to an existing handle. Disk-backed
           store-and-forward slots are not deleted and stay drainable by a
           later process, but this one will not send them.

           To recover, build a new provider **and** rebuild each sender, pool
           and reader that used the old one. Where that matters, sign in on a
           provider before attaching it and keep re-authentication on a
           separate, unattached provider.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        if self._raw == NULL:
            self._closed = True
            self._renderer = None
            return
        # Deliberately NOT short-circuited on ``self._closed``. A close
        # published while a renderer callback is active marks the provider
        # closed without draining, because the callback runs inside the very
        # critical section the drain waits on.
        # Skipping the native call here on that flag left the drain permanently
        # unperformed: the later ``close()`` (or ``__exit__``) that could safely
        # drain became a no-op. Native close is idempotent and cheap on an
        # already-closed provider, so calling through unconditionally restores
        # the documented behaviour at no meaningful cost.
        _ensure_doesnt_have_gil(&gs)
        ok = questdb_oidc_auth_close(self._raw, &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise _oidc_err_to_py(err)
        self._closed = True
        # Detach presentation state eagerly. Besides releasing resources owned
        # by a renderer, this breaks a renderer -> provider back-reference on
        # explicit close without waiting for cyclic GC.
        self._renderer = None

    def __enter__(self):
        """Return this open provider for use as a context manager."""
        self._require_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the provider permanently when leaving a ``with`` block."""
        self.close()
        return False

    @property
    def config(self):
        """The resolved OIDC configuration as an :class:`OidcConfig`.

        Reports the client id, the token and device-authorization endpoints, the
        scope, the selected token kind (``groups_in_token``), and the optional
        audience / issuer.

        Readable after :meth:`close`: the resolved configuration is immutable
        native state that closing does not invalidate. Raises ``RuntimeError``
        only on a provider that was never initialized.
        """
        cdef questdb_oidc_config_view view
        from questdb.auth._config import OidcConfig
        if self._raw == NULL:
            raise RuntimeError('OidcDeviceAuth is not initialized')
        memset(&view, 0, sizeof(questdb_oidc_config_view))
        view.struct_size = sizeof(questdb_oidc_config_view)
        if not questdb_oidc_auth_get_config(self._raw, &view):
            raise RuntimeError('native OIDC config view is unavailable')
        # The callee replaces struct_size with the prefix it wrote. Static
        # linkage supplies the full v1 view today; checking still prevents an
        # out-of-prefix read if packaging ever loads an older shared library.
        if not _oidc_config_view_is_full(view.struct_size):
            raise RuntimeError('native OIDC config view is shorter than v1')
        client_id = _oidc_text(view.client_id, view.client_id_len)
        token_endpoint = _oidc_text(
            view.token_endpoint, view.token_endpoint_len)
        device_authorization_endpoint = _oidc_text(
            view.device_authorization_endpoint,
            view.device_authorization_endpoint_len)
        scope = _oidc_text(view.scope, view.scope_len)
        audience = _oidc_text(view.audience, view.audience_len)
        issuer = _oidc_text(view.issuer, view.issuer_len)
        # A built provider always resolves these; guard anyway so a weakened
        # native invariant surfaces clearly instead of seating None into
        # OidcConfig's non-optional str fields. audience / issuer stay optional.
        if (client_id is None or token_endpoint is None
                or device_authorization_endpoint is None or scope is None):
            raise RuntimeError(
                'native OIDC config view is missing a required field')
        # Native config-view strings are not guaranteed display-sanitized (only
        # the device-flow event text is), yet OidcConfig's repr can reach a
        # terminal / notebook or a logged traceback. Strip control/bidi/zero-
        # width chars from every field so a MITM'd or hostile /settings response
        # cannot inject ANSI/bidi through this sink -- matching the sanitize-
        # every-sink posture of the renderers and OidcError. _render is stdlib-
        # only, so this lazy import introduces no cycle. Optional fields keep
        # None (absent) distinct from '' (present-but-empty).
        #
        # OidcConfig.__post_init__ enforces the same strip for every instance,
        # so this is belt-and-braces: it keeps the sanitization visible at the
        # native boundary, where the untrusted input actually enters, and
        # _strip_control is idempotent.
        from questdb.auth._render import _strip_control
        return OidcConfig(
            client_id=_strip_control(client_id),
            token_endpoint=_strip_control(token_endpoint),
            device_authorization_endpoint=_strip_control(
                device_authorization_endpoint),
            scope=_strip_control(scope),
            groups_in_token=bool(view.groups_in_token),
            audience=(_strip_control(audience)
                      if audience is not None else None),
            issuer=_strip_control(issuer) if issuer is not None else None)


def _debug_oidc_renderer_attached(OidcDeviceAuth provider):
    """Internal test seam: whether construction published a renderer edge."""
    return provider._renderer is not None


cdef object _oidc_provider_from_user_data(void* user_data):
    """Resolve the provider a native event belongs to. Requires the GIL.

    ``user_data`` is the integer key handed to native at registration, never a
    Python object pointer -- see ``_oidc_user_data_release_trampoline``. A
    missing key or a dead weakref both mean the provider is gone, which is not
    an error: the flow is simply no longer observed.
    """
    ref = _OIDC_PROVIDERS.get(<size_t>user_data)
    if ref is None:
        return None
    return ref()
