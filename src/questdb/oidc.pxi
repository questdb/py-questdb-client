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
# `_OIDC_REGISTRY_LOCK` guards the id counter and the paired dict writes. Under
# the GIL those are already atomic and the lock is nearly free -- it is taken
# once per provider construction, never on a hot path. It is here so the
# invariant does not silently depend on the GIL: `_oidc_last_provider_id += 1`
# is a read-modify-write, and on a free-threaded build two providers could take
# the same id and evict each other's entries. The weakref callback deliberately
# does NOT take it: it can run from inside a collection, and a non-reentrant
# lock held by the same thread would deadlock there.
#
# A module-level `cdef object` is a C static rather than an entry in the module
# dict, so `_PyModule_Clear` never swaps it for None at interpreter shutdown:
# readers need no None guard.
cdef object _OIDC_PROVIDERS = {}
cdef object _OIDC_NATIVE_HANDLES = {}
cdef object _OIDC_REGISTRY_LOCK = threading.Lock()
cdef size_t _oidc_last_provider_id = 0


def _debug_oidc_registry_size():
    """Internal test hook: live entries in the provider registry.

    Native owns no Python reference. A provider weakref callback removes both
    registry entries; either one left behind would retain bookkeeping or the
    native auth handle for the life of the process.
    """
    if len(_OIDC_PROVIDERS) != len(_OIDC_NATIVE_HANDLES):
        raise AssertionError('OIDC provider/native registries are out of sync')
    return len(_OIDC_PROVIDERS)


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
    _OIDC_PROVIDERS.pop(provider_id, None)
    _OIDC_NATIVE_HANDLES.pop(provider_id, None)


cdef inline object _oidc_text(const char* buf, size_t length):
    if buf == NULL:
        return None
    return PyUnicode_FromStringAndSize(buf, <Py_ssize_t>length)


# Resolved on first use and cached. See `_oidc_errors_module`.
cdef object _OIDC_ERRORS_MOD = None


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
    if _OIDC_ERRORS_MOD is None:
        mod = sys.modules.get('questdb.auth._errors')
        if mod is None:
            try:
                import questdb.auth._errors as mod
            except BaseException:
                return None
        _OIDC_ERRORS_MOD = mod
    return _OIDC_ERRORS_MOD


cdef object _oidc_err_to_py_unowned(questdb_error* err):
    cdef const char* msg_buf = NULL
    cdef size_t msg_len = 0
    cdef questdb_oidc_error_view view
    cdef object message
    cdef object idp_error = None
    cdef object description = None
    cdef object status = None
    cdef object retry_after = None
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
        return QuestDBError(
            c_err_code_to_py(questdb_error_get_code(err)),
            _oidc_text(msg_buf, msg_len) or 'Unknown native OIDC error.',
            in_doubt=questdb_error_in_doubt(err))
    OidcConfigError = errors.OidcConfigError
    OidcCancelledError = errors.OidcCancelledError
    OidcDeviceFlowError = errors.OidcDeviceFlowError
    OidcError = errors.OidcError
    OidcInteractionRequired = errors.OidcInteractionRequired
    OidcNetworkError = errors.OidcNetworkError
    OidcTimeoutError = errors.OidcTimeoutError

    if err == NULL:
        return OidcError('Unknown native OIDC error.')
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
    memset(&view, 0, sizeof(questdb_oidc_error_view))
    view.struct_size = sizeof(questdb_oidc_error_view)
    if questdb_error_oidc_get_view(err, &view):
        idp_error = _oidc_text(view.idp_error, view.idp_error_len)
        description = _oidc_text(
            view.idp_error_description, view.idp_error_description_len)
        status = view.status if view.has_status else None
        retry_after = (
            view.retry_after_seconds if view.has_retry_after else None)
        if view.kind == QUESTDB_OIDC_ERROR_CONFIG:
            exc = OidcConfigError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code)
        elif view.kind == QUESTDB_OIDC_ERROR_NETWORK:
            exc = OidcNetworkError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code)
        elif view.kind == QUESTDB_OIDC_ERROR_DEVICE_FLOW:
            exc = OidcDeviceFlowError(
                message,
                error=idp_error,
                error_description=description,
                status=status,
                retry_after=retry_after,
                in_doubt=in_doubt, code=code)
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
                in_doubt=in_doubt, code=code)
        elif view.kind == QUESTDB_OIDC_ERROR_INTERACTION_REQUIRED:
            exc = OidcInteractionRequired(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code)
        elif view.kind == QUESTDB_OIDC_ERROR_CANCELLED:
            exc = OidcCancelledError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code)
        else:
            exc = OidcError(
                message, status=status, retry_after=retry_after,
                in_doubt=in_doubt, code=code)
    else:
        exc = OidcError(message, in_doubt=in_doubt, code=code)
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


cdef void _oidc_cancel_from_callback(OidcDeviceAuth provider) noexcept:
    """Publish the provider's close from inside its own event callback.

    Native splits close into a signal that does not wait for authentication work
    and a drain, and skips only the drain while a callback is running. Publication
    may briefly contend with wait registration, but cannot wait on this callback's
    authentication critical section or trip the callback-reentry guard. Errors
    are swallowed deliberately: this runs on the interrupt path, where the
    pending ``KeyboardInterrupt`` is the thing worth surfacing.
    """
    cdef questdb_error* err = NULL
    if provider._raw == NULL:
        return
    if not questdb_oidc_auth_close(provider._raw, &err):
        if err != NULL:
            questdb_error_free(err)
        return
    provider._closed = True
    provider._renderer = None


cdef void _oidc_event_dispatch(
        void* user_data,
        const questdb_oidc_event* event) noexcept with gil:
    provider = _oidc_provider_from_user_data(user_data)
    if provider is None:
        return
    renderer = (<OidcDeviceAuth>provider)._renderer
    if renderer is None:
        return
    try:
        if event.kind == QUESTDB_OIDC_EVENT_PROMPT:
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
                'interval': event.interval_seconds,
                # This is the only native-vetted actionable URL. Built-in
                # renderers prefer it for links and QR codes.
                'browser_target': _oidc_text(
                    event.browser_target, event.browser_target_len),
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
        # is the only place Python bytecode runs on the caller's thread -- and
        # therefore where CPython delivers a pending SIGINT. Swallowing it here
        # made Ctrl-C print a traceback and change nothing, leaving sign_in()
        # polling until the device code expired, with every later Ctrl-C eaten
        # the same way. Stash it for sign_in() to re-raise, and cancel the flow
        # so it actually stops.
        (<OidcDeviceAuth>provider)._interrupt = exc
        if event.kind in (
                QUESTDB_OIDC_EVENT_PROMPT, QUESTDB_OIDC_EVENT_WAITING):
            # Only the waiting phase needs cancelling. On SUCCESS/FAILURE the
            # flow is already ending, and closing there would discard a token
            # that was just acquired.
            _oidc_cancel_from_callback(<OidcDeviceAuth>provider)
    except BaseException:
        logging.getLogger('questdb').exception('OIDC renderer callback failed')


cdef void _oidc_event_trampoline(
        void* user_data,
        const questdb_oidc_event* event) noexcept nogil:
    """Enter Python to run the renderer, from whichever thread native uses.

    Unlike the release callback -- which owns nothing and never enters Python
    (see ``_oidc_user_data_release_trampoline``) -- this one must, because
    running the renderer *is* its job. That is safe for a reason worth stating,
    since it is not local to this function:

    Renderer events are emitted only by the device flow, which is reached only
    through ``sign_in()``. That is a foreground call, so for the whole time
    events can arrive the interpreter is running, the calling thread is blocked
    inside the native call, and the provider is strongly referenced by that
    frame. There is no background refresh path that renders: ``token()`` is
    non-interactive and silent. So this cannot be reached by an abandoned worker
    after the interpreter has begun finalizing -- the hazard that made the
    release callback stop entering Python at all.

    The ``qdb_py_is_finalizing`` check is therefore belt-and-braces rather than
    load-bearing. It is retained for an embedder that tears down the interpreter
    while another thread is still inside ``sign_in()``, but note it is a TOCTOU:
    finalization can begin between the check and the GIL acquisition, and no
    check can close that. Keeping the flow foreground-only is what makes this
    path safe.
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
        if self.raw != NULL:
            questdb_oidc_auth_free(self.raw)
            self.raw = NULL


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
          from a phone. Ignored when a custom ``renderer`` is supplied.
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
        ``scope``, ``audience`` and ``issuer`` must be non-empty when provided.
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
        cdef size_t provider_id
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

        self._renderer = (
            renderer if renderer is not None else make_renderer(qr=qr is True))
        for callback_name in (
                'on_prompt', 'on_waiting', 'on_success', 'on_failure'):
            if not callable(getattr(self._renderer, callback_name, None)):
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
        # renderer, so provider/renderer cycles stay fully visible to Python's
        # cyclic GC. Attached transports retain the provider separately, and
        # native holds its own cloned handle on top of that.
        try:
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
            _ensure_doesnt_have_gil(&gs)
            native.raw = questdb_oidc_builder_build(builder, &err)
            _ensure_has_gil(&gs)
            if native.raw == NULL:
                raise _oidc_err_to_py(err)
            self._raw = native.raw
        except:
            # Dropping `_native` releases the handle if the build got that far;
            # `_OidcNativeHandle.__dealloc__` is NULL-safe when it did not.
            self._raw = NULL
            self._native = None
            self._provider_id = 0
            with _OIDC_REGISTRY_LOCK:
                _OIDC_PROVIDERS.pop(provider_id, None)
                _OIDC_NATIVE_HANDLES.pop(provider_id, None)
            raise

    def sign_in(self):
        """Run interactive sign-in if no cached or refreshable token exists.

        ``Ctrl-C`` during the wait cancels the flow and raises
        ``KeyboardInterrupt``.

        Only one ``sign_in()`` call may run on a provider at a time. A concurrent
        call raises :class:`~questdb.auth.OidcError` instead of waiting behind
        the interactive flow.

        .. warning::

           Cancelling **closes the provider permanently**, and closing is
           shared state: every :class:`~questdb.Sender`, :func:`questdb.connect`
           pool and reader already attached with ``oidc_auth=`` holds a handle
           on the same provider and is closed with it. Their next token pull
           fails terminally -- native classifies a closed provider as
           non-retryable, so reconnect loops stop and queued store-and-forward
           frames are abandoned -- and there is no way to attach a replacement
           provider to an existing handle.

           So a ``Ctrl-C`` at a re-authentication prompt does not just abandon
           that sign-in: it ends every transport built from this provider. To
           recover, build a new ``OidcDeviceAuth`` **and** rebuild each sender,
           pool and reader that used the old one. Where that matters, sign in
           on a provider before attaching it and keep re-authentication on a
           separate, unattached provider.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
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
            _ensure_doesnt_have_gil(&gs)
            ok = questdb_oidc_auth_sign_in(self._raw, &err)
            _ensure_has_gil(&gs)
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
        self._require_open()
        _ensure_doesnt_have_gil(&gs)
        token = questdb_oidc_auth_token(self._raw, &err)
        _ensure_has_gil(&gs)
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
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        if self._raw == NULL:
            return
        _ensure_doesnt_have_gil(&gs)
        ok = questdb_oidc_auth_clear(self._raw, &err)
        _ensure_has_gil(&gs)
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
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        if self._raw == NULL:
            self._closed = True
            self._renderer = None
            return
        # Deliberately NOT short-circuited on ``self._closed``. A close
        # published while a renderer callback is active -- including the Ctrl-C
        # cancel path -- marks the provider closed without draining, because the
        # callback runs inside the very critical section the drain waits on.
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
        self._require_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
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
