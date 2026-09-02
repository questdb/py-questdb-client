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


cdef inline object _oidc_text(const char* buf, size_t length):
    if buf == NULL:
        return None
    return PyUnicode_FromStringAndSize(buf, <Py_ssize_t>length)


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

    from questdb.auth._errors import (
        OidcConfigError,
        OidcCancelledError,
        OidcDeviceFlowError,
        OidcError,
        OidcInteractionRequired,
        OidcNetworkError,
        OidcTimeoutError,
    )

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

    Native splits close into a lock-free signal and a drain, and skips only the
    drain while a callback is running, so this neither blocks nor trips the
    callback-reentry guard. Errors are swallowed deliberately: this runs on the
    interrupt path, where the pending ``KeyboardInterrupt`` is the thing worth
    surfacing.
    """
    cdef questdb_error* err = NULL
    if provider._raw == NULL:
        return
    if not questdb_oidc_auth_close(provider._raw, &err):
        if err != NULL:
            questdb_error_free(err)
        return
    provider._closed = True


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
                # matching Java's complete device challenge.
                'expires_in': <uint64_t>event.expires_in_seconds,
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
    if qdb_py_is_finalizing():
        return
    _oidc_event_dispatch(user_data, event)


cdef void _oidc_user_data_release(void* user_data) noexcept with gil:
    Py_DECREF(<object>user_data)


cdef void _oidc_user_data_release_trampoline(
        void* user_data) noexcept nogil:
    # At shutdown Python deliberately leaks this final reference rather than
    # attempting to enter an interpreter whose runtime is already torn down.
    if qdb_py_is_finalizing():
        return
    _oidc_user_data_release(user_data)


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


cdef class OidcDeviceAuth:
    """Native-backed OAuth 2.0 device-flow token provider for QuestDB."""

    cdef object __weakref__
    cdef questdb_oidc_auth* _raw
    cdef object _renderer
    cdef bint _closed
    # A KeyboardInterrupt/SystemExit delivered inside a renderer callback,
    # parked for sign_in() to re-raise once the native call returns.
    cdef object _interrupt

    def __cinit__(self):
        self._raw = NULL
        self._renderer = None
        self._closed = False
        self._interrupt = None

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
            open_browser=True,
            interactive=None,
            qr=False,
            renderer=None,
            default_interval=5,
            timeout=30,
            token_store=None):
        """Configure a provider from explicit IdP endpoints.

        ``groups_in_token`` selects the token kind: ``False`` (the default)
        returns the access token; ``True`` selects the ID token.
        It does not modify ``scope``; include ``openid`` explicitly when the
        identity provider requires it to issue an ID token. Unlike
        :meth:`from_questdb` there is no server-advertised default to inherit
        here, so the kind is always chosen explicitly and defaults to ``False``.
        """
        cdef questdb_oidc_builder* builder
        _oidc_validate_bool(groups_in_token, 'groups_in_token', False)
        _oidc_validate_bool(insecure, 'insecure', False)
        _oidc_validate_bool(open_browser, 'open_browser', False)
        _oidc_validate_bool(interactive, 'interactive', True)
        _oidc_validate_bool(qr, 'qr', False)
        builder = questdb_oidc_builder_new()
        if builder == NULL:
            raise MemoryError()
        try:
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
            open_browser=True,
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
        _oidc_validate_bool(open_browser, 'open_browser', False)
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
        cdef object event_user_data
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
        if not questdb_oidc_builder_open_browser(
                builder,
                open_browser is True and not in_ipython_kernel(),
                &err):
            raise _oidc_err_to_py(err)
        if interactive is None:
            interactive = detect_interactive()
        if not questdb_oidc_builder_interactive(
                builder, interactive is True, &err):
            raise _oidc_err_to_py(err)

        if (not isinstance(default_interval, int)
                or isinstance(default_interval, bool)
                or default_interval <= 0
                or default_interval > 0xffffffffffffffff):
            raise OidcConfigError(
                'default_interval must be a positive integer number of seconds')
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
        event_user_data = PyWeakref_NewRef(self, None)
        Py_INCREF(event_user_data)
        if not questdb_oidc_builder_event_handler(
                builder,
                _oidc_event_trampoline,
                <void*>event_user_data,
                _oidc_user_data_release_trampoline,
                &err):
            Py_DECREF(event_user_data)
            raise _oidc_err_to_py(err)
        # Native callback state owns only the extra reference to this weakref.
        # The provider owns the renderer, keeping provider/renderer cycles fully
        # visible to Python's cyclic GC. Attached transports retain the provider.
        _ensure_doesnt_have_gil(&gs)
        self._raw = questdb_oidc_builder_build(builder, &err)
        _ensure_has_gil(&gs)
        if self._raw == NULL:
            raise _oidc_err_to_py(err)

    def sign_in(self):
        """Run interactive sign-in if no cached or refreshable token exists.

        ``Ctrl-C`` during the wait cancels the flow and raises
        ``KeyboardInterrupt``. Cancelling closes the provider, so build a new
        one to try again.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        self._require_open()
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

    def token(self):
        """Return a cached or silently refreshed token; never prompt."""
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
            return PyUnicode_FromStringAndSize(data, <Py_ssize_t>length)
        finally:
            questdb_oidc_token_free(token)

    def headers(self):
        """Return an HTTP Authorization header containing the current token."""
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
        running on another thread is asked to stop. The call waits for that
        operation to leave the native authentication critical section. Cloned
        native handles retained by attached transports share the closed state.
        Idempotent.
        """
        cdef questdb_error* err = NULL
        cdef bint ok
        cdef PyThreadState* gs = NULL
        if self._raw == NULL or self._closed:
            self._closed = True
            return
        _ensure_doesnt_have_gil(&gs)
        ok = questdb_oidc_auth_close(self._raw, &err)
        _ensure_has_gil(&gs)
        if not ok:
            raise _oidc_err_to_py(err)
        self._closed = True

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

    def __dealloc__(self):
        if self._raw != NULL:
            questdb_oidc_auth_free(self._raw)
            self._raw = NULL


cdef object _oidc_provider_from_user_data(void* user_data):
    cdef PyObject* provider_obj = NULL
    cdef int provider_state
    try:
        provider_state = PyWeakref_GetRef(
            <object>user_data, &provider_obj)
        if provider_state <= 0:
            if provider_state < 0:
                PyErr_Clear()
            return None
        return <object>provider_obj
    finally:
        Py_XDECREF(provider_obj)
