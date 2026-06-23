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

"""The OAuth 2.0 device authorization grant (RFC 8628) token manager."""

from __future__ import annotations

import base64
import binascii
import json
import threading
import time
import webbrowser
from dataclasses import replace
from typing import Any, Dict, Optional

from ._cache import TokenSet, make_cache
from ._discovery import OidcConfig, resolve_config, validate_endpoint_origins
from ._errors import (
    OidcConfigError,
    OidcDeviceFlowError,
    OidcError,
    OidcInteractionRequired,
    OidcNetworkError,
    OidcTimeoutError,
)
from ._http import build_ssl_context, post_form, safe_urlparse
from ._render import (
    Renderer,
    _safe_link_url,
    detect_interactive,
    in_ipython_kernel,
    make_renderer,
)

DEVICE_CODE_GRANT = 'urn:ietf:params:oauth:grant-type:device_code'
REFRESH_GRANT = 'refresh_token'

# A non-positive expires_in is non-conformant; treat it as "unknown".
_DEFAULT_EXPIRES_IN = 3600

# Clamp the device-authorization timing fields (RFC 8628): a hostile/buggy
# response must not time the flow out before its first poll, pin the polling
# thread (which holds the acquisition lock) in one huge sleep, or keep the loop
# (and lock) alive indefinitely.
_DEFAULT_DEVICE_CODE_LIFETIME = 600   # expires_in fallback (absent/invalid/<=0)
_MAX_DEVICE_CODE_LIFETIME = 1800      # cap on how long we keep polling
_MIN_POLL_INTERVAL = 5                # floor on the poll interval (RFC 8628 default)
_MAX_POLL_INTERVAL = 60               # cap on the poll interval (incl. slow_down)


class _SystemClock:
    """Real time source; the default for :class:`OidcDeviceAuth`."""
    sleep = staticmethod(time.sleep)
    monotonic = staticmethod(time.monotonic)
    now = staticmethod(time.time)


_SYSTEM_CLOCK = _SystemClock()


def _decode_jwt_claims(token: Optional[str]) -> Dict[str, Any]:
    """
    Best-effort decode of a JWT payload **without signature verification**.

    Used only to show a friendly identity in the sign-in message; QuestDB does
    the real validation. Returns ``{}`` for opaque/invalid tokens.
    """
    if not token or token.count('.') < 2:
        return {}
    try:
        payload = token.split('.')[1]
        payload += '=' * (-len(payload) % 4)  # restore base64 padding
        raw = base64.urlsafe_b64decode(payload.encode('ascii'))
        claims = json.loads(raw)
        return claims if isinstance(claims, dict) else {}
    except (ValueError, binascii.Error, UnicodeDecodeError, RecursionError):
        # RecursionError (deeply-nested JSON exhausts the decoder stack) isn't a
        # ValueError, so list it explicitly: a hostile token must not crash
        # token()/refresh here.
        return {}


def _identity_from_claims(claims: Dict[str, Any]) -> Optional[str]:
    for key in ('email', 'preferred_username', 'upn', 'name', 'sub'):
        value = claims.get(key)
        if value:
            return str(value)
    return None


def _http_status_is_terminal_4xx(status: Optional[int]) -> bool:
    """
    True for a 4xx that is a definitive rejection.

    A non-JSON body with such a status (e.g. an HTML ``403`` from a WAF/proxy or
    non-conformant IdP) is never an ``authorization_pending`` / ``slow_down``
    (those are always JSON), so the poll must fail fast rather than retry to a
    misleading "code expired". ``429`` is excluded — it's a transient rate-limit.
    """
    return status is not None and 400 <= status < 500 and status != 429


def _http_status_is_transient(status: Optional[int]) -> bool:
    """True for a server-side (5xx) or rate-limit (429) status worth retrying."""
    return status is not None and (status >= 500 or status == 429)


class OidcDeviceAuth:
    """
    Acquire and refresh an OIDC token via the device authorization grant.

    The token is presented to QuestDB over the auth paths it already supports:
    HTTP ``Authorization: Bearer`` or PG-wire ``_sso`` (token as password). The
    flow runs entirely client-side; QuestDB is never in the acquisition path.

    Most users only call :meth:`token` (or :meth:`headers`). The first call runs
    the interactive device flow; later calls return the cached token, refreshing
    it silently and synchronously once it nears expiry (no background thread).
    Acquisition is serialized so concurrent callers don't double-prompt, while a
    valid cached token is returned without blocking on another's sign-in.

    **Concurrency note.** The lock is held for a whole interactive sign-in (up
    to the device-code lifetime, ~30 min): a caller with a *valid* cached token
    never blocks, but one whose token is missing/expired waits behind the
    signer. So when threads share an auth object (e.g. a SQLAlchemy/psycopg
    pool), sign in once up front — call :meth:`token` once on the main thread
    before the pool opens connections.

    .. code-block:: python

        from questdb.auth import OidcDeviceAuth

        # Discover everything from the QuestDB server:
        auth = OidcDeviceAuth.from_questdb("https://questdb.example.com:9000")
        token = auth.token()        # device flow on first use, else cached

    Or fully explicit (no server discovery):

    .. code-block:: python

        auth = OidcDeviceAuth(
            client_id="questdb",
            device_authorization_endpoint="https://idp/.../device",
            token_endpoint="https://idp/.../token",
            scope="openid groups",
            groups_in_token=True,
            audience="questdb",
            cache="memory")
    """

    def __init__(
            self,
            client_id: str,
            device_authorization_endpoint: str,
            token_endpoint: str,
            *,
            scope: str = 'openid',
            groups_in_token: bool = False,
            audience: Optional[str] = None,
            issuer: Optional[str] = None,
            cache: Any = 'memory',
            insecure: bool = False,
            ca_bundle: Optional[str] = None,
            open_browser: bool = True,
            interactive: Optional[bool] = None,
            qr: bool = False,
            renderer: Optional[Renderer] = None,
            default_interval: int = 5,
            timeout: float = 30,
            _clock=None):  # injectable time source for testing
        if not client_id:
            raise OidcConfigError('client_id is required')
        if not device_authorization_endpoint:
            raise OidcConfigError('device_authorization_endpoint is required')
        if not token_endpoint:
            raise OidcConfigError('token_endpoint is required')

        # Sending the id_token requires the ``openid`` scope.
        if groups_in_token and 'openid' not in scope.split():
            scope = ('openid ' + scope).strip()

        self.config = OidcConfig(
            client_id=client_id,
            token_endpoint=token_endpoint,
            device_authorization_endpoint=device_authorization_endpoint,
            scope=scope,
            groups_in_token=groups_in_token,
            audience=audience,
            issuer=issuer)

        # Enforce the credential-endpoint co-location / issuer pin here too (not
        # just on the discovery path), so the guarantee holds for this
        # constructor as well.
        validate_endpoint_origins(
            self.config.token_endpoint,
            self.config.device_authorization_endpoint,
            self.config.issuer)

        # `insecure` permits plaintext http only to QuestDB (e.g. local dev).
        # _idp_post always holds the IdP to https (or loopback http), so the
        # device code / refresh token are never sent in cleartext even when set.
        self.insecure = insecure
        self.open_browser = open_browser
        # Kept so adapters with their own transport (QuestDB.sender's ILP Sender)
        # can forward the same private CA as _ctx rather than the default roots.
        self._ca_bundle = ca_bundle
        self._interactive = interactive
        self._default_interval = default_interval
        # Per-request network timeout for every IdP call (device-code, each poll,
        # refresh). Bounds how long one network leg pins the acquisition lock if
        # the IdP stalls; the total poll duration is separately capped by
        # _MAX_DEVICE_CODE_LIFETIME.
        self._timeout = timeout
        self._cache = make_cache(cache)
        self._ctx = build_ssl_context(ca_bundle)
        self._renderer = renderer if renderer is not None else make_renderer(qr=qr)
        # Serializes token *acquisition* (silent refresh or interactive sign-in)
        # only. Without it, threaded SQLAlchemy/psycopg connections opening as
        # the token expires would run overlapping refreshes — and with
        # refresh-token rotation all but one would fail and re-prompt. NOT held
        # on the fast path, so a valid cached token never blocks behind a
        # sign-in.
        self._lock = threading.Lock()
        self._tokens: Optional[TokenSet] = None
        clock = _clock or _SYSTEM_CLOCK
        self._sleep = clock.sleep
        self._monotonic = clock.monotonic
        self._now = clock.now

    # -- construction -------------------------------------------------------

    @classmethod
    def from_questdb(
            cls,
            url: str,
            *,
            client_id: Optional[str] = None,
            scope: Optional[str] = None,
            audience: Optional[str] = None,
            groups_in_token: Optional[bool] = None,
            issuer: Optional[str] = None,
            discovery_url: Optional[str] = None,
            token_endpoint: Optional[str] = None,
            device_authorization_endpoint: Optional[str] = None,
            cache: Any = 'memory',
            insecure: bool = False,
            ca_bundle: Optional[str] = None,
            open_browser: bool = True,
            interactive: Optional[bool] = None,
            qr: bool = False,
            renderer: Optional[Renderer] = None,
            default_interval: int = 5,
            timeout: float = 30,
            _clock=None) -> 'OidcDeviceAuth':  # injectable time source
        """
        Build an :class:`OidcDeviceAuth` by discovering config from QuestDB.

        Reads ``{url}/settings`` for the OIDC client id, scope, endpoints and
        groups mode, falling back to the IdP ``.well-known`` document for the
        device-authorization endpoint when QuestDB doesn't advertise it. Any
        explicit keyword overrides discovery.
        """
        ctx = build_ssl_context(ca_bundle)
        cfg = resolve_config(
            questdb_url=url,
            client_id=client_id,
            scope=scope,
            audience=audience,
            groups_in_token=groups_in_token,
            token_endpoint=token_endpoint,
            device_authorization_endpoint=device_authorization_endpoint,
            issuer=issuer,
            discovery_url=discovery_url,
            ctx=ctx,
            insecure=insecure,
            timeout=timeout)
        return cls(
            client_id=cfg.client_id,
            device_authorization_endpoint=cfg.device_authorization_endpoint,
            token_endpoint=cfg.token_endpoint,
            scope=cfg.scope,
            groups_in_token=cfg.groups_in_token,
            audience=cfg.audience,
            issuer=cfg.issuer,
            cache=cache,
            insecure=insecure,
            ca_bundle=ca_bundle,
            open_browser=open_browser,
            interactive=interactive,
            qr=qr,
            renderer=renderer,
            default_interval=default_interval,
            timeout=timeout,
            _clock=_clock)

    # -- public API ---------------------------------------------------------

    def token(self) -> str:
        """
        Return a valid token for QuestDB, acquiring or refreshing as needed.

        Returns the ``id_token`` when the server expects groups encoded in the
        token (``acl.oidc.groups.encoded.in.token=true``), else the
        ``access_token`` — mirroring QuestDB's own selection logic.
        """
        return self._select(self._obtain_tokens())

    def headers(self) -> Dict[str, str]:
        """Return ``{"Authorization": "Bearer <token>"}``."""
        return {'Authorization': f'Bearer {self.token()}'}

    @property
    def cache_key(self) -> str:
        """
        Identifies the token's security context for caching.

        Two sessions share a cached token only when they'd accept the same one:
        same IdP token endpoint (**path included**, so multi-tenant realms on one
        host don't collide), client id, scope *set* (order-insensitive),
        audience, and token-kind mode (``groups_in_token`` — id_token vs
        access_token). The QuestDB URL is excluded — the same IdP token is valid
        against any QuestDB that trusts it.

        ``groups_in_token`` is keyed because it selects the token kind
        :meth:`_select` returns; otherwise two sessions differing only in that
        mode would collide and repeatedly evict each other's token (self-
        correcting, but at the cost of avoidable refreshes / re-prompts).
        """
        c = self.config
        scope = ' '.join(sorted(c.scope.split())) if c.scope else ''
        return '\x1f'.join([
            c.issuer or '',
            _normalize_url(c.token_endpoint),
            c.client_id,
            scope,
            c.audience or '',
            'groups' if c.groups_in_token else 'access'])

    def clear(self) -> None:
        """Forget the cached token (forces a fresh sign-in next time)."""
        # self._lock serializes against THIS instance's acquisition; the shared
        # MemoryCache also bumps a per-key generation, so an in-flight acquire on
        # ANOTHER instance sharing the process-global store can't repopulate the
        # entry (its _store sees the bumped generation and drops the write).
        # Resets the local/process cache only — does not revoke at the IdP.
        with self._lock:
            self._tokens = None
            self._cache.clear(self.cache_key)

    # -- token lifecycle ----------------------------------------------------

    def _select(self, tokens: TokenSet) -> str:
        if self.config.groups_in_token:
            if not tokens.id_token:
                raise OidcConfigError(
                    'Server expects groups encoded in the token but the IdP '
                    'returned no id_token. Ensure the "openid" scope is '
                    'requested (current scope: '
                    f'{self.config.scope!r}).')
            return tokens.id_token
        if not tokens.access_token:
            raise OidcConfigError('IdP returned no access_token.')
        return tokens.access_token

    def _has_required_token(self, tokens: TokenSet) -> bool:
        """
        True if ``tokens`` carries the kind :meth:`_select` will return (the
        ``id_token`` in groups mode, else the ``access_token``). The cache gate
        and post-refresh check share this predicate so they can't disagree with
        ``_select``.
        """
        if self.config.groups_in_token:
            return bool(tokens.id_token)
        return bool(tokens.access_token)

    def _missing_required_token_error(self) -> OidcDeviceFlowError:
        """
        Terminal error for a *completed* grant whose response omits the kind
        :meth:`_select` needs. Mirrors :meth:`_select`'s diagnostics but as an
        :class:`OidcDeviceFlowError`, so the poll can raise it without first
        caching an unusable response.
        """
        if self.config.groups_in_token:
            return OidcDeviceFlowError(
                'Device authorization completed but the IdP returned no '
                'id_token, which this server requires (it expects groups '
                'encoded in the token). Ensure the "openid" scope is requested '
                f'(current scope: {self.config.scope!r}).')
        return OidcDeviceFlowError(
            'Device authorization completed but the IdP returned no '
            'access_token.')

    def _obtain_tokens(self) -> TokenSet:
        # Fast path: return a valid token without the lock, so a caller with a
        # usable token never blocks behind another thread's refresh/sign-in.
        # READ-ONLY — never writes self._tokens; every write to that field is
        # under the lock (the promotion below, _store, clear), so this lock-free
        # reader can't race a write or resurrect a just-cleared token.
        tokens = self._valid_cached()
        if tokens is not None:
            return tokens
        # Slow path: serialize acquisition so concurrent callers don't overlap
        # refreshes or double-prompt; the loser re-checks and reuses the
        # winner's token.
        with self._lock:
            # Capture the generation before reading/acquiring, so a racing
            # clear() — including on another instance sharing the process-global
            # MemoryCache (whose per-instance lock doesn't serialize against
            # ours) — invalidates the store below instead of resurrecting the
            # cleared entry.
            generation = self._cache_generation()
            # Promote a cached token under the lock (even expired, so _acquire
            # can reuse its refresh_token). Here, not on the fast path, so every
            # write to self._tokens stays serialized.
            if self._tokens is None:
                cached = self._cache.load(self.cache_key)
                if cached is not None:
                    self._tokens = cached
            tokens = self._valid_cached()
            if tokens is not None:
                return tokens
            return self._acquire(generation)

    def _valid_cached(self) -> Optional[TokenSet]:
        # Read-only: reads the published field, falling back to the shared cache
        # backend. Never writes self._tokens (that's lock-only), so it's safe on
        # the lock-free fast path.
        tokens = self._tokens
        if tokens is None:
            tokens = self._cache.load(self.cache_key)
        if (tokens is not None and tokens.is_valid(self._now())
                and self._has_required_token(tokens)):
            return tokens
        return None

    def _acquire(self, generation: int) -> TokenSet:
        # Holds self._lock. Try a silent refresh, else run the device flow.
        # `generation` was captured before the cache read in _obtain_tokens;
        # _store drops its write if a concurrent clear() bumped it since.
        tokens = self._tokens
        if tokens is not None and tokens.refresh_token:
            try:
                refreshed = self._refresh(tokens)
            except OidcNetworkError:
                # Transient: the refresh token is still valid, so the interactive
                # flow (same network) wouldn't help and would needlessly
                # re-prompt. Surface it; the cached token is kept for a retry.
                raise
            except OidcError:
                # Refresh token rejected (expired/revoked) or unusable response:
                # fall through to a fresh interactive sign-in.
                pass
            else:
                # Accept only a refresh that yields the kind we need: some IdPs
                # don't re-issue the id_token on refresh, so fall through rather
                # than cache an unusable response and loop on every call.
                if self._has_required_token(refreshed):
                    self._store(refreshed, generation)
                    return refreshed

        fresh = self._run_device_flow()
        self._store(fresh, generation)
        return fresh

    def _store(self, tokens: TokenSet, generation: int) -> None:
        # self._tokens is this instance's own view, so always set it (the caller
        # uses what it just acquired). The shared-cache write is conditional: a
        # clear() (here or on another instance sharing the store) that bumped the
        # generation drops the write, so clear() isn't silently undone. Backends
        # without generation support (NullCache / custom TokenCache) store
        # unconditionally.
        self._tokens = tokens
        store_if_current = getattr(self._cache, 'store_if_current', None)
        if store_if_current is not None:
            store_if_current(self.cache_key, tokens, generation)
        else:
            self._cache.store(self.cache_key, tokens)

    def _cache_generation(self) -> int:
        # MemoryCache tracks a per-key clear()-generation for the cross-instance
        # CAS in _store; other backends don't, so default to 0 (unconditional
        # store).
        generation = getattr(self._cache, 'generation', None)
        return generation(self.cache_key) if generation is not None else 0

    def _tokenset_from_response(self, body: Dict[str, Any]) -> TokenSet:
        try:
            expires_in = int(body.get('expires_in', _DEFAULT_EXPIRES_IN))
        except (TypeError, ValueError, OverflowError):
            # OverflowError: a JSON Infinity (json.loads accepts it) → int(inf)
            # isn't a ValueError, so list it to keep the typed contract.
            expires_in = _DEFAULT_EXPIRES_IN
        if expires_in <= 0:
            # A non-positive lifetime marks a just-issued token as expired,
            # causing refresh/re-prompt churn. Treat it as unknown.
            expires_in = _DEFAULT_EXPIRES_IN
        claims = (_decode_jwt_claims(body.get('id_token'))
                  or _decode_jwt_claims(body.get('access_token')))
        now = self._now()
        return TokenSet(
            access_token=body.get('access_token'),
            id_token=body.get('id_token'),
            refresh_token=body.get('refresh_token'),
            expires_at=now + expires_in,
            issued_at=now,
            token_type=body.get('token_type', 'Bearer'),
            scope=body.get('scope', self.config.scope),
            sub=claims.get('sub'))

    def _idp_post(self, url: str, form: Dict[str, Any]):
        # IdP POSTs carry the device code / refresh token, so always https
        # (loopback http is fine for local dev); the user's `insecure` flag (the
        # QuestDB link) never downgrades them. The timeout bounds how long this
        # leg can hold the acquisition lock if the IdP stalls.
        return post_form(
            url, form, ctx=self._ctx, insecure=False, timeout=self._timeout)

    def _refresh(self, tokens: TokenSet) -> TokenSet:
        try:
            status, body = self._idp_post(
                self.config.token_endpoint,
                {
                    'grant_type': REFRESH_GRANT,
                    'refresh_token': tokens.refresh_token,
                    'client_id': self.config.client_id,
                    'scope': self.config.scope,
                    # Re-send the audience (mirroring the device-authorization
                    # request): some IdPs (e.g. Auth0) need it to keep the
                    # rotated token's `aud`, else they mint one QuestDB rejects
                    # only after a silent refresh. Others ignore it; post_form
                    # drops it when audience is None.
                    'audience': self.config.audience,
                })
        except OidcNetworkError:
            # Already transient (socket drop / DNS / timeout): propagate so
            # _acquire keeps the still-valid refresh token and retries later.
            raise
        except OidcError as e:
            # Non-JSON HTTP error body (e.g. an HTML 5xx from a proxy). 5xx/429
            # is transient → re-raise as a network error so _acquire keeps the
            # refresh token; a 4xx is a genuine rejection, so let it fall through
            # to a fresh interactive sign-in.
            if _http_status_is_transient(getattr(e, 'status', None)):
                raise OidcNetworkError(str(e)) from e
            raise
        if status == 200:
            refreshed = self._tokenset_from_response(body)
            # Many IdPs don't rotate the refresh token; keep the old one.
            # TokenSet is frozen, so derive a copy.
            if not refreshed.refresh_token:
                refreshed = replace(
                    refreshed, refresh_token=tokens.refresh_token)
            return refreshed
        # A transient 5xx/429 during a silent refresh must not tear down the
        # session: the refresh token is still valid, so surface it as a network
        # error for _acquire to retry — matching the poll loop. Only a genuine
        # rejection (expired/revoked token, 4xx invalid_grant) falls through to a
        # fresh sign-in.
        if _http_status_is_transient(status):
            raise OidcNetworkError(
                f'Token refresh hit a transient IdP error (HTTP {status}); '
                'the refresh token is still valid — retry later.')
        raise OidcDeviceFlowError(
            f"Token refresh failed: {body.get('error', 'unknown error')}",
            error=body.get('error'),
            error_description=body.get('error_description'))

    # -- device flow (RFC 8628) ---------------------------------------------

    def _run_device_flow(self) -> TokenSet:
        if not self._is_interactive():
            raise OidcInteractionRequired(
                'Interactive sign-in is required, but no interactive terminal '
                'or notebook was detected (e.g. papermill / cron / CI). Use a '
                'QuestDB service-account REST token or the OAuth2 '
                'client-credentials grant for non-interactive contexts.')

        resp = self._request_device_code()
        self._renderer.on_prompt(resp)
        self._maybe_open_browser(resp)
        tokens = self._poll_for_token(resp)
        claims = (_decode_jwt_claims(tokens.id_token)
                  or _decode_jwt_claims(tokens.access_token))
        identity = _identity_from_claims(claims)
        self._renderer.on_success(
            identity, max(0.0, tokens.expires_at - self._now()))
        return tokens

    def _request_device_code(self) -> Dict[str, Any]:
        form = {
            'client_id': self.config.client_id,
            'scope': self.config.scope,
        }
        if self.config.audience:
            form['audience'] = self.config.audience
        status, body = self._idp_post(
            self.config.device_authorization_endpoint, form)
        if status == 200 and body.get('device_code') and body.get('user_code'):
            return body
        error = body.get('error')
        if status == 200:
            # 200 but the guard above failed: device_code/user_code missing.
            # A non-conformant body, not an HTTP failure — say so plainly rather
            # than a contradictory "failed (HTTP 200)".
            raise OidcDeviceFlowError(
                'The IdP returned a 200 device-authorization response that is '
                'missing the required "device_code"/"user_code" fields; cannot '
                'start the device flow.',
                error=error,
                error_description=body.get('error_description'))
        if status in (400, 404, 405) or error in (
                'invalid_client', 'unauthorized_client',
                'unsupported_grant_type'):
            raise OidcDeviceFlowError(
                'The IdP rejected the device-authorization request '
                f'(HTTP {status}, error={error!r}). Ensure the OIDC client '
                f'{self.config.client_id!r} has the device grant '
                "('urn:ietf:params:oauth:grant-type:device_code') enabled and "
                'is registered as a public client.',
                error=error,
                error_description=body.get('error_description'))
        raise OidcDeviceFlowError(
            f'Device authorization request failed (HTTP {status}): '
            f'{body.get("error_description") or error or body}',
            error=error,
            error_description=body.get('error_description'))

    def _poll_for_token(self, resp: Dict[str, Any]) -> TokenSet:
        device_code = resp['device_code']
        try:
            interval = int(resp.get('interval', self._default_interval))
        except (TypeError, ValueError, OverflowError):
            interval = self._default_interval
        # Floor at the RFC 8628 default (5s) so we never poll faster than the
        # spec baseline; cap so a hostile value can't pin the polling thread
        # (which holds the lock) in one enormous sleep.
        interval = min(_MAX_POLL_INTERVAL, max(_MIN_POLL_INTERVAL, interval))
        try:
            expires_in = int(resp.get('expires_in', _DEFAULT_DEVICE_CODE_LIFETIME))
        except (TypeError, ValueError, OverflowError):
            expires_in = _DEFAULT_DEVICE_CODE_LIFETIME
        # A non-positive lifetime would time out before the first poll (the code
        # is already shown); treat it as unknown. Cap the upper end so a hostile
        # value can't keep the loop — and the lock — alive indefinitely.
        if expires_in <= 0:
            expires_in = _DEFAULT_DEVICE_CODE_LIFETIME
        expires_in = min(expires_in, _MAX_DEVICE_CODE_LIFETIME)
        deadline = self._monotonic() + expires_in

        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                self._renderer.on_failure(
                    'Code expired — run the cell again to retry.')
                raise OidcTimeoutError(
                    'The device code expired before authorization completed. '
                    'Run the sign-in again.',
                    error='expired_token')
            self._renderer.on_waiting(remaining)
            # Never sleep past the deadline (remaining > 0 here).
            self._sleep(min(interval, remaining))

            try:
                status, body = self._idp_post(
                    self.config.token_endpoint,
                    {
                        'grant_type': DEVICE_CODE_GRANT,
                        'device_code': device_code,
                        'client_id': self.config.client_id,
                    })
            except OidcError as e:
                # A non-JSON 4xx is a terminal rejection (e.g. an HTML error page
                # from a WAF/proxy, or a non-conformant IdP): a conformant OAuth
                # error is JSON, so it can never be authorization_pending /
                # slow_down. Fail fast instead of polling on to "code expired".
                if _http_status_is_terminal_4xx(getattr(e, 'status', None)):
                    self._renderer.on_failure(
                        'Sign-in failed: the identity provider rejected the '
                        'request.')
                    raise OidcDeviceFlowError(
                        f'Device flow failed: the IdP rejected the token '
                        f'request ({e}).') from e
                # Otherwise transient: a dropped connection / DNS blip / timeout
                # (OidcNetworkError) or a non-JSON 5xx/429 from a proxy (bare
                # OidcError). The user may already have authorized, and RFC 8628
                # §3.4 expects polling to continue until the code expires, so
                # poll again rather than discard the sign-in (the deadline bounds
                # the total wait; a genuine JSON rejection arrives below).
                if getattr(e, 'status', None) == 429:
                    interval = min(_MAX_POLL_INTERVAL, interval + 5)
                continue

            if status == 200:
                # The RFC 6749 §5.1 token response: the grant completed. Accept
                # it only if it carries the kind _select hands to QuestDB, using
                # the same predicate as the cache gate and post-refresh check so
                # the three can't disagree.
                tokens = self._tokenset_from_response(body)
                if self._has_required_token(tokens):
                    return tokens
                # Grant completed but the required kind is absent: a stable
                # misconfiguration, not a transient poll state. Raise a terminal
                # error rather than cache an unusable token and silently re-run
                # the whole flow on every later token() call.
                self._renderer.on_failure(
                    'Sign-in failed: the identity provider did not return the '
                    'token this server requires.')
                raise self._missing_required_token_error()

            # A 5xx/429 with a JSON body is also transient (server error or
            # rate-limit), not a terminal rejection: back off on 429 and keep
            # polling until the deadline, as above.
            if status >= 500 or status == 429:
                if status == 429:
                    interval = min(_MAX_POLL_INTERVAL, interval + 5)
                continue

            error = body.get('error')
            if error == 'authorization_pending':
                continue
            if error == 'slow_down':
                interval = min(_MAX_POLL_INTERVAL, interval + 5)
                continue
            if error == 'expired_token':
                self._renderer.on_failure(
                    'Code expired — run the cell again to retry.')
                raise OidcTimeoutError(
                    'The device code expired before authorization completed. '
                    'Run the sign-in again.',
                    error=error)
            # access_denied or any other terminal error.
            description = body.get('error_description') or error or 'unknown error'
            self._renderer.on_failure(f'Sign-in failed: {description}')
            raise OidcDeviceFlowError(
                f'Device flow failed: {description}',
                error=error,
                error_description=body.get('error_description'))

    # -- helpers ------------------------------------------------------------

    def _is_interactive(self) -> bool:
        if self._interactive is not None:
            return self._interactive
        return detect_interactive()

    def _maybe_open_browser(self, resp: Dict[str, Any]) -> None:
        # Open on a local terminal by default; never on a (possibly remote)
        # notebook kernel, where the prompt is already a clickable link and the
        # kernel host isn't the user's machine. Suppress with open_browser=False.
        if not self.open_browser or in_ipython_kernel():
            return
        # Only http(s) — never a javascript:/data: scheme from a malicious or
        # MITM'd device response.
        target = _safe_link_url(
            resp.get('verification_uri_complete')
            or resp.get('verification_uri')
            or resp.get('verification_url'))
        if target:
            try:
                webbrowser.open(target)
            except Exception:
                pass


def _normalize_url(url: str) -> str:
    # Full URL with scheme/host lower-cased and default port dropped, but path
    # kept (it distinguishes multi-tenant realms). Used for the cache key so
    # trivial spelling differences don't cause a spurious re-prompt.
    parts, port = safe_urlparse(url)
    scheme = (parts.scheme or '').lower()
    host = (parts.hostname or '').lower()
    default_port = {'https': 443, 'http': 80}.get(scheme)
    if port and port != default_port:
        netloc = f'{host}:{port}'
    else:
        netloc = host
    query = f'?{parts.query}' if parts.query else ''
    return f'{scheme}://{netloc}{parts.path}{query}'
