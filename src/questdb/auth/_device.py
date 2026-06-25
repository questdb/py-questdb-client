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
import math
import threading
import time
import webbrowser
from dataclasses import replace
from typing import Any, Dict, Optional

from ._cache import MemoryCache, TokenSet
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
    _safe_target,
    _verification_uri,
    _verification_uri_complete,
    detect_interactive,
    in_ipython_kernel,
    make_renderer,
)

DEVICE_CODE_GRANT = 'urn:ietf:params:oauth:grant-type:device_code'
REFRESH_GRANT = 'refresh_token'

# Clamp the token lifetime (access/id-token TTL) the same way as the Java
# client. An absent or non-positive expires_in is non-conformant; fall back to a
# short, conservative lifetime so a token with no stated lifetime is refreshed
# promptly. A very long (or hostile) IdP-stated lifetime is capped so a cached
# token is re-validated at least hourly.
_DEFAULT_EXPIRES_IN = 300   # token TTL fallback (absent/invalid/<=0)
_MAX_EXPIRES_IN = 3600      # cap on the token TTL

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


def _str_or_none(value: Any) -> Optional[str]:
    """
    A credential/token field from an untrusted JSON response as a ``str``, else
    ``None``.

    A non-string token (a JSON number/bool/object from a buggy or hostile IdP)
    reads as absent so it is never stored, sent on a refresh, or emitted as
    ``Bearer <non-str>`` — and can't crash the best-effort JWT decode. A missing
    required kind then raises the clear terminal error rather than caching an
    unusable token.
    """
    return value if isinstance(value, str) else None


def _int_or_default(value: Any, default: int) -> int:
    """
    ``int(value)`` for an untrusted numeric field (``expires_in`` / ``interval``),
    else ``default``.

    ``bool`` is an ``int`` subclass, but a JSON ``true``/``false`` is never a
    meaningful duration: ``int(True) == 1`` would mint a 1-second lifetime and
    churn refreshes, so map a bool to the default. A missing key (``None``), a
    non-numeric string, or a JSON ``Infinity``/``NaN`` (``json.loads`` accepts
    both) raises ``TypeError``/``ValueError``/``OverflowError`` from ``int()``
    and falls back too, keeping the typed contract.
    """
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _decode_jwt_claims(token: Optional[str]) -> Dict[str, Any]:
    """
    Best-effort decode of a JWT payload **without signature verification**.

    Used only to show a friendly identity in the sign-in message; QuestDB does
    the real validation. Returns ``{}`` for opaque/invalid or non-string tokens.
    """
    if not isinstance(token, str) or token.count('.') < 2:
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


def _http_status_is_terminal(status: Optional[int]) -> bool:
    """
    True for an HTTP status that is a definitive rejection, not a transient poll
    state worth retrying.

    A conformant token-endpoint poll reply is JSON (a 200 success body, or a 4xx
    whose JSON body carries ``authorization_pending`` / ``slow_down``). A NON-JSON
    body — which makes ``post_form`` raise with the status attached — therefore
    means a proxy / WAF / non-conformant IdP, never a poll state. Any such status
    that is not a transient 5xx/429 (and not a bare network error, which carries
    no status) is terminal, so the poll fails fast instead of retrying to a
    misleading "code expired". This covers a non-JSON ``3xx`` redirect (these
    endpoints never legitimately redirect, and ``_NoRedirect`` refuses to follow
    one) and a non-conformant non-JSON ``2xx``, as well as a non-JSON ``4xx``;
    ``429`` stays transient.
    """
    return status is not None and status < 500 and status != 429


def _http_status_is_transient(status: Optional[int]) -> bool:
    """True for a server-side (5xx) or rate-limit (429) status worth retrying."""
    return status is not None and (status >= 500 or status == 429)


def _backoff_interval(interval: int, retry_after: Optional[int]) -> int:
    """
    The next poll interval after a 429 / ``slow_down``.

    Honors a server ``Retry-After`` (delta-seconds) when present, else the RFC
    8628 §3.5 +5s slow-down step. Clamped to ``[_MIN_POLL_INTERVAL,
    _MAX_POLL_INTERVAL]`` so a hostile/huge value can't pin the polling thread —
    the device-code deadline still bounds the total wait.
    """
    target = retry_after if retry_after is not None else interval + 5
    return min(_MAX_POLL_INTERVAL, max(_MIN_POLL_INTERVAL, target))


def _validate_positive_number(value: Any, name: str) -> None:
    """
    Require a duration argument to be a positive, finite number of seconds.

    Mirrors the constructor's other up-front type checks: a value that is
    non-numeric, non-finite, or too large for the platform clock would otherwise
    surface later as a bare ``TypeError``/``OverflowError`` from the poll-interval
    clamp (``max(_MIN_POLL_INTERVAL, default_interval)``) or a urllib socket call
    (``timeout`` -> ``socket.settimeout``), escaping the module's typed-error
    contract. ``bool`` is an ``int`` subclass, so reject it explicitly. ``NaN``
    fails ``> 0``; ``inf`` *passes* ``> 0`` yet overflows ``settimeout``, and a
    too-large ``int`` does too — both are caught by the finite check below (which
    is wrapped because ``math.isfinite`` itself raises ``OverflowError`` on an
    ``int`` too large to convert to ``float``).
    """
    ok = isinstance(value, (int, float)) and not isinstance(value, bool)
    if ok:
        try:
            ok = math.isfinite(value) and value > 0
        except (OverflowError, ValueError):
            # A too-large int: finite in principle but unusable as a timeout
            # (settimeout would raise its own OverflowError later).
            ok = False
    if not ok:
        raise OidcConfigError(
            f'{name} must be a positive, finite number of seconds, '
            f'got {value!r}')


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
    before the pool opens connections. (A custom ``renderer``'s callbacks run
    while this lock is held, so they must not call back into the same instance's
    :meth:`token` / :meth:`clear` — the lock is not reentrant.)

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
            audience="questdb")
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
            insecure: bool = False,
            ca_bundle: Optional[str] = None,
            open_browser: bool = True,
            interactive: Optional[bool] = None,
            qr: bool = False,
            renderer: Optional[Renderer] = None,
            default_interval: int = 5,
            timeout: float = 30,
            _clock=None):  # injectable time source for testing
        # Validate types up front so a bad-typed arg raises the module's typed
        # error, not a bare AttributeError/TypeError surfacing later from
        # scope.split(), safe_urlparse(<non-str>), or the cache-key join.
        # from_questdb is unaffected: resolve_config already returns strings.
        if not isinstance(client_id, str) or not client_id:
            raise OidcConfigError(
                'client_id is required and must be a non-empty string')
        if (not isinstance(device_authorization_endpoint, str)
                or not device_authorization_endpoint):
            raise OidcConfigError(
                'device_authorization_endpoint is required and must be a '
                'non-empty string')
        if not isinstance(token_endpoint, str) or not token_endpoint:
            raise OidcConfigError(
                'token_endpoint is required and must be a non-empty string')
        if not isinstance(scope, str):
            raise OidcConfigError('scope must be a string')
        if audience is not None and not isinstance(audience, str):
            raise OidcConfigError('audience must be a string or None')
        # Normalize an empty audience to None so it is omitted consistently:
        # _request_device_code skips a falsy audience, but _refresh puts it in
        # the form unconditionally and post_form drops only None — so an empty
        # string would be sent as `audience=` on refresh yet not on device-auth.
        if not audience:
            audience = None
        if issuer is not None and not isinstance(issuer, str):
            raise OidcConfigError('issuer must be a string or None')
        # default_interval feeds the poll-interval clamp and timeout every IdP
        # socket call; a non-numeric value would otherwise escape as a bare
        # TypeError rather than the typed error this block exists to raise.
        _validate_positive_number(default_interval, 'default_interval')
        _validate_positive_number(timeout, 'timeout')

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

        # Enforce credential-endpoint co-location here too (not just on the
        # discovery path), so the guarantee holds for this constructor as well.
        # The issuer-ORIGIN pin is provenance-aware and lives in resolve_config
        # (it applies only to endpoints from the untrusted /settings); endpoints
        # reaching this constructor are caller-explicit (authoritative), so a
        # cross-origin issuer — e.g. Google's accounts.google.com issuer with
        # oauth2.googleapis.com endpoints — is intentionally accepted here.
        validate_endpoint_origins(
            self.config.token_endpoint,
            self.config.device_authorization_endpoint)

        # `insecure` permits plaintext http only to QuestDB (e.g. local dev).
        # _idp_post always holds the IdP to https (or loopback http), so the
        # device code / refresh token are never sent in cleartext even when set.
        self.insecure = insecure
        self.open_browser = open_browser
        # Kept so a caller wiring the token into their own transport (e.g. the
        # ingestion Sender) can forward the same private CA as _ctx rather than
        # the default roots.
        self._ca_bundle = ca_bundle
        self._interactive = interactive
        self._default_interval = default_interval
        # Per-request network timeout for every IdP call (device-code, each poll,
        # refresh). Applied to connect+headers and then again to the body read,
        # so one network leg can pin the acquisition lock for up to ~2x this
        # value if the IdP stalls; the total poll duration is separately capped
        # by _MAX_DEVICE_CODE_LIFETIME.
        self._timeout = timeout
        self._cache = MemoryCache()
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
            token_endpoint: Optional[str] = None,
            device_authorization_endpoint: Optional[str] = None,
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

        When the server does not advertise the device-authorization endpoint (so
        it must be discovered from the IdP), ``issuer=`` is **required** to pin
        the identity provider — the helper refuses to derive the discovery origin
        from a server-supplied endpoint, so a tampered ``/settings`` cannot
        redirect the device-code / refresh-token POSTs. See :ref:`oidc_auth`.
        Raises :class:`OidcConfigError` if the configuration can't be resolved.
        """
        # Validate before resolve_config consumes `timeout` on its /settings and
        # discovery HTTP calls (which run before cls() would validate it), so a
        # bad timeout fails fast with the typed error rather than a bare
        # TypeError from urllib.
        _validate_positive_number(default_interval, 'default_interval')
        _validate_positive_number(timeout, 'timeout')
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
        same pinned ``issuer`` (when one is set), IdP token endpoint (**path
        included**, so multi-tenant realms on one host don't collide), client id,
        scope *set* (order-insensitive), audience, and token-kind mode
        (``groups_in_token`` — id_token vs access_token). The QuestDB URL is
        excluded — the same IdP token is valid against any QuestDB that trusts it.

        ``groups_in_token`` is keyed because it selects the token kind
        :meth:`_select` returns; otherwise two sessions differing only in that
        mode would collide and repeatedly evict each other's token (self-
        correcting, but at the cost of avoidable refreshes / re-prompts).
        """
        c = self.config
        scope = ' '.join(sorted(c.scope.split())) if c.scope else ''
        # Normalize the issuer like the token endpoint (lower-case scheme/host,
        # drop a default port) and strip a trailing slash, so a discovered
        # "https://idp/" and an explicit "https://idp" — or a stray :443 / case
        # difference — don't yield different keys and force an avoidable
        # re-prompt. The realm path is kept (multi-tenant issuers differ by it).
        issuer = _normalize_url(c.issuer).rstrip('/') if c.issuer else ''
        return '\x1f'.join([
            issuer,
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
        # under the lock (the promotion below, _store, clear). On the GIL build
        # the single-reference read is atomic and ordered, so it can't see a torn
        # value or race a write. On a free-threaded build the read is
        # intentionally race-TOLERANT, not race-free: a stale None falls through
        # to the locked slow path, and a stale-but-valid (frozen) TokenSet is
        # returned once — never a torn or wrong-context one — and the read writes
        # nothing, so it can't resurrect a cleared entry in the shared cache.
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
            # cleared entry. Paired with release() in the finally so the cache
            # reclaims the per-key generation once no acquisition is in flight
            # for it (bounds the process-global maps; see MemoryCache.release).
            generation = self._cache_generation()
            try:
                # Promote a cached token under the lock (even expired, so
                # _acquire can reuse its refresh_token). Here, not on the fast
                # path, so every write to self._tokens stays serialized.
                if self._tokens is None:
                    cached = self._cache.load(self.cache_key)
                    if cached is not None:
                        self._tokens = cached
                tokens = self._valid_cached()
                if tokens is not None:
                    return tokens
                return self._acquire(generation)
            finally:
                self._cache.release(self.cache_key)

    def _valid_cached(self) -> Optional[TokenSet]:
        # Read-only: reads the published field, falling back to the shared cache
        # backend. Never writes self._tokens (that's lock-only), so it's safe on
        # the lock-free fast path.
        #
        # PRECONDITION for that lock-free safety (preserve both if refactoring):
        #  (a) TokenSet is frozen, so a concurrently-published reference is never
        #      mutated under the reader (no torn read); and
        #  (b) self.config / self.cache_key are set once in __init__ and never
        #      reassigned, so a token published by another thread is always for
        #      THIS instance's security context (never a wrong-context token).
        # (a)/(b) concern the pointed-TO object. The atomicity of the reference
        # READ itself, and the guarantee the object isn't freed between the load
        # below and its use, come from the CPython memory model (an atomic pointer
        # load, plus free-threaded QSBR / deferred ref-counting on a no-GIL build)
        # — NOT from frozen-ness; a non-CPython runtime lacking those would need
        # the lock. If any of this is broken, this read must move under
        # self._lock. Exercised under real contention by
        # TestConcurrency.test_token_clear_stress.
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
            # The refresh path is exhausted: the refresh_token is proven useless
            # (rejected, or the IdP won't re-issue the required kind), so the
            # device flow below is the only way forward. Drop the stale token —
            # from this instance AND the shared cache — before the flow, so that
            # if it then FAILS (non-interactive, user cancels, IdP rejects the
            # device request) the doomed token isn't left cached to be reloaded
            # and re-refreshed fruitlessly on every later token() call. evict()
            # doesn't bump the clear()-generation, so the _store below still
            # lands `fresh` (unless a genuine concurrent clear() intervenes).
            self._tokens = None
            self._cache.evict(self.cache_key)

        fresh = self._run_device_flow()
        self._store(fresh, generation)
        return fresh

    def _store(self, tokens: TokenSet, generation: int) -> None:
        # self._tokens is this instance's own view, so always set it (the caller
        # uses what it just acquired). The shared-cache write is conditional: a
        # clear() (here or on another instance sharing the process-global store)
        # that bumped the generation drops the write, so clear() isn't silently
        # undone.
        self._tokens = tokens
        self._cache.store_if_current(self.cache_key, tokens, generation)

    def _cache_generation(self) -> int:
        # Per-key clear()-generation for the cross-instance CAS in _store.
        return self._cache.generation(self.cache_key)

    def _tokenset_from_response(self, body: Dict[str, Any]) -> TokenSet:
        expires_in = _int_or_default(
            body.get('expires_in'), _DEFAULT_EXPIRES_IN)
        if expires_in <= 0:
            # A non-positive lifetime marks a just-issued token as expired,
            # causing refresh/re-prompt churn. Treat it as unknown.
            expires_in = _DEFAULT_EXPIRES_IN
        # Cap a long (or hostile) IdP-stated lifetime so a cached token is
        # re-validated at least hourly (matches the Java client).
        expires_in = min(expires_in, _MAX_EXPIRES_IN)
        # Coerce the credential fields to str-or-None up front: a non-string
        # token from a buggy/hostile IdP must read as absent rather than be
        # stored, re-sent on a refresh, or emitted as ``Bearer <non-str>`` — and
        # the best-effort JWT decode below must not see a non-string. A missing
        # required kind then raises the clear terminal error (see _select).
        access_token = _str_or_none(body.get('access_token'))
        id_token = _str_or_none(body.get('id_token'))
        refresh_token = _str_or_none(body.get('refresh_token'))
        claims = (_decode_jwt_claims(id_token)
                  or _decode_jwt_claims(access_token))
        now = self._now()
        return TokenSet(
            access_token=access_token,
            id_token=id_token,
            refresh_token=refresh_token,
            expires_at=now + expires_in,
            issued_at=now,
            # Coerce like the credential fields: a non-string token_type/scope
            # from a buggy/hostile IdP falls back to the default rather than
            # landing in the dataclass as a raw object.
            token_type=_str_or_none(body.get('token_type')) or 'Bearer',
            scope=_str_or_none(body.get('scope')) or self.config.scope,
            # Coerce like the credential fields: a non-string sub from a hostile
            # JWT reads as absent rather than landing in an Optional[str] field.
            sub=_str_or_none(claims.get('sub')))

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
            status=status,
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
        self._renderer.on_success(identity, self._display_lifetime(tokens, claims))
        return tokens

    def _display_lifetime(
            self, tokens: TokenSet, claims: Dict[str, Any]) -> float:
        # Remaining lifetime to SHOW in the sign-in message. tokens.expires_at is
        # deliberately clamped to _MAX_EXPIRES_IN so a cached token is
        # re-validated at least hourly — reporting that would under-state a token
        # that genuinely lives longer. Prefer the JWT `exp` claim (the
        # authoritative token expiry); fall back to the clamped value for an
        # opaque token with no exp. Bounded to ~1y so a hostile/garbage exp
        # (incl. inf/nan, which fail the comparison) can't overflow on_success's
        # int(round(...)).
        exp = claims.get('exp')
        if isinstance(exp, (int, float)) and not isinstance(exp, bool):
            remaining = float(exp) - self._now()
            if 0 < remaining <= 366 * 24 * 3600:
                return remaining
        return max(0.0, tokens.expires_at - self._now())

    def _request_device_code(self) -> Dict[str, Any]:
        form = {
            'client_id': self.config.client_id,
            'scope': self.config.scope,
        }
        if self.config.audience:
            form['audience'] = self.config.audience
        status, body = self._idp_post(
            self.config.device_authorization_endpoint, form)
        # RFC 8628 §3.2 requires device_code, user_code AND a verification URI
        # (RFC spells it verification_uri; some IdPs, e.g. older Google, use
        # verification_url). Require the URI too: without it the prompt would
        # render a blank "Open  and enter code" gap, so its absence is a
        # non-conformant response, not a usable one.
        if (status == 200 and _str_or_none(body.get('device_code'))
                and _str_or_none(body.get('user_code'))
                and (_str_or_none(body.get('verification_uri'))
                     or _str_or_none(body.get('verification_url')))):
            return body
        error = body.get('error')
        if status == 200:
            # 200 but the guard above failed: a required field is missing or
            # non-string (coerced via _str_or_none, so a JSON number/list reads
            # as absent instead of being stringified into the prompt / poll
            # request). A non-conformant body, not an HTTP failure — say so
            # plainly rather than a contradictory "failed (HTTP 200)".
            raise OidcDeviceFlowError(
                'The IdP returned a 200 device-authorization response missing a '
                'required field (device_code, user_code, or verification_uri); '
                'cannot start the device flow.',
                status=status,
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
                status=status,
                error=error,
                error_description=body.get('error_description'))
        raise OidcDeviceFlowError(
            f'Device authorization request failed (HTTP {status}): '
            f'{body.get("error_description") or error or body}',
            status=status,
            error=error,
            error_description=body.get('error_description'))

    def _poll_for_token(self, resp: Dict[str, Any]) -> TokenSet:
        device_code = resp['device_code']
        interval = _int_or_default(
            resp.get('interval'), self._default_interval)
        # Floor at the RFC 8628 default (5s) so we never poll faster than the
        # spec baseline; cap so a hostile value can't pin the polling thread
        # (which holds the lock) in one enormous sleep.
        interval = min(_MAX_POLL_INTERVAL, max(_MIN_POLL_INTERVAL, interval))
        expires_in = _int_or_default(
            resp.get('expires_in'), _DEFAULT_DEVICE_CODE_LIFETIME)
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
                result = self._idp_post(
                    self.config.token_endpoint,
                    {
                        'grant_type': DEVICE_CODE_GRANT,
                        'device_code': device_code,
                        'client_id': self.config.client_id,
                    })
            except OidcError as e:
                # A non-JSON, non-transient status is a terminal rejection (an
                # HTML error page or a redirect from a WAF/proxy, or a non-
                # conformant IdP): a conformant OAuth poll reply is JSON, so it
                # can never be authorization_pending / slow_down. Fail fast —
                # including on a 3xx, which these endpoints never legitimately
                # return and _NoRedirect won't follow — instead of polling on to
                # a misleading "code expired".
                if _http_status_is_terminal(getattr(e, 'status', None)):
                    self._renderer.on_failure(
                        'Sign-in failed: the identity provider rejected the '
                        'request.')
                    raise OidcDeviceFlowError(
                        f'Device flow failed: the IdP rejected the token '
                        f'request ({e}).',
                        status=getattr(e, 'status', None)) from e
                # Otherwise transient: a dropped connection / DNS blip / timeout
                # (OidcNetworkError) or a non-JSON 5xx/429 from a proxy (bare
                # OidcError). The user may already have authorized, and RFC 8628
                # §3.4 expects polling to continue until the code expires, so
                # poll again rather than discard the sign-in (the deadline bounds
                # the total wait; a genuine JSON rejection arrives below).
                if getattr(e, 'status', None) == 429:
                    # No JSON body here (a non-JSON 429 from a proxy/WAF), so no
                    # Retry-After is surfaced; fall back to the +5s step.
                    interval = _backoff_interval(interval, None)
                continue

            status, body = result
            # post_form surfaces Retry-After on the JSON path via _PostResult; a
            # mocked _idp_post may return a plain 2-tuple, hence getattr/default.
            retry_after = getattr(result, 'retry_after', None)

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
            # rate-limit), not a terminal rejection: keep polling until the
            # deadline. Honor a Retry-After on either a 429 or a transient 5xx
            # (as _PostResult documents); apply the RFC 8628 §3.5 +5s slow-down
            # step only to a 429 with no header (a generic 5xx is a server error,
            # not a rate-limit, so its cadence is unchanged absent a Retry-After).
            if status >= 500 or status == 429:
                if status == 429 or retry_after is not None:
                    interval = _backoff_interval(interval, retry_after)
                continue

            # A 3xx with a JSON body is still a redirect these endpoints never
            # legitimately return (and _NoRedirect won't follow): treat it as
            # terminal, like the non-JSON 3xx the exception path above rejects, so
            # a proxy/WAF returning a JSON-bodied redirect that happens to carry
            # an OAuth error field can't be mistaken for a live poll state and
            # polled on to a misleading "code expired".
            if 300 <= status < 400:
                self._renderer.on_failure(
                    'Sign-in failed: the identity provider rejected the request.')
                raise OidcDeviceFlowError(
                    'Device flow failed: the IdP returned an unexpected '
                    f'redirect (HTTP {status}).',
                    status=status)

            error = body.get('error')
            if error == 'authorization_pending':
                continue
            if error == 'slow_down':
                interval = _backoff_interval(interval, retry_after)
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
                status=status,
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
        # Open the SAME _strip_control'd, vetted target the prompt shows — not the
        # raw response value — so a char stripped from the on-screen link can't
        # survive into the opened URL, and a javascript:/data: scheme (or
        # userinfo / non-ASCII host) is never opened. Vet each field
        # independently and fall back, exactly as the renderers do (_safe_target
        # per field, complete-then-plain), so a truthy-but-unsafe
        # verification_uri_complete can't shadow a usable verification_uri and
        # make the browser diverge from the displayed link / QR.
        target = (_safe_target(_verification_uri_complete(resp))
                  or _safe_target(_verification_uri(resp)))
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
