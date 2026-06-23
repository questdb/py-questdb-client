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

"""
OIDC configuration discovery.

Resolution order:

1. ``GET {questdb_url}/settings`` (public) -> QuestDB-authoritative
   ``acl.oidc.*`` values (client id, scope, endpoints, groups mode).
2. If QuestDB doesn't advertise the device-authorization endpoint, fall back to
   the IdP discovery document (``{issuer}/.well-known/openid-configuration``).
"""

from __future__ import annotations

import ssl
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ._errors import OidcConfigError
from ._http import get_json, safe_urlparse, _is_loopback

# QuestDB /settings keys (see EntPropServerConfiguration.exportConfiguration()).
_K_ENABLED = 'acl.oidc.enabled'
_K_CLIENT_ID = 'acl.oidc.client.id'
_K_SCOPE = 'acl.oidc.scope'
_K_TOKEN_ENDPOINT = 'acl.oidc.token.endpoint'
_K_AUTHORIZATION_ENDPOINT = 'acl.oidc.authorization.endpoint'
_K_DEVICE_ENDPOINT = 'acl.oidc.device.authorization.endpoint'  # design §7 (new)
_K_GROUPS_IN_TOKEN = 'acl.oidc.groups.encoded.in.token'
_K_AUDIENCE = 'acl.oidc.audience'


@dataclass
class OidcConfig:
    """Resolved OIDC parameters needed to run the device flow."""

    client_id: str
    token_endpoint: str
    device_authorization_endpoint: str
    scope: str = 'openid'
    groups_in_token: bool = False
    audience: Optional[str] = None
    issuer: Optional[str] = None
    authorization_endpoint: Optional[str] = None


def _as_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ('true', '1', 'yes', 'on'):
            return True
        if v in ('false', '0', 'no', 'off', ''):
            return False
    return default


def _str_setting(value: Any) -> Optional[str]:
    """
    A ``/settings`` value as a non-empty string, else ``None``.

    Drops a non-string ``acl.oidc.*`` value (a JSON list/number from a buggy or
    hostile server) so it can't reach ``scope.split()`` / the cache-key join as a
    raw object and escape the typed-error contract with an ``AttributeError`` /
    ``TypeError``. Mirrors :func:`_resolve_endpoint`.
    """
    return value if isinstance(value, str) and value else None


def settings_config(settings: Any) -> Dict[str, Any]:
    """
    Return the trusted config map from a ``/settings`` response.

    Modern QuestDB nests server-authoritative values under ``"config"``,
    alongside a **user-writable** ``"preferences"`` sibling (written via
    ``PUT /settings``). Read only ``"config"`` so a user who can write a
    preference can't smuggle an ``acl.oidc.*`` key (e.g. a redirected
    ``token.endpoint``) into the resolved config. A genuinely flat legacy
    response (no ``config`` / ``preferences`` split) is still tolerated.
    """
    if not isinstance(settings, dict):
        return {}
    cfg = settings.get('config')
    if isinstance(cfg, dict):
        return cfg
    # Either marker present => structured response: read "config" or nothing,
    # never the user-writable top level — even when "config" is absent/malformed.
    if 'config' in settings or 'preferences' in settings:
        return {}
    # Legacy flat response: no config/preferences split; tolerate top-level keys.
    return settings


def fetch_settings(
        questdb_url: str,
        *,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False,
        timeout: float = 30) -> Dict[str, Any]:
    """Fetch and return the QuestDB ``/settings`` config map."""
    base = questdb_url.rstrip('/')
    data = get_json(base + '/settings', ctx=ctx, insecure=insecure,
                    timeout=timeout)
    return settings_config(data)


_DEFAULT_PORTS = {'https': 443, 'http': 80}


def _normalized_origin(url: str) -> tuple:
    """(scheme, host, port) with default ports filled in, for comparison."""
    parts, explicit_port = safe_urlparse(url)
    scheme = (parts.scheme or '').lower()
    host = (parts.hostname or '').lower()
    port = explicit_port or _DEFAULT_PORTS.get(scheme)
    return (scheme, host, port)


def _origin_str(url: str) -> str:
    scheme, host, port = _normalized_origin(url)
    return f'{scheme}://{host}:{port}' if port else f'{scheme}://{host}'


def _settings_channel_is_plaintext(questdb_url: str) -> bool:
    """
    True if QuestDB ``/settings`` was fetched over plaintext http to a
    non-loopback host — a MITM-tamperable channel (only reachable with
    ``insecure=True``). Endpoints advertised over it must not route credentials
    without an out-of-band pin.
    """
    parts, _ = safe_urlparse(questdb_url)
    return (parts.scheme or '').lower() == 'http' and not _is_loopback(
        parts.hostname)


def _decode_path_segments(path: str) -> list:
    """
    Fully percent-decode a URL path and split it into ``/`` segments.

    Decoding repeats until stable so a multiply-encoded dot segment
    (``%252e%252e`` -> ``..``) or encoded slash (``%2f``) — which a server/proxy
    may unescape more than once before normalizing — is unmasked. Backslash is a
    separator (some proxies fold ``\\`` to ``/``). The containment check compares
    these decoded segments, not the raw wire string, so an encoding the server
    later undoes can't hide a ``..``. Loop bounded: a real path needs 0-1 passes.
    """
    decoded = path
    for _ in range(10):  # bounded; each pass peels one percent-encoding layer
        nxt = urllib.parse.unquote(decoded)
        if nxt == decoded:
            break
        decoded = nxt
    return decoded.replace('\\', '/').split('/')


def _strip_matrix_params(segment: str) -> str:
    """
    Reduce a decoded path segment to the form a server normalizes it to *before*
    dot-segment removal: drop a ``;`` matrix-parameter suffix and trim
    surrounding whitespace.

    So ``..;`` (and ``..\t`` from ``..%09``, or any ``;``-hidden / whitespace-
    padded dot segment) reduces to ``..`` and is caught by the traversal check —
    in *any* segment, not just the last. ``urllib`` only splits the **final**
    segment's ``;params`` off ``.path``; an inner ``;`` stays inside the segment,
    so this must run per-segment. Defeats the well-known ``..;/`` proxy-traversal
    class (Tomcat/Undertow & co. strip path parameters before normalizing the
    path), which an origin check and a last-segment-only fold can't catch.
    """
    return segment.split(';', 1)[0].strip()


def _endpoint_path_under_issuer(endpoint: str, issuer: str) -> bool:
    """
    True if ``endpoint``'s path is the issuer's path or a sub-path of it.

    Segment-aware, so ``/realms/prod`` does not match ``/realms/production``. A
    root issuer (no path) constrains the origin only and matches any path. Stops
    a tampered ``/settings`` from redirecting credentials to a different tenant
    on a path-based multi-tenant IdP (Keycloak issuers are
    ``https://host/realms/{realm}``), which an origin-only check can't catch.

    Compared on fully *decoded*, matrix-param-stripped path segments, not the raw
    wire string. A ``.`` / ``..`` segment is rejected outright: the server
    normalizes it, so ``/realms/prod/../attacker/token`` passes a naive prefix
    test yet resolves to a *different* realm. ``_decode_path_segments`` unmasks
    encoded dot segments (incl. an encoded slash / backslash), and
    ``_strip_matrix_params`` reduces *every* segment the way a proxy does before
    normalizing — dropping a ``;params`` suffix and surrounding whitespace — so a
    ``..;`` / ``..%09`` hidden in any segment can't mask a traversal. (urllib
    only splits the **final** segment's ``;params`` off ``.path``, hence both the
    fold below and the per-segment pass.) Legitimate paths have no dot segments.
    """
    base = (safe_urlparse(issuer)[0].path or '').rstrip('/')
    if not base:
        return True
    base_segs = [_strip_matrix_params(s) for s in _decode_path_segments(base)]
    eparts = safe_urlparse(endpoint)[0]
    # Fold the final segment's ;params back into the path so a traversal hidden
    # there (…/token;..%2f..%2fEVIL) is decoded and scanned; an inner-segment
    # ;params stays in .path and is handled per-segment by _strip_matrix_params.
    ep_path = eparts.path or ''
    if eparts.params:
        ep_path = f'{ep_path};{eparts.params}'
    ep_segs = [_strip_matrix_params(s) for s in _decode_path_segments(ep_path)]
    if '.' in ep_segs or '..' in ep_segs:
        return False
    return ep_segs[:len(base_segs)] == base_segs


def validate_endpoint_origins(
        token_endpoint: str,
        device_authorization_endpoint: str,
        issuer: Optional[str] = None) -> None:
    """
    Reject an OIDC configuration that would send credentials off-origin.

    The device code and long-lived refresh token are POSTed to the device-
    authorization and token endpoints. This limits a tampered or MITM'd config
    from steering those credentials to an attacker host:

    * the two credential endpoints must share a single origin (always co-located
      on the authorization server per RFC 8628); and
    * when ``issuer`` is known independently (explicit or from the IdP
      ``.well-known``), both endpoints must share its **origin**.

    Origin-level only: it does **not** isolate path-based multi-tenant realms
    (e.g. Keycloak ``https://host/realms/{realm}``, one origin per realm). That
    path-scoping lives in :func:`resolve_config`, and only for endpoints from the
    untrusted QuestDB ``/settings``; endpoints from IdP discovery or the caller
    are authoritative and not path-restricted (some IdPs, e.g. Azure AD,
    legitimately place endpoints outside the issuer path).

    Pass ``issuer=`` to pin the IdP when QuestDB advertises the endpoints
    directly, so a compromised server cannot redirect the token POST.
    """
    if _normalized_origin(token_endpoint) != _normalized_origin(
            device_authorization_endpoint):
        raise OidcConfigError(
            'OIDC token and device-authorization endpoints are on different '
            f'origins ({_origin_str(token_endpoint)} vs '
            f'{_origin_str(device_authorization_endpoint)}); refusing to send '
            'credentials. This indicates a misconfigured or tampered OIDC '
            'configuration.')
    if issuer:
        issuer_origin = _normalized_origin(issuer)
        for label, url in (
                ('token endpoint', token_endpoint),
                ('device-authorization endpoint',
                 device_authorization_endpoint)):
            if _normalized_origin(url) != issuer_origin:
                raise OidcConfigError(
                    f'OIDC {label} origin ({_origin_str(url)}) does not match '
                    f'the issuer origin ({_origin_str(issuer)}); refusing to '
                    'send credentials to an endpoint outside the trusted '
                    'issuer.')


def _resolve_endpoint(value: Any) -> Optional[str]:
    """
    A ``/settings`` endpoint, trusted only as a complete ``http(s)`` URL.

    Mirroring the Java client, a QuestDB-advertised endpoint is taken verbatim
    and only as an absolute URL. A path-only (or otherwise non-absolute, or
    non-string) value is treated as absent, so resolution falls back to the IdP
    ``.well-known`` document — which requires an issuer / ``discovery_url`` pin.
    We deliberately do **not** assemble a URL from ``acl.oidc.host`` /
    ``acl.oidc.port`` / ``acl.oidc.tls.enabled``: those are server building
    blocks, not a credential-routing source the client should trust.
    """
    value = _str_setting(value)
    if value and (value.startswith('https://') or value.startswith('http://')):
        return value
    return None


def well_known_url(issuer: str) -> str:
    return issuer.rstrip('/') + '/.well-known/openid-configuration'


def discover_device_endpoint_from_idp(
        *,
        issuer: Optional[str],
        discovery_url: Optional[str],
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False,
        timeout: float = 30) -> Dict[str, Any]:
    """
    Fetch the IdP ``.well-known/openid-configuration`` and return it.

    The discovery URL comes from ``discovery_url``, else built from ``issuer``;
    one is required. The discovery origin is **never** derived from a
    QuestDB-advertised endpoint — that would let a tampered ``/settings`` choose
    where credentials are sent, with the co-location / issuer-pin checks passing
    trivially because every value would share the attacker's origin.
    """
    url = discovery_url or (well_known_url(issuer) if issuer else None)
    if not url:
        raise OidcConfigError(
            'Cannot discover the IdP device-authorization endpoint: no issuer '
            'or discovery_url was given. Pass issuer=... (or '
            'device_authorization_endpoint=... to skip discovery).')
    doc = get_json(url, ctx=ctx, insecure=insecure, timeout=timeout)
    # get_json guarantees valid JSON, not a JSON *object*. Coerce a non-dict
    # document (from a captive portal, bad proxy, or hostile IdP) to empty so
    # resolve_config's doc.get(...) yields a clear "could not resolve" error
    # rather than an AttributeError. Mirrors settings_config.
    return doc if isinstance(doc, dict) else {}


def resolve_config(
        *,
        questdb_url: Optional[str] = None,
        client_id: Optional[str] = None,
        scope: Optional[str] = None,
        audience: Optional[str] = None,
        groups_in_token: Optional[bool] = None,
        token_endpoint: Optional[str] = None,
        device_authorization_endpoint: Optional[str] = None,
        authorization_endpoint: Optional[str] = None,
        issuer: Optional[str] = None,
        discovery_url: Optional[str] = None,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False,
        timeout: float = 30) -> OidcConfig:
    """
    Resolve a complete :class:`OidcConfig`.

    Explicit keyword arguments always win; anything left ``None`` is filled in
    from QuestDB ``/settings`` (if ``questdb_url`` is given) and, as a last
    resort for the device endpoint, the IdP discovery document.
    """
    cfg: Dict[str, Any] = {}
    if questdb_url:
        cfg = fetch_settings(
            questdb_url, ctx=ctx, insecure=insecure, timeout=timeout)
        enabled = _as_bool(cfg.get(_K_ENABLED), default=None)
        if enabled is False:
            raise OidcConfigError(
                f'QuestDB at {questdb_url} reports OIDC is disabled '
                f'({_K_ENABLED}=false). Nothing to authenticate against.')

    # _str_setting drops a non-string /settings value so a non-string client.id
    # reads as absent and hits the clear "Missing client_id" error below.
    client_id = client_id or _str_setting(cfg.get(_K_CLIENT_ID))
    if not client_id:
        raise OidcConfigError(
            'Missing OIDC client_id. QuestDB did not advertise '
            f'{_K_CLIENT_ID!r} via /settings; pass client_id=... explicitly.')

    if scope is None:
        scope = _str_setting(cfg.get(_K_SCOPE)) or 'openid'
    if groups_in_token is None:
        groups_in_token = _as_bool(cfg.get(_K_GROUPS_IN_TOKEN), default=False)
    if audience is None:
        audience = _str_setting(cfg.get(_K_AUDIENCE))

    # Track caller-supplied credential endpoints: those are trusted, whereas
    # /settings endpoints are only as trustworthy as the channel that delivered
    # them (see the insecure-channel guard below).
    explicit_token_endpoint = token_endpoint is not None
    explicit_device_endpoint = device_authorization_endpoint is not None

    token_endpoint = (
        token_endpoint or _resolve_endpoint(cfg.get(_K_TOKEN_ENDPOINT)))
    authorization_endpoint = (
        authorization_endpoint
        or _resolve_endpoint(cfg.get(_K_AUTHORIZATION_ENDPOINT)))
    device_authorization_endpoint = (
        device_authorization_endpoint
        or _resolve_endpoint(cfg.get(_K_DEVICE_ENDPOINT)))

    # Over a plaintext-http /settings channel (insecure=True, non-loopback), a
    # tampered response can advertise BOTH credential endpoints at one attacker
    # origin: the discovery path below is skipped, co-location passes trivially
    # (shared origin) and the issuer-pin check is vacuous (no issuer), so nothing
    # else catches it. Demand the same out-of-band pin (issuer= / discovery_url=)
    # before trusting /settings endpoints here. Caller-explicit endpoints and
    # those from an authenticated (https / loopback) /settings are unaffected.
    settings_supplied_credentials = (
        (token_endpoint and not explicit_token_endpoint)
        or (device_authorization_endpoint and not explicit_device_endpoint))
    if (questdb_url and settings_supplied_credentials
            and not issuer and not discovery_url
            and _settings_channel_is_plaintext(questdb_url)):
        raise OidcConfigError(
            'QuestDB was reached over plaintext http (insecure=True), so its '
            '/settings response — and the OIDC endpoints it advertises — can be '
            'tampered in transit and used to redirect the device-code and '
            'refresh-token requests to an attacker. Pin the identity provider '
            'out-of-band with issuer="https://your-idp" (or discovery_url=...), '
            'pass the endpoints explicitly (token_endpoint=..., '
            'device_authorization_endpoint=...), or connect to QuestDB over '
            'https so /settings is authenticated.')

    # For /settings endpoints with an out-of-band issuer, require each under the
    # issuer's PATH, not just its origin: path-based IdPs share one origin per
    # tenant (Keycloak https://host/realms/{realm}), so the origin check alone
    # can't stop a tampered /settings steering credentials to a different realm.
    # The out-of-band issuer can't be forged. Caller-explicit endpoints and those
    # from IdP discovery are authoritative and skip this — some IdPs (e.g. Azure
    # AD) legitimately place endpoints outside the issuer path.
    if issuer:
        for label, url, from_settings in (
                ('token endpoint', token_endpoint,
                 not explicit_token_endpoint),
                ('device-authorization endpoint',
                 device_authorization_endpoint, not explicit_device_endpoint)):
            if url and from_settings and not _endpoint_path_under_issuer(
                    url, issuer):
                raise OidcConfigError(
                    f'The OIDC {label} advertised by QuestDB /settings '
                    f'({url!r}) is not under the pinned issuer ({issuer!r}); '
                    'refusing to send credentials to an endpoint outside the '
                    'trusted issuer (e.g. a different realm on the same host). '
                    'If your IdP places endpoints outside the issuer path, pass '
                    'them explicitly (token_endpoint=..., '
                    'device_authorization_endpoint=...).')

    # Fall back to IdP discovery when QuestDB doesn't advertise the device
    # (and/or token) endpoint. This contacts the IdP, so it is held to
    # https/loopback (insecure=False) regardless of the QuestDB flag.
    if not device_authorization_endpoint or not token_endpoint:
        # Require an out-of-band trust anchor first. Otherwise the discovery
        # target would be guessed from the /settings token endpoint, so a
        # tampered /settings could steer discovery (and the credential POSTs) to
        # an attacker origin with co-location / issuer-pin passing trivially.
        if not issuer and not discovery_url:
            raise OidcConfigError(
                'QuestDB did not advertise the OIDC device-authorization '
                'endpoint (and/or the token endpoint), so it must be '
                'discovered from the identity provider, but the IdP is not '
                'pinned. Pass issuer="https://your-idp" (its origin) so a '
                'tampered or intercepted /settings response cannot redirect '
                'the device-code and refresh-token requests to an attacker. '
                'Alternatively pass the endpoint(s) explicitly '
                '(device_authorization_endpoint=..., token_endpoint=...) to '
                'skip discovery, or discovery_url=... to pin the discovery '
                'document.')
        doc = discover_device_endpoint_from_idp(
            issuer=issuer, discovery_url=discovery_url,
            ctx=ctx, insecure=False, timeout=timeout)
        # The discovery document is untrusted too: coerce its values like
        # /settings. A non-string endpoint / issuer reads as absent (clear
        # "could not resolve" below, or no issuer pin) instead of reaching
        # safe_urlparse / the cache-key join as a raw object.
        device_authorization_endpoint = (
            device_authorization_endpoint
            or _str_setting(doc.get('device_authorization_endpoint')))
        token_endpoint = (
            token_endpoint or _str_setting(doc.get('token_endpoint')))
        authorization_endpoint = (
            authorization_endpoint
            or _str_setting(doc.get('authorization_endpoint')))
        # OIDC Discovery §4.3 / RFC 8414 §3: when pinned ONLY by discovery_url,
        # the document's self-declared issuer (the anchor
        # validate_endpoint_origins would use) comes from that same untrusted
        # document, so it's a vacuous check. Anchor to the caller-pinned
        # discovery_url instead: require the credential endpoints on its origin
        # so the document can't redirect the POSTs off it. Origin-level; pass
        # issuer= and explicit endpoints if discovery and tokens differ in origin.
        if discovery_url and not issuer:
            discovery_origin = _normalized_origin(discovery_url)
            for label, url in (
                    ('token endpoint', token_endpoint),
                    ('device-authorization endpoint',
                     device_authorization_endpoint)):
                if url and _normalized_origin(url) != discovery_origin:
                    raise OidcConfigError(
                        f'The OIDC {label} ({url!r}) discovered via the pinned '
                        f'discovery_url ({discovery_url!r}) is on a different '
                        'origin; refusing to let a discovery document redirect '
                        'credentials off the pinned IdP origin (OIDC Discovery '
                        '§4.3). Pin the IdP with issuer="https://your-idp" and '
                        'pass token_endpoint=/device_authorization_endpoint= '
                        'explicitly if it serves discovery and tokens from '
                        'different origins.')
        issuer = issuer or _str_setting(doc.get('issuer'))

    if not token_endpoint:
        raise OidcConfigError(
            'Could not resolve the OIDC token endpoint from QuestDB /settings '
            'or IdP discovery. Pass token_endpoint=... explicitly.')
    if not device_authorization_endpoint:
        raise OidcConfigError(
            'Could not resolve the device-authorization endpoint. The IdP '
            'discovery document did not contain '
            '"device_authorization_endpoint". Ensure the IdP supports the '
            'device grant, or pass device_authorization_endpoint=... '
            'explicitly.')

    # The credential-endpoint origin check (validate_endpoint_origins) is
    # enforced centrally in OidcDeviceAuth.__init__, which every path goes
    # through.

    return OidcConfig(
        client_id=client_id,
        token_endpoint=token_endpoint,
        device_authorization_endpoint=device_authorization_endpoint,
        scope=scope,
        groups_in_token=bool(groups_in_token),
        audience=audience,
        issuer=issuer,
        authorization_endpoint=authorization_endpoint)
