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

import re
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


def _settings_url(questdb_url: str) -> str:
    # Build the /settings endpoint on the base URL's PATH, dropping any query or
    # fragment, so a base like "https://host:9000/?x=1" can't yield a malformed
    # ".../?x=1/settings". The path is rstrip('/')-ed to avoid a double slash.
    # safe_urlparse maps a malformed URL to OidcConfigError, not a bare ValueError.
    parts, _ = safe_urlparse(questdb_url)
    # Require an explicit http(s):// scheme. Without one urllib mis-parses a bare
    # "host:port" — "questdb.example.com:9000" parses with scheme
    # "questdb.example.com" — which would otherwise surface much later as a
    # confusing "insecure URL (scheme 'questdb.example.com')" from _require_secure.
    if (parts.scheme or '').lower() not in ('http', 'https'):
        raise OidcConfigError(
            f'The QuestDB URL {questdb_url!r} needs an explicit http(s):// '
            'scheme, e.g. "https://questdb.example.com:9000".')
    path = (parts.path or '').rstrip('/') + '/settings'
    return urllib.parse.urlunparse(
        (parts.scheme, parts.netloc, path, '', '', ''))


def fetch_settings(
        questdb_url: str,
        *,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False,
        timeout: float = 30) -> Dict[str, Any]:
    """Fetch and return the QuestDB ``/settings`` config map."""
    data = get_json(_settings_url(questdb_url), ctx=ctx, insecure=insecure,
                    timeout=timeout)
    return settings_config(data)


_DEFAULT_PORTS = {'https': 443, 'http': 80}


def _normalized_origin(url: str) -> tuple:
    """(scheme, host, port) with default ports filled in, for comparison."""
    parts, explicit_port = safe_urlparse(url)
    scheme = (parts.scheme or '').lower()
    host = (parts.hostname or '').lower()
    # `explicit_port or default` would collapse an explicit :0 (falsy) to the
    # default port; compare against None so :0 stays a distinct (if
    # unconnectable) origin rather than aliasing the default.
    port = (explicit_port if explicit_port is not None
            else _DEFAULT_PORTS.get(scheme))
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


def _has_control_char(segment: str) -> bool:
    """
    True if ``segment`` carries a C0 control character (including NUL) or DEL.

    Such a char survives :func:`_strip_matrix_params` (``str.strip`` trims only
    the whitespace controls, and only at the ends), so a percent-encoded
    ``..%00`` decodes to a segment that is not literally ``..`` and would slip
    the dot-segment check below — yet a NUL-truncating or control-stripping
    proxy/server can resolve it back to ``..`` and reach a different path.
    Legitimate credential-endpoint segments are plain printable ASCII, so any
    control char is rejected (fail closed).
    """
    return any(ord(ch) < 0x20 or ord(ch) == 0x7f for ch in segment)


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
    # A residual '%' means the path did not fully decode within the bounded loop
    # (an over-deeply multiply-encoded escape) or is a malformed escape; a server
    # may yet decode it further to a dot-segment, so fail closed rather than
    # prefix-match a value that could still resolve to a different path.
    # Legitimate credential-endpoint paths are plain ASCII with no encoding.
    if ('.' in ep_segs or '..' in ep_segs
            or any('%' in s for s in ep_segs)
            or any(_has_control_char(s) for s in ep_segs)):
        return False
    return ep_segs[:len(base_segs)] == base_segs


# An endpoint authority that urllib's urlparse() splits differently than the
# transport (http.client) connects to. Every origin / issuer-pin / cache-key
# check derives the host from ``urlparse(url).hostname``, which strips userinfo
# at the LAST ``@`` (rpartition); urllib, however, hands the FULL netloc to the
# connection. So ``https://attacker.evil\@idp.good/token`` validates as host
# ``idp.good`` (passing the issuer-origin pin) while urllib connects to the whole
# ``attacker.evil\@idp.good``. A real credential-endpoint authority never carries
# userinfo, a backslash, whitespace or a control char, so reject them — fail
# closed — mirroring the host hygiene already enforced in
# ``_adapters._ILLEGAL_HOST_CHARS`` and ``_render._SAFE_HOST_RE``.
_UNSAFE_AUTHORITY_RE = re.compile(r'[\\\s\x00-\x1f\x7f]')


def _reject_confusable_authority(url: str, *, label: str) -> None:
    """Reject a credential URL whose authority urllib may resolve unlike parsed."""
    netloc = safe_urlparse(url)[0].netloc
    if '@' in netloc or _UNSAFE_AUTHORITY_RE.search(netloc):
        raise OidcConfigError(
            f'The OIDC {label} URL {url!r} has an unsafe authority (userinfo '
            "'@', a backslash, whitespace, or a control character). A real "
            'endpoint host never contains these, so this indicates a malformed '
            'or tampered configuration; refusing to send credentials to a host '
            'the HTTP transport may resolve differently than the one validated.')


def validate_endpoint_origins(
        token_endpoint: str,
        device_authorization_endpoint: str) -> None:
    """
    Require the two credential endpoints to share a single origin.

    The device code and long-lived refresh token are POSTed to the device-
    authorization and token endpoints, which RFC 8628 always co-locates on one
    authorization server. A configuration that splits them across origins is
    therefore malformed or tampered; refuse it rather than POST the credentials
    to two different hosts.

    The issuer-**origin** pin for endpoints sourced from the untrusted QuestDB
    ``/settings`` response is enforced separately, in :func:`resolve_config`,
    where each endpoint's provenance is known. Endpoints passed explicitly by the
    caller, or discovered from the IdP's own (authoritative, TLS-fetched)
    ``.well-known`` document, are NOT pinned to the issuer origin: the OIDC
    issuer is an *identifier*, not necessarily the endpoints' host (e.g. Google
    issues from ``accounts.google.com`` but serves tokens from
    ``oauth2.googleapis.com``), so requiring issuer-origin equality there would
    reject a legitimate cross-origin IdP.
    """
    # Reject a confusable authority FIRST, so the origin comparison below (and
    # every later check) can't validate a host different from the one urllib
    # will connect to. Runs in OidcDeviceAuth.__init__, which every construction
    # path goes through, so it covers caller-explicit and discovered endpoints.
    _reject_confusable_authority(token_endpoint, label='token endpoint')
    _reject_confusable_authority(
        device_authorization_endpoint,
        label='device-authorization endpoint')
    if _normalized_origin(token_endpoint) != _normalized_origin(
            device_authorization_endpoint):
        raise OidcConfigError(
            'OIDC token and device-authorization endpoints are on different '
            f'origins ({_origin_str(token_endpoint)} vs '
            f'{_origin_str(device_authorization_endpoint)}); refusing to send '
            'credentials. This indicates a misconfigured or tampered OIDC '
            'configuration.')


def _resolve_endpoint(value: Any) -> Optional[str]:
    """
    A ``/settings`` endpoint, trusted only as a complete ``http(s)`` URL.

    Mirroring the Java client, a QuestDB-advertised endpoint is taken verbatim
    and only as an absolute URL. A path-only (or otherwise non-absolute, or
    non-string) value is treated as absent, so resolution falls back to the IdP
    ``.well-known`` document — which requires an ``issuer`` pin.
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
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False,
        timeout: float = 30) -> Dict[str, Any]:
    """
    Fetch the IdP ``.well-known/openid-configuration`` and return it.

    The discovery URL is built from the pinned ``issuer``
    (``{issuer}/.well-known/openid-configuration``). The discovery origin is
    **never** derived from a QuestDB-advertised endpoint — that would let a
    tampered ``/settings`` choose where credentials are sent, with the
    co-location / issuer-pin checks passing trivially because every value would
    share the attacker's origin.
    """
    if not issuer:
        raise OidcConfigError(
            'Cannot discover the IdP device-authorization endpoint: no issuer '
            'was given. Pass issuer=... (or device_authorization_endpoint=... '
            'to skip discovery).')
    doc = get_json(
        well_known_url(issuer), ctx=ctx, insecure=insecure, timeout=timeout)
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
        issuer: Optional[str] = None,
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
    device_authorization_endpoint = (
        device_authorization_endpoint
        or _resolve_endpoint(cfg.get(_K_DEVICE_ENDPOINT)))

    # Freeze each endpoint's provenance BEFORE discovery may fill a missing one.
    # Only endpoints that came from the untrusted /settings get pinned to the
    # issuer origin below; a caller-explicit or IdP-discovered endpoint is
    # authoritative. `doc_*_endpoint` record what IdP discovery advertised, so a
    # /settings endpoint the IdP's own document confirms can be trusted even when
    # it sits off the issuer origin (e.g. Google: accounts.google.com issuer vs
    # oauth2.googleapis.com endpoints).
    token_from_settings = bool(token_endpoint) and not explicit_token_endpoint
    device_from_settings = (
        bool(device_authorization_endpoint) and not explicit_device_endpoint)
    doc_token_endpoint: Optional[str] = None
    doc_device_endpoint: Optional[str] = None

    # Over a plaintext-http /settings channel (insecure=True, non-loopback), a
    # tampered response can advertise BOTH credential endpoints at one attacker
    # origin: the discovery path below is skipped, co-location passes trivially
    # (shared origin) and the issuer-pin check is vacuous (no issuer), so nothing
    # else catches it. Demand an out-of-band issuer pin before trusting /settings
    # endpoints here. Caller-explicit endpoints and those from an authenticated
    # (https / loopback) /settings are unaffected.
    settings_supplied_credentials = (
        (token_endpoint and not explicit_token_endpoint)
        or (device_authorization_endpoint and not explicit_device_endpoint))
    if (questdb_url and settings_supplied_credentials
            and not issuer
            and _settings_channel_is_plaintext(questdb_url)):
        raise OidcConfigError(
            'QuestDB was reached over plaintext http (insecure=True), so its '
            '/settings response — and the OIDC endpoints it advertises — can be '
            'tampered in transit and used to redirect the device-code and '
            'refresh-token requests to an attacker. Pin the identity provider '
            'out-of-band with issuer="https://your-idp", pass the endpoints '
            'explicitly (token_endpoint=..., device_authorization_endpoint=...), '
            'or connect to QuestDB over https so /settings is authenticated.')

    # Fall back to IdP discovery when QuestDB doesn't advertise the device
    # (and/or token) endpoint. This contacts the IdP, so it is held to
    # https/loopback (insecure=False) regardless of the QuestDB flag.
    if not device_authorization_endpoint or not token_endpoint:
        # Require an out-of-band trust anchor first. Otherwise the discovery
        # target would be guessed from the /settings token endpoint, so a
        # tampered /settings could steer discovery (and the credential POSTs) to
        # an attacker origin with co-location / issuer-pin passing trivially.
        if not issuer:
            raise OidcConfigError(
                'QuestDB did not advertise the OIDC device-authorization '
                'endpoint (and/or the token endpoint), so it must be '
                'discovered from the identity provider, but the IdP is not '
                'pinned. Pass issuer="https://your-idp" (its origin) so a '
                'tampered or intercepted /settings response cannot redirect '
                'the device-code and refresh-token requests to an attacker. '
                'Alternatively pass the endpoint(s) explicitly '
                '(device_authorization_endpoint=..., token_endpoint=...) to '
                'skip discovery.')
        doc = discover_device_endpoint_from_idp(
            issuer=issuer, ctx=ctx, insecure=False, timeout=timeout)
        # The discovery document is authoritative — fetched over TLS from the
        # pinned issuer's own origin — but still coerce its values: a non-string
        # endpoint reads as absent (clear "could not resolve" below) instead of
        # reaching safe_urlparse / the cache-key join as a raw object. These
        # discovered URLs are trusted as-is (no issuer-origin pin), so a
        # cross-origin IdP — e.g. Google, which issues from accounts.google.com
        # but serves tokens from oauth2.googleapis.com — resolves correctly.
        doc_token_endpoint = _str_setting(doc.get('token_endpoint'))
        doc_device_endpoint = _str_setting(
            doc.get('device_authorization_endpoint'))
        device_authorization_endpoint = (
            device_authorization_endpoint or doc_device_endpoint)
        token_endpoint = token_endpoint or doc_token_endpoint

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

    # Pin /settings-sourced credential endpoints to the out-of-band issuer.
    # /settings is untrusted (a tampered or MITM'd response can advertise an
    # attacker endpoint), so an endpoint it supplies must sit BOTH on the pinned
    # issuer's ORIGIN and — for a path-based multi-tenant IdP (Keycloak
    # https://host/realms/{realm}, where every tenant shares one origin) — under
    # the issuer's PATH; the origin check alone can't stop a tampered /settings
    # steering credentials to a different realm on the same host.
    #
    # Both checks are waived for an endpoint the IdP's OWN (authoritative,
    # TLS-fetched) discovery document advertised verbatim (url == confirmed_by_idp):
    # that confirms it independently of /settings, exactly as trustworthy as the
    # pinned IdP. So this runs AFTER discovery — a /settings endpoint the IdP
    # confirms is accepted consistently under BOTH pins (the issuer-PATH check
    # used to run before discovery and lacked this exemption, so it wrongly
    # rejected a discovery-confirmed endpoint sitting off the path of an issuer
    # that carries one, e.g. Azure AD's `.../{tenant}/v2.0` issuer).
    # Caller-explicit and IdP-discovered endpoints are authoritative and skip
    # this entirely (from_settings is False): the issuer is an OIDC *identifier*,
    # not necessarily the endpoints' host (Google issues from accounts.google.com
    # but serves tokens from oauth2.googleapis.com; Azure AD places endpoints
    # outside the issuer path), so pinning them would reject a legitimate IdP.
    # The co-location check in OidcDeviceAuth.__init__ still applies on top.
    if issuer:
        # The pin compares each /settings endpoint's origin against the issuer's;
        # a confusable issuer authority would pin to the wrong host, so vet it too.
        _reject_confusable_authority(issuer, label='issuer')
        issuer_origin = _normalized_origin(issuer)
        for label, url, from_settings, confirmed_by_idp in (
                ('token endpoint', token_endpoint, token_from_settings,
                 doc_token_endpoint),
                ('device-authorization endpoint',
                 device_authorization_endpoint, device_from_settings,
                 doc_device_endpoint)):
            if not from_settings or url == confirmed_by_idp:
                # Caller-explicit / IdP-discovered / discovery-confirmed: trusted.
                continue
            if _normalized_origin(url) != issuer_origin:
                raise OidcConfigError(
                    f'The OIDC {label} advertised by QuestDB /settings '
                    f'({url!r}) is not on the pinned issuer origin '
                    f'({_origin_str(issuer)}) and was not confirmed by the IdP '
                    'discovery document; refusing to send credentials to an '
                    'endpoint outside the trusted issuer. If your IdP serves '
                    'tokens from a different origin than its issuer, pass the '
                    'endpoint(s) explicitly (token_endpoint=..., '
                    'device_authorization_endpoint=...), or omit them from '
                    '/settings so they are taken from authoritative IdP '
                    'discovery.')
            if not _endpoint_path_under_issuer(url, issuer):
                raise OidcConfigError(
                    f'The OIDC {label} advertised by QuestDB /settings '
                    f'({url!r}) is not under the pinned issuer ({issuer!r}) and '
                    'was not confirmed by the IdP discovery document; refusing '
                    'to send credentials to an endpoint outside the trusted '
                    'issuer (e.g. a different realm on the same host). If your '
                    'IdP places endpoints outside the issuer path, pass them '
                    'explicitly (token_endpoint=..., '
                    'device_authorization_endpoint=...).')

    # The credential-endpoint CO-LOCATION check (validate_endpoint_origins) is
    # enforced centrally in OidcDeviceAuth.__init__, which every path goes
    # through; the issuer-ORIGIN pin for /settings-sourced endpoints is enforced
    # just above, where each endpoint's provenance is known.

    return OidcConfig(
        client_id=client_id,
        token_endpoint=token_endpoint,
        device_authorization_endpoint=device_authorization_endpoint,
        scope=scope,
        groups_in_token=bool(groups_in_token),
        audience=audience,
        issuer=issuer)
