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

Resolution order, mirroring the design doc:

1. ``GET {questdb_url}/settings`` (public, no auth) -> the QuestDB-authoritative
   ``acl.oidc.*`` values (client id, scope, endpoints, groups mode).
2. If the device-authorization endpoint is not advertised by QuestDB (today's
   servers), fall back to the IdP discovery document
   (``{issuer}/.well-known/openid-configuration``).
"""

from __future__ import annotations

import ssl
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
_K_HOST = 'acl.oidc.host'
_K_PORT = 'acl.oidc.port'
_K_TLS_ENABLED = 'acl.oidc.tls.enabled'


@dataclass
class OidcConfig:
    """Resolved OIDC parameters needed to run the device flow."""

    client_id: str
    token_endpoint: str
    device_authorization_endpoint: str
    scope: str = 'openid'
    groups_in_token: bool = True
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


def settings_config(settings: Any) -> Dict[str, Any]:
    """
    Return the flat config map from a ``/settings`` response.

    Modern servers nest values under a ``"config"`` object; older ones return
    them at the top level. We tolerate both.
    """
    if isinstance(settings, dict):
        cfg = settings.get('config')
        if isinstance(cfg, dict):
            return cfg
        return settings
    return {}


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
    non-loopback host — a channel a network MITM can tamper (only reachable
    with ``insecure=True``; ``_require_secure`` rejects it otherwise). IdP
    endpoints advertised by such an unauthenticated ``/settings`` response must
    not be trusted to route credentials without an out-of-band pin.
    """
    parts, _ = safe_urlparse(questdb_url)
    return (parts.scheme or '').lower() == 'http' and not _is_loopback(
        parts.hostname)


def validate_endpoint_origins(
        token_endpoint: str,
        device_authorization_endpoint: str,
        issuer: Optional[str] = None) -> None:
    """
    Reject an OIDC configuration that would send credentials off-origin.

    The device code and the long-lived refresh token are POSTed to the device-
    authorization and token endpoints. These come from QuestDB ``/settings``
    (or the IdP ``.well-known``), which the client trusts; this check limits a
    tampered or MITM'd configuration from redirecting those credentials to an
    attacker-controlled host:

    * the two credential endpoints must share a single origin (they are always
      co-located on the authorization server per RFC 8628); and
    * when the ``issuer`` is known independently (passed explicitly or resolved
      from the IdP ``.well-known``), both endpoints must belong to it.

    Pass ``issuer=`` to pin the IdP explicitly when QuestDB advertises the
    endpoints directly (so a compromised server cannot redirect the token POST).
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


def _resolve_endpoint(value: Optional[str], cfg: Dict[str, Any]) -> Optional[str]:
    """
    Turn a possibly-relative endpoint into a full URL.

    QuestDB usually exports fully-resolved URLs, but some deployments store
    only the path (e.g. ``/as/token.oauth2``) alongside ``acl.oidc.host``.
    """
    if not value:
        return None
    if not isinstance(value, str):
        # A non-string endpoint from /settings (e.g. a JSON number) is
        # malformed; treat it as absent so resolution falls through to a clear
        # OidcConfigError (or the IdP-discovery fallback) instead of an
        # AttributeError from .startswith() escaping the typed-error contract.
        return None
    if value.startswith('http://') or value.startswith('https://'):
        return value
    if value.startswith('/'):
        host = cfg.get(_K_HOST)
        if host:
            tls = _as_bool(cfg.get(_K_TLS_ENABLED), default=True)
            scheme = 'https' if tls else 'http'
            port = cfg.get(_K_PORT)
            netloc = f'{host}:{port}' if port else str(host)
            return f'{scheme}://{netloc}{value}'
    return value


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

    The discovery URL is taken from ``discovery_url``, else built from
    ``issuer``. One of the two is required: the discovery origin is **never**
    derived from a QuestDB-advertised endpoint, because that would let a
    tampered ``/settings`` choose where the device code and refresh token are
    sent (the resolved issuer and endpoints would then all share the attacker's
    origin and pass the co-location / issuer-pin checks trivially).
    """
    url = discovery_url or (well_known_url(issuer) if issuer else None)
    if not url:
        raise OidcConfigError(
            'Cannot discover the IdP device-authorization endpoint: no issuer '
            'or discovery_url was given. Pass issuer=... (or '
            'device_authorization_endpoint=... to skip discovery).')
    return get_json(url, ctx=ctx, insecure=insecure, timeout=timeout)


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

    client_id = client_id or cfg.get(_K_CLIENT_ID)
    if not client_id:
        raise OidcConfigError(
            'Missing OIDC client_id. QuestDB did not advertise '
            f'{_K_CLIENT_ID!r} via /settings; pass client_id=... explicitly.')

    if scope is None:
        scope = cfg.get(_K_SCOPE) or 'openid'
    if groups_in_token is None:
        groups_in_token = _as_bool(cfg.get(_K_GROUPS_IN_TOKEN), default=True)
    if audience is None:
        audience = cfg.get(_K_AUDIENCE) or None

    # Track which credential endpoints the caller supplied directly. Those are
    # trusted; endpoints learned from /settings are only as trustworthy as the
    # channel that delivered them (see the insecure-channel guard below).
    explicit_token_endpoint = token_endpoint is not None
    explicit_device_endpoint = device_authorization_endpoint is not None

    token_endpoint = (
        token_endpoint or _resolve_endpoint(cfg.get(_K_TOKEN_ENDPOINT), cfg))
    authorization_endpoint = (
        authorization_endpoint
        or _resolve_endpoint(cfg.get(_K_AUTHORIZATION_ENDPOINT), cfg))
    device_authorization_endpoint = (
        device_authorization_endpoint
        or _resolve_endpoint(cfg.get(_K_DEVICE_ENDPOINT), cfg))

    # When QuestDB itself was reached over plaintext http to a non-loopback host
    # (only possible with insecure=True), its /settings response can be tampered
    # in transit. Any IdP credential endpoint it advertises would then route the
    # device code and long-lived refresh token to an attacker origin. The
    # missing-endpoint discovery path below already demands an out-of-band pin,
    # but when a tampered /settings advertises BOTH endpoints at one attacker
    # origin that path is skipped, the co-location check passes trivially (they
    # share that origin) and the issuer-pin check is vacuous (no issuer) — so
    # nothing else catches it. Require the same out-of-band pin (issuer= /
    # discovery_url=) before trusting /settings-supplied endpoints over such a
    # channel. Endpoints the caller passed explicitly, and endpoints from an
    # authenticated (https / loopback) /settings, are unaffected.
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

    # Fall back to IdP discovery when QuestDB doesn't advertise the device
    # endpoint (and/or the token endpoint). This contacts the IdP, so it is
    # held to https/loopback (insecure=False) regardless of the QuestDB flag.
    if not device_authorization_endpoint or not token_endpoint:
        # Require a caller-supplied trust anchor before contacting the IdP for
        # discovery. Without issuer= / discovery_url=, the discovery target
        # would have to be guessed from the token endpoint that /settings
        # supplied; a tampered or MITM'd /settings (reachable in cleartext when
        # QuestDB is http:// with insecure=True) could then steer discovery —
        # and so the device-code and refresh-token POSTs — to an attacker
        # origin, with the co-location and issuer-pin checks passing trivially
        # because every value shares that one origin. issuer= is out-of-band,
        # so the server cannot forge it.
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
        device_authorization_endpoint = (
            device_authorization_endpoint
            or doc.get('device_authorization_endpoint'))
        token_endpoint = token_endpoint or doc.get('token_endpoint')
        authorization_endpoint = (
            authorization_endpoint or doc.get('authorization_endpoint'))
        issuer = issuer or doc.get('issuer')

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

    # Note: the credential-endpoint origin check (validate_endpoint_origins)
    # is enforced centrally in OidcDeviceAuth.__init__, which every path
    # (including the explicit constructor) goes through.

    return OidcConfig(
        client_id=client_id,
        token_endpoint=token_endpoint,
        device_authorization_endpoint=device_authorization_endpoint,
        scope=scope,
        groups_in_token=bool(groups_in_token),
        audience=audience,
        issuer=issuer,
        authorization_endpoint=authorization_endpoint)
