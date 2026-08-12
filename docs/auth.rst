.. _oidc_auth:

===================
OIDC Authentication
===================

QuestDB Enterprise can be secured with `OpenID Connect (OIDC)
<https://questdb.com/docs/operations/rbac/>`_. The Python client runs the OAuth
2.0 Device Authorization Grant (RFC 8628), including in remote Jupyter kernels
where the browser and Python process are on different machines.

The device flow, refresh, cache, file persistence, and transport token-provider
logic all run in the native QuestDB client. Python supplies the Jupyter/terminal
renderer and PG-wire convenience adapters.

Native QuestDB transports
=========================

Sign in explicitly, then attach the same rotating provider to a deployment-level
client or a standalone sender:

.. code-block:: python

    import questdb
    from questdb.auth import OidcDeviceAuth

    auth = OidcDeviceAuth.from_questdb(
        "https://questdb.example.com:9000")
    auth.sign_in()  # the only operation that may prompt or open a browser

    with questdb.connect(
            "wss::addr=questdb.example.com:9000;",
            oidc_auth=auth) as db:
        result = db.query("select * from trades limit 10")
        with db.sender() as sender:
            sender.row("events", columns={"value": 42},
                       at=questdb.ServerTimestamp)

    with questdb.Sender.from_conf(
            "https::addr=questdb.example.com:9000;",
            oidc_auth=auth) as sender:
        ...

The pool and sender retain shared native ownership of the provider. Every
connect and reconnect asks it for a current token, including silent refresh,
without copying a fixed token into the connection configuration.

Token lifecycle
===============

The lifecycle is deliberately split:

* :meth:`~questdb.auth.OidcDeviceAuth.sign_in` is interactive. It uses a cached
  or silently refreshable credential when possible and otherwise runs the
  device flow.
* :meth:`~questdb.auth.OidcDeviceAuth.token` is never interactive. It returns a
  cached, persisted, or silently refreshed token, and raises
  :class:`~questdb.auth.OidcInteractionRequired` when explicit sign-in is
  needed. Transport connect/reconnect paths have the same behavior.
* :meth:`~questdb.auth.OidcDeviceAuth.clear` clears memory and the configured
  persisted entry. It does not revoke the credential at the identity provider.

This prevents a reconnect, SQLAlchemy pool worker, or ingestion background
thread from unexpectedly launching a browser flow. Applications should call
``sign_in()`` on their UI/main thread before opening transports.

Configuration
=============

Discover configuration from QuestDB's public ``/settings`` endpoint:

.. code-block:: python

    auth = OidcDeviceAuth.from_questdb(
        "https://questdb.example.com:9000",
        issuer="https://idp.example.com/realms/questdb",
        audience="questdb")

Explicit keyword arguments override discovered values. Or skip discovery:

.. code-block:: python

    auth = OidcDeviceAuth(
        client_id="questdb",
        device_authorization_endpoint="https://idp.example.com/device",
        token_endpoint="https://idp.example.com/token",
        scope="openid groups",
        groups_in_token=True,
        audience="questdb")

``groups_in_token=True`` selects the ID token and ensures the ``openid`` scope
is requested. Otherwise the provider returns the access token, matching the
QuestDB server's selection.

Persistence
===========

Credentials stay in memory unless a :class:`~questdb.auth.FileTokenStore` is
configured:

.. code-block:: python

    from questdb.auth import FileTokenStore

    auth = OidcDeviceAuth.from_questdb(
        "https://questdb.example.com:9000",
        token_store=FileTokenStore.at_default_location())
    auth.sign_in()

The default directory is ``~/.questdb/oidc-tokens/``, overridable with
``QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR``. The native client writes plaintext JSON
using atomic replacement and cross-process coordination; on POSIX, directories
are mode ``0700`` and files mode ``0600``. Enabling it stores a long-lived
refresh token on disk, so use it only when that at-rest tradeoff is acceptable.
Custom Python token stores are not supported by the native provider.

PG-wire and manual token use
============================

The adapters inject the current token as QuestDB's ``_sso`` password. Sign in
before creating a pool:

.. code-block:: python

    from questdb.auth import sqlalchemy_engine, psycopg_connect

    auth.sign_in()
    engine = sqlalchemy_engine(auth, "https://questdb.example.com:9000")
    conn = psycopg_connect(auth, "https://questdb.example.com:9000")

SQLAlchemy calls non-interactive ``token()`` for every new pooled connection,
so it follows rotation and silent refresh. ``psycopg_connect`` captures one
token for that connection. For other HTTP clients, use ``auth.headers()``.

Rendering and non-interactive environments
==========================================

The default renderer produces a rich, clickable Jupyter prompt or terminal
text. Pass ``qr=True`` for a QR code, ``open_browser=False`` to suppress browser
opening, or ``renderer=`` for a custom :class:`~questdb.auth.Renderer`.

When no interactive terminal/frontend is available, ``sign_in()`` raises
:class:`~questdb.auth.OidcInteractionRequired` instead of waiting indefinitely.
Use a QuestDB service-account token or OAuth client-credentials flow for cron,
CI, and unattended notebook execution.

Security notes
==============

* IdP passwords and MFA stay in the browser; Python receives device and bearer
  tokens only.
* IdP credential endpoints require HTTPS, except loopback HTTP for local
  development. ``insecure=True`` applies only to QuestDB discovery transport.
* Renderer callbacks receive raw, untrusted IdP text. The built-in renderers
  sanitize it and use the separately vetted ``browser_target`` for links and QR
  codes. Custom renderers must sanitize callback fields for their terminal or
  HTML output sink and use ``browser_target`` for actionable URLs.
* Avoid logging tokens, authorization headers, or PG connection parameters.

Optional dependencies
=====================

OIDC itself uses the native client and needs no extra package. ``sqlalchemy``
and ``psycopg``/``psycopg2`` support the PG adapters, ``qrcode`` enables QR
rendering, and ``IPython`` enables the rich Jupyter renderer. All are imported
lazily.
