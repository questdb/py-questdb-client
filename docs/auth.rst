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
        df = db.query("select * from trades limit 10").to_pandas()
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
* :meth:`~questdb.auth.OidcDeviceAuth.close` permanently closes the shared
  provider. Call it from another thread to cancel device polling or a bundled
  file-token-store lock wait; attached transports observe the same closed
  state. ``OidcDeviceAuth`` is also a context manager.

This prevents a reconnect, SQLAlchemy pool worker, or ingestion background
thread from unexpectedly launching a browser flow. Applications should call
``sign_in()`` on their UI/main thread before opening transports.

Error handling
==============

All :mod:`questdb.auth` failures are :class:`~questdb.auth.OidcError`
subclasses — :class:`~questdb.auth.OidcConfigError`,
:class:`~questdb.auth.OidcCancelledError`,
:class:`~questdb.auth.OidcNetworkError`,
:class:`~questdb.auth.OidcInteractionRequired`,
:class:`~questdb.auth.OidcDeviceFlowError`, and
:class:`~questdb.auth.OidcTimeoutError`. ``OidcError`` is a
:class:`QuestDBError <questdb.QuestDBError>` subclass. Its ``code`` mirrors the
client's own classification: ``QuestDBErrorCode.AuthError`` for a terminal auth
failure, ``SocketError`` for one treated as retryable (a transient token pull on
a reconnect), and ``ConfigError`` for a misconfiguration — so retry logic that
keys on ``code`` handles an OIDC failure exactly as it handles any other.

Because a token is fetched on every connect, reconnect, and flush, a
transport attached with ``oidc_auth=`` can raise a token failure (an
``OidcError``, e.g. :class:`~questdb.auth.OidcInteractionRequired` when
sign-in has lapsed) as well as an ordinary data / server / transport
``QuestDBError`` from the same ``flush()``, ``dataframe()``, ``row()``,
``query()``, or :func:`questdb.connect` call. Because ``OidcError`` is a
``QuestDBError``, an existing ``except QuestDBError`` retry or dead-letter
handler keeps catching auth failures; to react to them specifically, catch
``OidcError`` (or a typed subclass) *before* ``QuestDBError``:

.. code-block:: python

    from questdb import QuestDBError
    from questdb.auth import OidcError, OidcInteractionRequired

    try:
        sender.flush()
    except OidcInteractionRequired:
        auth.sign_in()      # token lapsed; re-authenticate interactively
    except OidcError:
        raise               # other auth failure — not a retriable data error
    except QuestDBError:
        ...                 # data / server / transport failure

.. note::

   The typed ``OidcError`` reaches you only when the result is delivered
   through a Python call that can raise it: ``flush()``, ``dataframe()``,
   ``row()``, ``query()``, materializing a query result with ``to_pandas()``
   or ``to_arrow()``, or :func:`questdb.connect`. If you instead consume a
   query result through the zero-copy Arrow C-stream interface
   (``__arrow_c_stream__`` — e.g. ``polars.from_arrow(db.query(sql))`` or a
   ``pyarrow.RecordBatchReader``), a token failure that happens *mid-stream*
   (a failover reconnect between batches needing a fresh token) surfaces as a
   generic Arrow / ``OSError`` from the consumer, **not** an ``OidcError``:
   the Arrow C-stream boundary carries only an error string, not a Python
   exception type. Call :meth:`~questdb.auth.OidcDeviceAuth.sign_in` up front
   so no interactive token acquisition is needed mid-stream, or materialize
   with ``to_pandas()`` / ``to_arrow()`` when you need to catch the typed
   error.

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

``groups_in_token=True`` selects the ID token but preserves ``scope`` exactly;
include ``openid`` explicitly when the identity provider requires it to issue
an ID token. Otherwise the provider returns the access token, matching the
QuestDB server's selection. Preserving the configured scope also keeps persisted
token identities compatible with the Java client.

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
the ``questdb.client.oidc.token.store.dir`` environment variable shared with
Java. The native client writes plaintext JSON using atomic replacement and
cross-process coordination; on POSIX, directories are mode ``0700`` and files
mode ``0600``. Enabling it stores a long-lived refresh token on disk, so use it
only when that at-rest tradeoff is acceptable. Custom Python token stores are
not supported by the native provider.

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
The custom renderer's prompt dictionary includes ``user_code``, both
verification URLs, ``expires_in`` and ``interval`` in seconds, plus the vetted
``browser_target``.

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
  codes. :func:`~questdb.auth.sanitize_display_text` performs that stripping and
  is exported for this purpose.
  Custom renderers must sanitize callback fields for their terminal or
  HTML output sink and use ``browser_target`` for actionable URLs.
* Avoid logging tokens, authorization headers, or PG connection parameters.

Optional dependencies
=====================

OIDC itself uses the native client and needs no extra package. ``sqlalchemy``
and ``psycopg``/``psycopg2`` support the PG adapters, ``qrcode`` enables QR
rendering, and ``IPython`` enables the rich Jupyter renderer. All are imported
lazily.
