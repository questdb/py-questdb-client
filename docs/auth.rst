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
  file-token-store lock wait. Closing is **terminal for every attached
  transport**, not merely a state they observe: each ``Sender``,
  :func:`questdb.connect` pool and reader built from the provider fails its
  next token pull non-retryably, so reconnect loops stop and a QWP/WebSocket
  publication store is terminalized with accepted frames still queued.
  Disk-backed store-and-forward slots stay drainable by a later process.
  Recovering means building a new provider *and* rebuilding every transport
  that used the old one. ``OidcDeviceAuth`` is also a context manager, so a
  ``with`` block has the same effect at exit.

This prevents a reconnect, SQLAlchemy pool worker, or ingestion background
thread from unexpectedly launching a browser flow. Applications should call
``sign_in()`` on their UI/main thread before opening transports.

Error handling
==============

Failures produced by QuestDB's OIDC authentication, configuration and token
lifecycle are :class:`~questdb.auth.OidcError` subclasses —
:class:`~questdb.auth.OidcConfigError`,
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
QuestDB server's selection. The configured value is sent on the initial request
and remains part of the persisted token identity. Refresh requests omit
``scope``, which tells the identity provider to preserve the scope originally
granted.

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
Java. That override **must be an absolute path**: a relative one follows the
working directory, and ``~`` is expanded by shells rather than by any QuestDB
client, so neither names a single store the clients would actually share.
Both are rejected rather than silently resolved. A path passed straight to
``FileTokenStore(...)`` is a Python path, not the shared setting, and is
expanded and absolutised as usual. The native client writes plaintext JSON using atomic replacement and
cross-process coordination; on POSIX, directories are mode ``0700`` and files
mode ``0600``. Enabling it stores a long-lived refresh token on disk, so use it
only when that at-rest tradeoff is acceptable. Custom Python token stores are
not supported by the native provider.

.. _oidc_pgwire:

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

Because the token travels as the PG password, both adapters default to
``sslmode="verify-full"`` for remote hosts. This authenticates the server as
well as encrypting the connection. Numeric loopback IPs instead use ``prefer``,
so local QuestDB is accepted with or without TLS. ``localhost`` retains
``verify-full`` because its resolved addresses are not pinned; use
``127.0.0.1`` or ``::1`` for automatic local plaintext fallback::

    engine = sqlalchemy_engine(
        auth, "https://questdb.example.com:9000",
        connect_args={"sslrootcert": "/etc/ssl/questdb-ca.pem"})

Pass ``sslmode=None`` to set nothing and manage TLS through the environment or
a service file; an ``sslmode`` you supply yourself always wins.

Missing SQLAlchemy or PostgreSQL-driver dependencies raise ``ImportError``.
Token acquisition failures from either adapter raise ``OidcError``; SQLAlchemy,
psycopg and psycopg2 construction, connection, TLS and server-authentication
exceptions otherwise propagate with their original third-party types.

Rendering and non-interactive environments
==========================================

The default renderer produces a rich, clickable Jupyter prompt or terminal
text. Pass ``qr=True`` for a QR code or ``renderer=`` for a custom
:class:`~questdb.auth.Renderer`. ``open_browser`` is tri-state: the default
``None`` opens a browser except inside a Jupyter kernel, where it may be on a
different machine from the reader; pass ``True`` to open one anyway (a *local*
``jupyter lab``) or ``False`` to never open one.
The custom renderer's prompt dictionary includes ``user_code``, both
verification URLs, ``expires_in`` and ``interval`` in seconds, plus the vetted
``browser_target``.

``sign_in()`` prompts by default, wherever it is called from: a missing TTY is
not evidence of a missing human, so there is no terminal detection to refuse a
sign-in that would have worked. A flow nobody answers is bounded by the device
code's own lifetime.

The one exception is a notebook executed headlessly (papermill, ``nbclient``,
``jupyter nbconvert --execute``), whose kernel states that it accepts no input;
there ``sign_in()`` raises :class:`~questdb.auth.OidcInteractionRequired`
immediately rather than rendering a prompt into an output nobody will open.
Pass ``interactive=False`` to get that fail-fast behaviour anywhere else, such
as cron or CI. For unattended contexts generally, prefer a QuestDB
service-account token or the OAuth client-credentials flow.

Security notes
==============

* IdP passwords and MFA stay in the browser; Python receives device and bearer
  tokens only.
* IdP credential endpoints require HTTPS, except loopback HTTP for local
  development. ``insecure=True`` applies only to QuestDB discovery transport.
* Renderer callbacks receive bounded, native display-normalized but still
  untrusted IdP text. Prompt fields are single-line and visibly ASCII-escaped;
  identity/failure text may retain ordinary Unicode and HTML metacharacters.
  Custom renderers must apply their output sink's encoding (for example HTML
  escaping), and use only ``browser_target`` for links, browser opening or QR
  codes. :func:`~questdb.auth.sanitize_display_text` remains available for raw
  values from other sources or defense-in-depth; it does not HTML-escape.
* Avoid logging tokens, authorization headers, or PG connection parameters.
* The PG-wire adapters send the token as the ``_sso`` password, so remote hosts
  and hostnames such as ``localhost`` default to ``sslmode="verify-full"``.
  Numeric loopback IPs use ``prefer`` to support local servers without TLS. See
  :ref:`the PG-wire section <oidc_pgwire>`.
* ``Ctrl-C`` during :meth:`~questdb.auth.OidcDeviceAuth.sign_in` closes the
  provider permanently, and closing is shared: every ``Sender``,
  :func:`questdb.connect` pool and reader attached with ``oidc_auth=`` is closed
  with it and cannot be revived. Recovering means building a new provider *and*
  rebuilding every transport that used the old one. Where a long-running
  ingestion must survive a cancelled re-authentication, keep the interactive
  provider separate from the one you attach.

Optional dependencies
=====================

OIDC itself uses the native client and needs no extra package. ``sqlalchemy``
and ``psycopg``/``psycopg2`` support the PG adapters, ``qrcode`` enables QR
rendering, and ``IPython`` enables the rich Jupyter renderer. All are imported
lazily.
