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

.. warning::

   Use ``https::`` or ``wss::``. Over plain ``http::`` or ``ws::`` to a
   non-loopback host, the IdP-issued token is sent as an ``Authorization:
   Bearer`` header in cleartext on every HTTP flush or WebSocket
   (re)connect, and can be captured in transit. Nothing rejects or warns
   about that configuration; reserve plaintext for a loopback server.

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
  that used the old one. Attaching a closed provider to a new transport raises
  :class:`~questdb.auth.OidcCancelledError`, like the provider's own
  operations. ``OidcDeviceAuth`` is also a context manager, so a ``with``
  block has the same effect at exit.

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
failing call's native category, **not** a recovery instruction. For example,
``OidcInteractionRequired`` reports ``AuthError`` from ``auth.token()`` or the
PG adapters, but ``SocketError`` from an HTTP sender flush in the very same
state (no sign-in yet). While another thread's sign-in or callback holds the
provider, every caller instead gets ``SocketError`` with ``acquisition_busy``
set. The sender cannot recover by retrying until someone
signs in. Catch ``OidcInteractionRequired`` before general error-code-based
retry logic. Its public ``acquisition_busy`` property is true when another
thread is acquiring a token or rendering a callback: defer the operation
until that thread finishes. If false, arrange interactive sign-in (after
returning from any callback) rather than retrying the failing operation.
Similarly, a query's ``RoleMismatch`` code does not promise that another
immediate query will succeed: it can mean that every endpoint advertised the
wrong role until failover exhausted its time or attempts. Inspect the error
and retry only after the deployment has a suitable endpoint. A handshake
``AuthError`` (401/403) means the offered credential was rejected; repair or
replace it before retrying, rather than blindly repeating the same request.

HTTP senders fetch a token on each flush; QWP/WebSocket senders and readers
fetch one on connect/reconnect (and may retry after a handshake 401), **not**
on every flush of an already-connected sender. A token pull can raise an
``OidcError`` (e.g. :class:`~questdb.auth.OidcInteractionRequired` when
sign-in has lapsed) alongside ordinary data / server / transport
``QuestDBError`` from ``flush()`` (HTTP), ``dataframe()``, ``row()``,
``query()``, or :func:`questdb.connect`. A background QWP/WebSocket reconnect
may instead report the failure through a connection event. Because
``OidcError`` is a ``QuestDBError``, an existing ``except QuestDBError`` retry
or dead-letter handler keeps catching auth failures; to react to them
specifically, catch ``OidcError`` (or a typed subclass) *before*
``QuestDBError``.

A failed ``flush()`` of the sender's internal buffer **discards those rows**,
exactly as for any other flush failure (see
:meth:`Sender.flush <questdb.Sender.flush>`), so signing in again does not
resend them. To retry a batch after re-authenticating, build it in a
caller-owned buffer and flush it with ``clear=False``, which keeps the rows in
the buffer when the flush fails. This example handles a synchronous token
failure from an HTTP sender; a connected QWP/WebSocket sender does not pull a
token for each flush:

.. code-block:: python

    import questdb
    from questdb import QuestDBError
    from questdb.auth import OidcError, OidcInteractionRequired

    buf = sender.new_buffer()
    buf.row('trades', symbols={'sym': 'ETH-USD'}, columns={'px': 2615.54},
            at=questdb.ServerTimestamp)
    try:
        sender.flush(buf, clear=False)
    except OidcInteractionRequired as exc:
        if exc.acquisition_busy:
            raise           # defer the batch until the other thread finishes
        auth.sign_in()      # token lapsed; re-authenticate outside callbacks
        sender.flush(buf, clear=False)   # the rows are still in `buf`
    except OidcError:
        raise               # other auth failure — not a retriable data error
    except QuestDBError:
        ...                 # data / server / transport failure
    buf.clear()

.. note::

   The typed ``OidcError`` reaches you only when the result is delivered
   through a Python call that can raise it: ``flush()``, ``dataframe()``,
   ``row()``, ``query()``, materializing a query result with ``to_pandas()``
   or ``to_arrow()``, or :func:`questdb.connect`. If you instead consume a
   query result through the zero-copy Arrow C-stream interface
   (``__arrow_c_stream__`` — e.g. ``polars.DataFrame(db.query(sql))`` or a
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

The two credential endpoints must share one origin (scheme, host and port),
however they were obtained: the device code and refresh token are posted to
both. When you pass both endpoints explicitly *and* an ``issuer``, both
endpoints must also be on the issuer's origin. Some providers host their
endpoints on a different origin from their issuer -- Google's issuer is
``https://accounts.google.com`` while its endpoints are on
``https://oauth2.googleapis.com`` -- so configure those without ``issuer``.
Either violation raises :class:`~questdb.auth.OidcConfigError` at
construction.

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

The default directory is ``~/.questdb/oidc-tokens/``, overridable with the
``QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR`` environment variable, which the Python
and native clients share. (Java spells the same setting
``questdb.client.oidc.token.store.dir``, but as a JVM system property
``-Dquestdb.client.oidc.token.store.dir=...``, which does not reach this
process's environment; set both if you need one store across all three.)
That override **must be an absolute path** -- on Windows one with a drive
letter or a UNC prefix: a relative one follows the working directory, a
drive-less ``\tokens`` follows the current drive, and ``~`` is expanded by
shells rather than by any QuestDB client, so none names a single store the
clients would actually share. All are rejected rather than silently resolved.
The value is passed on unchanged, so ``..`` is resolved by the operating
system exactly as the native client resolves it. A path passed straight to
``FileTokenStore(...)`` is a Python path, not the shared setting: ``~`` is
expanded and a relative path is made absolute against the working directory,
but ``..`` is likewise left for the operating system to resolve. The native client writes plaintext JSON
using atomic replacement and cross-process coordination; on POSIX, directories
are mode ``0700`` and files mode ``0600``. An existing directory is tightened to
``0700`` on first use, and one that was group- or other-writable is swept before
its contents are trusted: entries named like store records (64 lowercase hex
characters then ``.json``, or a ``.tmp`` temporary) are deleted, whoever wrote
them. Point the store at a directory dedicated to it. Non-POSIX platforms currently reject
file-store *mutations* before changing the stored entry because they lack the
durable metadata barrier required for safe refresh-token rotation; reads remain
available, so the coordination protocol may still leave empty ``.lock`` files
(never credential material) in the store directory there. Python callers must
use in-memory authentication there; custom keychain-backed stores are currently
available only through the Rust ``TokenStore`` API. Every failed store operation
is logged at ``WARNING`` on the ``questdb`` logger during normal operation.
Persistence-warning handlers must not call ``sign_in()``, ``clear()``, an
uncached ``token()``, or an attached transport operation that needs a token from
the same provider; those operations are rejected before they can deadlock.
For QWP/WebSocket, publishing a frame may succeed before its background
reconnect needs a token. An ACK wait made inside the handler then raises
:class:`~questdb.auth.OidcInteractionRequired` promptly once that sender's
reconnect needs a token the provider cannot supply until the handler returns,
whichever sender's refresh raised the warning; the frame remains queued.
Retry the ACK wait after the handler returns. A connected sender whose ACK
does not need a token can still complete the wait inside the handler, and an
ACK wait on any other thread is not rejected: it waits for the handler to
return. Make the wait on the handler's own thread: a handler that hands the
wait to another thread and blocks on its result can deadlock until the wait's
timeout.
Cached token reads and provider ``cancel_sign_in()`` / ``close()`` remain
callback-safe. An attached **Sender** may not be closed or mutated by a
persistence-warning handler while it is performing a native flush: those
operations raise ``QuestDBError(InvalidApiCall)`` rather than freeing the
sender or changing its buffer while native code still holds it. Likewise, a
persistence-warning handler must not close a :class:`~questdb.QuestDB` pool
attached to the same provider: ``db.close()`` called from the handler raises
``QuestDBError(InvalidApiCall)``, because the close would otherwise wait for a
lease whose acknowledgement depends on the warning callback returning. A
``db.close()`` on any other thread waits for the handler to return, and gives
up with the same error after two seconds. That bound is what releases a close
the handler delegated to another thread and joins; it also means an unrelated
close fails, and can simply be retried, if a handler runs for longer. Close the
pool after the handler returns instead. The binding imports
``logging`` before registering its own shutdown hook, so the hook detaches OIDC
callbacks while logging handlers are still live;
``logging.shutdown()`` runs afterwards. Diagnostics produced after the detach
are deliberately suppressed rather than entering Python during finalization. A
failed save or automatic clear is otherwise reported only there and leaves the
in-memory credential usable; a failed load, or a refresh lease lost
mid-refresh, is also raised to the caller as
:class:`~questdb.auth.OidcNetworkError`, because an uncoordinated refresh could
resubmit a rotating token. The exception is ``sign_in()`` against a store that
can never be used as configured -- a directory it may not create or write, a
read-only filesystem, or a path that is not a directory: that raises
:class:`~questdb.auth.OidcConfigError` before any device code is shown, rather
than a retryable error that would fail identically on every retry. Enabling persistence stores a long-lived refresh token on disk, so use it
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
    # verify-full needs a trust root; see below.
    engine = sqlalchemy_engine(
        auth, "https://questdb.example.com:9000",
        connect_args={"sslrootcert": "/etc/ssl/questdb-ca.pem"})
    conn = psycopg_connect(
        auth, "https://questdb.example.com:9000",
        sslrootcert="/etc/ssl/questdb-ca.pem")

SQLAlchemy calls non-interactive ``token()`` for every new pooled connection,
so it follows rotation and silent refresh. ``psycopg_connect`` captures one
token for that connection. For other HTTP clients, use ``auth.headers()``.

Because the token travels as the PG password, both adapters default to
``sslmode="verify-full"`` for remote hosts. This authenticates the server as
well as encrypting the connection. Numeric loopback IPs instead use ``prefer``,
so local QuestDB is accepted with or without TLS. ``localhost`` retains
``verify-full`` because its resolved addresses are not pinned; use
``127.0.0.1`` or ``::1`` for automatic local plaintext fallback.

``verify-full`` needs a **trust root**, and libpq does not use the operating
system's certificate store by default: with no ``sslrootcert``, no
``PGSSLROOTCERT`` and no ``~/.postgresql/root.crt``, the connection fails with
``root certificate file ... does not exist`` before any TLS handshake -- even
for a server whose certificate a browser would trust. Supply one of:

* ``sslrootcert="/path/to/ca.pem"`` -- the CA that signed the server
  certificate (a private CA, or a public CA bundle);
* ``sslrootcert="system"`` -- the system trust store; requires libpq 16 or
  later (``psycopg.pq.version()`` reports it);
* the ``PGSSLROOTCERT`` environment variable or ``~/.postgresql/root.crt``.

An explicit ``sslrootcert`` overrides both ``PGSSLROOTCERT`` and
``~/.postgresql/root.crt``, so do not pass ``"system"`` if your CA is
configured there. Pass it only for a remote (``verify-full``) host: libpq 16
and later refuse ``sslrootcert="system"`` with any weaker ``sslmode``, so
combined with a numeric loopback URL, which defaults to ``prefer``, every
connection fails with ``weak sslmode "prefer" may not be used with
sslrootcert=system``.

Pass it through ``connect_args`` for ``sqlalchemy_engine``, or as a keyword
argument to ``psycopg_connect``::

    engine = sqlalchemy_engine(
        auth, "https://questdb.example.com:9000",
        connect_args={"sslrootcert": "system"})

Pass ``sslmode=None`` to set nothing and manage TLS through the environment or
a service file; an ``sslmode`` you supply yourself always wins.

``sslmode`` is a libpq parameter, so ``sqlalchemy_engine`` accepts only libpq
drivers (``postgresql+psycopg``, ``postgresql+psycopg2``) while it is set. A
driver such as ``postgresql+pg8000`` raises ``OidcConfigError`` at construction
instead of failing every connection; to use one, pass ``sslmode=None`` and
configure that driver's own TLS through ``connect_args`` (``ssl_context`` for
pg8000), or the token travels unencrypted.

The adapters own the connection *destination*: it comes from the validated URL
or from ``host=`` / ``pg_port=``. A destination in the driver passthrough
(``host``, ``hostaddr``, ``port``, ``service``, ``dsn`` or ``conninfo`` in
``connect_args`` / ``connect_kwargs``) is rejected with ``OidcConfigError``
before any token is acquired, because the token is a bearer credential and
SQLAlchemy applies ``connect_args`` *after* the arguments built from that URL.
The adapters likewise own the login: they always authenticate as ``_sso`` with
the current token as the password, so a ``user``, ``password``, ``dbname`` or
``database`` in the passthrough is also rejected with ``OidcConfigError``
before any token is acquired. Pass the database name as ``database=``.
For libpq drivers, the adapters also pass an explicitly empty ``hostaddr``:
libpq otherwise uses an inherited ``PGHOSTADDR`` even when ``host`` is set,
and could send the bearer password to that address. Empty ``hostaddr`` keeps
normal hostname resolution while overriding the environment variable on every
physical connection, including connections created after an engine is built.
A ``do_connect`` listener registered at any point must also leave ``host`` and
``port`` in the driver's keyword arguments and supply no positional connection
arguments; the adapter checks the destination after all listeners have run,
just before attaching the token at the dialect's connect boundary. A listener
that returns its own DBAPI connection bypasses this boundary and will not
receive a token from the adapter.

Missing SQLAlchemy or PostgreSQL-driver dependencies raise ``ImportError``.
SQLAlchemy 1.x has no psycopg (v3) dialect, so there ``sqlalchemy_engine()``
needs psycopg2 and defaults to ``postgresql+psycopg2`` even when psycopg 3 is
also installed.
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
``browser_target``. On PyPy, a custom renderer holding a strong reference back
to its ``OidcDeviceAuth`` creates a cycle that cpyext cannot collect. Use a weak
back-reference or call ``auth.close()`` explicitly (for example via a ``with``
block) to break the cycle; closing an attached provider is terminal for its
transports, so plan their lifecycle accordingly. Renderer callbacks must return
promptly: interpreter shutdown suppresses later callbacks without waiting for
one already running, so work left unfinished in a renderer is abandoned. Run
``sign_in()`` on a daemon thread if a callback can block for a long time:
a non-daemon worker is joined before the shutdown hook runs and would delay
process exit. For the same reason a ``sign_in()`` started after that hook,
from an ``atexit`` handler registered before ``questdb`` was imported, cannot
show a prompt: it succeeds
from a cached or silently refreshable credential, and otherwise raises
:class:`~questdb.auth.OidcInteractionRequired` at once unless the provider opens
a browser.

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

.. _auth-fork:

Forked processes
================

OIDC does not survive ``fork()`` without ``exec()``:

* A provider inherited by a forked child cannot be used, attached to a
  transport, or closed there: its native locks may belong to parent threads
  that no longer exist. Those calls raise
  :class:`~questdb.auth.OidcConfigError`.
* Once the parent has constructed any
  :class:`~questdb.auth.OidcDeviceAuth` -- constructing one is enough, it
  need not have been used -- constructing a *new* provider in a forked child
  raises :class:`~questdb.auth.OidcConfigError` too.

This affects pre-fork worker models: ``gunicorn --preload``, Celery's prefork
pool, and :mod:`multiprocessing` with the ``fork`` start method (the default
on Linux before Python 3.14). Either construct the provider only inside each
worker, never in the parent before it forks, or start workers with ``spawn``
or ``forkserver``, which run a fresh interpreter::

    import multiprocessing
    multiprocessing.set_start_method('forkserver')

Security notes
==============

* IdP passwords and MFA stay in the browser; Python receives device and bearer
  tokens only.
* IdP credential endpoints require HTTPS, except loopback HTTP for local
  development. ``insecure=True`` applies only to QuestDB discovery transport.
* Attach the provider to ``https::`` / ``wss::`` transports. Over plaintext
  ``http::`` / ``ws::`` to a non-loopback host the Bearer token travels in
  cleartext; nothing rejects or warns about that configuration.
* Renderer callbacks receive bounded, native display-normalized but still
  untrusted IdP text. Prompt fields are single-line and visibly ASCII-escaped;
  identity/failure text may retain ordinary Unicode and HTML metacharacters.
  Custom renderers must apply their output sink's encoding (for example HTML
  escaping), and use only ``browser_target`` for links, browser opening or QR
  codes. A renderer that does so must also show ``browser_target`` as the place
  the user is sent: display URLs are length-capped before invisible characters
  are removed, so a padded ``verification_uri`` can display a different host
  from the one the browser opens.
  :func:`~questdb.auth.sanitize_display_text` remains available for raw
  values from other sources or defense-in-depth; it does not HTML-escape.
* Token-endpoint diagnostics are scanned before they reach renderers,
  exceptions, C views, or logs. If an IdP reflects the submitted device code or
  refresh token in a non-issued-token string, that occurrence is replaced with
  the literal ``[redacted credential]``. Issued ``access_token``, ``id_token``
  and ``refresh_token`` fields are preserved exactly so non-rotating refresh
  tokens remain usable. In Python the sanitized IdP fields are available as
  :attr:`OidcDeviceFlowError.error
  <questdb.auth.OidcDeviceFlowError.error>` and
  :attr:`OidcDeviceFlowError.error_description
  <questdb.auth.OidcDeviceFlowError.error_description>`.
* Avoid logging tokens, authorization headers, or PG connection parameters.
* The PG-wire adapters send the token as the ``_sso`` password, so remote hosts
  and hostnames such as ``localhost`` default to ``sslmode="verify-full"``.
  Numeric loopback IPs use ``prefer`` to support local servers without TLS. See
  :ref:`the PG-wire section <oidc_pgwire>`.
* ``Ctrl-C`` during :meth:`~questdb.auth.OidcDeviceAuth.sign_in` cancels only
  the current attempt and raises ``KeyboardInterrupt``. The provider remains
  open, every attached ``Sender``, :func:`questdb.connect` pool and reader stays
  usable, and a later ``sign_in()`` on the same provider can retry. A custom UI
  can provide the same non-destructive behaviour through
  :meth:`~questdb.auth.OidcDeviceAuth.cancel_sign_in`. Once the identity
  provider has issued tokens, the attempt is committed: a cancel that arrives
  later is a no-op, ``sign_in()`` completes, and the renderer receives its
  ``on_success`` or ``on_failure`` call. Use
  :meth:`~questdb.auth.OidcDeviceAuth.close` only for permanent shutdown.

Optional dependencies
=====================

OIDC itself uses the native client and needs no extra package. ``sqlalchemy``
and ``psycopg``/``psycopg2`` support the PG adapters. ``qrcode`` enables the
terminal QR; notebook PNG QR rendering additionally needs Pillow (install
``qrcode[pil]``). ``IPython`` enables the rich Jupyter renderer. All are
imported lazily.
