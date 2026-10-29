.. _changelog:


Changelog
=========

6.1.0 (unreleased)
------------------

Breaking changes
~~~~~~~~~~~~~~~~

A sender's own callbacks can no longer drive that sender
********************************************************

A :class:`Sender <questdb.Sender>`'s own ``connection_listener`` or
``error_handler`` could already not call ``flush()`` or ``close()`` on it, nor
``row()`` while auto-flush is on. The same now applies to ``row()`` with
``auto_flush=off``, ``dataframe()``, ``new_buffer()``, ``establish()``,
``transaction()`` and every
:class:`SenderTransaction <questdb.SenderTransaction>` operation: called from
the sender's own callback they raise :class:`QuestDBError <questdb.QuestDBError>`
with ``code`` set to ``QuestDBErrorCode.InvalidApiCall``, even while the sender
is idle. A 6.0 listener that records its events through its own sender with
``auto_flush=off`` now logs ``connection event listener failed`` and drops
those rows. Separately, the
read-only ``max_name_len``, ``protocol_version``, ``connection_events_dropped``
and ``connection_events_delivered`` -- and, over ``ws`` and ``wss``,
``auto_flush_bytes``, ``published_fsn()``, ``acked_fsn()``, ``poll_error()``
and ``error_events_dropped()`` -- now raise the same error, from any thread,
while the sender is inside a native call such as ``flush()``. Write listener
events to a different sender or queue them for the owning thread, and read the
counters when the sender is not flushing.

Likewise, a :class:`Buffer <questdb.ingress.Buffer>` that a native call is modifying --
the sender's internal buffer during ``flush()``, an explicit buffer during
``flush(buffer)`` (``clear=True``) or ``flush_and_get_fsn(buffer)``, or any
buffer during a row-serializing ``dataframe()`` (``SenderTransaction.dataframe()``
included) -- now raises that error from ``len()``,
``bytes()``, truthiness (``if buffer:`` / ``if sender:``), ``capacity()``,
``reserve()``, ``clear()`` and ``row()``, from any other thread, instead of
reading or modifying memory the native call is using. Code that a
``dataframe()`` itself runs on its own thread -- a cell's conversion, a
handler on the ``questdb`` logger -- follows the rules for re-entering
``row()`` instead: reads are answered, and every call that would change the
buffer is refused. A ``flush(buffer, clear=False)`` or
``flush_and_keep_and_get_fsn(buffer)`` only reads the buffer, so other threads
may keep reading it and flushing it with ``clear=False`` to other senders;
only modifying it raises.

An established sender rides out a rejected credential
*****************************************************

Once a QWP/WebSocket sender -- a :class:`Sender <questdb.Sender>` over
``ws::`` or ``wss::``, or one borrowed from a
:class:`QuestDB <questdb.QuestDB>` pool -- has connected, a server that
answers its reconnect with HTTP 401 or 403 no longer stops it. The sender
keeps reconnecting with capped backoff, keeps its unacknowledged rows, and
reports each failed attempt as a :attr:`ConnectionEventKind.AuthFailed
<questdb.ConnectionEventKind.AuthFailed>` event. A wait, a ``close()``
draining the queue, or a ``flush()`` blocked on a full store-and-forward
queue that times out meanwhile names the rejection. In 6.0 the first such
rejection made the sender terminal, and its next call raised ``AuthError``.

This applies to a fixed ``token=`` or ``username=`` / ``password=`` as well
as to token providers. While an identity platform or its signing keys are
unavailable, a server rejects even a valid credential for a while, and
store-and-forward must not drop a live producer over it. A connection that is
waited for still fails on the first rejection: a sender's first connection, an
eager :func:`questdb.connect`, a pool borrow that opens a connection.

To stop on a rejection instead, close the sender from your own code when you
see ``AuthFailed`` events -- not from inside its connection listener, which
cannot close its own sender. A store-and-forward slot left by an earlier
sender (``drain_orphans=on``) whose fixed credential is rejected is now left on
disk for the next session at once, rather than retried for the rest of this
one. See :ref:`token_rejections`.

Token-provider keys in a configuration string are rejected
**********************************************************

The configuration-string keys ``token_provider``, ``azure_resource``,
``azure_client_id`` and ``azure_credential`` now fail with
:class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
``QuestDBErrorCode.ConfigError`` on every protocol. In 6.0 an ILP/HTTP,
ILP/TCP or QWP/UDP :meth:`Sender.from_conf <questdb.Sender.from_conf>` or
:meth:`Sender.from_env <questdb.Sender.from_env>` ignored them silently, so a
configuration string shared with another client that reads them stops
working. Remove them and pass ``bearer_token_provider=`` instead; see
:ref:`refreshing_token_provider`.

Subclasses of Sender see its attribute guard
********************************************

:class:`Sender <questdb.Sender>` now has an instance dictionary, which holds
the bearer-token holder on PyPy, and defines ``__setattr__`` so that a
misspelt setting such as ``sender.request_timeout = 5000`` still raises
``AttributeError``. Python subclasses see the difference:

* On CPython 3.10 to 3.12, ``object.__setattr__`` and ``object.__delattr__``
  raise ``TypeError`` when applied to a sender, for example from a subclass's
  own ``__setattr__``. Call ``super().__setattr__`` instead, which works on
  every version.
* A class that defines ``__setattr__`` or ``__delattr__`` and comes after
  ``Sender`` in a subclass's bases -- ``Mixin`` in ``class S(Sender, Mixin)``
  -- makes defining the subclass raise ``TypeError``: ``Sender``'s own method
  comes first, so the mixin's would never run. List the mixin before
  ``Sender``.
* ``__slots__`` on a subclass no longer stops new attribute names, and cannot
  name ``__dict__``; ``vars(sender)`` returns the instance dictionary rather
  than raising ``TypeError``.

Query results refuse re-entry from the thread reading them
**********************************************************

Code that runs on the thread reading a query result while it reads -- a
finalizer, a ``gc`` callback or a signal handler, during a native read or
while a batch is decoded -- can no longer call ``QueryResult.close()``,
``QueryResult.cancel()`` or ``PooledReader.close()``, or pull another batch
from the same cursor: these raise :class:`QuestDBError <questdb.QuestDBError>`
with ``code`` set to ``QuestDBErrorCode.InvalidApiCall``. In 6.0 such a
``close()`` was accepted, and the cursor was freed once the decoder let go
of the batch. Close the result from the reading code once the read returns
-- a ``with`` block does -- or from another thread, whose ``close()`` and
``cancel()`` wait for the read in progress.

New
~~~

Rotating Bearer tokens (:class:`questdb.auth.RefreshingTokenProvider`)
**********************************************************************

New ``bearer_token_provider=`` on :func:`questdb.connect`,
:meth:`QuestDB.from_conf <questdb.QuestDB.from_conf>`,
:class:`Sender <questdb.Sender>`, :meth:`Sender.from_conf
<questdb.Sender.from_conf>` and :meth:`Sender.from_env
<questdb.Sender.from_env>`, for machine credentials that rotate: Microsoft
Entra ID managed identities and service principals, OAuth2 client credentials,
tokens from a secrets vault.

* :class:`questdb.auth.RefreshingTokenProvider` is a native, proactively
  refreshing cache in front of a token source you write -- for example one
  that calls Azure Identity, which stays your own dependency. It refreshes at
  about half each token's lifetime on its own thread, retries failures with
  backoff, refreshes early when a server rejects the token with HTTP 401, and
  never shows a token in an error, log line or ``repr``.
* Any callable returning a token, or an object with ``token()`` and an
  optional ``on_rejected()``, works as a provider too. Reusing the same Python
  provider across clients shares its native single-flight acquisition, including
  when passing repeated bound-method lookups. A ``RefreshingTokenProvider``
  such a provider calls -- ``bearer_token_provider=lambda: tokens.token()`` --
  still refreshes early when a server rejects its token with HTTP 401.
* Such a provider that knows retrying can never succeed raises its own
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.InvalidApiCall``, as a Rust provider returns
  ``InvalidApiCall``. Its clients then stop: a QWP/WebSocket sender, query or
  pool fails with ``ConfigError`` and stops reconnecting, and an ILP/HTTP
  flush fails at once. A token source cannot do this; close its
  ``RefreshingTokenProvider`` instead. An ``InvalidApiCall`` that a client
  call inside the provider raised -- a pool at its ``sender_pool_max`` cap, a
  client that is closing -- and that escapes ``token()`` counts as
  retryable.
* ``RefreshingTokenProvider.close(wait=True, timeout=30)`` optionally drains an
  active source with a deadline. A timeout leaves the provider closed and raises
  ``TimeoutError``. Sources must bound their own I/O: interpreter exit cannot
  safely abandon an executing Python callback even after provider close.
* While the interpreter exits, an application provider still supplies the
  token for a flush made on the main thread -- from ``logging.shutdown()``,
  or from an ``atexit`` callback registered before ``questdb`` was imported --
  so an ILP/HTTP flush there delivers its rows. From that point no Python
  token code runs on a native thread: a QWP/WebSocket client that has to
  connect at exit, or a ``RefreshingTokenProvider`` whose token needs a
  refresh, gets no token. Flush and close such clients before the interpreter
  exits; see :ref:`refreshing_token_provider`.
* An application provider that runs on your own thread -- in every ILP/HTTP
  flush, and in a connection the call waits for -- runs inside the client call
  that needs the token, and a Python signal handler can run there too.
  Neither may call into that client: a sender refuses such a call with
  ``InvalidApiCall``, so a ``SIGTERM`` handler that closes it mid-flush closes
  nothing; see :ref:`token_provider_reentrancy`. An exception a signal handler
  raises there is deferred to the native-return boundary, without repeatedly
  interrupting provider retries or swallowing signal-handler deadline
  exceptions.
* An application provider that runs on a native thread -- a QWP/WebSocket
  sender, query or pool reconnecting -- or a logging handler it triggers,
  that uses a :class:`~questdb.PooledSender` lease another thread is blocked
  in (for example ``wait()``) gives up after two seconds with
  ``QuestDBError(InvalidApiCall)`` instead of deadlocking both threads, as an
  OIDC callback does: that thread may be waiting for this very token. So does
  a ``db.close()`` made there that would wait for such a lease. If that
  refusal escapes ``token()``, it counts as a retryable failure, not as a
  request to stop; see :ref:`token_provider_reentrancy`.
* Pooled dataframe calls fail fast on native foreground OIDC failures rather
  than spinning through the reconnect budget.
* A successful OIDC refresh can revalidate the same still-valid token after a
  transient server rejection. Token-store stale-lock recovery preserves live
  displaced-owner captures as acquisition fences, and displaced refresh actions
  cannot clear or restore a successor's state. Concurrent writers in other
  clients must implement the same capture-fence rule.
* :class:`questdb.auth.TokenUnavailableError` classifies a source's failure
  as retryable or permanent; a connection the caller waits for retries only a
  retryable one.
* A store-and-forward slot left by an earlier sender (``drain_orphans=on``)
  rides out the server rejecting the provider's tokens -- at least 6
  rejections persisting for ``min(reconnect_max_duration_millis, 5 min)`` --
  before it is left on disk for the next session.
* A query whose failover gives up because the provider cannot supply a token
  raises :class:`~questdb.auth.TokenUnavailableError`, naming the cause,
  instead of the connection failure that started the failover. A result read
  through its Arrow C stream (``pyarrow.table(result)``,
  ``polars.DataFrame(result)``) can carry only the message: the consumer
  raises its own exception type.
* A provider used over ``http::`` or ``ws::`` is warned about, once per
  process, on the ``questdb`` logger: its tokens cross the network in
  cleartext.

Behaviour follows the cross-language dynamic-credential specification shared
with the Java client. No provider is selected from a configuration string:
``token_provider`` and the ``azure_*`` keys are rejected. See
:ref:`refreshing_token_provider`.

OIDC Authentication (:mod:`questdb.auth`)
************************************************

Interpreter shutdown suppresses new OIDC callbacks and drains every admitted
renderer or persistence-diagnostic callback before finalization, including
callbacks on daemon sign-in threads. Callbacks must return promptly; a blocked
renderer or logging handler can delay process exit.

New :mod:`questdb.auth` module backed by the native QuestDB client. It runs the
OAuth 2.0 Device Authorization Grant (RFC 8628), including from remote Jupyter
kernels, and supplies rotating Bearer tokens directly to QuestDB transports.

.. code-block:: python

    import questdb
    from questdb.auth import OidcDeviceAuth

    auth = OidcDeviceAuth.from_questdb("https://questdb.example.com:9000")
    auth.sign_in()
    db = questdb.connect(
        "wss::addr=questdb.example.com:9000;", oidc_auth=auth)

Highlights:

* :func:`questdb.connect`, :class:`questdb.Sender`,
  :meth:`questdb.Sender.from_conf`, :meth:`questdb.Sender.from_env` and
  :meth:`questdb.QuestDB.from_conf` accept ``oidc_auth=``. They retain the
  shared native provider and pull a fresh token for each connect or reconnect.
  It is mutually exclusive with a fixed ``token``, ``username`` or ``password``.
* :meth:`~questdb.auth.OidcDeviceAuth.sign_in` is the only interactive
  operation. :meth:`~questdb.auth.OidcDeviceAuth.token` and every transport
  path remain non-interactive, silently refreshing when possible and otherwise
  raising :class:`~questdb.auth.OidcInteractionRequired`.
* Silent refresh retains both the in-memory and persisted refresh token after
  retryable identity-provider responses (HTTP 408, 429 and 5xx, or OAuth
  ``temporarily_unavailable``), and later retries use exponential backoff. A
  status-less failure after request dispatch
  remains genuinely ambiguous: the possibly rotated parent is discarded and
  the call raises ``OidcInteractionRequired`` with that reason. A QuestDB 401
  only forces refresh when a refresh token exists; otherwise the still-valid
  access/ID token remains usable until expiry. Cached tokens also remain on the
  non-blocking fast path until their actual expiry instead of making every
  shared transport wait through a refresh during the 30-second skew window.
* A QWP/WebSocket close whose drain times out while reconnecting keeps
  failing returns the latest reconnect/OIDC cause, in either progress mode;
  manual progress observes ``close_flush_timeout_millis`` meanwhile instead of
  spinning indefinitely. A ``flush()`` that times out on a full
  store-and-forward queue (``sf_append_deadline_millis``) names that cause
  too, instead of advising a larger ``sf_max_total_bytes``, which cannot help
  while nothing is acknowledged.
* Auth failures are typed :class:`~questdb.auth.OidcError` subclasses of
  :class:`~questdb.QuestDBError`. Their ``code`` reports the failing call's
  native error category, not a recovery instruction: a flush with no usable
  credential reports ``SocketError`` although only a sign-in fixes it, so catch
  :class:`~questdb.auth.OidcInteractionRequired` (and check its
  ``acquisition_busy``) before retry logic keyed on ``code``. A
  transport attached with ``oidc_auth=`` can raise one from the same
  ``flush`` / ``dataframe`` / ``row`` / ``query`` / :func:`questdb.connect`
  call, so an existing ``except QuestDBError`` retry or dead-letter handler
  keeps catching auth failures; catch :class:`~questdb.auth.OidcError` (or a
  typed subclass such as :class:`~questdb.auth.OidcInteractionRequired`) for
  auth-specific handling.
* Token-endpoint diagnostics are credential-safe before they reach a renderer,
  exception, C view or log. If an IdP reflects the submitted device code or
  refresh token in a non-issued-token field, the occurrence becomes
  ``[redacted credential]``. Python exposes the sanitized fields as
  ``OidcDeviceFlowError.error`` and ``.error_description``. Issued token fields
  remain unchanged, including a non-rotating refresh token.
* OIDC discovery, endpoint validation, token selection, caching, refresh, and
  concurrency control use the same native implementation as the C/C++ clients.
  Discovery confirmation compares normalized origins plus the exact request
  target, so QuestDB's explicit ``:443`` matches an Entra/Google discovery URL
  that omits the default HTTPS port.
  The explicit :class:`~questdb.auth.OidcDeviceAuth` constructor takes its two
  endpoints as keyword-only arguments, preventing an accidental endpoint swap.
* OIDC does not survive ``fork()`` without ``exec()``. A provider inherited by
  a forked child is refused there, and once the parent has constructed any
  :class:`~questdb.auth.OidcDeviceAuth` (used or not), constructing a new one
  in a forked child raises :class:`~questdb.auth.OidcConfigError`. Pre-fork
  servers (``gunicorn --preload``, Celery prefork, :mod:`multiprocessing` with
  the ``fork`` start method) must construct the provider inside each worker or
  start workers with ``spawn`` / ``forkserver``; see :ref:`auth-fork`.
* OIDC scopes are preserved exactly for groups-mode token selection and the
  persisted token-store identity. Refresh requests intentionally omit ``scope``,
  matching the Java client, so the identity provider preserves the scope that
  was originally granted. Include ``openid`` explicitly when the identity
  provider requires it to issue an ID token.
* OIDC device-flow polling tolerates transient transport failures until the
  device code expires, matching the Java client. When no poll ever reached the
  token endpoint, the expiry reports that transport failure instead of
  ``expired_token``'s "you did not authorize in time", so a misconfigured or
  firewalled ``token_endpoint`` is named rather than blamed on the user. The
  error class and the ``expired_token`` tag are unchanged.
* A refresh token that is not printable ASCII is rejected when the identity
  provider returns it, rather than being cached, persisted, and then silently
  dropped by the stricter check applied when the store is read back — which
  left the credential unusable after a restart while the plaintext file stayed
  on disk.
* Opt-in :class:`~questdb.auth.FileTokenStore` persistence writes plaintext
  credentials atomically and coordinates refresh across processes. Its
  directories/files are owner-only (``0700``/``0600``) on Unix. Recovery of an
  insecure directory durably publishes distrust before tightening permissions
  and makes record deletion durable before removing the distrust marker.
  Non-Unix platforms reject mutation before changing the stored entry until an
  equivalent durable metadata barrier is available; reads, and therefore the
  empty ``.lock`` files of the shared coordination protocol, remain available,
  as does in-memory authentication.
  Its directory is overridable with the
  ``QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR`` environment variable, shared with the
  native client. Custom Python token stores are not supported by the native
  provider.
* PG adapters reject malformed driver-option keys and every destination alias,
  including ``unix_sock``; the validated ``host``/``hostaddr``/``port`` values
  are inserted last before the bearer password is handed to the driver.
* :meth:`~questdb.auth.OidcDeviceAuth.sign_in` fails up front when the
  configured token store cannot hold a credential — a directory whose
  owner-only permissions cannot be enforced, as on WSL ``drvfs`` without
  ``metadata``, CIFS/SMB with a fixed ``file_mode``, or vfat/exFAT. Persistence
  is opt-in, so this is reported rather than silently skipped; previously the
  sign-in succeeded, wrote nothing, and the whole device flow ran again on
  every start. The check runs before any device code is shown, and a provider
  with no token store is unaffected. Transient preflight contention and I/O
  failures are warnings rather than terminal configuration errors. Transient
  write failures (a full disk, an NFS blip) warn through the ``questdb`` logger and keep the in-process
  credential working.
* A ``FileTokenStore`` directory is expanded with :func:`os.path.expanduser`
  and made absolute at construction; one whose leading ``~`` cannot be
  expanded (an unknown ``~user``, or no resolvable home directory) is refused
  rather than creating a directory literally named ``~`` under the working
  directory and leaving a plaintext refresh token in it. On POSIX ``..``
  components are left for the operating system to resolve, as the native
  client does, so a path through a symlink names the same directory in both.
  The ``QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR`` override must already be
  absolute -- on Windows with a drive letter or UNC prefix, as the native
  client requires -- and is passed on unchanged. The ``ca_bundle`` argument
  of :class:`~questdb.auth.OidcDeviceAuth` takes the same path forms
  (``str``, ``bytes`` or any ``os.PathLike``) and is expanded and made
  absolute the same way.
* An OIDC flush or connect that needs a fresh token can wait up to the
  provider's ``timeout`` for the refresh -- or six times it behind a refresh
  already running on another thread -- regardless of the sender's
  ``request_timeout`` and ``retry_timeout``; see :ref:`auth-token-wait`.
* A persistence-warning handler or renderer callback that uses a
  :class:`~questdb.PooledSender` lease another thread is blocked in (for
  example ``wait()``) gives up after two seconds with
  ``QuestDBError(InvalidApiCall)`` instead of deadlocking both threads. So
  does a renderer callback that closes the :class:`~questdb.QuestDB` pool while
  another thread still holds a lease.
* Convenience adapters (:func:`~questdb.auth.sqlalchemy_engine`,
  :func:`~questdb.auth.psycopg_connect`) that wire the token into PG-wire as the
  ``_sso`` password — ``sqlalchemy_engine`` re-supplies a fresh, auto-refreshed
  token on every new pooled connection, ``psycopg_connect`` captures it at
  connect time. They take an ``OidcDeviceAuth`` or any provider
  ``bearer_token_provider=`` takes -- a ``RefreshingTokenProvider``, any other
  object with a ``token()`` method, or a function returning the token -- and
  refuse anything else with ``OidcConfigError`` up front: when
  ``sqlalchemy_engine`` builds the engine, and before ``psycopg_connect``
  connects. They authenticate
  remote PG servers with ``verify-full`` by
  default, which needs a trust root: libpq does **not** consult the operating
  system's certificate store unless told to, so without
  ``~/.postgresql/root.crt`` or ``PGSSLROOTCERT`` the connection fails with
  ``root certificate file ... does not exist``. Pass ``sslrootcert`` -- a CA
  file, or ``"system"`` with libpq 16 or later -- through ``connect_args``
  (``sqlalchemy_engine``) or as a keyword argument (``psycopg_connect``), as
  ``docs/auth.rst`` shows. Numeric loopback literals (``127.0.0.1``, ``::1``) resolve to
  ``prefer`` instead, so a local QuestDB without TLS still works; the name
  ``localhost`` does **not** — it keeps ``verify-full``, because its resolved
  addresses are not pinned. Use a numeric literal for local development, or
  pass an explicit ``sslmode``. Both adapters own the connection destination:
  a ``host``, ``hostaddr``, ``port``, ``service``, ``dsn``, ``conninfo`` or
  ``unix_sock`` in
  the driver passthrough (``connect_args`` / ``connect_kwargs``) raises
  ``OidcConfigError`` before any token is acquired, because the token is a
  bearer credential and SQLAlchemy merges ``connect_args`` over the arguments
  built from the validated URL. Pass ``host=`` / ``pg_port=`` instead. They
  also own the login: a ``user``, ``password``, ``dbname`` or ``database`` in
  the passthrough raises ``OidcConfigError`` up front too, instead of a bare
  ``TypeError`` after a token fetch (``psycopg_connect``) or a silent login as
  someone other than ``_sso`` (``sqlalchemy_engine``); pass ``database=``.
  Libpq connections explicitly pass an empty ``hostaddr`` to suppress an
  inherited ``PGHOSTADDR`` that could otherwise redirect the bearer password
  despite the validated ``host``. This is applied on every pooled connection.
  ``sqlalchemy_engine`` accepts a non-libpq ``drivername`` (``pg8000``, say)
  only with ``sslmode=None``, since such a driver takes no ``sslmode``.
* :meth:`~questdb.auth.OidcDeviceAuth.close` permanently closes a provider and
  cancels a device flow, silent-refresh coordination, or token-store lock wait
  running on another thread; ``OidcDeviceAuth`` is also a context manager.
  Operations on a closed provider -- including attaching it to a new
  ``Sender``, pool or reader -- raise the new
  :class:`~questdb.auth.OidcCancelledError`, except
  :meth:`~questdb.auth.OidcDeviceAuth.clear`, which stays available so the
  persisted credential can still be removed;
  :meth:`~questdb.auth.OidcDeviceAuth.cancel_sign_in`, which remains an
  idempotent no-op when no flow is running; and ``config``, which remains
  readable. ``Ctrl-C`` during ``sign_in()`` cancels only the current attempt and
  raises ``KeyboardInterrupt``; the provider remains open, already-attached
  ``Sender``/pool/reader instances remain usable, and a later ``sign_in()`` can
  retry on the same provider. Custom UIs can invoke the new attempt-scoped
  ``cancel_sign_in()`` method directly. Only ``close()`` is permanent.
  :meth:`~questdb.auth.OidcDeviceAuth.clear` never waits behind a
  ``sign_in()`` running on another thread: it raises
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.InvalidApiCall`` and clears nothing, so cancel the sign-in
  first or retry once it completes.
* On CPython, failed pending-call registration no longer strands a signal
  exception caught during a synchronous transport persistence diagnostic.
  An exception-safe native-return boundary delivers it without rerunning the
  signal handler; additional saved exceptions retain their context chain.
* Renderer prompts receive the device code's bounded lifetime and polling
  interval (``expires_in`` / ``interval``), matching the complete Java device
  challenge, plus ``browser_target``, the single natively vetted URL that
  built-in renderers use for links and QR codes. A prompted sign-in that is
  cancelled, closed or interrupted reports ``on_failure('Sign-in cancelled.')``
  so the renderer's output does not stay on "waiting".
* OIDC requires no additional Python dependency; ``sqlalchemy`` / ``psycopg`` /
  ``qrcode`` / ``IPython`` are imported lazily for optional conveniences.

See the :ref:`OIDC authentication guide <oidc_auth>` for details.

New ``ConnectionEventKind.CredentialUnavailable``
*************************************************

:attr:`ConnectionEventKind.CredentialUnavailable <questdb.ConnectionEventKind.CredentialUnavailable>`
is a new event kind reporting a token provider that failed to supply a
credential — an ``oidc_auth=`` provider with no cached or refreshable
credential, or a ``bearer_token_provider=`` that raised or returned no usable
token — either before any endpoint was dialled or when replacing a token a
server rejected with HTTP 401. It has no counterpart before 6.1, because token
providers did not exist: a listener written against 6.0 cannot have seen it.

``AuthFailed`` keeps its meaning: the server rejected a credential the client
presented, and ``host`` / ``port`` are always set. It is terminal before a
sender's first connection; once connected, a sender keeps reconnecting and
reports each rejected attempt (see "An established sender rides out a rejected
credential" above). ``CredentialUnavailable`` sets ``host`` and
``port`` to ``None`` when the provider failed before any dial; after a 401
they name the endpoint that rejected the previous token and ``cause_msg``
includes the 401. It reports the provider's classification in ``cause_code``,
which is what says whether the sender will carry on:

* ``SocketError`` — retryable, and the ordinary case. The sender keeps
  reconnecting so queued rows survive while the identity provider recovers or
  a human signs in, and nothing is raised to the caller. A foreground call
  such as :meth:`questdb.QuestDB.dataframe` raises instead: with
  ``oidc_auth=`` it fails fast, and with ``bearer_token_provider=`` it fails
  fast on a failure the provider classified permanent but first retries a
  retryable one for up to ``reconnect_max_duration_millis``. An ACK wait
  (:meth:`PooledSender.wait <questdb.PooledSender.wait>`) whose timeout expires
  meanwhile raises :class:`~questdb.auth.OidcInteractionRequired` -- ``code``
  ``FailoverRetry``, rows still queued -- when the provider needs a new sign-in.
* ``AuthError`` / ``ConfigError`` — the provider cannot recover in this
  process, so the reconnect is **terminal** and the sender stops. Reached by a
  permanently closed provider (``close()`` is one-way; cancelling one
  ``sign_in()`` attempt does not close an ``OidcDeviceAuth``), by a scope that
  cannot yield the required token kind, and by an application
  ``bearer_token_provider=`` that raised its own :class:`QuestDBError
  <questdb.QuestDBError>` with ``code`` ``QuestDBErrorCode.InvalidApiCall``,
  reported as ``ConfigError``. Queued rows are not deleted — a disk-backed
  store-and-forward slot stays drainable by a later process — but this process
  will not send them.

A listener that pages on a permanent stop must qualify on ``cause_code``, not
on the kind alone.

This matches the Java client, whose ``QwpCredentialUnavailableException`` is
likewise distinct from its ``QwpAuthFailedException``.

This is additive and needs no migration: existing enum ordinals are
unchanged, the new kind is appended as ``7``, and a 6.0 listener keeps
working untouched. A listener that wants to distinguish the two conditions
opts in by handling the new kind.

Other changes
~~~~~~~~~~~~~

- A connection that is waited for retries a token provider's failure within
  its budget only with initial-connect retry on -- a sender's
  ``initial_connect_retry=sync`` connect, which any ``reconnect_*`` key
  implies, or an eager :func:`questdb.connect` with
  ``initial_connect_retry=on`` -- and only a retryable failure. A
  ``bearer_token_provider=`` failure is retryable unless it is a
  :class:`~questdb.auth.TokenUnavailableError` with ``retryable=False`` or
  the provider's own ``InvalidApiCall``. An ``oidc_auth`` network error fails
  the connection at once.
  Without initial-connect retry, the connection makes one attempt.
  Background reconnects keep retrying every provider failure, except
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.InvalidApiCall`` that an application provider raises
  itself, which stops them.

- A 401 is retried with a refreshed token only when the server's
  ``WWW-Authenticate`` challenge allows it: no Bearer ``error``, or
  ``invalid_token``.

- Finalizers and garbage collection no longer block on, or free, a query
  cursor that another operation is using: an abandoned ``QueryResult``,
  ``PooledReader``, Arrow stream or batch iterator is reclaimed without
  waiting. Code on the reading thread can no longer close the result while
  it reads (see "Query results refuse re-entry from the thread reading them"
  above).

- A DataFrame error that gains a note about a partially consumed input
  stream keeps its exception class, ``in_doubt`` and ``sender_error`` instead
  of being rebuilt as a plain :class:`QuestDBError <questdb.QuestDBError>`, so
  ``except OidcError`` and ``except QuestDBServerRejectionError`` still match.

6.0.0 (2026-10-21)
------------------

Highlights
~~~~~~~~~~

- Query results as Python rows, without pyarrow or pandas:
  :meth:`QueryResult.iter_rows <questdb.QueryResult.iter_rows>` yields a
  tuple per row, and :meth:`QueryResult.columns <questdb.QueryResult.columns>`
  gives the column names and QuestDB types.
- Per-query timeouts through the ``query_timeout_ms`` configuration key, for
  servers that support them.
- :meth:`QuestDB.execute <questdb.QuestDB.execute>` returns the statement's
  ``EXEC_DONE`` report, including ``rows_affected``.
- ``row()`` on QWP senders writes UUID, IPV4, BINARY, CHAR, DATE, LONG256 and
  GEOHASH columns.
- UUID raw bytes use RFC 4122 byte order, the bytes of ``uuid.UUID.bytes``,
  everywhere they cross the API. This is a breaking change; see below.
- ``duckdb.from_arrow(result)`` works on duckdb 1.5.
- Requires QuestDB server version 10 or later.

Breaking changes
~~~~~~~~~~~~~~~~

- **A standalone QWP/WebSocket flush too large for one frame raises
  ``BatchTooLarge``**, as a pooled one does, instead of ``InvalidApiCall``.
  The message names the limit that applies: ``max_buf_size``, the server's
  advertised limit, or the store-and-forward segment capacity, which the
  standalone sender now also checks. Code that caught ``InvalidApiCall`` for
  this case must catch ``BatchTooLarge``.

- **A standalone QWP/WebSocket ``Sender`` keeps auto-flushing with
  ``auto_flush_rows=off`` and ``auto_flush_interval=off``.** Its byte
  trigger is now on by default (see the ``row()`` auto-flush entry under
  *Fixed*), so turning off the other two triggers leaves auto-flush on:
  ``Sender.auto_flush`` is ``True``, a batch that reaches 90% of the frame
  limit is published in parts, and ``flush_and_get_fsn()`` covers only the
  rows after the last of those parts. To batch by hand, set
  ``auto_flush=off``. ``Sender.auto_flush_bytes`` reports the threshold in
  effect: between ``establish()`` and ``close()`` that is 90% of the frame
  limit, or an explicit threshold lowered to it. Outside them the default
  reads ``None`` and an explicit threshold reads as configured.

- **A standalone QWP/WebSocket ``Sender`` refuses ``row()`` from its own
  ``error_handler`` or ``connection_listener`` while auto-flush is on**,
  raising ``InvalidApiCall`` before adding the row, as it already did for
  ``flush()`` and ``close()``. Reading ``auto_flush_bytes`` there is
  refused too. Such a callback runs while the native sender is borrowed,
  and auto-flush reads the frame limit from it on every row. An exception
  that escapes the callback is only logged through the ``questdb`` logger,
  so the row is lost unless the callback catches the error. This includes
  a handler on the ``questdb`` logger that writes its records through the
  same sender while no ``error_handler`` is set, because the sender then
  logs server rejections from inside that callback. Give such code a
  sender of its own.

- :meth:`QuestDB.execute <questdb.QuestDB.execute>` and
  :meth:`PooledReader.execute <questdb.PooledReader.execute>` return the
  statement's ``EXEC_DONE`` report as an
  :class:`ExecDone <questdb.ExecDone>` named tuple ``(op_type,
  rows_affected)`` instead of ``None``; ``None`` is now returned only for a
  statement that streamed a result set. The protocol does carry the count —
  the previous documentation saying otherwise was wrong. ``rows_affected``
  is meaningful for ``INSERT`` and ``UPDATE``, and ``0`` for a statement
  that carries no count, such as one executed at parse time; ``op_type`` is
  the server's operation-type byte, carried through as an opaque ``int``.
  The same report is on
  :attr:`QueryResult.exec_done <questdb.QueryResult.exec_done>` once a
  result has been drained, and stays there after the result is closed.

- A :class:`QueryResult <questdb.QueryResult>` cancelled before it was read
  can no longer be read: ``to_pandas()``, ``to_arrow()``, the ``iter_*``
  methods and the Arrow PyCapsule stream raise
  ``QuestDBErrorCode.InvalidApiCall`` instead of returning an empty result
  with no columns, which was indistinguishable from a statement that has
  no result set.

- **UUID raw bytes are now RFC 4122 byte order** (big-endian, the same bytes
  as ``uuid.UUID.bytes``) everywhere they cross the API. This affects Arrow
  and binary DataFrame columns you write, and the bytes you read back from a
  UUID column. If your code byte-swapped to match QuestDB's old wire layout,
  remove that swapping. Passing ``uuid.UUID`` objects is unaffected, in
  ``row()``, in object-dtype columns, and as query binds — so are the UUIDs
  that plain ``to_pandas()``, without ``dtype_backend``, gives you.
  Arrow-shaped results you saved from 5.0 need a one-time fix before you
  write them back; see *UUIDs in Arrow-shaped results saved from 5.0* below.

- **A 16- or 32-byte Arrow column is no longer assumed to be a UUID or a
  LONG256.** Width alone no longer decides the type. Native Arrow input
  writes such a column as BINARY with a warning, and a pandas frame refuses
  it, as *Unlabelled 16- and 32-byte columns* below explains. To get the
  old behaviour back, say what the column is: build it with ``pa.uuid()``,
  attach ``questdb.column_type`` field metadata, or pass
  ``schema_overrides={'col': 'uuid'}`` (or ``'long256'``).

  ``pa.uuid()`` needs pyarrow 18 or newer, and the column must carry the
  extension *type* rather than the metadata key — a pandas ``ArrowDtype``
  holds a type and no field, so the key does not survive. ``uuid.UUID``
  cells and ``schema_overrides`` work on every pyarrow version.

- **If you write UUIDs as raw bytes, change the byte order too.** Two of the
  changes above affect such a column, and only one of them produces an
  error. Your unlabelled 16-byte column is now written as BINARY or
  refused, and the usual fix for that is
  ``schema_overrides={'col': 'uuid'}``. It
  gets the UUID column back and leaves the byte order still wrong. Any 16 bytes are a valid UUID, so
  nothing fails at any point — the values are simply stored reversed.

  Change how you produce the bytes at the same time::

      value.int.to_bytes(16, 'little')   # before: QuestDB's old wire layout
      value.bytes                        # now: RFC 4122 big-endian

  Keeping the old form stores
  ``123e4567-e89b-12d3-a456-426614174000`` as
  ``00401714-6642-56a4-d312-9be867453e12``.

- **Unlabelled 16- and 32-byte columns.** A label is ``pa.uuid()``,
  ``questdb.column_type`` field metadata of ``uuid`` or ``long256``,
  ``schema_overrides``, or a ``df.attrs['questdb']`` claim; another
  extension type or ``questdb.column_type`` value is not one. What happens
  to an unlabelled column depends on where it comes from. Native Arrow input — a ``pa.Table``,
  ``pa.RecordBatch``, ``pa.RecordBatchReader`` or any other
  ``__arrow_c_stream__`` producer — keeps its field metadata, so an
  unlabelled column there is written as BINARY, and once the write is done
  the ``questdb`` logger records a WARNING naming the column and the
  remedies. A pandas frame,
  fully Arrow-backed or not, refuses it with ``BadDataFrame``: a pandas
  ``ArrowDtype`` holds no field metadata, so the column may be a UUID or
  LONG256 column that lost its label on the way into pandas. To write
  either width as BINARY without the warning or the refusal, label it as
  plain bytes: ``schema_overrides={'col': 'binary'}``, or
  ``{"questdb.column_type": "binary"}`` field metadata on native Arrow
  input. Either sends the column as it is, without a copy.

- **UUIDs in Arrow-shaped results saved from 5.0 are stored reversed when
  written back.** In 5.0, ``to_arrow()``, ``iter_arrow()``,
  ``pa.table(result)``, and ``to_pandas()`` / ``iter_pandas()`` with
  ``dtype_backend='pyarrow'`` or ``types_mapper=pd.ArrowDtype`` returned UUID
  columns as ``arrow.uuid`` values in QuestDB's old wire layout, and 5.0's
  ``dataframe()`` wrote those bytes back unchanged. This release reads every
  ``arrow.uuid`` column as RFC 4122. If you kept such a result from 5.0, in a
  Parquet, Feather or Arrow IPC file or pickled, every UUID in it is stored
  reversed when you write it back now. Nothing raises and nothing is logged.
  The saved result has the same Arrow type and metadata as one from this
  release, so the client cannot tell the two apart and fix it for you.
  ``to_polars()`` returned the same bytes as ``Binary``. Those land as
  BINARY, and are stored reversed too once you add
  ``schema_overrides={'col': 'uuid'}``.

  Fix each saved 5.0 result once, before you write it back. Results read
  with this release are already in the right order, and so are the
  ``uuid.UUID`` values that plain ``to_pandas()`` gave you in 5.0. With
  pyarrow 18 or newer::

      import uuid
      import pyarrow as pa

      def fixed_uuids(column):
          return pa.array(
              [None if v is None else uuid.UUID(bytes=v.bytes[::-1])
               for v in column.to_pylist()],
              pa.uuid())

      # A pyarrow Table
      table = table.set_column(table.schema.get_field_index('col'), 'col',
                               fixed_uuids(table['col']))
      # A pandas frame from to_pandas(dtype_backend='pyarrow')
      df['col'] = pd.arrays.ArrowExtensionArray(
          fixed_uuids(pa.array(df['col'])))
      # A polars frame from to_polars(); write it with
      # schema_overrides={'col': 'uuid'}
      pl_df = pl_df.with_columns(pl.col('col').map_elements(
          lambda b: b[::-1], return_dtype=pl.Binary))

  Keep the pandas column Arrow-backed, as above. An object column of
  ``uuid.UUID`` would send the frame to the NumPy planner, which refuses any
  LONG256 column in it. On the Arrow path such a LONG256 column lands as
  BINARY unless you pass ``schema_overrides={'col': 'long256'}``.

- **Arrow field metadata that does not fit the column's Arrow type is
  refused.** Every column of a result from ``to_arrow()``, ``iter_arrow()``
  or ``pa.table(result)`` carries a ``questdb.column_type`` metadata key
  naming its QuestDB type. The key stays on the field when you change the
  column's type with ``Table.cast`` and a schema built by
  ``field.with_type(...)``, or with ``pa.concat_tables(...,
  promote_options='permissive')``. 5.0's ``dataframe()`` ignored a key that
  no longer fitted and wrote the column by its Arrow type. It now raises
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.ArrowIngest`` and sends nothing. An INT column widened
  to ``pa.int64()``, for example, gives::

      [column='n'] column 'n' has column_type='int', which is not applicable to Arrow type Int64

  The check covers BYTE, SHORT, INT, CHAR, IPV4, SYMBOL, UUID, LONG256 and
  GEOHASH columns. A ``df.attrs['questdb']`` claim that does not fit is
  logged instead and the write goes ahead; see *A claim the column's type
  can never carry is now logged* under *Fixed*. The difference is
  deliberate. The refusal is the native client's rule for Arrow field
  metadata in every language, and a field claim goes stale only when a
  retyped column keeps its old field, while ``df.attrs`` survives
  ``astype`` and arithmetic. Once you change a column's type, drop its
  stale metadata::

      i = table.schema.get_field_index('col')
      table = table.cast(table.schema.set(
          i, table.schema.field(i).remove_metadata()))

- **A ``df.attrs['questdb']`` claim with a version other than 1 is
  ignored.** The version is checked rather than just carried, so that a
  future claim vocabulary is not applied under today's rules. Frames from
  ``to_pandas()`` always carry ``'version': 1``, and a hand-written mapping
  without the key still reads as version 1. A claim that is not read, or a
  column entry in it of the wrong shape or with an unknown kind, is
  reported through the ``questdb`` logger and the write goes ahead. The
  full shape is documented on ``QuestDB.dataframe()``::

      df.attrs['questdb'] = {'version': 1,
                             'columns': {'src_ip': {'kind': 'ipv4'}}}

- **``to_pandas()`` returns a GEOHASH column as a signed integer**, sized by
  the column's precision: ``int8`` up to 7 bits, ``int16`` to 15, ``int32``
  to 31, ``int64`` to 60. This matches what ``to_arrow()`` and
  ``to_pandas(dtype_backend='pyarrow')`` already give you. Code that reads
  the dtype of a GEOHASH column will see a signed type where it saw an
  unsigned one. Writing a frame back keeps the column GEOHASH either way,
  so frames saved from 5.0 still write back as GEOHASH, including after
  ``df.convert_dtypes(dtype_backend='pyarrow')``.

- **DECIMAL now requires CPython.** Writing a DECIMAL reinterprets the
  memory of a ``decimal.Decimal`` using the layout CPython's ``_decimal``
  module gives it, which is undefined behaviour anywhere else. ``row()`` now
  raises ``ValueError`` naming the interpreter instead, and ``dataframe()``
  raises ``QuestDBError`` (``BadDataFrame``) whose message includes that
  explanation. Over QWP/WebSocket, ``dataframe()`` converts the column
  through pyarrow and is unaffected. This affects the PyPy wheels this package publishes: DECIMAL
  was never safe there, and now says so rather than writing corrupt values.

- **QWP-only column values raise ``QuestDBError`` on ILP senders**, where
  they previously raised ``TypeError``. See the new ``row()`` types below.

- **``df.attrs['questdb']`` is now cheap to copy and refuses the ordinary
  mutating ``dict`` operations.** pandas deep-copies the whole of ``attrs``
  every time it propagates a frame, once per column while slicing or
  exporting, which made those passes quadratic.
  ``pyarrow.table(df)`` on a 1024-column frame took
  730 ms, of which 710 ms was copying the claim; it now takes 26 ms, against
  23 ms with no claim at all.

  It is still a ``dict`` — it indexes, iterates, compares, pickles and
  serializes to JSON as before, and a hand-written ``dict`` is read the same
  way. A pickled frame loads without this package installed: the claim comes
  back as a plain ``dict``, which pandas copies the ordinary way until the
  table is read from QuestDB again. To change what a frame claims, assign a
  whole new mapping — the nested ``columns`` mapping is frozen too, so unpack
  that one as well::

      df.attrs['questdb'] = {
          'version': 1,
          'columns': {
              **df.attrs['questdb']['columns'],
              'grade': {'kind': 'char'}}}

  Or use ``schema_overrides`` / ``symbols``, which outrank the claim.

- **``ipaddress.IPv4Interface`` cells are rejected** rather than silently
  written with their network prefix discarded. The old messages read as
  contradictions, because ``IPv4Interface`` subclasses ``IPv4Address`` and
  they listed ``IPv4Address`` as accepted. The new ones name the prefix as
  the reason and give the fix: ``value.ip`` for ``row()``, and
  ``df['col'] = df['col'].map(lambda value: value.ip)`` for ``dataframe()``.

- **``Buffer.clear()`` now raises while a row is being written.** A column
  value whose conversion runs Python code — a ``uuid.UUID`` or an
  ``ipaddress.IPv4Address`` subclass — can call back into the buffer that
  ``row()`` or ``dataframe()`` is part-way through filling. Clearing there
  destroyed the rows already buffered and left the write half-finished, so
  it now raises :class:`QuestDBError <questdb.QuestDBError>` with ``code``
  set to ``QuestDBErrorCode.InvalidApiCall`` and says which call is in
  progress. Flushing from the same place, and returning a pooled sender to
  its pool, are refused for the same reason. Clearing a buffer that holds
  finished rows, or one whose last ``row()`` failed, is unaffected.

- **``Sender.row()`` raises while ``Sender.dataframe()`` on the same sender
  is still running, over ``tcp``, ``http`` and ``udp``.** On those
  protocols the frame is written into the sender's own buffer, which it
  holds from the moment it starts, and it can run your Python before its
  first row — a logging handler on the ``questdb`` logger, for example. A
  row written from there could not be flushed until the frame returned, so
  ``row()`` raises :class:`QuestDBError <questdb.QuestDBError>` with
  ``code`` set to ``QuestDBErrorCode.InvalidApiCall`` before writing
  anything. A logging handler that ships records through the sender it is
  logging about needs a ``Sender`` of its own. Over ``ws`` and ``wss`` the
  frame goes out on a connection of its own, and a row written from inside
  it is accepted and flushed as usual.

- If a ``ws::`` or ``wss::`` configuration contains ``max_in_flight`` or
  ``in_flight_window``, remove it. These options are no longer supported and now
  raise :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.ConfigError`` during startup.

- **``QuestDB.close()`` stops waiting for leases after 60 seconds.** It used
  to wait for every ``sender()`` and ``reader()`` lease however long it
  stayed open, which hung for ever on a lease nobody was going to close. It
  now raises :class:`QuestDBError <questdb.QuestDBError>` when a lease is
  still open after 60 seconds, even one another thread is still using for a
  long load. Close your leases before closing the handle, or call
  ``close()`` again once they are closed. ``close(timeout=None)`` waits
  for leases without a limit, as before. A call in progress, such as a long
  ``QuestDB.dataframe()``, is still waited for without a limit. See *Fixed*
  below for the rest of the change, and *New* for ``timeout``.

- **With greenlets, ``QuestDB.close()`` refuses to wait for another
  greenlet's call.** This covers gevent, eventlet, and ``greenlet`` used
  directly. Calls are counted per OS thread, and every greenlet on a
  thread counts as the same caller. A ``close()`` from one greenlet
  therefore raises :class:`QuestDBError <questdb.QuestDBError>` with
  ``code`` set to ``QuestDBErrorCode.InvalidApiCall`` while another
  greenlet on that thread is suspended inside a call on the handle or on
  one of its leases. 5.0 waited instead. Call ``close()`` once in-flight
  work has finished, or call it again after the refusal.

- To read a ``QueryResult`` into polars without pyarrow, use
  ``polars.DataFrame(result)`` (polars 1.32 or later). The 5.0.0 notes said to
  use ``polars.from_arrow(result)``, but on Polars 2.0 that call returns a
  struct ``Series``, not a ``DataFrame``. On Polars 1.21 and older, it raises
  an error. On Polars 1.22 to 1.31, it returned wrong ``SYMBOL`` values
  without an error, so when a polars before 1.32 reads the result directly
  (``polars.DataFrame(result)``, ``polars.from_arrow(result)``), the call
  now raises ``InvalidApiCall``; use ``to_polars()`` there. A stream that
  reaches polars through another library is not checked. Ingesting polars
  frames is tested with polars 1.32 or later.

- The ``dataframe`` extra now requires ``pandas>=2.2.0`` and
  ``pyarrow>=14.0.1``, the oldest versions that CI tests. The old minimums did
  not work: with pyarrow before 14, pyarrow sources, ``iter_polars()`` and
  ``pa.table(result)`` failed. With pandas before 1.5, every DataFrame load
  failed with an ``AttributeError``; it now raises a clear error. Reading
  ``DECIMAL`` columns with a precision of 18 or less through pyarrow needs
  pyarrow 20 or later. See :doc:`installation`.

  If the extra moves you from pandas 2.0 or 2.1 to 2.2 or later,
  ``QuestDB.dataframe()`` writes a pyarrow-backed pandas frame differently.
  On pandas 2.0 and 2.1 the frame goes through the NumPy path; the client
  does not check the pandas version, so this still happens when you install
  pandas yourself. On pandas 2.2 and later it takes the Arrow path, where:

  * an index other than a ``RangeIndex`` is written as columns, an unnamed
    level as ``__index_level_0__``. A filtered or sorted frame has such an
    index; ``reset_index(drop=True)`` drops it.
  * ``symbols=False``, or a ``symbols`` list that leaves a dictionary column
    out, sends that column as ``VARCHAR``, not ``SYMBOL``. A ``symbols``
    list may name a dictionary column.
  * ``table_name`` is required; the index name is not used.
  * ``schema_overrides`` is accepted.

- A second read of a ``QueryResult`` now names the method that read it first,
  for example "QueryResult was already read by to_pandas()". A read after
  ``close()`` now says that the result is closed.
  Code that matched the old texts ("QueryResult already consumed",
  "QueryResult cursor was closed") should check
  ``err.code == QuestDBErrorCode.InvalidApiCall`` instead.

New
~~~

- :class:`QueryResult <questdb.QueryResult>` can now be read as rows of
  Python objects, without pyarrow or pandas. :meth:`iter_rows
  <questdb.QueryResult.iter_rows>` streams one batch at a time and yields a
  tuple per row, with ``None`` for SQL NULL; :meth:`columns
  <questdb.QueryResult.columns>` gives ``(name, type_name)`` pairs using the
  QuestDB DDL spelling (``VARCHAR``, ``TIMESTAMP_NS``, ``IPv4``,
  ``GEOHASH(8c)``, ``DECIMAL(18,2)``, ``DOUBLE[]``), so they can be compared
  against ``tables()`` / ``information_schema.questdb_columns()`` without
  normalising. Two things the wire does not carry: a ``DECIMAL`` is reported
  at its storage width's precision (18, 38 or 76), and an array column as
  ``DOUBLE[]`` / ``LONG[]`` whatever its dimensionality. Asking for the
  columns does not consume the rows, so a DB-API-style caller can read its
  ``description`` first; a ``LIMIT 0`` schema probe returns its connection
  to the pool, while a result with unread rows needs ``cancel()`` before
  ``close()`` to do the same. Intended for row-shaped consumers and small
  result sets — metadata lookups, test rows, ``limit 0`` schema probes;
  large analytical results still belong on ``to_pandas()`` / ``to_arrow()``,
  which stay columnar.

  Timestamps come back timezone-aware in UTC, matching what ``to_arrow()``
  labels them. ``TIMESTAMP_NS`` is rounded down to microseconds, because
  ``datetime`` has no nanosecond field. A ``CHAR`` whose code point is 0 is
  ``None``, matching how the server's own text protocol renders it. A
  ``DECIMAL`` keeps its scale. ``BOOLEAN``, ``BYTE`` and ``SHORT`` have no
  NULL representation in QuestDB, so an inserted ``null`` reads back as
  ``False`` / ``0``.

  ``UUID`` columns come back as the canonical RFC-4122 value, agreeing with
  ``to_arrow()``, with ``to_pandas()`` and with the server's own text output.

- Queries can be given their own timeout instead of running under the
  server-wide ``query.timeout``. ``query_timeout_ms`` in the connection
  string sets a default for every query on that handle (``0``, the default,
  means none), and ``timeout=`` on ``query()`` / ``execute()`` of both
  :class:`QuestDB <questdb.QuestDB>` and
  :class:`PooledReader <questdb.PooledReader>` overrides it per query, with
  ``timeout=0`` clearing it. An ``int`` is milliseconds (any integer type);
  a ``datetime.timedelta`` or ``pandas.Timedelta`` works too, with a
  positive value below one millisecond rounded *up* to 1 ms. The server
  applies no ceiling, so a value above ``query.timeout`` is honoured — which
  is the point for a long transform.

  Requires a server advertising ``CAP_QUERY_TIMEOUT``
  (questdb/questdb#7768). Against an older one a non-zero timeout is refused
  before anything is sent, with ``QuestDBErrorCode.UnsupportedServer``, rather
  than silently running under the server default; the connection is
  untouched by the refusal, so raise the server's ``query.timeout`` instead,
  or pass ``timeout=0`` on the same handle or lease. On expiry the error
  carries the new :class:`QuestDBErrorCode.QueryTimeout
  <questdb.QuestDBErrorCode>` and the pooled connection stays open and
  authenticated, so the next query runs on it without reconnecting. **Do
  not retry a write after a timeout:** a
  statement that completes past its timeout is reported as ``EXEC_DONE``
  rather than a timeout, precisely so a retry cannot apply it twice, and a
  DDL / ``INSERT`` / ``UPDATE`` that timed out waiting for the table writer
  may still be applied afterwards.

- A pooled connection whose query ended in a server-side error — a parse
  error, a timeout — now goes back to the pool instead of being dropped, on
  every path: the result reached a terminal frame, so the connection is as
  healthy as after a clean drain. A :class:`PooledReader
  <questdb.PooledReader>` lease likewise stays usable after such an error.
  A result abandoned mid-stream still drops its connection.
- ``datetime.date`` and ``decimal.Decimal`` are bind parameter types. A date
  is bound as ``DATE``; a decimal as the narrowest ``DECIMAL`` that holds it
  — 64, 128 or 256 bits for up to 18, 38 or 76 digits — with its fractional
  digits as the scale, so it reaches a ``DECIMAL`` column exactly instead of
  through a ``DOUBLE``. ``NaN``, infinities, and values needing more than 76
  digits or a scale above 76 raise ``ValueError``.

- **``QuestDB.close(timeout=...)`` sets how long closing may wait.** With
  no argument, ``close()`` waits for a call in progress until it returns
  and for an open lease up to 60 seconds. A number of seconds bounds both;
  ``0`` closes only if nothing is in flight. ``None`` waits for both
  without a limit. When the limit runs out, ``close()`` raises
  :class:`QuestDBError <questdb.QuestDBError>` and the handle stays
  closing; a later ``close()`` picks the wait up again. To bound leaving
  a ``with`` block, call ``db.close(timeout=...)`` as the block's last
  statement: the close the block makes on its way out waits only for
  what is left of that timeout.

- **``query_timeout_ms`` sets a per-query timeout.** The configuration
  string key bounds each statement sent through ``query()``,
  ``execute()`` or a reader lease, in place of the server's own
  ``query.timeout``. A statement that runs past it raises
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to the
  new ``QuestDBErrorCode.QueryTimeout``. Do not retry a write on that
  error: a statement that timed out waiting for the table writer may
  still be applied. It needs a server that supports per-query timeouts.
  QuestDB 10.0.1 does not, so while the key is set every statement fails
  with ``QuestDBErrorCode.UnsupportedServer`` before it is sent.

- **``row()`` on QWP senders now writes UUID, IPV4, BINARY, CHAR, DATE,
  LONG256, and GEOHASH columns.** Pass ``uuid.UUID``,
  ``ipaddress.IPv4Address``, ``bytes``, ``bytearray`` or ``memoryview``
  directly. For the rest use the new :class:`Char <questdb.Char>`,
  :class:`DateMillis <questdb.DateMillis>`,
  :class:`Long256 <questdb.Long256>` and :class:`Geohash <questdb.Geohash>`
  wrappers. These types need a QWP sender (``udp::``, ``ws::`` or
  ``wss::``) and QuestDB 10 or newer, the first production QWP
  implementation. The four wrappers are also exported by the deprecated
  ``questdb.ingress`` shim, so ``from questdb.ingress import *`` now
  brings in ``Char``, ``DateMillis``, ``Geohash`` and ``Long256`` too.

- **``schema_overrides`` accepts ``'uuid'`` and ``'long256'``.** Both work on
  fixed-size and variable-length binary columns — which is how polars frames
  get there, since polars has no fixed-size binary dtype. Every non-null
  value must be exactly 16 or 32 bytes.

- **``schema_overrides`` accepts ``'binary'``.** It says a 16- or 32-byte
  binary column holds plain bytes, so the column is written as BINARY
  without the refusal or warning an unlabelled one gets, and without a copy.
  It applies to binary columns only, and is refused on a column whose
  ``pa.uuid()`` type or ``uuid`` / ``long256`` field metadata already says
  otherwise.

- **A ``('geohash', bits)`` override accepts an unsigned integer column.**
  Until now it had to be signed (``int8`` to ``int64``), and an unsigned
  column was refused with "override 'geohash' is not applicable". It now
  goes out as the signed integer of the same width holding the same bits,
  as a GEOHASH claim on an unsigned column already does, in pandas,
  pyarrow and polars frames. A one-shot stream such as a
  ``RecordBatchReader`` still needs a signed column.

- **DataFrame BINARY columns accept ``bytearray`` and ``memoryview`` cells**
  as well as ``bytes``. An object column of ``numpy.bytes_`` now works too.

- **Query results keep their column types when you write them back.**
  ``to_pandas(dtype_backend=...)`` and ``iter_pandas(dtype_backend=...)`` now
  attach ``df.attrs['questdb']``, which plain ``to_pandas()`` already
  carried, and ``dataframe()`` reads it. Without it, IPV4, CHAR and GEOHASH
  columns went back out as plain integers, and with ``'numpy_nullable'``
  UUID and LONG256 went back out as BINARY too, so a new destination table
  was created with the wrong types.

  All three built-in backends round-trip these five types, in whatever shape
  they hand them back — Arrow-backed columns, NumPy columns, and the object
  columns of Python ints or ``bytes`` that plain ``to_pandas()`` and
  ``'numpy_nullable'`` produce. LONG256 in particular now survives a plain
  ``to_pandas()``: that backend returns it as Python ints, and values must
  satisfy ``0 <= value < 2**256``.

  Editing the frame is safe. A column you dropped, renamed or retyped simply
  loses its claim, and ``symbols`` / ``schema_overrides`` outrank it. BYTE
  and SHORT columns still come back as INT, and INT as LONG. A custom
  ``types_mapper`` gets the same metadata, but the claim only applies if the
  dtype it picked can still hold the values — map one of the five to a float
  or a string and the column lands as that dtype implies.

- **An all-null column claimed as UUID, LONG256, IPV4, CHAR or GEOHASH keeps
  its claim** and is written as the claimed type. On its own such a column
  names no type, so it would be left out of the write and out of the table
  the write creates. Claims of other kinds do not rescue an all-null column;
  it is still dropped from the write.

- Applications may now create a ``QueryResult`` on one thread and process it on
  another, including through its Arrow stream. Hand it off with normal thread
  synchronization and never use it from two threads at once. If it came from a
  ``PooledReader``, keep that reader on its original thread until processing
  finishes.

- WebSocket connections keep a dictionary of ``SYMBOL`` values to avoid sending
  repeated text in full. Repeated values do not grow it, but a long-lived
  connection fills it after 2,000,000 distinct values or 256 MiB of symbol
  text. The client then raises
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.SymbolDictFull``. Pooled clients replace the connection;
  standalone senders should call ``close_drain()`` and reconnect. Before
  retrying, check ``err.in_doubt``; when it is true, some rows may already be
  stored.

- ``duckdb.from_arrow(result)`` now works on duckdb 1.5. Before, it failed
  with "QueryResult already consumed", because duckdb 1.5 requested the
  stream more than once. ``QueryResult`` now has ``__arrow_c_schema__``,
  which duckdb 1.5 uses for the schema. ``pa.schema(result)`` and
  ``polars.Schema(result)`` call it too. It reads the first batch, so after
  it, read the result only through the Arrow stream (``pa.table(result)``,
  ``polars.DataFrame(result)``, duckdb). ``to_pandas()``, ``to_polars()``
  and the other readers then raise ``InvalidApiCall``; run the query again
  to use them.

- New ``polars`` and ``duckdb`` extras install the oldest tested versions or
  newer: ``pip install questdb[polars]`` installs ``polars>=1.32.0`` and
  ``pyarrow>=14.0.1`` (``to_polars()`` and ``iter_polars()`` need pyarrow), and
  ``pip install questdb[duckdb]`` installs ``duckdb>=1.2.0``.

Fixed
~~~~~

- **QWP/WebSocket ``row()`` auto-flush no longer fails on rows that each fit
  a frame** (`#161 <https://github.com/questdb/py-questdb-client/issues/161>`_).
  In 5.0 a standalone ``ws::`` / ``wss::`` ``Sender`` had no byte trigger, so
  600 rows of 4 KiB already exceeded the server's default 2 MiB frame limit,
  and a pooled lease checked its byte trigger only after appending, so one
  large row could push a batch past the limit. Either way the whole batch
  failed: the standalone sender discarded it, and a pooled lease kept it
  but could never publish it. Now both senders auto-flush at 90% of the
  frame limit, and publish as soon as the size estimate reaches the full
  limit, even with ``auto_flush_bytes=off``. When the estimate is past the
  limit or the whole buffer does not fit in one frame, the rows buffered
  before the newest one go out as a frame of their own and the newest row
  starts the next batch. The estimate counts each symbol value at its
  largest encoded size, so it is not below the size of the encoded frame
  unless store-and-forward has fallen back to sending full symbol
  dictionaries. ``len()`` of a QWP/WebSocket ``Buffer`` or standalone
  ``Sender`` with symbol columns therefore reads higher than before, and
  frames that the byte trigger publishes carry fewer rows. Until the server
  advertises its frame limit, as while a lazily connected lease is still
  connecting, both senders plan for the limit a QuestDB server has by
  default, 2 MiB less 14 bytes, instead of the local queue limit, so the
  rows buffered meanwhile still fit a default server once its limit is
  known.

  Whatever a QWP/WebSocket ``row()`` raises, the row was not added.
  ``BatchTooLarge`` means
  it does not fit in a frame on its own, or the rows before it no longer
  fit, for example after the frame limit shrank on reconnect, in which case
  those rows are discarded and the error says how many. After any other
  auto-publish error a pooled lease keeps the rows before it and a
  standalone sender clears them. In 5.0 a pooled lease also kept the row
  itself. It now leaves it out, as on every other error, so a failed split
  no longer leaves the lease holding a batch too large for one frame.

- A :class:`PooledReader <questdb.PooledReader>` that had become reachable
  only through a reference cycle could be collected by the cyclic garbage
  collector while one of its results was being decoded, on the same
  thread; its finaliser then freed the native cursor under the decoder and
  the process crashed. ``to_pandas()`` and ``iter_pandas()`` were exposed.
  The free is now deferred until the decoder
  has let go of the batch, and the consumer sees a plain
  ``QuestDBError('cursor is closed')``. For the same reason,
  :meth:`QueryResult.cancel <questdb.QueryResult.cancel>` called from a
  signal handler while a batch of that result is being decoded now raises
  ``QuestDBErrorCode.InvalidApiCall`` instead of invalidating the batch.
- On POSIX, a ``KeyboardInterrupt`` that arrived while
  ``QueryResult.close()`` or ``PooledReader.close()`` waited for another
  thread's in-flight batch (Ctrl-C while joining a worker, say) was lost,
  and the connection leaked. The interrupt now propagates at once, and the
  worker stops after its current batch and releases the connection.
- The default ``iter_pandas()`` stopped as if the result were complete when
  the result was closed underneath it; it now raises
  ``QuestDBError('cursor is closed')``, like the other iterators.
- A sub-microsecond ``pandas.Timedelta`` given as a sender timeout or
  interval (``auth_timeout``, ``retry_timeout``, ...) became 0 ms instead of
  rounding up to 1 ms.
- ``copy.copy()`` of a :class:`QueryResult <questdb.QueryResult>` produced
  a second result over the same cursor: reading from both lost rows, and
  dropping the copy closed the original's stream. It now raises
  ``TypeError``.
- :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` did not count Arrow
  ``string_view`` columns, plain or dictionary-encoded, as strings when
  resolving ``symbols``. Naming one in ``symbols=[...]`` raised
  ``Bad argument `symbols` ... is not a strings column``, ``symbols=True``
  stored it as ``VARCHAR`` instead of ``SYMBOL``, and ``symbols=False`` left
  a dictionary-encoded one as ``SYMBOL``. They now behave like ``string``
  columns.

- **Python ``True`` in an object-dtype DataFrame column was written as
  ``False``** by ``QuestDB.dataframe()``, ``PooledSender.dataframe()`` and
  the WebSocket ``Sender.dataframe()`` in 5.0. An object column of Python
  bools stored every row as ``false``, and a Python bool inside an
  object column of integers stored ``0`` in place of ``1``. Nothing was
  raised. NumPy and Arrow bool columns, and the ILP path, were not
  affected. Rows written this way in 5.0 hold ``false`` or ``0`` where
  the frame held ``True``; re-ingest them from the source if they matter.

- **An object cell that replaces itself in the frame no longer crashes the
  interpreter.** ``QuestDB.dataframe()``, ``PooledSender.dataframe()`` and
  the WebSocket ``Sender.dataframe()`` read an object-dtype column through
  the frame's own array. In 5.0, code that a cell ran while it was being
  read, such as a ``__class__`` property consulted by ``isinstance()``,
  could replace that cell in the array, freeing it while the client still
  used it. UUID and ``datetime`` object columns were affected, and so was
  the check that picks an object column's type. The client now holds its own
  reference to each cell while it reads it.

- **A column name containing a NUL character (``'\0'``) no longer aborts the
  process or lands under a shortened name.** ``QuestDB.dataframe()``,
  ``PooledSender.dataframe()`` and the WebSocket ``Sender.dataframe()`` pass
  a polars, pyarrow or Arrow-backed pandas frame, or a polars Series, to the
  client through the Arrow C Data Interface, which carries a name only up to
  its first NUL. In 5.0 such a name in a polars frame or Series, including a
  struct field name, aborted the Python process inside polars. In a pyarrow
  Table, RecordBatch or RecordBatchReader, or an Arrow-backed pandas frame,
  the column was written under the name cut at the NUL, so ``'a\0b'``
  became ``a``. The same
  happened to the name of a pandas index other than a ``RangeIndex``, which
  that route writes as a column. These frames now raise
  :class:`QuestDBError <questdb.QuestDBError>` before anything is sent. A
  column or index name gets the ``InvalidName`` error that ``row()`` raises
  for it, and a polars struct field name gets a ``BadDataFrame`` error. For
  other Arrow sources (for example DuckDB, nanoarrow or arro3) the client
  does not read the names before the export, so it cannot see the NUL.

- **A polars ``Object`` column gets a clear error on every DataFrame
  route.** polars infers ``Object`` for a list of ``uuid.UUID`` or
  ``ipaddress.IPv4Address`` values and exports it as an Arrow
  ``fixed_size_binary(8)`` column of handles to objects in its own process,
  with nothing else marking it. Stored, those would be meaningless BINARY
  blobs. ``QuestDB.dataframe()``, ``PooledSender.dataframe()`` and the
  WebSocket ``Sender.dataframe()`` refuse such a column with
  ``QuestDBErrorCode.ArrowUnsupportedColumnKind``, the code 5.0 refused it
  with, and now say why and what to do instead. That covers a polars
  ``DataFrame``, ``LazyFrame`` or ``Series`` with an ``Object`` column, and
  an 8-byte fixed-size binary column from any other source: a pyarrow
  ``Table`` such as ``df.to_arrow()`` of a polars frame, a
  ``RecordBatchReader``, a pandas ``ArrowDtype`` column, or any object with
  ``__arrow_c_stream__``. To write the UUIDs or addresses, pass
  ``df.to_pandas()`` instead, or convert them first: UUIDs to 16-byte
  ``pl.Binary`` with ``schema_overrides={'col': 'uuid'}``, addresses to
  ``pl.UInt32`` with ``schema_overrides={'col': 'ipv4'}``. Genuine 8-byte
  values land as BINARY once the column is variable-length binary:
  ``pa.binary()``, ``pl.Binary``, or an object column of ``bytes``.

- Direct QWP DataFrame ingestion reports ``in_doubt=True`` when a failure
  follows an earlier batch that may have committed, including local validation
  and Arrow stream failures. Delivery status is aggregated by the DataFrame
  call across its internal checkpoints; low-level sender errors retain their
  current-operation scope. The client does not automatically replay the whole
  source after publication. A consumed one-shot stream retains its fresh-reader
  guidance.

- On the guarded ``QuestDB.dataframe()``/C-ABI path, addressable malformed
  Arrow streams from hand-rolled C Data Interface exporters (for example
  nanoarrow, DuckDB, or arro3) are rejected before the known panicking
  arrow-rs operations covered by native preflight. For example, a batch whose
  slice extends past a column raises :class:`QuestDBError
  <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.ArrowIngest`` before import. Producer-owned pointers,
  allocations, and strings must still satisfy the Arrow ABI; dangling
  pointers, undersized allocations, unterminated strings, and concurrent
  producer mutation are outside this guarantee. Ordinary pandas, Polars, and
  pyarrow exports do not normally emit the hand-rolled batch-slice shape.

- Python extension builds now use the committed ``questdb-rs-ffi`` Cargo
  lockfile and require its Arrow dependency graph to remain at 59.0.0. Wheel
  and source-distribution builds fail when that lockfile drifts instead of
  silently compiling against a different Arrow implementation. The published
  ``questdb-rs`` crate keeps its existing compatible Arrow version range.

- **NumPy and pandas integers now work everywhere a Python ``int`` does in the
  round-trip vocabulary.** One rule covers every number this client reads out
  of a ``df.attrs['questdb']`` claim, a ``schema_overrides`` entry, or one of
  the QWP-only wrapper classes: a whole number that is not a boolean. A
  ``numpy.int64`` ``precision_bits`` — which is what a claim rebuilt from
  array metadata carries, and what ``df.loc[i, 'bits']`` hands you — was
  dropped along with the rest of the claim, and the column was created as
  LONG where the caller asked for GEOHASH. ``Long256``, ``DateMillis`` and
  ``Geohash`` take the same numbers now. ``True`` is still refused everywhere.

- **``QuestDB.close()`` from inside one of the handle's own calls is refused
  before anything is published.** The refusal is decided under the lock that
  publishes ``close()``'s state, so a ``close()`` on another thread can no
  longer see the handle closing, wait for that to clear, and return reporting
  success against a handle that is still open — nor can the two ends of that
  window wait on each other.

- **A Ctrl-C during ``QuestDB.close()`` no longer leaves the handle unable to
  close.** The native teardown runs without the GIL, so a Ctrl-C there is
  raised as soon as it finishes. That used to skip recording the close as
  finished: every later ``close()`` then waited for a "concurrent close()"
  that did not exist, and the handle's callbacks stayed referenced for good.
  The close is now recorded first. The ``KeyboardInterrupt`` still comes out
  of ``close()``, and a later ``close()`` returns at once.

  The same holds for a Ctrl-C at any other point in ``close()``, or in the
  close a ``with QuestDB(...)`` block makes on its way out. One landing just
  as ``close()`` took over the handle used to skip the native teardown
  altogether, leaving the same stuck handle and leaking its connections.
  And one landing as ``close()`` let go of the handle's internal lock could
  leave that lock held, so every other thread using the handle blocked for
  good, which usually showed as a program that would not exit.

- **Returning a pooled sender to its pool cannot raise.** Every refusal a
  lease makes belongs to ``close()``, which runs them before it releases. The
  release path is also the deallocation path, where a raise would skip the
  return to the pool and leave ``QuestDB.close()`` waiting for a lease that
  can never come back.

- **A transaction whose commit cannot flush is over.** ``commit()`` used to
  mark the transaction complete before the flush that carries it, so a flush
  that failed left the transaction closed with its rows still buffered, for
  the next ordinary flush to send outside the transaction you asked for. A
  commit that reached the wire and failed now ends the transaction, and one
  refused before it got that far leaves the transaction open for you to
  finish or roll back. A caller who caught the error and called ``commit()``
  again now gets ``Transaction already completed`` where it previously made
  a second attempt.

- **A rollback requested while a row or dataframe is being written is
  refused.** In 5.0.0, a ``rollback()`` called from code that ``row()``
  runs while writing, such as a column value's conversion, cleared the
  buffer under the row being written, and the interrupted ``row()`` then
  failed with ``Can't rewind to the marker``. Now ``rollback()`` called from
  code that ``row()`` or ``dataframe()`` runs while writing raises
  ``InvalidApiCall`` before it changes anything. When that error
  propagates, the ``with`` block rolls the transaction back as usual. If the
  caller catches it and the block exits normally, the transaction commits.

- **A completed transaction object cannot append rows outside a transaction
  or be entered again.** ``row()``, ``dataframe()`` and ``__enter__()`` now
  raise ``Transaction already completed`` after either commit or rollback.
  Previously those writing methods accepted more data after the sender had
  already left transaction mode, leaving it in the ordinary buffer to be sent
  without the transaction the caller had requested.

- **A transaction refuses every competing call while its commit is in
  progress.** ``commit()`` releases the GIL during HTTP I/O, so another call
  through the same transaction could previously overwrite its state, append
  to the buffer, or clear it while the flush still borrowed it. Such calls now
  raise ``Transaction commit is already in progress`` until the commit has
  published its success or failure state.

- **A pooled lease cannot be returned to the pool from inside its own
  ``dataframe()``.** The plan build runs your Python — reading ``attrs``,
  sniffing object cells, pulling ``__arrow_c_stream__`` — and closing the
  lease from there left the rest of that call working against a lease that
  was already back in the pool, so every later use in the enclosing block
  raised ``Sender is closed``. Closing a *different* lease from there is
  unaffected: it has nothing to do with the call that is running.

- **A sender lease is used by one thread at a time, and every call on it
  holds it for the whole call.** ``dataframe()`` now holds the lease's lock
  for the whole load, as its other methods already did through their own
  work, so a call on the same lease from a second thread waits until the
  running one returns. A ``close()`` from another thread during a load
  therefore waits for the load, then flushes the lease's rows. To ingest
  from several threads, share the ``QuestDB`` handle and borrow one lease
  per thread; code that runs inside a lease's call, such as the Arrow
  stream a load reads or a handler on the ``questdb`` logger, which a
  load logs its warnings to, must not wait on another thread using that
  lease.

- **A sender lease garbage-collected without ``close()`` reports the rows
  it discards.** Such a lease returns its connection to the pool without
  sending the rows still buffered in it, and nothing said so. It now logs
  how many rows it discarded through the ``questdb`` logger at ``WARNING``.
  Close every lease, or use it in a ``with`` block.

- **``close()`` says which buffer it holds to "no row part-way through".**
  It is the sender's internal buffer, because that is the buffer
  ``close(flush=True)`` flushes. A buffer of your own from ``new_buffer()``
  is neither flushed nor checked there — the sender is never told which
  buffers exist. Nothing is lost if you close mid-row on one of those: every
  byte stays in your buffer. What you lose is the sender that could have
  sent it, so finish the row and flush before closing. This is now written
  down on ``close()`` rather than left to be discovered.

- **``bytes(sender)`` and ``len(sender)`` are answered mid-row, not
  refused**, and the answer may end mid-line. They read and act on nothing,
  so a snapshot taken from inside a column conversion is the honest state of
  the buffer at that moment; every call that would *change* the buffer is
  still refused there.

- **``QuestDB.close()`` from inside a pooled lease's own call is refused
  instead of hanging.** ``close()`` waits for outstanding leases to come
  back, and a lease's ``row()`` runs your Python for every column value
  whose conversion is not pure C. Closing the handle from there waited on
  the very frame that would return the lease — a warning every five seconds
  and no return. It now raises, and says to close the handle after the call
  returns.

- **``QuestDB.close()`` is one-way and its wait is bounded.** The
  first ``close()`` that proceeds — one refused outright, from inside
  the handle's own calls or callbacks, changes nothing — puts the
  handle into a closing state it never leaves: new calls and leases
  are refused with ``QuestDB is closing``, while work already in
  flight drains — a call in progress (``dataframe()``, ``query()``,
  …) runs to completion, and an outstanding lease keeps working until
  its holder closes it. The exception is ``PooledSender.dataframe()``,
  which forwards to ``QuestDB.dataframe()`` over its own pooled
  connection: started after ``close()``, it is new work and is refused;
  one already running finishes. The native pool is torn down by the first
  ``close()`` that finds nothing using the handle, or when the handle
  itself is collected.
  With no ``timeout``, ``close()`` waits for a call in progress until it
  returns. It waits for leases too, but not for ever: a lease held by the
  calling thread, or by one that has since finished, can never be
  returned, and neither is distinguishable from a lease still in use.
  While a lease is still outstanding after 60 seconds, ``close()`` raises
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.InvalidApiCall``, saying how many leases and how
  many calls are still outstanding — so close every ``sender()`` and
  ``reader()`` lease before closing the handle. ``close()`` cannot
  interrupt a call, so a ``dataframe()`` reading a stream that never ends
  keeps it waiting; pass ``timeout`` (see *New*) to bound that too. While
  it waits, it reports what it is still waiting for through the
  ``questdb`` logger at ``WARNING``, every five seconds for the first
  minute and then once a minute (these notices were ``UserWarning``\ s
  before, which ended the wait at the first notice under ``-W error``).
  The handle stays closing: close the remaining leases and call
  ``close()`` again to finish the teardown. No ``close()`` returns success unless the
  teardown has run — when several race, one runs it and the others
  return once it is done.
  Called from inside one of the handle's own ``error_handler`` /
  ``connection_listener`` callbacks, ``close()`` never waits — while
  the callback runs, that thread delivers nothing, including whatever
  a wait would need. It closes immediately when nothing is using the
  handle, refuses when leases or calls are outstanding, and returns
  without waiting when a concurrent close is already running —
  possibly before that close finishes; the handle only moves toward
  closed, never back to open.

- **Leaving a ``with questdb.connect(...) as db:`` block on an
  exception no longer replaces it with a close failure.** The frames
  unwinding out of the block are often the ones holding the leases
  ``close()`` waits for, so the close could fail on account of the
  very exception being reported — and an ``except`` clause around the
  block is written for the original, not for a shutdown complaint that
  follows from it. ``__exit__`` still closes; when the close cannot
  finish it reports through the ``questdb`` logger and lets the
  original exception through. Leaving the block normally still raises,
  because a lease left open there is a leak worth hearing about.

- **Every call that would change or end the buffer is refused while a row
  is being written**: ``clear()``, ``flush()``, ``flush_and_get_fsn()``,
  ``flush_and_keep_and_get_fsn()``, ``close()``, ``close_drain()``,
  ``commit()``, ``transaction()``, entering a transaction, ``rollback()`` of
  a transaction that was never entered, and ``dataframe()``, on senders and
  on pooled leases alike. A column value whose conversion runs Python can
  call back into the sender that is part-way through a row, and each of
  these would have destroyed or reordered what that row was writing.
  ``close_drain()`` is the sharpest of them: draining stops the sender
  accepting anything further, so the row still being written can never be
  finished and the complete rows buffered before it go with it — after
  which ``close(flush=True)`` reported success having sent nothing. A
  transaction entered from there swallowed the whole frame, whether
  ``transaction()`` made it at that point or earlier, or it was constructed
  directly as ``SenderTransaction``: a ``dataframe()`` part-way through its
  plan build still has the clear buffer a transaction requires, and the
  rollback that ends such a block then discarded it. Rolling back a
  transaction that was never entered discarded the frame the same way.

- **Bulk GEOHASH values retain the permissive narrowing contract.** The client
  validates the declared precision and the integer carrier width, but does not
  check individual NumPy, Arrow or object-column values against that
  precision. A wrong precision can therefore be accepted and store a different
  GEOHASH location or ``NULL``: for example, ``32`` is accepted under
  ``GEOHASH(5b)`` even though it needs six bits. Only the low
  ``ceil(precision / 8)`` bytes are encoded, and the exact result is
  unspecified. This is a semantic data-integrity risk, not a memory-safety
  risk for otherwise valid NumPy/Arrow buffers. The scalar
  :class:`Geohash <questdb.Geohash>` wrapper remains strict, and malformed
  custom Arrow C Data structures remain invalid input.

  Byte-aligned GEOHASH precisions now carry an explicit validity bitmap even
  when every row is present. This keeps the maximum legitimate value
  distinct from the all-ones null sentinel without adding a value scan.

- **A claim the column can never carry is reported the same way by both
  DataFrame planners**, for all five kinds rather than only for GEOHASH. A
  claim the column *does* carry stays quiet, including the ``uuid.UUID`` and
  ``ipaddress.IPv4Address`` object columns plain ``to_pandas()`` hands back.

- **A query result whose schema changes between batches is refused** on the
  pandas NumPy backend. Every batch after the first was decoded against the
  first one's columns, and the only guard compared byte widths, which agree
  for any same-width type swap. A GEOHASH precision or DECIMAL scale that
  changed mid-stream also left the frame's ``df.attrs['questdb']``
  disagreeing with its own values.


- **A claim the column's type can never carry is now logged.** The write still
  goes ahead as the column's own type implies — a frame retyped since you
  read it must not fail on that account — but a claim guaranteed to do
  nothing, such as a float column under ``geohash``, is a mistake
  rather than drift, and the two were indistinguishable in silence. A claim
  the column does carry, and one whose column has gone, stay quiet.

- **A claim a string column cannot carry is now logged on the Arrow path
  too.** A ``df.attrs['questdb']`` claim naming ``uuid``, ``long256``,
  ``ipv4``, ``char`` or ``geohash`` on a column whose dtype holds no Arrow
  type was dropped there without a word, while the NumPy planner said so.
  On pandas 3 that includes an ordinary string column, and those reach the
  Arrow columnar path, so this is the common way to meet it. Both planners
  now emit a warning-level record on the ``questdb`` logger.

  Its message starts with ``questdb: column``. It is deliberately a log
  record rather than a ``UserWarning``: ``-W error`` must not turn a valid
  frame into ``QuestDBErrorCode.InvalidApiCall`` or stop its write. Logging
  has no warnings-style deduplication: every write emits one record for every
  claim it cannot apply, so repeated writes of the same frame repeat the
  notice. Cast the column to a type the kind fits, name the type outright
  with ``schema_overrides``, or drop the claim. To silence these notices
  instead, configure the standard-library logger::

      import logging

      logging.getLogger('questdb').setLevel(logging.ERROR)

  A claim whose column has been dropped or renamed is still ignored in
  silence. That is ordinary schema drift, and surviving it quietly is what
  the claim is for.

- **An object pretending to be a ``decimal.Decimal`` is refused.** DECIMAL
  cells are encoded by reading the object's raw memory, and the check in
  front of that was ``isinstance``, which asks the object for its
  ``__class__`` and believes it. An object setting ``__class__ = Decimal``
  sent the encoder walking unrelated memory: a short one crashed the
  interpreter with no traceback, a longer one read heap contents into the
  mantissa, once surfacing a heap address in an error message. The check now
  reads the object's real type, which cannot be forged. Genuine ``Decimal``
  subclasses still work.

- **An ``int`` too wide for a LONG column now says so.** On a QWP sender
  ``row()`` raises ``OverflowError`` pointing at
  :class:`Long256 <questdb.Long256>`, and ``dataframe()`` raises
  ``QuestDBError`` pointing at the claim that makes the column a LONG256.
  Both used to surface CPython's bare "int too large to convert".

- **``QuestDB.dataframe()`` explains ``table_name_col`` properly.** It writes
  one table per call and has never accepted that argument, but a column it
  could not type by itself raised first and suggested
  ``schema_overrides`` — which the same call then refused. It now checks
  ``table_name_col`` up front and tells you to split the frame and make one
  call per table. To vary the table name per row, use ``Sender.row()``.

- **Malformed round-trip metadata is skipped on both ingest paths**, as the
  contract always said. A ``kind`` that was not hashable (a list left by a
  JSON round trip, say) raised ``TypeError`` from the Arrow path, and an
  ``attrs['questdb']`` that was not a dict raised ``AttributeError`` from
  the NumPy one. Both paths now read the claim through one shared entry
  point, so they cannot drift apart.

- **DATE columns are written differently by the two ingestion styles**, which
  is now documented. ``row()`` takes the new ``DateMillis`` wrapper.
  ``dataframe()`` takes the column's Arrow type — ``pa.timestamp('ms')``,
  ``pa.date32()`` or ``pa.date64()``, in a fully Arrow-backed frame or in a
  mixed one — so there is no DATE cell type and no ``'date'`` kind for
  ``schema_overrides``. What has no route of its own to DATE is the NumPy
  ``datetime64[ms]`` dtype, which widens to TIMESTAMP, and its tz-aware
  ``datetime64[ms, tz]`` form, which is rejected. Both styles need a QWP
  sender.

- **A DATE column survives a read-modify-write on every pandas backend.**
  Reading back, a DATE column arrives as ``pa.timestamp('ms', 'UTC')``, so
  ``to_arrow()`` and ``dtype_backend='pyarrow'`` feed it straight back as
  DATE. Plain ``to_pandas()`` gives a NumPy ``datetime64[ms]`` column,
  which used to go back out as a microsecond TIMESTAMP with nothing said,
  auto-creating the destination table with the wrong column type. The
  ``df.attrs['questdb']`` claim that comes with the frame names the column
  DATE, and ``dataframe()`` now puts the Arrow type back on before writing.
  A column retyped to another unit since it was read is drift and still
  goes out as its own type implies.

- If a DataFrame load fails, batches waiting in memory are now discarded. Most
  loads commit once at the end, but very large Arrow loads may commit an earlier
  checkpoint. If a later batch fails, that prefix from the same DataFrame call
  remains, so retrying the whole DataFrame can duplicate rows.

- When ``sf_dir`` enables disk-backed delivery, shutdown now obeys its configured
  timeout and recovery is safer after a crash or an incomplete final write.

- A result read through the Arrow stream (``polars.DataFrame(result)``,
  ``pa.table(result)``, duckdb) now releases its connection when the consumer
  reads the last batch, not when it releases the stream. A reader lease from
  ``QuestDB.reader()`` can run its next query as soon as the stream is
  drained.

- ``to_pandas(dtype_backend=...)`` now checks its arguments before it reads
  the result. Before, an invalid value consumed the result.

- ``QueryResult.__arrow_c_stream__`` now ignores ``requested_schema``, also
  one that does not match the result. ``pa.table(result, schema=...)`` casts
  the result; ``pa.RecordBatchReader.from_stream(result, schema=...)``
  (pyarrow 15 or later) returns the result's own schema, so check the schema
  you get. Before, any ``requested_schema`` raised ``NotImplementedError``.

- If the polars ``Categorical`` fast path of ``to_polars()`` or
  ``iter_polars()`` fails, the client now issues a ``RuntimeWarning``. It still
  builds the frame with ``polars.from_arrow``, but it no longer hides the
  failure. ``iter_polars()`` warns once and builds the remaining frames the
  same way, cast to the ``Categories`` of the frames before, so
  ``polars.concat`` still stitches them. If that cast fails too, the warning
  says so: those frames have a plain ``Categorical`` dtype, and
  ``polars.concat`` with the frames before fails.

- The docs said that a failover during a ``__arrow_c_stream__`` read raises
  ``FailoverWouldDuplicate``. The consumer raises its own exception type, and
  only the message contains ``[FailoverWouldDuplicate]``. polars before 1.37.1
  raises a ``PanicException``, which ``except Exception`` does not catch.

- When an Arrow stream read of a ``QueryResult`` fails, the consumer now
  raises the stream's error. Before, ``pa.table(result)`` raised
  ``SystemError`` instead, a later ``close()`` from another thread could
  hang, and ``QuestDB.dataframe(result)`` could crash the process.

- With duckdb before 1.5, ``duckdb.from_arrow(result)`` no longer crashes
  the process when the query fails on its first fetch, for example on a
  missing table. duckdb raises its own error, which does not contain the
  query's error; duckdb 1.5 raises the query's error.

- Under ``-W error`` (or pytest's ``filterwarnings = error``), a
  ``QueryResult`` that is dropped without ``close()`` is now closed before its
  ``ResourceWarning`` raises. Before, a ``QuestDB.reader()`` lease then said
  that the previous result was still open, not that its connection was
  terminal.

Faster
~~~~~~

- **Writing rows over QWP/WebSocket is no longer quadratic in the number of
  rows buffered.** ``row()`` sets a rewind point before each row, so that a
  row which fails part-way can be taken back out of the buffer. On a
  WebSocket buffer, setting that point copied everything already buffered,
  so each row cost more than the one before it, and filling a batch took
  time proportional to the square of its size. The rewind point now records
  only lengths; the change is in the bundled ``c-questdb-client``. This
  applies to ``Sender.row()`` over ``ws::`` and ``wss::``, to
  ``PooledSender.row()`` on a lease from ``QuestDB.sender()``, and to a
  buffer from such a sender's ``new_buffer()``. With CPython 3.14 on an
  arm64 Mac, a row of one symbol and five columns cost about 5 µs with
  2,000 rows buffered and about 62 µs with 40,000; it now costs under 1 µs
  at either size. ILP buffers were not affected.

- **Writing a frame whose ``df.attrs['questdb']`` is a plain ``dict`` is no
  longer quadratic in the column count.** pandas 2.2 and later deep-copy the
  whole of ``attrs`` into every Series they derive, and writing a frame
  derives one per column. A claim from ``to_pandas()`` declines to be copied,
  but one that is a plain ``dict`` (from a pickle, or written or replaced by
  hand) was copied in full once per column. ``dataframe()`` now freezes such
  a claim for the length of the write, on a shallow copy of the frame; the
  caller's frame and claim are left as they are. With pandas 3.0.3 and 1000
  columns, ``Buffer.dataframe()`` drops from 944 ms to 63 ms, and
  ``QuestDB.dataframe()`` from 1395 ms to 498 ms for a NumPy-backed frame
  and from 1352 ms to 492 ms for an Arrow-backed one. Each is about the time
  with no claim.

- **Reading round-trip metadata is no longer quadratic in the column count.**
  Reading it off an Arrow schema and applying it to a pandas frame both
  re-read ``schema.names`` and ``frame.dtypes`` once per column. At 1024
  columns the read pass drops from about 85 ms to 0.7 ms, and
  ``iter_pandas`` was paying it per batch.

- **A streaming read builds ``df.attrs['questdb']`` once per result**, not
  once per batch. One schema covers every batch, and the claim is now shared
  rather than copied. At 1024 columns that was 1.2 ms per batch on
  ``iter_pandas``, so a 10-million-row read in 10,000-row batches spent
  about 1.2 seconds on nothing else.

- **The DataFrame IPV4 builder is about 3x faster per cell**, from roughly
  95 ns to about 30. Its type test looked ``IPv4Address`` and
  ``IPv4Interface`` up through the ``ipaddress`` module on every cell,
  costing about 65 ns; both are now bound once at import, and an exact type
  test settles the common cell in under 2 ns. What remains per cell is the
  ``int()`` conversion, which is a Python-level call and dominates what is
  left.

- **``Buffer.row()`` cell dispatch is cheaper.** The QWP-only types moved out
  of the inlined dispatch function, and the wrapper classes are now tested
  before ``uuid.UUID`` and IPV4. A ``Geohash`` cell is about 33% cheaper and
  an ``IPv4Address`` cell about 15%, and the common ``int`` / ``float`` /
  ``str`` path no longer has that code laid out around it.

Build & dependencies
~~~~~~~~~~~~~~~~~~~~

- The bundled ``c-questdb-client`` native library is upgraded to 7.2.0.

5.0.0 (2026-07-27)
------------------

Highlights
~~~~~~~~~~

- One QWP/WebSocket handle for everything: :func:`questdb.connect` returns
  a pooled, thread-safe :class:`QuestDB <questdb.QuestDB>` that streams
  rows, bulk-loads DataFrames, and runs SQL queries.
- Acknowledged, durable ingestion: rows publish into a store-and-forward
  queue with automatic reconnect and replay; opt into disk persistence
  with ``sf_dir`` to survive client restarts.
- Server rejections are never silent: every rejection is pushed to a
  handler (or the ``questdb`` logger), with terminal failures also raised
  from the affected sender.
- Query egress: SQL with bind parameters streamed back as Arrow record
  batches into pandas, polars, or pyarrow — pyarrow-free by default.
- Columnar DataFrame ingestion for pandas / polars / pyarrow and any
  Arrow C Data Interface source.
- Requires QuestDB server version 10 or later


Breaking changes
~~~~~~~~~~~~~~~~~

- The public API now lives at the top level of the ``questdb`` package:
  ``from questdb import Sender, QuestDB, QueryResult, ...`` (the compiled
  module is now the private ``questdb._client``). ``questdb.ingress`` remains
  as a deprecated compatibility shim re-exporting the 4.x ILP/HTTP and
  ILP/TCP surface (``Sender``, ``Buffer``, ``Protocol``, ``IngressError``,
  ``VERSION``, ...) with a single import-time ``DeprecationWarning``; new
  code imports from ``questdb``.
- The QWP/WebSocket entry point is :func:`questdb.connect`, returning a
  :class:`QuestDB <questdb.QuestDB>` handle. The handle lends
  row-building senders (:meth:`QuestDB.sender <questdb.QuestDB.sender>`), bulk-loads DataFrames
  (:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`), runs queries (:meth:`QuestDB.query <questdb.QuestDB.query>`) and
  lends reader leases (:meth:`QuestDB.reader <questdb.QuestDB.reader>`). Besides the configuration
  string, :func:`questdb.connect` also accepts the equivalent keywords
  (``host=``, ``port=``, ``tls=`` plus any further settings).
- The sender borrowed from the pool is a lean pooled row sender: it
  exposes ``row()``, ``dataframe()``, ``flush()``, ``wait()``, the FSN
  delivery receipts (``flush_and_get_fsn``,
  ``flush_and_keep_and_get_fsn``, ``published_fsn`` / ``acked_fsn``,
  ``await_acked_fsn``), lease-scoped rejection polling (``poll_error``,
  ``error_events_dropped``) and ``close()``, and is not a ``Sender``
  subclass — the standalone-only API such as ``establish()``, manual
  progress or explicit buffers simply does not exist on it. One
  concurrency rule applies everywhere: borrow one sender per thread.
  Pooled row senders auto-flush by default at 1,000 rows, 100 milliseconds,
  or a byte threshold derived from the QWP frame cap; ``auto_flush=off``
  restores fully explicit publishing. Auto-flush publishes without waiting
  for an acknowledgement; use ``flush(wait=True)`` or ``wait()`` as the
  acknowledgement barrier.
  The lease types are exported as :class:`questdb.PooledSender` and
  :class:`questdb.PooledReader` for ``isinstance`` checks and type
  annotations; instances are only ever constructed by
  :meth:`QuestDB.sender <questdb.QuestDB.sender>` and :meth:`QuestDB.reader <questdb.QuestDB.reader>`.
- ``Buffer`` is no longer part of the top-level API. Buffers are managed
  internally by senders; for concurrent serialization borrow more senders.
  Legacy explicit-buffer code keeps working through the ``questdb.ingress``
  shim.
- Over ``ws::`` / ``wss::``, DataFrame bulk loads use the direct columnar
  path — a database operation, independent of ``sf_dir``, committed on
  return, with precise ``in_doubt`` failure semantics. Available as
  :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` on the handle, as ``dataframe()`` on a pooled
  sender from :meth:`QuestDB.sender <questdb.QuestDB.sender>` (which borrows a direct connection from
  the pool for that call), and as ``dataframe()`` on any standalone
  :class:`Sender <questdb.Sender>` (which opens a poolless direct connection from the sender's
  own configuration for that call, carrying its auth/TLS regardless of how
  the sender was built). In all cases the direct load has no ordering
  relationship with rows buffered via ``row()`` and does not flush them.
  Over ILP/HTTP, ILP/TCP and QWP/UDP, ``Sender.dataframe()`` remains fully
  supported and is no longer deprecated (over UDP it serializes row by row
  into fire-and-forget datagrams, with the same delivery caveats as
  ``row()``).
- ``IngressError`` is renamed to :class:`QuestDBError <questdb.QuestDBError>` and
  ``IngressErrorCode`` to :class:`QuestDBErrorCode <questdb.QuestDBErrorCode>` (the names now also cover
  the query egress path, which raises the same types).
- The error model is unified across ingestion and queries. Query/reader
  failures now surface their own :class:`QuestDBErrorCode <questdb.QuestDBErrorCode>` members
  (``HandshakeError``, ``UnsupportedServer``, ``ProtocolError``,
  ``InvalidBind``, ``ServerSchemaMismatch``, ``ServerParseError``,
  ``ServerInternalError``, ``ServerSecurityError``, ``LimitExceeded``,
  ``ServerLimitExceeded``, ``SchemaDrift``, ``NoSchema``, ``ArrowExport``)
  rather than being bucketed under ``ServerFlushError``. Existing member
  names are unchanged and the numeric codes 0–13 are identical to 4.x. The
  only shifted 4.x member is ``BadDataFrame``, whose ``.value`` moved from
  14 to ``0x10000`` (it is a Python-only sentinel with no backing FFI
  code); raw value 14 now belongs to the new ``ServerRejection``. Compare
  by member identity, e.g. ``err.code is QuestDBErrorCode.BadDataFrame`` —
  that is the stable contract, not the integer value.
- Catching errors from the ``questdb.ingress`` shim is broader than in 4.x:
  ``IngressError`` is an alias of the unified :class:`QuestDBError <questdb.QuestDBError>`, so
  ``except IngressError`` now also catches query, pool and egress failures
  raised through the same handle.
- Pre-epoch ``datetime`` values passed as scalars (``at=`` or ``row()``
  column values) now floor toward negative infinity when converted to an
  epoch timestamp. 4.1.0 truncated toward zero, encoding pre-epoch instants
  with a fractional second one second too high; such values now serialize
  lower than before, at the correct instant.
- :meth:`TimestampNanos.from_datetime <questdb.TimestampNanos.from_datetime>` now raises ``ValueError`` for
  datetimes outside the signed 64-bit nanosecond range (1677-09-21 to
  2262-04-11 UTC); 4.x silently overflowed.
- Millisecond-duration parameters given as a negative sub-second
  ``datetime.timedelta`` (e.g. ``timedelta(milliseconds=-500)``) now raise
  ``ValueError``; 4.x silently mis-accepted them as positive durations.
- Naive (timezone-unaware) timestamps are interpreted as UTC everywhere:
  DataFrame column cells, scalar ``at=`` values, ``datetime`` values passed
  to ``row()``, ``TimestampNanos.from_datetime`` /
  ``TimestampMicros.from_datetime``, and query bind parameters. 4.x
  interpreted naive *scalars* in the process-local timezone
  (``datetime.timestamp()`` semantics) — the first naive conversion on such
  a path now emits a one-per-process ``UserWarning``. Note that
  ``datetime.now()`` is your local wall clock: for "now", use
  ``TimestampNanos.now()`` or ``datetime.now(timezone.utc)``.

Features
~~~~~~~~

QWP Ingestion Protocol
**********************

Adds support for the QuestDB Wire Protocol (QWP) alongside the existing
ILP transports.

- **QWP/UDP** (``udp::``): fire-and-forget datagram ingestion,
  defaulting to port 9007. New configuration keys ``max_datagram_size``
  and ``multicast_ttl``; ``protocol_version`` does not apply.
- **QWP/WebSocket** (``ws::`` / ``wss::``): acknowledged streaming
  ingestion with frame-sequence-number (FSN) tracking. New ``Sender``
  methods ``flush_and_get_fsn``, ``flush_and_keep_and_get_fsn``,
  ``published_fsn``, ``acked_fsn``, ``await_acked_fsn``, ``drive_once``,
  ``poll_error``, ``error_events_dropped`` and ``close_drain``.
  Server diagnostics are reported through an ``error_handler``
  callback or polled as :class:`SenderError <questdb.SenderError>` values (classified by the
  :class:`SenderErrorCategory <questdb.SenderErrorCategory>`, :class:`SenderErrorPolicy <questdb.SenderErrorPolicy>` and
  :class:`QwpWsProgress <questdb.QwpWsProgress>` enums); terminal server rejections raise
  :class:`QuestDBServerRejectionError <questdb.QuestDBServerRejectionError>`.

Additional configuration keys ``tls_roots_password``,
``retry_max_backoff_millis`` and ``qwp_ws_progress`` are also accepted.
Every new key is equally available as a ``Sender`` / ``Sender.from_conf`` /
``Sender.from_env`` keyword argument (``max_datagram_size``,
``multicast_ttl``, ``tls_roots_password``, ``retry_max_backoff``,
``qwp_ws_progress`` and ``error_handler``).

The pooled :class:`QuestDB <questdb.QuestDB>` handle exposes :meth:`QuestDB.sender <questdb.QuestDB.sender>`, returning
a context-managed row sender for row-at-a-time QWP/WebSocket ingestion.
Row builders, whole-dataframe ingestion, and queries share one
``questdb.connect("ws::...")`` configuration without exposing separate row
and column sender pools. The pooled sender's ``dataframe()`` routes to the
same direct columnar path as :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`, borrowing a direct
connection from the pool for that call only (prefer calling
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` directly).

Store-and-forward delivery
**************************

Pooled row senders publish into a local store-and-forward queue: ``flush()``
returns on local acceptance and a background runner delivers the frames,
reconnecting and replaying across transient failures within the
``reconnect_*`` budget. With the opt-in ``sf_dir`` directory the queue is
disk-backed per sender slot and unacknowledged frames are replayed after a
client restart (``sender_id`` names the slots; ``sf_max_segment_bytes`` /
``sf_max_total_bytes`` / ``sf_append_deadline_millis`` /
``close_flush_timeout_millis`` bound it — see the configuration reference).
``flush(wait=True)`` / ``wait()`` observe the server acknowledgement;
``request_durable_ack=on`` upgrades acknowledgements to durable ones on
servers that support them.

Server rejections are pushed to the ``error_handler`` callable
passed to :func:`questdb.connect` (one :class:`questdb.SenderError` per
rejection, on a dedicated dispatcher thread; totals via
:attr:`QuestDB.error_events_delivered <questdb.QuestDB.error_events_delivered>` /
:attr:`QuestDB.error_events_dropped <questdb.QuestDB.error_events_dropped>`). Without a handler every
rejection is logged through the ``questdb`` logger, so rejections are
never silent. ``PooledSender.flush(wait=True)`` / ``wait()`` are pure ack
barriers scoped to the lease's own publications: only a terminal
connection failure raises there.

Query connections take their own keys — result-set ``compression``
(Zstandard) and the mid-query ``failover_*`` / ``target`` policy — all
documented in the configuration reference.

Query Egress
************

Adds :meth:`QuestDB.query <questdb.QuestDB.query>`, returning a
:class:`QueryResult <questdb.QueryResult>` that streams rows as Arrow record batches over the
QWP/WebSocket read endpoint. Queries take positional bind parameters
matching ``$1``..``$N`` placeholders —
``db.query('SELECT * FROM trades WHERE ts > $1 AND sym = $2', [ts, 'BTC-USD'])``
— covering ``None`` (SQL NULL), ``bool``, ``int``, ``float``, ``str``,
``datetime.datetime``, :class:`TimestampMicros <questdb.TimestampMicros>`, :class:`TimestampNanos <questdb.TimestampNanos>`
and ``uuid.UUID``; always prefer binds over interpolating values into the
SQL text. Results can be consumed via ``to_arrow``,
``to_pandas``, ``to_polars``, ``iter_arrow``, ``iter_pandas``,
``iter_polars`` or the Arrow C stream PyCapsule protocol
(``__arrow_c_stream__``). ``to_polars`` / ``iter_polars`` use pyarrow to
buffer failover-safe batches; ``__arrow_c_stream__`` (consumed as
``polars.from_arrow(result)``) is the pyarrow-free polars path. SYMBOL
columns are dictionary-encoded on the wire and map to a pandas
``Categorical`` (``to_pandas`` / ``iter_pandas``) or a polars
``Categorical`` (``to_polars`` / ``iter_polars``), the latter sharing one
persistent ``Categories`` identity across streamed batches so
``polars.concat`` stitches them without a categories-mismatch error.

``to_pandas`` / ``iter_pandas`` default to a native (no-pyarrow) build
straight from the QWP column buffers: a nullable integer column becomes a
pandas nullable ``Int*`` when it contains nulls and plain numpy otherwise,
``double`` stays numpy with ``NaN``, ``TIMESTAMP`` → ``datetime64``, and
the QuestDB column kinds are recorded in ``df.attrs['questdb']`` for a type
round-trip through :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`. Pass
``dtype_backend="pyarrow"`` / ``"numpy_nullable"`` (or ``types_mapper=``)
to select the pyarrow-backed conversion instead.

:class:`QuestDB <questdb.QuestDB>` is a context manager and exposes :meth:`QuestDB.close <questdb.QuestDB.close>` and
:meth:`QuestDB.reap_idle <questdb.QuestDB.reap_idle>` for pooled-connection lifecycle management.
Pool sizing follows the Java client's configuration keys:
``sender_pool_min`` / ``sender_pool_max`` (ingestion, defaults 1/4),
``query_pool_min`` / ``query_pool_max`` (readers, defaults 1/4),
``acquire_timeout_ms`` (default 5000 — how long a borrow waits at the cap
before failing; ``0`` fails fast), ``idle_timeout_ms`` (default 60000 —
after which above-minimum idle connections are reaped) and ``pool_reap``
(``auto`` | ``manual``, default ``auto`` — whether a background thread
reaps idle connections, or the application calls :meth:`QuestDB.reap_idle <questdb.QuestDB.reap_idle>`
on its own cadence).

- Closing or abandoning a :class:`QueryResult <questdb.QueryResult>` now releases its pooled
  connection immediately instead of at garbage collection; abandoning a
  result without closing it emits a ``ResourceWarning``.

- :meth:`QuestDB.reader <questdb.QuestDB.reader>` returns a reader lease
  (the read-side twin of :meth:`QuestDB.sender <questdb.QuestDB.sender>`) that borrows one pooled
  reader connection for its lifetime and runs queries on it sequentially —
  ``with db.reader() as r: r.query(sql)`` — optionally keeping the
  connection's SYMBOL dictionary warm across queries via
  ``reset_symbol_dict=False``.

- :meth:`QuestDB.execute <questdb.QuestDB.execute>` (also on the reader
  lease) runs a statement, drains whatever it returns, and hands the
  pooled connection back in one call —
  ``db.execute('CREATE TABLE ...')`` — replacing the
  ``with db.query(ddl) as r: r.to_pandas()`` drain ceremony for DDL and
  DML. Statement output is discarded and ``None`` is returned; use
  :meth:`QuestDB.query <questdb.QuestDB.query>` when you want the result.

Columnar DataFrame Ingestion
****************************

Adds :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`, ingesting pandas / polars / pyarrow and
any Arrow C Data Interface object over QWP/WebSocket. A
``schema_overrides`` keyword reclassifies columns as ``symbol``,
``ipv4``, ``char`` or ``geohash`` (e.g. ``{'addr': 'ipv4', 'loc':
('geohash', 20)}``); it requires fully Arrow-backed input — on input that
falls back to the NumPy planner it raises
:class:`UnsupportedDataFrameShapeError <questdb.UnsupportedDataFrameShapeError>` rather than being silently
ignored. A ``max_rows_per_batch`` keyword (default 16384) bounds the
rows sent per columnar batch.

The designated-timestamp argument ``at`` is the timestamp column itself,
given by name (``str``) or position (``int``), a fixed
``TimestampNanos`` / ``datetime`` shared by every row, or the explicit
``ServerTimestamp`` sentinel. A frame
produced by :meth:`QueryResult.to_pandas <questdb.QueryResult.to_pandas>` round-trips back to the same
QuestDB column types automatically: the kinds recorded in
``df.attrs['questdb']`` and pandas nullable extension dtypes are honoured.

Connection Observability
************************

A ``connection_listener`` callable (accepted by :func:`questdb.connect`,
:meth:`QuestDB.from_conf <questdb.QuestDB.from_conf>` and the ``Sender`` constructors, alongside a
``connection_event_inbox_capacity`` bound) receives one
:class:`ConnectionEvent <questdb.ConnectionEvent>` per connection-state transition — initial
connect, per-endpoint attempt failures, failover, terminal auth
rejection — classified by :class:`ConnectionEventKind <questdb.ConnectionEventKind>`. The listener
runs on a dedicated dispatcher thread fed by a bounded drop-oldest
inbox; delivery totals are exposed as ``connection_events_delivered`` /
``connection_events_dropped`` on both :class:`QuestDB <questdb.QuestDB>` and
:class:`Sender <questdb.Sender>`. :meth:`QuestDB.server_info <questdb.QuestDB.server_info>` snapshots the server's
``SERVER_INFO`` handshake as a :class:`ServerInfo <questdb.ServerInfo>` (role as
:class:`ServerRole <questdb.ServerRole>`, failover epoch, capabilities and cluster / node /
zone ids).

Errors
******

Adds :class:`UnsupportedDataFrameShapeError <questdb.UnsupportedDataFrameShapeError>` (raised when a DataFrame
cannot be expressed on the QWP columnar path) and the
:class:`QuestDBErrorCode <questdb.QuestDBErrorCode>` members ``ServerRejection``, ``RoleMismatch``,
``ArrowUnsupportedColumnKind``, ``ArrowIngest``, ``FailoverRetry``,
``ConnectTimeout``, ``FailoverWouldDuplicate``, ``Cancelled``,
``BatchTooLarge`` and ``StoreResendRequired``.
:class:`QuestDBError <questdb.QuestDBError>` gains a ``sender_error`` property exposing the
structured :class:`SenderError <questdb.SenderError>` view on a server-side QWP/WebSocket
rejection, plus an ``in_doubt`` property indicating that failed ingress input
may already have been delivered and must not be blindly replayed without an
appropriate table-level deduplication guarantee.

Build & dependencies
~~~~~~~~~~~~~~~~~~~~~~

- The bundled ``c-questdb-client`` native library is upgraded to the 7.0.0
  development series (pinned at ``6.1.0-441-g2158029``), providing the QWP
  transports, the columnar ``column_sender`` API and the ``line_reader``
  query API that back the new features above.
- ``numpy>=1.21.0`` is now a hard runtime dependency (previously it was
  pulled in only via the ``dataframe`` extra).
- **pyarrow is now optional.** It is imported lazily, only when actually
  needed (``pd.ArrowDtype`` columns, pyarrow sources, ``schema_overrides``,
  ``to_polars`` / ``iter_polars``, and the ``to_arrow`` / ``iter_arrow`` /
  ``dtype_backend`` helpers). The ``__arrow_c_stream__`` egress path
  (consumed as ``polars.from_arrow(result)``) and the default
  ``to_pandas`` / ``iter_pandas`` work without pyarrow.
- The ``dataframe`` extra now pins ``pandas>=1.3.5`` and
  ``pyarrow>=10.0.1``.

Deprecations
~~~~~~~~~~~~

- The ``questdb.ingress`` module is deprecated (kept as a compatibility shim
  for 4.x ILP/HTTP and ILP/TCP code); it emits one ``DeprecationWarning`` at
  import. New code imports from ``questdb``.

4.1.0 (2025-11-28)
------------------

Features
~~~~~~~~

Decimal Data Type Support
*************************

This release adds support for ingesting data into QuestDB's native
``DECIMAL(precision, scale)`` column type introduced in QuestDB 9.2.0.
Use decimals when you need exact-precision values (e.g. prices, balances)
without floating-point rounding issues.

Decimal values can be sent using Python ``decimal.Decimal`` objects in both
``row()`` and ``dataframe()`` methods. When sending dataframes, you can also use
PyArrow decimal types for better performance.

Sending decimals requires protocol version 3. When using HTTP, this protocol
version is auto-negotiated. For TCP connections, you must explicitly specify
``protocol_version=3`` in the configuration string:

.. code-block:: python

    # HTTP - protocol version 3 is auto-negotiated
    conf = 'http::addr=localhost:9000;'

    # TCP - must specify protocol_version=3 explicitly
    conf = 'tcp::addr=localhost:9009;protocol_version=3;'

.. important::
    **Server Requirement**: This feature requires QuestDB server version 9.2.0 or higher.

    Unlike other column types, DECIMAL columns **must be created in advance** via SQL
    before ingesting data. Auto-creation is not supported for DECIMAL columns.

    For details on creating DECIMAL columns and working with this data type, see the
    `QuestDB Decimal documentation <https://questdb.com/docs/concept/decimal/>`_.

**Usage with Python Decimal objects:**

.. code-block:: python

    from decimal import Decimal
    from questdb import Sender, TimestampNanos

    # First, create the table with DECIMAL column via SQL:
    # CREATE TABLE trades (
    #     symbol SYMBOL,
    #     price DECIMAL(18,4),
    #     amount DECIMAL(18,8),
    #     timestamp TIMESTAMP
    # ) TIMESTAMP(timestamp);

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD'},
            columns={
                'price': Decimal('2615.5400'),
                'amount': Decimal('0.00044000')},
            at=TimestampNanos.now())

**Usage with Python Decimal objects in Pandas dataframes:**

.. code-block:: python

    import pandas as pd
    from decimal import Decimal

    # Create DataFrame with Python Decimal objects
    df = pd.DataFrame({
        'symbol': ['ETH-USD', 'BTC-USD'],
        'price': [Decimal('2615.5400'), Decimal('43210.1234')],
        'volume': [Decimal('1234.56789012'), Decimal('98.76543210')]
    })

    with Sender.from_conf(conf) as sender:
        sender.dataframe(
            df,
            table_name='trades',
            symbols='symbol',
            at=TimestampNanos.now())

**Usage with PyArrow Decimal types in Pandas dataframes:**

.. code-block:: python

    import pandas as pd
    import pyarrow as pa
    from decimal import Decimal

    # Create DataFrame with Arrow decimal types
    df = pd.DataFrame({
        'prices': pd.array(
            [Decimal('-99999.99'), Decimal('-678.00')],
            dtype=pd.ArrowDtype(pa.decimal128(18, 2))
        )
    })

    with Sender.from_conf(conf) as sender:
        sender.dataframe(df, table_name='prices', at=TimestampNanos.now())

Additional Arrow Data Type Support
**********************************

Added support for additional PyArrow column types commonly encountered when
deserializing from Parquet files or converting Polars dataframes to Pandas:

* ``int16``, ``int32``, ``float32`` (float), ``bool``
* ``string``, ``large_string`` (including as symbol types)
* ``timestamp[us]`` with timezone support for microsecond-precision timestamps

Microsecond Timestamp Precision
*******************************

Microsecond-precision timestamp columns (``datetime64[us]`` in NumPy and
``timestamp[us]`` in PyArrow) are now fully supported. When using
``protocol_version`` 2 or higher, microsecond timestamps are sent with full
precision, including for the designated timestamp column.

Bug fixes
~~~~~~~~~

* Updated type hints to allow ``None`` as a valid column value in
  ``Sender.row()``. This brings the type annotations in line with the actual
  behavior, where ``None`` values have always been supported to represent NULL
  values.

Python Version Support
~~~~~~~~~~~~~~~~~~~~~~

* Added support for Python 3.14 on all platforms and 3.14t (free threaded) on
  all platforms except Windows.
* Dropped support for Python 3.9 (end of life).

4.0.0 (2025-10-17)
------------------

New Breaking Change Feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~

From QuestDB 9.1.0 onwards you can use ``CREATE TABLE`` SQL statements with
``TIMESTAMP_NS`` column types, or rely on column auto-creation.

This client release adds support for sending nanoseconds timestamps to the
server without loss of precision.

This release does not introduce new APIs, instead enhancing the sender/buffer's
``.row()`` API to additionally accept nanosecond precision.

.. code-block:: python

    conf = 'http::addr=localhost:9000;'
    # or `conf = 'tcp::addr=localhost:9009;protocol_version=2;'`
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trade_executions',
            symbols={
                'product': 'VOD.L',
                'parent_order': '65d1ba36-390e-49a2-93e3-a05ef004b5ff',
                'side': 'buy'},
            columns={
                'order_sent': TimestampNanos(1759246702031355012)},
            at=TimestampNanos(1759246702909423071))

If you're using dataframes, nanosecond timestamps are now also transferred with
full precision.

The change is backwards compatible with older QuestDB releases which will simply
continue using the ``TIMESTAMP`` column, even when nanoseconds are specified in
the client.

This is a breaking change because it introduces new breaking timestamp
`column auto-creation <https://questdb.com/docs/reference/api/ilp/overview/#table-and-column-auto-creation>`_
behaviour. For full details and upgrade advice, see the
`nanosecond PR on GitHub <https://github.com/questdb/py-questdb-client/pull/113>`_.

3.0.0 (2025-07-07)
------------------

Features
~~~~~~~~

This is the first major release of the QuestDB Python client library
which supports n-dimensional arrays of doubles for QuestDB servers 9.0.0 and up.

.. code-block:: python

        import numpy as np

        # Create 2D numpy array
        array_2d = np.array([
            [1.1, 2.2, 3.3],
            [4.4, 5.5, 6.6]], dtype=np.float64)

        sender.row(
            'table',
            columns={'array_2d': array_2d},
            at=timestamp)

The array data is sent over a new protocol version (2) that is auto-negotiated
when using HTTP(s), or can be specified explicitly via the ``protocol_version=2``
parameter when using TCP(s).

We recommend using HTTP(s), but here is an TCP example, should you need it::

  tcp::addr=localhost:9009;protocol_version=2;

When using ``protocol_version=2`` (with either TCP(s) or HTTP(s)), the sender
will now also serialize ``float`` (double-precision) columns as binary.
You might see a performance uplift if this is a dominant data type in your
ingestion workload.

When compared to 2.0.4, this release includes all the changes from 3.0.0rc1 and
additionally:

* Has optimised ingestion performance from C-style contiguous NumPy arrays.

* Warns at most every 10 minutes when burst of reconnections are detected.
  This is to warn about code patterns that may lead to performance issues, such as

  .. code-block:: python

    # Don't do this! Sender objects should be reused.
    for row_fields in data:
        with Sender.from_conf(conf) as sender:
            sender.row(**row_fields)

  This feature can be disabled in code by setting:

  .. code-block:: python

    import questdb as qi
    qi.WARN_HIGH_RECONNECTS = False

* Fixed ILP/TCP connection shutdown on Windows where some rows could be
  lost when closing the ``Sender``, even if explicitly flushed.

* Added a "Good Practices" section to the "Sending Data over ILP" section of
  the documentation.

Breaking Changes
~~~~~~~~~~~~~~~~
Refer to the release notes for 3.0.0rc1 for the breaking changes introduced
in this release compared to 2.x.x.


3.0.0rc1 (2025-06-02)
---------------------

This is the pre-release of a major release introducing array ingestion and some
minor breaking changes.

Features
~~~~~~~~
* Array Data Type Support. Adds native support for NumPy arrays
  (currently only for ``np.float64`` element type and up to 32 dimensions).

.. note::
    **Server Requirement**: This feature requires QuestDB server version 9.0.0 or higher.
    Ensure your server is upgraded before ingesting array types, otherwise data ingestion will fail.

.. code-block:: python

        import numpy as np

        # Create 2D numpy array
        array_2d = np.array([
            [1.1, 2.2, 3.3],
            [4.4, 5.5, 6.6]], dtype=np.float64)

        sender.row(
            'table',
            columns={'array_2d': array_2d},
            at=timestamp)

* Implements binary protocol for columns of ``float`` (double-precision) and
  ``numpy.ndarray[np.float64]``, with performance improvements for these
  two datatypes.

Breaking Changes
~~~~~~~~~~~~~~~~
* Buffer Constructor Changes. The ``Buffer`` constructor now requires the ``protocol_version`` parameter.
  You can create buffer through the sender for automatic ``protocol_version`` management:

.. code-block:: python

    buf = sender.new_buffer()  # protocol_version determined automatically
    buf.row(
      'table',
      columns={'arr': np.array([1.5, 3.0], dtype=np.float64)},
      at=timestamp)

* To access the raw payload, call ``bytes(sender)`` or ``bytes(buffer)`` (
  rather than calling the ``str`` function on the same objects as in version
  2.x.x of the questdb library) method.

* **NumPy Dependency**

  Array functionality mandates NumPy installation.

* **Sender/Buffer String Conversion Removal**

  The legacy string conversion via `str(sender)` is removed.
  Access raw binary payloads through the `bytes(sender)` method:

  .. code-block:: python

      # for debugging
      payload = bytes(sender)

* Python 3.8 support is dropped.

  The minimum supported Python version is now 3.9.

2.0.4 (2025-04-02)
------------------

Building for Python 3.13.

2.0.3 (2024-06-06)
------------------

Patch release with bug fixes. No breaking changes.

Bug fixes
~~~~~~~~~
* HTTP timeout wasn't always being correctly applied in the downstream ``c-questdb-client`` dependency.
* ``request_timeout > 0`` will now be enforced. This was always required, but would not error.
* Fixed the source distribution "sdist" package: This allows the package to be installed from source
  via "pip install" on previously unsupported platforms (YMMV).

2.0.2 (2024-04-11)
------------------

Patch release with a performance bug fix. No breaking changes.

Bug fixes
~~~~~~~~~
* Fixed the defaulting logic for ``auto_flush_rows`` parameter for HTTPS.
  It is now correctly set to 75000 rows by default. The old incorrect default
  of 600 rows was causing the sender to flush too often, impacting performance.
  Note that TCP, TCPS and HTTP were not affected.

Features
~~~~~~~~
* The sender now exposes the ``auto_flush`` settings as read-only properties.
  You can inspect the values in use with ``.auto_flush``, ``.auto_flush_rows``,
  ``.auto_flush_interval`` and ``.auto_flush_bytes``.

2.0.1 (2024-04-03)
------------------

Patch release with bug fixes, no API changes and some documentation tweaks.

Bug fixes
~~~~~~~~~
* Fixed a bug where an internal "last flushed" timestamp used
  by ``auto_flush_interval`` wasn't updated correctly causing the auto-flush
  logic to trigger after each row.

* Removed two unnecessary debugging ``print()`` statements that were
  accidentally left in the code in ``Sender.from_conf()`` and
  ``Sender.from_env()``.

Documentation
~~~~~~~~~~~~~
* Introduced the ability to optionally install ``pandas`` and ``pyarrow`` via
  ``python3 -m pip install -U questdb[dataframe]`` and updated the documentation
  to reflect this.


2.0.0 (2024-03-19)
------------------

This is a major release with new features and breaking changes.

Features
~~~~~~~~

* Support for ILP over HTTP. The sender can now send data to QuestDB via HTTP
  instead of TCP. This provides error feedback from the server and new features.

  .. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(...)
        sender.dataframe(...)

        # Will raise `QuestDBError` if there is an error from the server.
        sender.flush()

* New configuration string construction. The sender can now be also constructed
  from a :ref:`configuration string <sender_conf>` in addition to the
  constructor arguments.
  This allows for more flexible configuration and is the recommended way to
  construct a sender.
  The same string can also be loaded from the ``QDB_CLIENT_CONF`` environment
  variable.
  The constructor arguments have been updated and some options have changed.

* Explicit transaction support over HTTP. A set of rows for a single table can
  now be committed via the sender transactionally. You can do this using a
  ``with sender.transaction('table_name') as txn:`` block.

  .. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        with sender.transaction('test_table') as txn:
            # Same arguments as the sender methods, minus the table name.
            txn.row(...)
            txn.dataframe(...)

* A number of documentation improvements.


Breaking Changes
~~~~~~~~~~~~~~~~

* New ``protocol`` parameter in the
  :ref:`Sender <sender_programmatic_construction>` constructor.

  In previous version the protocol was always TCP.
  In this new version you must specify the protocol explicitly.

* New auto-flush defaults. In previous versions
  :ref:`auto-flushing <sender_auto_flush>` was enabled by
  default and triggered by a maximum buffer size. In this new version
  auto-flushing is enabled by row count (600 rows by default) and interval
  (1 second by default), while auto-flushing by buffer size is disabled by
  default.

  The old behaviour can be still be achieved by tweaking the auto-flush
  settings.

  .. list-table::
    :header-rows: 1

    * - Setting
      - Old default
      - New default
    * - **auto_flush_rows**
      - off
      - 600
    * - **auto_flush_interval**
      - off
      - 1000
    * - **auto_flush_bytes**
      - 64512
      - off

* The ``at=..`` argument of :func:`row <questdb.Sender.row>` and
  :func:`dataframe <questdb.Sender.dataframe>` methods is now mandatory.
  Omitting it would previously use a server-generated timestamp for the row.
  Now if you want a server generated timestamp, you can pass the :ref:`ServerTimestamp <sender_server_timestamp>`
  singleton to this parameter. _The ``ServerTimestamp`` behaviour is considered legacy._

* The ``auth=(u, t, x, y)`` argument of the ``Sender`` constructor has now been
  broken up into multiple arguments: ``username``, ``token``, ``token_x``, ``token_y``.

* The ``tls`` argument of the ``Sender`` constructor has been removed and
  replaced with the ``protocol`` argument. Use ``Protocol.Tcps``
  (or ``Protocol.Https``) to enable TLS.
  The ``tls`` values have been moved to new ``tls_ca`` and ``tls_roots``
  :ref:`configuration settings <sender_conf_tls>`.

* The ``net_interface`` argument of the ``Sender`` constructor has been renamed
  to ``bind_interface`` and is now only available for TCP connections.

The following example shows how to migrate to the new API.

**Old questdb 1.x code**

.. code-block:: python

    from questdb import Sender

    auth = (
        'testUser1',
        '5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48',
        'token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU',
        'token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac')
    with Sender('localhost', 9009, auth=auth, tls=True) as sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0})  # `at=None` was defaulted for server time

**Equivalent questdb 2.x code**

.. code-block:: python

    from questdb import Sender, Protocol, ServerTimestamp

    sender = Sender(
        Protocol.Tcps,
        'localhost',
        9009,
        username='testUser1',
        token='5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48',
        token_x='token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU',
        token_y='token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac',
        auto_flush_rows='off',
        auto_flush_interval='off',
        auto_flush_bytes=64512)
    with sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0},
            at=ServerTimestamp)

**Equivalent questdb 2.x code with configuration string**

.. code-block:: python

    from questdb import Sender

    conf = (
        'tcp::addr=localhost:9009;' +
        'username=testUser1;' +
        'token=5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48;' +
        'token_x=token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU;' +
        'token_y=token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac;' +
        'auto_flush_rows=off;' +
        'auto_flush_interval=off;' +
        'auto_flush_bytes=64512;')
    with Sender.from_conf(conf) as sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0},
            at=ServerTimestamp)


1.2.0 (2023-11-23)
------------------

This is a minor release bringing in minor new features and a few bug fixes,
without any breaking changes.

Most changes are inherited by internally upgrading to version ``3.1.0`` of
the ``c-questdb-client``.

Features
~~~~~~~~

* ``Sender(..., tls=True)`` now also uses the OS-provided certificate store.
  The `tls` argument can now also be set to ``tls='os_roots'`` (to *only* use
  the OS-provided certs) or ``tls='webpki_roots'`` (to *only* use the certs
  provided by the ``webpki-roots``, i.e. the old behaviour prior to this
  release). The new default behaviour for ``tls=True`` is equivalent to setting
  ``tls='webpki_and_os_roots'``.

* Upgraded dependencies to newer library versions. This also includes the latest
  `webpki-roots <https://github.com/rustls/webpki-roots>`_ crate providing
  updated TLS CA certificate roots.

* Various example code and documentation improvements.

Bug fixes
~~~~~~~~~

* Fixed a bug where timestamp columns could not accept values before Jan 1st
  1970 UTC.

* TCP connections now enable ``SO_KEEPALIVE``: This should ensure that
  connections don't drop after a period of inactivity.

1.1.0 (2023-01-04)
------------------

Features
~~~~~~~~

* High-performance ingestion of `Pandas <https://pandas.pydata.org/>`_
  dataframes into QuestDB via ILP.
  We now support most Pandas column types. The logic is implemented in native
  code and is orders of magnitude faster than iterating the dataframe
  in Python and calling the ``Buffer.row()`` or ``Sender.row()`` methods: The
  ``Buffer`` can be written from Pandas at hundreds of MiB/s per CPU core.
  The new ``dataframe()`` method continues working with the ``auto_flush``
  feature.
  See API documentation and examples for the new ``dataframe()`` method
  available on both the ``Sender`` and ``Buffer`` classes.

* New ``TimestampNanos.now()`` and ``TimestampMicros.now()`` methods.
  *These are the new recommended way of getting the current timestamp.*

* The Python GIL is now released during calls to ``Sender.flush()`` and when
  ``auto_flush`` is triggered. This should improve throughput when using the
  ``Sender`` from multiple threads.

Errata
~~~~~~

* In previous releases the documentation for the ``from_datetime()`` methods of
  the ``TimestampNanos`` and ``TimestampMicros`` types recommended calling
  ``datetime.datetime.utcnow()`` to get the current timestamp. This is incorrect
  as it will (confusingly) return an object with the local timezone instead of UTC.
  This documentation has been corrected and now recommends calling
  ``datetime.datetime.now(tz=datetime.timezone.utc)`` or (more efficiently) the
  new ``TimestampNanos.now()`` and ``TimestampMicros.now()`` methods.

1.0.2 (2022-10-31)
------------------

Features
~~~~~~~~

* Support for Python 3.11.
* Updated to version 2.1.1 of the ``c-questdb-client`` library:

  * Setting ``SO_REUSEADDR`` on outbound socket. This is helpful to users with large number of connections who previously ran out of outbound network ports.


1.0.1 (2022-08-16)
------------------

Features
~~~~~~~~

* As a matter of convenience, the ``Buffer.row`` method can now take ``None`` column
  values. This has the same semantics as skipping the column altogether.
  Closes `#3 <https://github.com/questdb/py-questdb-client/issues/3>`_.

Bug fixes
~~~~~~~~~

* Fixed a major bug where Python ``int`` and ``float`` types were handled with
  32-bit instead of 64-bit precision. This caused certain ``int`` values to be
  rejected and other ``float`` values to be rounded incorrectly.
  Closes `#13 <https://github.com/questdb/py-questdb-client/issues/13>`_.
* Fixed a minor bug where an error auto-flush caused a second clean-up error.
  Closes `#4 <https://github.com/questdb/py-questdb-client/issues/4>`_.


1.0.0 (2022-07-15)
------------------

Features
~~~~~~~~

* First stable release.
* Insert data into QuestDB via ILP.
* Sender and Buffer APIs.
* Authentication and TLS support.
* Auto-flushing of buffers.


0.0.3 (2022-07-14)
------------------

Features
~~~~~~~~

* Initial set of features to connect to the database.
* ``Buffer`` and ``Sender`` classes.
* First release where ``pip install questdb`` should work.


0.0.1 (2022-07-08)
------------------

Features
~~~~~~~~

* First release on PyPI.
