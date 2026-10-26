.. _refreshing_token_provider:

==========================
Rotating Bearer Tokens
==========================

A fixed ``token=`` is captured once and stops working when it expires. Machine
credentials that rotate -- Microsoft Entra ID managed identities and service
principals, OAuth2 client credentials, tokens from a secrets vault -- need a
*token provider* instead: an object the client asks for the current token on
every connection and reconnection (on every request, for ILP/HTTP).

Attach one with ``bearer_token_provider=`` on :func:`questdb.connect`,
:meth:`QuestDB.from_conf <questdb.QuestDB.from_conf>`,
:class:`~questdb.Sender`, :meth:`Sender.from_conf
<questdb.Sender.from_conf>` and :meth:`Sender.from_env
<questdb.Sender.from_env>`. The client selects no provider from a
configuration string: ``token_provider`` and the ``azure_*`` keys are rejected
with a pointer here.

For interactive sign-in by a person, use :ref:`oidc_auth` instead.

Refreshing token providers
==========================

A :class:`~questdb.auth.RefreshingTokenProvider` wraps a *token source*: a
function you write that fetches a new token and its expiry however you like.
The provider caches the token and refreshes it on its own thread at about half
its lifetime, so connections read the cached token and never wait for the
identity platform:

.. code-block:: python

    import questdb
    from questdb.auth import ExpiringToken, RefreshingTokenProvider

    def fetch_token():
        response = my_identity_platform.issue_token()   # any way you like
        return ExpiringToken.expires_in(
            response['access_token'], response['expires_in'])

    with RefreshingTokenProvider(fetch_token) as tokens:
        with questdb.connect(
                'wss::addr=questdb.example.com:9000;',
                bearer_token_provider=tokens) as db:
            db.query('select 1')

A source returns an :class:`~questdb.auth.ExpiringToken`, a ``(token,
expires_at)`` tuple, or any object with ``token`` and ``expires_on`` (epoch
seconds) attributes -- which includes Azure Core's ``AccessToken`` and
``AccessTokenInfo``. It may be a plain function or an object with a
``fetch_token()`` method.

The provider:

* fetches the first token as soon as it is created; use
  :meth:`~questdb.auth.RefreshingTokenProvider.await_ready` to gate startup on
  it;
* refreshes each token at a jittered half-life, and earlier when the platform
  gives a refresh hint (``refresh_on``) or the token is short-lived;
* hands a token out only while more than ``handout_floor`` (60 seconds by
  default) of its lifetime remains, so a reconnect never presents a token that
  is about to expire. A source whose tokens live no longer than that needs a
  lower ``handout_floor``;
* retries failed fetches with jittered exponential backoff, honouring a
  ``retry_after``, while it keeps serving the current token;
* refreshes early when a server rejects its current token with HTTP 401, at
  most once every 30 seconds;
* makes a client that holds no usable token wait up to ``cold_wait`` (30
  seconds by default) for one.

The source runs only on the provider's own thread, never concurrently with
itself. :meth:`~questdb.auth.RefreshingTokenProvider.token` and the
transports release the GIL while they wait, so your source can run.

Failures
--------

Raise :class:`~questdb.auth.TokenUnavailableError` from a source to classify a
failure: ``retryable=True`` (the default) for network failures, timeouts,
HTTP 429 and 5xx; ``retryable=False`` for missing or wrong configuration. Any
other exception counts as retryable. Never put a token in the message.

The classification matters when a connection is waited for -- an eager
:func:`questdb.connect`, or a sender's synchronous initial connect
(``initial_connect_retry=sync``): it retries a retryable failure within its
budget and fails at once on a permanent one. A connection that is already
established reconnects in the background and keeps retrying either kind, so
store-and-forward data is never abandoned because a token could not be
fetched for a while. A failure surfaces as a
:class:`~questdb.auth.TokenUnavailableError`, which is a
:class:`~questdb.QuestDBError`. A source cannot stop those reconnects: to
end them, close the provider (see `Closing`_).

Inspect the provider with :attr:`~questdb.auth.RefreshingTokenProvider.status`:
the token's expiry, the last successful fetch, and the last failure. Nothing
the provider logs, raises or reports contains the token: at most its length.

Closing
-------

Clients keep the provider alive while they use it, but never close it. Close
it, or use it as a context manager, once no client needs it: closing fails
every client still using it.

Closing prevents new source invocations but cannot cancel one already running.
Give every source's network calls a deadline and, for deterministic shutdown,
call ``provider.close(wait=True, timeout=30)`` before leaving the application.
This releases the GIL while waiting and raises ``TimeoutError`` if the source
has not returned; the provider stays closed, and a later call can retry the
drain. A source cannot wait for itself with ``close(wait=True)``.
Interpreter exit must wait for admitted Python callbacks even after a provider
is closed. That finalization wait cannot safely abandon Python frames and is
not interrupted by Ctrl-C: a source without bounded I/O can prevent exit.

When the interpreter starts to exit, the client stops running Python token
code on its native threads. Its own ``atexit`` hook does this, and it runs
before ``logging.shutdown()`` and before any ``atexit`` callback registered
before ``questdb`` was imported. A flush made from such exit-time code on the
main thread still reaches an application provider, because an ILP/HTTP flush
calls it on the flushing thread. What runs on a native thread can no longer
supply a token: a QWP/WebSocket sender, pool or query that has to open a
connection then, and a refreshing provider whose token needs a refresh.
Flush and close those clients before the interpreter exits.

Providers and their clients must not be used across ``fork()``. Start workers
with ``spawn`` (or ``exec()`` after forking), then construct providers inside
them. Merely constructing a fresh provider in a child of a multithreaded
process does not make it safe: native runtimes, locks and isolated-worker
accounting can still describe vanished parent threads. Pre-fork worker models
must fork before constructing any providers or clients.

Application providers
=====================

If you already cache tokens, pass ``bearer_token_provider=`` a function that
returns the current token, or an object with a ``token()`` method and an
optional ``on_rejected(token, http_status)`` (see
:class:`~questdb.auth.BearerTokenProvider`). It runs on every connection
attempt of a QWP/WebSocket sender, query or pool, possibly on a native
thread, and on every flush of an ILP/HTTP sender, on the flushing thread, so
it must be quick.

When a server answers HTTP 401, the client calls ``on_rejected`` with the
rejected token, asks for a token again, and retries the same endpoint once if
the token changed -- unless the server's ``WWW-Authenticate`` challenge names
an error other than ``invalid_token``, which a new token cannot fix. A
:class:`~questdb.auth.RefreshingTokenProvider` that your provider calls --
``bearer_token_provider=lambda: tokens.token()`` -- refreshes early on such a
401 too, as it does when attached directly.

A provider reports failures as a source does: raise
:class:`~questdb.auth.TokenUnavailableError` to classify one; any other
exception counts as retryable. A QWP/WebSocket sender, query or pool keeps
retrying either kind in its background reconnects, and a connection that is
waited for uses the classification as described under `Failures`_.

An ILP/HTTP sender has no background reconnects. Its flush retries a
retryable failure on the flushing thread for up to ``retry_timeout``, fails
at once on a permanent one, and then raises. Like any failed flush, it
discards the rows in the sender's internal buffer, and so does a failed
auto-flush from ``row()`` or ``dataframe()``. Build a batch you may need to
resend in a buffer of your own and flush it with ``clear=False``, which keeps
the rows when the flush fails:

.. code-block:: python

    import questdb

    buf = sender.new_buffer()
    buf.row('trades', symbols={'sym': 'ETH-USD'}, columns={'px': 2615.54},
            at=questdb.ServerTimestamp)
    sender.flush(buf, clear=False)  # if this raises, the rows stay in buf
    buf.clear()

When retrying can never succeed -- the provider is misconfigured beyond
repair, or your application has retired it -- raise
:class:`~questdb.QuestDBError` with ``code`` ``QuestDBErrorCode.InvalidApiCall``
instead, as a Rust provider returns ``InvalidApiCall``:

.. code-block:: python

    import questdb

    class VaultTokens:
        def token(self):
            if self.revoked:
                raise questdb.QuestDBError(
                    questdb.QuestDBErrorCode.InvalidApiCall,
                    'the vault revoked this service credential')
            return self.cached_token

A QWP/WebSocket sender, query or pool that gets it stops reconnecting and
fails with ``QuestDBErrorCode.ConfigError``, which a connection listener sees
as a :attr:`ConnectionEventKind.CredentialUnavailable
<questdb.ConnectionEventKind.CredentialUnavailable>` event; an ILP/HTTP flush
fails with it at once. Queued store-and-forward rows are not deleted, and a
disk-backed slot stays drainable by a later process. Raised from the source of
a :class:`~questdb.auth.RefreshingTokenProvider`, it counts as retryable like
any other exception.

.. _token_rejections:

When the server keeps rejecting the token
=========================================

A rejection can persist: while an identity platform or its signing keys are
unavailable, QuestDB answers HTTP 401 or 403 even to a fresh token. What
happens then depends on where the client is:

* A connection that is waited for -- a sender's first connection, an eager
  :func:`questdb.connect` -- fails with ``QuestDBErrorCode.AuthError``, so a
  wrong credential is seen at once.
* A QWP/WebSocket sender that has connected keeps reconnecting in the
  background with capped backoff, keeps its unacknowledged rows, and reports
  each failed attempt as a :attr:`ConnectionEventKind.AuthFailed
  <questdb.ConnectionEventKind.AuthFailed>` event. A wait, a ``close()``
  draining the queue, or a ``flush()`` blocked on a full store-and-forward
  queue (``sf_append_deadline_millis``) that times out meanwhile names the
  rejection. The same applies to a fixed ``token=`` or
  ``username=`` / ``password=``. To give up instead, close the sender from
  your own code -- not from its connection listener, which cannot close its
  own sender.
* A store-and-forward slot left by an earlier sender (``drain_orphans=on``)
  keeps being retried with the provider's tokens until at least 6 rejections
  have persisted for ``min(reconnect_max_duration_millis, 5 min)``, or after
  256. The slot is then left on disk, with the reason in its ``.last_error``,
  for the next session to drain. With a fixed credential it is left at the
  first rejection.
* A query fails. If its failover gives up -- also when the provider cannot
  supply a token, which raises :class:`~questdb.auth.TokenUnavailableError`
  -- the pool discards the connection and the next query opens a new one.

Use ``https::`` and ``wss::``: over ``http::`` and ``ws::`` every token
crosses the network in cleartext. The client logs a warning, once per
process, on the ``questdb`` logger when a provider is used without TLS.

Recipes
=======

Microsoft Entra ID
------------------

Install `azure-identity <https://pypi.org/project/azure-identity/>`_ yourself
-- it is not a dependency of this package -- and return the credential's token
from the source:

.. code-block:: python

    from azure.identity import ManagedIdentityCredential
    from questdb.auth import RefreshingTokenProvider

    credential = ManagedIdentityCredential()   # or WorkloadIdentityCredential
    tokens = RefreshingTokenProvider(
        lambda: credential.get_token_info('api://<questdb-app-id>/.default'),
        name='entra')

``get_token_info`` returns the refresh hint as well. In production prefer a
specific credential over ``DefaultAzureCredential``: a credential chain probes
the managed-identity endpoint once, without retrying, so a brief endpoint
outage looks like a host without a credential. The QuestDB Enterprise server
must validate application tokens locally (``acl.oidc.groups.encoded.in.token=
true``, ``acl.oidc.groups.claim=roles``, ``acl.oidc.sub.claim=oid``), and the
application registration must issue version 2 tokens.

OAuth2 client credentials
-------------------------

.. code-block:: python

    import json
    import urllib.error
    import urllib.parse
    import urllib.request
    from questdb.auth import (
        ExpiringToken, RefreshingTokenProvider, TokenUnavailableError)

    def client_credentials():
        body = urllib.parse.urlencode({
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'scope': SCOPE,
        }).encode()
        try:
            with urllib.request.urlopen(TOKEN_URL, body, timeout=30) as resp:
                grant = json.load(resp)
        except urllib.error.HTTPError as e:
            # 4xx other than 429 is configuration; the rest can clear.
            raise TokenUnavailableError(
                f'token endpoint answered HTTP {e.code}',
                retryable=e.code == 429 or e.code >= 500) from None
        return ExpiringToken.expires_in(
            grant['access_token'], grant['expires_in'])

    tokens = RefreshingTokenProvider(client_credentials)

PG-wire
-------

:func:`~questdb.auth.sqlalchemy_engine` and
:func:`~questdb.auth.psycopg_connect` accept any provider with a ``token()``
method, so a ``RefreshingTokenProvider`` works with them too.
