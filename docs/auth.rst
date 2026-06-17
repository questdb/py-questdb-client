.. _oidc_auth:

===================
OIDC Authentication
===================

QuestDB Enterprise can be secured with `OpenID Connect (OIDC)
<https://questdb.com/docs/operations/rbac/>`_. The :mod:`questdb.auth` module
lets you sign in interactively from Python — including from a **remote** kernel
(JupyterHub, SageMaker, Colab, VS Code-remote, containers) where there is no
local browser.

It runs the `OAuth 2.0 Device Authorization Grant (RFC 8628)
<https://datatracker.ietf.org/doc/html/rfc8628>`_ entirely client-side: you
authorize in **any** browser (your laptop or your phone), while the kernel only
makes outbound calls to your identity provider (IdP). The resulting token is
then presented to QuestDB over the auth paths it already supports — HTTP
``Authorization: Bearer`` or PG-wire ``_sso`` — so **no server change is
required**.

.. note::

    This feature targets **QuestDB Enterprise with OIDC enabled**. The IdP
    client referenced by ``acl.oidc.client.id`` must have the device grant
    (``urn:ietf:params:oauth:grant-type:device_code``) enabled and be a public
    client. See :ref:`oidc_idp_requirements`.

Two ways to use it
==================

You can let the helper drive everything, or you can just take the token and use
it with your own tooling.

Just the token (PG-wire / HTTP / anything)
------------------------------------------

If you connect to QuestDB yourself — over PG-wire, raw HTTP, or any other
client — you only need a valid token. This path has **no extra dependencies**.

.. code-block:: python

    from questdb.auth import OidcDeviceAuth

    # Discover the OIDC configuration from the QuestDB server:
    auth = OidcDeviceAuth.from_questdb("https://questdb.example.com:9000")

    token = auth.token()        # runs the device flow on first use, else cached
    headers = auth.headers()    # {"Authorization": "Bearer <token>"}

    # Use the token however you like, e.g. PG-wire via psycopg:
    import psycopg
    conn = psycopg.connect(
        host="questdb.example.com", port=8812, dbname="qdb",
        user="_sso", password=token)

The integrated session
----------------------

The high-level :func:`questdb.auth.connect` returns a :class:`~questdb.auth.QuestDB`
session that signs you in and adapts the token into the common Python access
paths.

.. code-block:: python

    from questdb.auth import connect

    qdb = connect("https://questdb.example.com:9000")   # interactive sign-in
    df = qdb.sql("SELECT * FROM trades WHERE ts > dateadd('h', -1, now())")

    # Bring-your-own client, same auto-refreshed token:
    engine = qdb.sqlalchemy_engine()                    # PG-wire, token as _sso
    with qdb.psycopg() as conn:                         # raw psycopg
        ...
    with qdb.sender() as sender:                        # ingestion (ILP/HTTP)
        sender.row("trades", columns={"price": 101.5},
                   at=TimestampNanos.now())

On first use you will see a sign-in prompt (rendered as a clickable link in
Jupyter, plain text on a terminal)::

    🔐 Sign in to QuestDB
       Open https://idp.example.com/device  and enter code:  WDJB-MJHT
       (or open directly: https://idp.example.com/device?user_code=WDJB-MJHT)
       ⏳ waiting for authorization… (4:51 left)
    ✅ Signed in as alice@example.com — token cached, expires in 60 min

Re-running any cell is silent — the token is cached and refreshed silently on
the next use once it nears expiry.

How it works
============

Configuration discovery
------------------------

:meth:`OidcDeviceAuth.from_questdb <questdb.auth.OidcDeviceAuth.from_questdb>`
(and :func:`~questdb.auth.connect`) resolve the OIDC configuration in this
order:

1. ``GET {url}/settings`` (public, no auth) for the QuestDB-authoritative
   values: ``acl.oidc.client.id``, ``acl.oidc.scope``, ``acl.oidc.token.endpoint``,
   ``acl.oidc.groups.encoded.in.token`` and (on newer servers)
   ``acl.oidc.device.authorization.endpoint``.
2. If the device-authorization endpoint is not advertised, the helper falls
   back to the IdP discovery document
   (``{issuer}/.well-known/openid-configuration``). This path **requires** an
   explicit ``issuer=`` (or ``discovery_url=``) argument.

Anything you pass explicitly overrides discovery. You can also skip discovery
entirely:

.. code-block:: python

    auth = OidcDeviceAuth(
        client_id="questdb",
        device_authorization_endpoint="https://idp/.../device",
        token_endpoint="https://idp/.../token",
        scope="openid groups",
        groups_in_token=True,     # send id_token (True) vs access_token (False)
        audience="questdb",       # optional; some IdPs need it to set `aud`
        cache="memory")

Which token is sent
-------------------

The helper mirrors QuestDB's own selection logic
(``groupsEncodedInToken ? idToken : accessToken``):

============================================ =================
``acl.oidc.groups.encoded.in.token``         Helper sends
============================================ =================
``true``                                      ``id_token``
``false``                                     ``access_token``
============================================ =================

When sending the ``id_token`` the ``openid`` scope is requested automatically.

Token lifecycle (cache + refresh)
---------------------------------

``token()`` returns the cached token while it is valid (with a small clock-skew
margin). When it nears expiry the helper silently refreshes it using the
``refresh_token`` if one was issued. If the refresh token is missing or rejected
(expired/revoked), it re-runs the interactive sign-in; a transient network error
is raised instead, so you can retry without being needlessly re-prompted. A lock
serializes refresh so parallel cells/threads don't double-prompt.

Cache backends (``cache=`` argument):

* ``"memory"`` *(default)* — process-global, nothing written to disk.
  Re-running cells is silent; a kernel restart re-prompts once.
* ``None`` — never persist; prompt every time.

Tokens are deliberately never written to disk: a kernel restart re-prompts
(an interactive sign-in is cheap relative to the risk of a refresh token
sitting in a plaintext file at rest).

Non-interactive contexts
-------------------------

Scheduled / non-interactive notebooks (papermill, cron, CI) have no human to
authorize the device. The helper detects this and raises
:class:`~questdb.auth.OidcInteractionRequired` instead of hanging. Use a QuestDB
**service-account REST token** or the **client-credentials** grant there.

Connection adapters
===================

* :meth:`QuestDB.sql <questdb.auth.QuestDB.sql>` — query over REST ``/exec`` to a
  pandas DataFrame using ``Authorization: Bearer``. Recommended: there is no
  token-length limit (a groups-encoded JWT can be several KB).
* :meth:`QuestDB.sqlalchemy_engine <questdb.auth.QuestDB.sqlalchemy_engine>` —
  PG-wire engine that injects a fresh token as the ``_sso`` password for every
  new connection. Requires ``acl.oidc.pg.token.as.password.enabled=true``.
* :meth:`QuestDB.psycopg <questdb.auth.QuestDB.psycopg>` — a raw psycopg /
  psycopg2 connection.
* :meth:`QuestDB.sender <questdb.auth.QuestDB.sender>` — a
  :class:`~questdb.ingress.Sender` for ingestion (ILP over HTTP).

.. note::

    QuestDB validates the token at **authentication** time, not per query. An
    already-open PG connection survives token expiry; only **new** connections
    need a fresh token — which is why the PG-wire adapter supplies the token
    per-connect.

.. _oidc_idp_requirements:

IdP requirements
===============

The OIDC client referenced by ``acl.oidc.client.id`` must:

* have the **Device Authorization grant** enabled;
* be a **public client** (no secret in a notebook);
* optionally issue **refresh tokens** for the device grant (for silent refresh);
* issue tokens whose ``aud`` matches ``acl.oidc.audience`` (some IdPs need an
  ``audience``/``resource`` request parameter);
* include the **groups** claim in the token (``groups.encoded.in.token=true``)
  or expose it via the **userinfo** endpoint (``false``), matching the server.

Security notes
=============

* No IdP passwords are ever entered in the notebook; MFA/SSO happen at the IdP.
* ``https`` is required. Plaintext ``http`` to a **loopback** address
  (``localhost`` / ``127.0.0.1`` / ``::1``) is always allowed — it never leaves
  the host. ``insecure=True`` additionally permits plaintext to a non-loopback
  **QuestDB** host (local development only); it does **not** downgrade the
  **IdP**, so the device code and refresh token are never sent in cleartext
  over the network. Certificate verification is never disabled.
* **Endpoint trust.** The device code and the long-lived refresh token are sent
  to the device-authorization and token endpoints, which are discovered from
  QuestDB ``/settings``. The helper requires both endpoints to share a single
  origin and rejects the configuration otherwise. Because ``/settings`` is
  authoritative-by-QuestDB, a compromised server could in principle point them
  elsewhere; pass ``issuer=`` to **pin** the IdP so the endpoints are verified
  to belong to it and credentials can't be redirected to another host. When the
  server does not advertise the device-authorization endpoint (so it must be
  discovered from the IdP), ``issuer=`` (or ``discovery_url=``) is **required**
  for exactly this reason — the helper refuses to guess the discovery origin
  from the server-supplied token endpoint.
* Adapters avoid logging the token / PG DSN. Avoid logging them yourself.
* Standard proxy / CA settings (``HTTPS_PROXY``, ``REQUESTS_CA_BUNDLE``,
  ``SSL_CERT_FILE``) are honoured; you can also pass ``ca_bundle=``.

Dependencies
===========

``token()`` / ``headers()`` need nothing beyond the standard library. The
following are imported lazily, only when used:

* ``pandas`` — for :meth:`QuestDB.sql`;
* ``sqlalchemy`` and ``psycopg`` / ``psycopg2`` — for the PG-wire adapters;
* ``qrcode`` — to render a QR code for phone-based authorization (``qr=True``);
* ``IPython`` — for the rich Jupyter prompt (falls back to plain text).
