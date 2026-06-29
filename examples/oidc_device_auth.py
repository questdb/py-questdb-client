"""
Interactive OIDC sign-in to QuestDB Enterprise from Python (e.g. a notebook).

Runs the OAuth 2.0 Device Authorization Grant (RFC 8628) client-side: you
authorize in any browser (laptop or phone), while the code runs on a possibly
remote kernel that only makes outbound calls to your identity provider.

This requires QuestDB Enterprise with OIDC enabled and an IdP client that has
the device grant enabled. It cannot run unattended (there is a human in the
loop), so it is not part of the automated example suite.
"""

import sys

from questdb.auth import (
    FileTokenStore,
    OidcDeviceAuth,
    OidcError,
    psycopg_connect,
    sqlalchemy_engine,
)


QUESTDB_URL = 'https://questdb.example.com:9000'


def sign_in(url: str = QUESTDB_URL) -> OidcDeviceAuth:
    """Discover config from QuestDB and sign in interactively (once)."""
    auth = OidcDeviceAuth.from_questdb(url)
    # The first token() triggers the interactive device-flow sign-in; the token
    # is cached and refreshed silently, so re-running is silent until it
    # expires. Sign in once up front, before any connection pool opens.
    auth.token()
    return auth


def pg_wire(url: str = QUESTDB_URL):
    """Query over PG-wire, with the token wired in as the ``_sso`` password.

    Requires ``acl.oidc.pg.token.as.password.enabled=true`` on the server.
    """
    auth = sign_in(url)

    # SQLAlchemy: a fresh token is injected as the password on every new
    # (pooled) connection, so the engine keeps working as the token rotates.
    from sqlalchemy import text
    engine = sqlalchemy_engine(auth, url)
    with engine.connect() as conn:
        for row in conn.execute(text('SELECT * FROM trades LIMIT 10')):
            print(row)

    # Or a raw psycopg / psycopg2 connection (token captured at connect time):
    with psycopg_connect(auth, url) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT count() FROM trades')
            print(cur.fetchone())


def persist_across_restarts(url: str = QUESTDB_URL) -> OidcDeviceAuth:
    """Survive a process restart without prompting again.

    By default the token is in-memory only, so a restarted kernel / script
    re-runs the device flow. Pass a ``token_store`` to persist it: the restarted
    process resumes from the saved refresh token (a silent token-endpoint call)
    instead of re-prompting. ``FileTokenStore`` writes one file per identity
    under ``~/.questdb/oidc-tokens/`` (``0600``, owner-only); supply your own
    ``TokenStore`` to back it with an OS keychain for at-rest encryption.
    """
    auth = OidcDeviceAuth.from_questdb(
        url, token_store=FileTokenStore.at_default_location())
    auth.token()  # prompts the first time; silent on later runs / after restart
    return auth


def bring_your_own_client(url: str = QUESTDB_URL):
    """You just want the token (REST / ingestion / anything)."""
    auth = OidcDeviceAuth.from_questdb(url)

    token = auth.token()               # valid, auto-refreshed id/access token
    headers = auth.headers()           # {"Authorization": "Bearer <token>"}
    print('Authorization header ready:', 'Authorization' in headers)

    # Ingestion: hand the token to the ILP Sender. questdb.ingress is the
    # compiled extension; import it lazily so this module loads without it.
    from questdb.ingress import Sender, TimestampNanos
    with Sender.from_conf(
            'https::addr=questdb.example.com:9000;', token=token) as sender:
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD', 'side': 'sell'},
            columns={'price': 2615.54, 'amount': 0.00044},
            at=TimestampNanos.now())

    return token


def main():
    try:
        pg_wire()
    except OidcError as e:
        sys.stderr.write(f'OIDC sign-in failed: {e}\n')


if __name__ == '__main__':
    main()
