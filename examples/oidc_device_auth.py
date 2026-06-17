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

from questdb.auth import connect, OidcDeviceAuth, OidcError


QUESTDB_URL = 'https://questdb.example.com:9000'


def integrated(url: str = QUESTDB_URL):
    """The high-level path: sign in, then query / ingest with one object."""
    # First call triggers the interactive device-flow sign-in; the token is
    # cached, so re-running this is silent until it expires.
    qdb = connect(url)

    # Query straight to a pandas DataFrame over REST (Authorization: Bearer).
    df = qdb.sql("SELECT * FROM trades WHERE ts > dateadd('h', -1, now())")
    print(df)

    # Feed the same auto-refreshed token into your existing tooling:
    #   engine = qdb.sqlalchemy_engine()   # PG-wire, token as _sso password
    #   with qdb.psycopg() as conn: ...    # raw psycopg
    #
    # questdb.ingress is the compiled extension; import it lazily (only the
    # ingestion path needs it) so this module also loads for the pure-Python
    # bring_your_own_client() path, which needs no extension.
    from questdb.ingress import TimestampNanos
    with qdb.sender() as sender:           # ingestion (ILP over HTTP)
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD', 'side': 'sell'},
            columns={'price': 2615.54, 'amount': 0.00044},
            at=TimestampNanos.now())


def bring_your_own_client(url: str = QUESTDB_URL):
    """The low-level path: you just want the token (PG-wire / HTTP / anything)."""
    auth = OidcDeviceAuth.from_questdb(url)

    token = auth.token()               # valid, auto-refreshed id/access token
    headers = auth.headers()           # {"Authorization": "Bearer <token>"}
    print('Authorization header ready:', 'Authorization' in headers)

    # e.g. hand the token to psycopg yourself over PG-wire:
    #   import psycopg
    #   conn = psycopg.connect(host='questdb.example.com', port=8812,
    #                          dbname='qdb', user='_sso', password=token)
    return token


def main():
    try:
        integrated()
    except OidcError as e:
        sys.stderr.write(f'OIDC sign-in failed: {e}\n')


if __name__ == '__main__':
    main()
