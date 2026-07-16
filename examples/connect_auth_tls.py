import sys

import questdb
from questdb import QuestDBError, TimestampNanos


def on_connection_event(event):
    print(event.kind, event.host, event.port, event.cause_msg,
          file=sys.stderr)


def example():
    # A production-shaped configuration: TLS with OS trust roots, a
    # bearer token, every node of the deployment in one addr list, and
    # a disk-backed store-and-forward spool that replays
    # unacknowledged frames after a process restart with the same
    # sender_id. For HTTP basic auth, replace the token with
    # 'username=...;password=...;'.
    conf = (
        'wss::addr=db-primary.example.com:9000,db-replica.example.com:9000;'
        'token=YOUR_BEARER_TOKEN;'
        'tls_ca=os_roots;'
        'sf_dir=/var/spool/questdb;'
        'sender_id=ingest-01;'
    )
    try:
        with questdb.connect(
                conf, connection_listener=on_connection_event) as db:
            info = db.server_info()
            print('connected to', info.role)

            with db.sender() as sender:
                sender.row(
                    'trades',
                    symbols={'symbol': 'ETH-USD'},
                    columns={'price': 2615.54, 'amount': 0.00044},
                    at=TimestampNanos.now())
                # Block until the server acknowledges everything
                # published on this lease.
                sender.flush(wait=True)

            print('events delivered:', db.connection_events_delivered())

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')
        sys.exit(1)


if __name__ == '__main__':
    example()
