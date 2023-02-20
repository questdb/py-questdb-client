from questdb.ingress import Sender, IngressError, TimestampNanos
import sys
import datetime


def example(host: str = 'localhost', port: int = 9009):
    try:
        # See: https://questdb.io/docs/reference/api/ilp/authenticate
        auth = (
            "testUser1",                                    # kid
            "5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48",  # d
            "fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU",  # x
            "Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac")  # y
        with Sender(host, port, auth=auth) as sender:
            # Record with provided designated timestamp (using the 'at' param)
            # Notice the designated timestamp is expected in Nanoseconds,
            # but timestamps in other columns are expected in Microseconds. 
            # The API provides convenient functions
            sender.row(
                'trades',
                symbols={
                    'pair': 'USDGBP',
                    'type': 'buy'},
                columns={
                    'traded_price': 0.83,
                    'limit_price': 0.84,
                    'qty': 100,
                    'traded_ts': datetime.datetime(
                        2022, 8, 6, 7, 35, 23, 189062,
                        tzinfo=datetime.timezone.utc)},
                at=TimestampNanos.now())

            # If no 'at' param is passed, the server will use its own timestamp.
            sender.row(
                'trades',
                symbols={'pair': 'EURJPY'},
                columns={
                    'traded_price': 135.97,
                    'qty': 400,
                    'limit_price': None})  # NULL columns can be passed as None,
                                           # or simply be left out.

            # We recommend flushing periodically, for example every few seconds.
            # If you don't flush explicitly, the client will flush automatically
            # once the buffer is reaches 63KiB and just before the connection
            # is closed.
            sender.flush()

    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
