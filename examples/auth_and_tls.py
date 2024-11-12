from questdb.ingress import Sender, IngressError, TimestampNanos
import sys


def example(host: str = 'localhost', port: int = 9009):
    try:
        conf = (
            f"tcps::addr={host}:{port};" +
            "username=testUser1;" +
            "token=5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48;" +
            "token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU;" +
            "token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac;")
        with Sender.from_conf(conf) as sender:
            # Record with provided designated timestamp (using the 'at' param)
            # Notice the designated timestamp is expected in Nanoseconds,
            # but timestamps in other columns are expected in Microseconds.
            # The API provides convenient functions
            sender.row(
                'trades',
                symbols={
                    'symbol': 'ETH-USD',
                    'side': 'sell'},
                columns={
                    'price': 2615.54,
                    'amount': 0.00044,
                   },
                at=TimestampNanos.now())

            # You can call `sender.row` multiple times inside the same `with`
            # block. The client will buffer the rows and send them in batches.

            # You can flush manually at any point.
            sender.flush()

            # If you don't flush manually, the client will flush automatically
            # when a row is added and either:
            #   * The buffer contains 75000 rows (if HTTP) or 600 rows (if TCP)
            #   * The last flush was more than 1000ms ago.
            # Auto-flushing can be customized via the `auto_flush_..` params.

        # Any remaining pending rows will be sent when the `with` block ends.

    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
