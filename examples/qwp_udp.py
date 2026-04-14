from questdb.ingress import Sender, Protocol, IngressError, TimestampNanos
import sys


def example(
        host: str = 'localhost',
        port: int = 9007,
        table_name: str = 'trades'):
    try:
        with Sender(
                Protocol.QwpUdp,
                host,
                port,
                max_datagram_size=1400) as sender:
            sender.row(
                table_name,
                symbols={
                    'symbol': 'ETH-USD',
                    'side': 'sell'},
                columns={
                    'price': 2615.54,
                    'amount': 0.00044,
                },
                at=TimestampNanos.now())

            # QWP/UDP defaults `auto_flush_bytes` to the datagram size.
            # Flush manually here to send the row immediately.
            sender.flush()

    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
