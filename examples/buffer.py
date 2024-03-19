from questdb.ingress import Sender, TimestampNanos


def example(host: str = 'localhost', port: int = 9000):
    with Sender.from_conf(f"http::addr={host}:{port};") as sender:
        buffer = sender.new_buffer()
        buffer.row(
            'line_sender_buffer_example',
            symbols={'id': 'Hola'},
            columns={'price': 111222233333, 'qty': 3.5},
            at=TimestampNanos(111222233333))
        buffer.row(
            'line_sender_example',
            symbols={'id': 'Adios'},
            columns={'price': 111222233343, 'qty': 2.5},
            at=TimestampNanos(111222233343))
        sender.flush(buffer)


if __name__ == '__main__':
    example()
