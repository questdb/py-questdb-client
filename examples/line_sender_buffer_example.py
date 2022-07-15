from questdb.ingress import Sender, Buffer, TimestampNanos

if __name__ == '__main__':
    with Sender('localhost', 9009) as sender:
        buffer = sender.new_buffer()
        buffer.row(
            'line_sender_buffer_example',
            symbols={'id': 'Hola'},
            columns={'price': '111222233333i', 'qty': 3.5},
            at=TimestampNanos(111222233333)
        )
        buffer.row(
            'line_sender_example',
            symbols={'id': 'Adios'},
            columns={'price': '111222233343i', 'qty': 2.5},
            at=TimestampNanos(111222233343)
        )
        # last line is not flushed
        sender.flush(buffer)
