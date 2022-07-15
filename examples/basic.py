from questdb.ingress import Sender


def example():
    with Sender('localhost', 9009) as sender:
        sender.row(
            'line_sender_example',
            symbols={'id': 'OMEGA'},
            columns={'price': 111222233333, 'qty': 3.5})
        sender.row(
            'line_sender_example',
            symbols={'id': 'ZHETA'},
            columns={'price': 111222233330, 'qty': 2.5})
        sender.flush()


if __name__ == '__main__':
    example()