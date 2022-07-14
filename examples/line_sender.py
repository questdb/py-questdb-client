from questdb.ilp import Sender

if __name__ == "__main__":
    with Sender(host='localhost', port=9009) as sender:
        for _ in range(3):
            sender.row(
                'tab',
                symbols={
                    'name_a': 'val_a'
                },
                columns={
                    'name_b': True,
                    'name_c': 42,
                    'name_d': 2.5,
                    'name_e': 'val_b'
                }
            )
        pending = str(sender)
        print(pending)