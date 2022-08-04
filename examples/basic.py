from questdb.ingress import Sender

import examples.util as util


def example(host: str = 'localhost', port: int = 9009, table_name: str = util.rand_table_name()):
    with Sender(host=host, port=port) as sender:
        total_rows = 0
        try:
            print("Ctrl^C to terminate...")
            while True:
                if util.randint(1, 100) > 70:
                    sender.row(
                        table_name,
                        symbols={
                            'src': util.rand_symbol(),
                            'dst': util.rand_symbol()
                        },
                        columns={
                            'price': util.randint(200, 500),
                            'qty': util.randint(1, 5)
                        }
                    )
                    total_rows += 1
        except KeyboardInterrupt:
            print(f"table: {table_name}, total rows sent: {total_rows}")
            print("(wait commitLag for all rows to be available)")
            print("bye!")


if __name__ == '__main__':
    example()
