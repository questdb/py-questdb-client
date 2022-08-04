from questdb.ingress import Sender

import examples.util as util


def example(
        host: str = 'localhost',
        port: int = 9009,
        table_name: str = util.rand_table_name(),
        batch_row_limit: int = 200 * 1000
):
    sender = Sender(host=host, port=port, auto_flush=False)
    sender.connect()
    buffer = sender.new_buffer()
    batch_rows = 0
    total_rows = 0
    try:
        sensors = util.Sensors()
        print("Ctrl^C to terminate...")
        while True:
            participants = sensors.tick(util.randint(1, 2))
            if participants > 0:
                buffer.row(
                    table_name,
                    symbols={'id': 'tick'},
                    columns={f"sensor_{p}": sensors[p] for p in range(participants)}
                )
            else:
                buffer.row(table_name, symbols={'id': 'RadioSilence'})
            batch_rows += 1
            if batch_rows > batch_row_limit:
                sender.flush(buffer, clear=True)
                total_rows += batch_rows
                batch_rows = 0
                print(f"table: {table_name}, rows sent so far: {total_rows}")
    except KeyboardInterrupt:
        buffer.row(table_name, symbols={'id': 'Adios'})
        sender.flush(buffer, clear=True)
        sender.close()
        total_rows += batch_rows + 1
        print(f"table: {table_name}, total rows sent: {total_rows}")
        print("(wait commitLag for all rows to be available)")
        print("bye!")


if __name__ == '__main__':
    example()
