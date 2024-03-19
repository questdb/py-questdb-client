from questdb.ingress import Sender, TimestampNanos
import random
import uuid
import time


def example(host: str = 'localhost', port: int = 9009):
    table_name: str = str(uuid.uuid1())
    # Flush if the internal buffer exceeds 1KiB
    conf: str = f"tcp::addr={host}:{port};auto_flush_bytes=1024;"
    with Sender.from_conf(conf) as sender:
        total_rows = 0
        last_flush = time.monotonic()
        try:
            print("Ctrl^C to terminate...")
            while True:
                time.sleep(random.randint(0, 750) / 1000)  # sleep up to 750 ms

                print('Inserting row...')
                sender.row(
                    table_name,
                    symbols={
                        'src': random.choice(('ALPHA', 'BETA', 'OMEGA')),
                        'dst': random.choice(('ALPHA', 'BETA', 'OMEGA'))},
                    columns={
                        'price': random.randint(200, 500),
                        'qty': random.randint(1, 5)},
                    at=TimestampNanos.now())
                total_rows += 1

                # If the internal buffer is empty, then auto-flush triggered.
                if len(sender) == 0:
                    print('Auto-flush triggered.')
                    last_flush = time.monotonic()

                # Flush at least once every five seconds.
                if time.monotonic() - last_flush > 5:
                    print('Timer-flushing triggered.')
                    sender.flush()
                    last_flush = time.monotonic()

        except KeyboardInterrupt:
            print(f"table: {table_name}, total rows sent: {total_rows}")
            print("(wait commitLag for all rows to be available)")
            print("bye!")


if __name__ == '__main__':
    example()
