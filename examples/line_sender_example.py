from questdb.ingress import Sender


#  buffer.tabular(
#     #             'table_name',
#     #             [['abc', 123, 3.14, 'xyz'],
#     #              ['def', 456, 6.28, 'abc'],
#     #              ['ghi', 789, 9.87, 'def']],
#     #             header=['col1', 'col2', 'col3', 'col4'],
#     #             symbols=True)  # `col1` and `col4` are SYMBOL columns.
def tabular(sender, table_name):
    pass


if __name__ == '__main__':
    with Sender('localhost', 9009) as sender:
        sender.row(
            'line_sender_example',
            symbols={'id': 'OMEGA'},
            columns={'price': '111222233333i', 'qty': 3.5}
        )
        sender.row(
            'line_sender_example',
            symbols={'id': 'ZHETA'},
            columns={'price': '111222233330i', 'qty': 2.5}
        )
        sender.flush()
