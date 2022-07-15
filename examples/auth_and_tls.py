from questdb.ingress import Sender


def example():
    # See: https://questdb.io/docs/reference/api/ilp/authenticate
    auth = (
        "testUser1",                                    # kid
        "5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48",  # d
        "fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU",  # x
        "Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac")  # y
    with Sender('localhost', 9009, auth=auth, tls=True) as sender:
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