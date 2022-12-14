import patch_path
patch_path.patch()

import pandas as pd
import questdb.ingress as qi

import os, psutil
process = psutil.Process(os.getpid())

def get_rss():
    return process.memory_info().rss 


def serialize_and_cleanup():
    # qi.Buffer().row(
    #     'table_name',
    #     symbols={'x': 'a', 'y': 'b'},
    #     columns={'a': 1, 'b': 2, 'c': 3})
    df = pd.DataFrame({
        'a': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        'b': [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        'c': [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]})
    qi.Buffer().dataframe(df, table_name='test')


def main():
    warmup_count = 0
    for n in range(1000000):
        if n % 1000 == 0:
            print(f'[iter: {n:09}, RSS: {get_rss():010}]')
        if n > warmup_count:
            before = get_rss()
        serialize_and_cleanup()
        if n > warmup_count:
            after = get_rss()
            if after != before:
                msg = f'RSS changed from {before} to {after} after {n} iters'
                print(msg)


if __name__ == '__main__':
    main()
    