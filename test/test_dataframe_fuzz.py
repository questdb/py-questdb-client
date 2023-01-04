"""
# On Linux, ensure `clang` is installed.
pyenv shell 3.10
./proj clean
./proj build_fuzzing
./proj test_fuzzing
"""

import sys
import struct
import patch_path
patch_path.patch()
import numpy as np
from numpy.random import Generator, PCG64
import pandas as pd
import pyarrow as pa
import re
import atheris


with atheris.instrument_imports():
    import questdb.ingress as qi


@atheris.instrument_func
def get_test_alphabet():
    include_ranges = [
        (0x0021, 0x0021),
        (0x0023, 0x0026),
        (0x0028, 0x007E),
        (0x00A1, 0x00AC),
        (0x00AE, 0x00FF),
        (0x0100, 0x017F),
        (0x0180, 0x024F),
        (0x2C60, 0x2C7F),
        (0x16A0, 0x16F0),
        (0x0370, 0x0377),
        (0x037A, 0x037E),
        (0x0384, 0x038A),
        (0x038C, 0x038C)]
    return [
        chr(code_point)
        for current_range in include_ranges
            for code_point in range(current_range[0], current_range[1] + 1)]


TEST_ALPHABET = get_test_alphabet()


def get_random_unicode(rand, length, none_val_prob=0):
    """
    Adapted from https://stackoverflow.com/questions/1477294
    """
    if none_val_prob and (rand.random() < none_val_prob):
        return None
    return ''.join(rand.choice(TEST_ALPHABET) for _ in range(length))


@atheris.instrument_func
def gen_string_series(rand, n_rows, none_val_prob, length, dtype):
    series_n_rows = n_rows
    if dtype == 'categorical':
        series_n_rows //= 4
    data = [
        get_random_unicode(rand, length, none_val_prob)
        for _ in range(series_n_rows)]
    if dtype == 'categorical':
        data = data * 6
        data = data[:n_rows]
        rand.shuffle(data)
    return pd.Series(data, dtype=dtype)


def gen_numpy_series(rand, n_rows, dtype):
    return pd.Series(
        rand.integers(
            np.iinfo(dtype).min,
            np.iinfo(dtype).max,
            size=n_rows,
            dtype=dtype))


@atheris.instrument_func
def gen_series_i8_numpy(rand, n_rows, none_val_prob):
    return gen_numpy_series(rand, n_rows, np.int8)


@atheris.instrument_func
def gen_series_pyobj_str(rand, n_rows, none_val_prob):
    return gen_string_series(rand, n_rows, none_val_prob, 6, 'object')


# TODO: Test all datatypes
# TODO: Include None, NA and NaN.
series_generators = [
    gen_series_i8_numpy,
    # gen_series_i16_numpy,
    gen_series_pyobj_str]



@atheris.instrument_func
def parse_input_bytes(input_bytes):
    fdp = atheris.FuzzedDataProvider(input_bytes)
    rand_seed = fdp.ConsumeUInt(1)
    none_val_prob = fdp.ConsumeProbability()
    table_name_type = fdp.ConsumeIntInRange(0, 4)
    table_name_len = fdp.ConsumeIntInRange(1, 32)
    n_cols = fdp.ConsumeIntInRange(10, 40)
    col_generators = [
        series_generators[fdp.ConsumeIntInRange(0, len(series_generators) - 1)]
        for _ in range(n_cols)]
    n_rows = fdp.ConsumeIntInRange(10, 5000)
    rand = Generator(PCG64(rand_seed))
    series_list = []
    col_name = lambda: f'{get_random_unicode(rand, 4)}_{len(series_list)}'
    table_name = None
    table_name_col = None
    symbols = 'auto'
    at = None
    if table_name_type == 0:
        table_name = get_random_unicode(rand, table_name_len)
    else:
        table_name_col = col_name()
        dtype = {
            1: 'object',
            2: 'string',
            3: 'string[pyarrow]',
            4: 'category'}[table_name_type]
        series = gen_string_series(rand, n_rows, 0, table_name_len, dtype)
        series_list.append((table_name_col, series))

    for index in range(n_cols):
        name = col_name()
        series = col_generators[index](rand, n_rows, none_val_prob)
        series_list.append((name, series))
    rand.shuffle(series_list)
    series = dict([
        (name, series)
        for name, series in series_list])
    df = pd.DataFrame(series)
    return df, table_name, table_name_col, symbols, at


@atheris.instrument_func
def test_dataframe(input_bytes):
    # print(f'input_bytes: {input_bytes}')
    params = parse_input_bytes(input_bytes)
    df, table_name, table_name_col, symbols, at = params

    try:
        BUF = qi.Buffer()
        BUF.clear()
        try:
            BUF.dataframe(
                df,
                table_name=table_name,
                table_name_col=table_name_col,
                symbols=symbols,
                at=at)
        except Exception as e:
            if isinstance(e, (qi.IngressError)):
                msg = str(e)
                if 'Bad argument `table_name`' in msg:
                    return
                if re.search(r'Failed .*Bad string.*', msg):
                    return
                if re.search(r'Bad string .*: Column names', msg):
                    return
                if 'Ensure at least one column is not null.' in msg:
                    return
            raise e
    except:
        print('>>>>>>>>>')
        print(f'input_bytes: {input_bytes!r}')
        print(f'df: {df}')
        print(f'table_name: {table_name}')
        print(f'table_name_col: {table_name_col}')
        print(f'symbols: {symbols}')
        print(f'at: {at}')
        print('<<<<<<<<<')
        raise


def main():
    args = list(sys.argv)
    atheris.Setup(args, test_dataframe)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
