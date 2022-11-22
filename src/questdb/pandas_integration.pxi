# See: pandas_integration.md for technical overview.

# cdef struct dtype_t:
#     # See: https://numpy.org/doc/stable/reference/generated/numpy.dtype.html
#     #          ?highlight=dtype#numpy.dtype
#     # See: https://numpy.org/doc/stable/reference/c-api
#     #          /types-and-structures.html#c.PyArray_Descr
#     int alignment
#     char kind
#     int itemsize
#     char byteorder
#     bint hasobject


cdef enum col_access_tag_t:
    numpy
    arrow


cdef struct col_chunks_t:
    size_t n_chunks
    ArrowArray* chunks  # We calloc `n_chunks + 1` of these.


cdef struct col_cursor_t:
    ArrowArray* chunk  # Current chunk.
    size_t chunk_index
    size_t offset  # i.e. the element index (not byte offset)


cdef enum col_target_t:
    col_target_skip = 0
    col_target_table = 1
    col_target_symbol = 2
    col_target_column_bool = 3
    col_target_column_i64 = 4
    col_target_column_f64 = 5
    col_target_column_str = 6
    col_target_column_ts = 7
    col_target_at = 8


cdef dict _TARGET_NAMES = {
    col_target_t.col_target_skip: "skipped",
    col_target_t.col_target_table: "table name",
    col_target_t.col_target_symbol: "symbol",
    col_target_t.col_target_column_bool: "boolean",
    col_target_t.col_target_column_i64: "integer",
    col_target_t.col_target_column_f64: "float",
    col_target_t.col_target_column_str: "string",
    col_target_t.col_target_column_ts: "timestamp",
    col_target_t.col_target_at: "designated timestamp",
}


cdef enum col_source_t:
    col_source_nulls =                0
    col_source_bool_pyobj =      101000
    col_source_bool_numpy =      102000
    col_source_bool_arrow =      103000
    col_source_int_pyobj =       201000
    col_source_u8_numpy =        202000
    col_source_i8_numpy =        203000
    col_source_u16_numpy =       204000
    col_source_i16_numpy =       205000
    col_source_u32_numpy =       206000
    col_source_i32_numpy =       207000
    col_source_u64_numpy =       208000
    col_source_i64_numpy =       209000
    col_source_u8_arrow =        210000
    col_source_i8_arrow =        211000
    col_source_u16_arrow =       212000
    col_source_i16_arrow =       213000
    col_source_u32_arrow =       214000
    col_source_i32_arrow =       215000
    col_source_u64_arrow =       216000
    col_source_i64_arrow =       217000
    col_source_float_pyobj =     301000
    col_source_f32_numpy =       302000
    col_source_f64_numpy =       303000
    col_source_f32_arrow =       304000
    col_source_f64_arrow =       305000
    col_source_str_pyobj =       401000
    col_source_str_arrow =       402000
    col_source_str_i8_cat =      403000
    col_source_str_i16_cat =     404000
    col_source_str_i32_cat =     405000
    col_source_str_i64_cat =     406000
    col_source_dt64ns_numpy =    501000
    col_source_dt64ns_tz_numpy = 502000


cdef dict _TARGET_TO_SOURCE = {
    col_target_t.col_target_skip: {
        col_source_t.col_source_nulls,
    },
    col_target_t.col_target_table: {
        col_source_t.col_source_str_pyobj,
        col_source_t.col_source_str_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
        col_source_t.col_source_str_i64_cat,
    },
    col_target_t.col_target_symbol: {
        col_source_t.col_source_str_pyobj,
        col_source_t.col_source_str_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
        col_source_t.col_source_str_i64_cat,
    },
    col_target_t.col_target_column_bool: {
        col_source_t.col_source_bool_pyobj,
        col_source_t.col_source_bool_numpy,
        col_source_t.col_source_bool_arrow,
    },
    col_target_t.col_target_column_i64: {
        col_source_t.col_source_int_pyobj,
        col_source_t.col_source_u8_numpy,
        col_source_t.col_source_i8_numpy,
        col_source_t.col_source_u16_numpy,
        col_source_t.col_source_i16_numpy,
        col_source_t.col_source_u32_numpy,
        col_source_t.col_source_i32_numpy,
        col_source_t.col_source_u64_numpy,
        col_source_t.col_source_i64_numpy,
        col_source_t.col_source_u8_arrow,
        col_source_t.col_source_i8_arrow,
        col_source_t.col_source_u16_arrow,
        col_source_t.col_source_i16_arrow,
        col_source_t.col_source_u32_arrow,
        col_source_t.col_source_i32_arrow,
        col_source_t.col_source_u64_arrow,
        col_source_t.col_source_i64_arrow,
    },
    col_target_t.col_target_column_f64: {
        col_source_t.col_source_float_pyobj,
        col_source_t.col_source_f32_numpy,
        col_source_t.col_source_f64_numpy,
        col_source_t.col_source_f32_arrow,
        col_source_t.col_source_f64_arrow,
    },
    col_target_t.col_target_column_str: {
        col_source_t.col_source_str_pyobj,
        col_source_t.col_source_str_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
        col_source_t.col_source_str_i64_cat,
    },
    col_target_t.col_target_column_ts: {
        col_source_t.col_source_dt64ns_numpy,
        col_source_t.col_source_dt64ns_tz_numpy,
    },
    col_target_t.col_target_at: {
        col_source_t.col_source_dt64ns_numpy,
        col_source_t.col_source_dt64ns_tz_numpy,
    },
}


# Targets associated with col_meta_target.field.
cdef tuple _FIELD_TARGETS = (
    col_target_t.col_target_skip,
    col_target_t.col_target_column_bool,
    col_target_t.col_target_column_i64,
    col_target_t.col_target_column_f64,
    col_target_t.col_target_column_str,
    col_target_t.col_target_column_ts)


# Targets that map directly from a meta target.
cdef set _DIRECT_META_TARGETS = {
    col_target_t.col_target_table,
    col_target_t.col_target_symbol,
    col_target_t.col_target_at,
}


# This is verbose, but..
#   * Enums give us constants.
#   * Constants allow unfolding `if` statements into `switch`
#   * Switch statements can be more heavily optimized by the C compiler.
cdef enum col_dispatch_code_t:
    col_dispatch_code_skip_nulls = \
        col_target_t.col_target_skip + col_source_t.col_source_nulls

    col_dispatch_code_table__str_pyobj = \
        col_target_t.col_target_table + col_source_t.col_source_str_pyobj
    col_dispatch_code_table__str_arrow = \
        col_target_t.col_target_table + col_source_t.col_source_str_arrow
    col_dispatch_code_table__str_i8_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i8_cat
    col_dispatch_code_table__str_i16_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i16_cat
    col_dispatch_code_table__str_i32_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i32_cat
    col_dispatch_code_table__str_i64_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i64_cat

    col_dispatch_code_symbol__str_pyobj = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_pyobj
    col_dispatch_code_symbol__str_arrow = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_arrow
    col_dispatch_code_symbol__str_i8_cat = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_i8_cat
    col_dispatch_code_symbol__str_i16_cat = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_i16_cat
    col_dispatch_code_symbol__str_i32_cat = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_i32_cat
    col_dispatch_code_symbol__str_i64_cat = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_i64_cat

    col_dispatch_code_column_bool__bool_pyobj = \
        col_target_t.col_target_column_bool + col_source_t.col_source_bool_pyobj
    col_dispatch_code_column_bool__bool_numpy = \
        col_target_t.col_target_column_bool + col_source_t.col_source_bool_numpy
    col_dispatch_code_column_bool__bool_arrow = \
        col_target_t.col_target_column_bool + col_source_t.col_source_bool_arrow

    col_dispatch_code_column_i64__int_pyobj = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_int_pyobj
    col_dispatch_code_column_i64__u8_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u8_numpy
    col_dispatch_code_column_i64__i8_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i8_numpy
    col_dispatch_code_column_i64__u16_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u16_numpy
    col_dispatch_code_column_i64__i16_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i16_numpy
    col_dispatch_code_column_i64__u32_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u32_numpy
    col_dispatch_code_column_i64__i32_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i32_numpy
    col_dispatch_code_column_i64__u64_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u64_numpy
    col_dispatch_code_column_i64__i64_numpy = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i64_numpy
    col_dispatch_code_column_i64__u8_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u8_arrow
    col_dispatch_code_column_i64__i8_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i8_arrow
    col_dispatch_code_column_i64__u16_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u16_arrow
    col_dispatch_code_column_i64__i16_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i16_arrow
    col_dispatch_code_column_i64__u32_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u32_arrow
    col_dispatch_code_column_i64__i32_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i32_arrow
    col_dispatch_code_column_i64__u64_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_u64_arrow
    col_dispatch_code_column_i64__i64_arrow = \
        col_target_t.col_target_column_i64 + col_source_t.col_source_i64_arrow

    col_dispatch_code_column_f64__float_pyobj = \
        col_target_t.col_target_column_f64 + col_source_t.col_source_float_pyobj
    col_dispatch_code_column_f64__f32_numpy = \
        col_target_t.col_target_column_f64 + col_source_t.col_source_f32_numpy
    col_dispatch_code_column_f64__f64_numpy = \
        col_target_t.col_target_column_f64 + col_source_t.col_source_f64_numpy
    col_dispatch_code_column_f64__f32_arrow = \
        col_target_t.col_target_column_f64 + col_source_t.col_source_f32_arrow
    col_dispatch_code_column_f64__f64_arrow = \
        col_target_t.col_target_column_f64 + col_source_t.col_source_f64_arrow

    col_dispatch_code_column_str__str_pyobj = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_pyobj
    col_dispatch_code_column_str__str_arrow = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_arrow
    col_dispatch_code_column_str__str_i8_cat = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_i8_cat
    col_dispatch_code_column_str__str_i16_cat = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_i16_cat
    col_dispatch_code_column_str__str_i32_cat = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_i32_cat
    col_dispatch_code_column_str__str_i64_cat = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_i64_cat

    col_dispatch_code_column_ts__dt64ns_numpy = \
        col_target_t.col_target_column_ts + col_source_t.col_source_dt64ns_numpy
    col_dispatch_code_column_ts__dt64ns_tz_numpy = \
        col_target_t.col_target_column_ts + \
        col_source_t.col_source_dt64ns_tz_numpy


cdef struct col_t:
    size_t orig_index
    line_sender_column_name name
    col_access_tag_t tag
    Py_buffer pybuf
    ArrowSchema arrow_schema  # Schema of first chunk.
    col_chunks_t chunks
    col_cursor_t cursor
    col_source_t source
    col_target_t target
    col_dispatch_code_t dispatch_code  # source + target. Determines serializer.


cdef void col_t_release(col_t* col):
    # We can use `col.tag` to determine what to release.
    # Arrow chunks might be "synthetic" borrowing from `col.pybuf`.
    # TODO: Implement cleanup, PyBuffer_Release, etc.
    # TODO: Do we need to call Arrow's cleanup functions when done?
    pass


# Calloc'd array of col_t.
cdef struct col_t_arr:
    size_t size
    col_t* d


cdef col_t_arr col_t_arr_blank():
    cdef col_t_arr arr
    arr.size = 0
    arr.d = NULL
    return arr


cdef col_t_arr col_t_arr_new(size_t size):
    cdef col_t_arr arr
    arr.size = size
    arr.d = <col_t*>calloc(size, sizeof(col_t))
    return arr


cdef void col_t_arr_release(col_t_arr* arr):
    cdef size_t index
    if arr.d:
        for index in range(arr.size):
            col_t_release(&arr.d[index])
        free(arr.d)
        arr.size = 0
        arr.d = NULL


cdef object _NUMPY = None  # module object
cdef object _NUMPY_BOOL = None
cdef object _NUMPY_UINT8 = None
cdef object _NUMPY_INT8 = None
cdef object _NUMPY_UINT16 = None
cdef object _NUMPY_INT16 = None
cdef object _NUMPY_UINT32 = None
cdef object _NUMPY_INT32 = None
cdef object _NUMPY_UINT64 = None
cdef object _NUMPY_INT64 = None
cdef object _NUMPY_FLOAT32 = None
cdef object _NUMPY_FLOAT64 = None
cdef object _NUMPY_DATETIME64_NS = None
cdef object _NUMPY_OBJECT = None
cdef object _PANDAS = None  # module object
cdef object _PANDAS_NA = None  # pandas.NA
cdef object _PYARROW = None  # module object, if available or None


cdef object _pandas_may_import_deps():
    """"
    Lazily import module dependencies on first use to avoid startup overhead.

    $ cat imp_test.py 
    import numpy
    import pandas
    import pyarrow

    $ time python3 ./imp_test.py
    python3 ./imp_test.py  0.56s user 1.60s system 852% cpu 0.254 total
    """
    global _NUMPY, _PANDAS, _PYARROW, _PANDAS_NA
    global _NUMPY_BOOL
    global _NUMPY_UINT8
    global _NUMPY_INT8
    global _NUMPY_UINT16
    global _NUMPY_INT16
    global _NUMPY_UINT32
    global _NUMPY_INT32
    global _NUMPY_UINT64
    global _NUMPY_INT64
    global _NUMPY_FLOAT32
    global _NUMPY_FLOAT64
    global _NUMPY_DATETIME64_NS
    global _NUMPY_OBJECT
    if _NUMPY is not None:
        return
    import numpy
    _NUMPY = numpy
    _NUMPY_BOOL = type(_NUMPY.dtype('bool'))
    _NUMPY_UINT8 = type(_NUMPY.dtype('uint8'))
    _NUMPY_INT8 = type(_NUMPY.dtype('int8'))
    _NUMPY_UINT16 = type(_NUMPY.dtype('uint16'))
    _NUMPY_INT16 = type(_NUMPY.dtype('int16'))
    _NUMPY_UINT32 = type(_NUMPY.dtype('uint32'))
    _NUMPY_INT32 = type(_NUMPY.dtype('int32'))
    _NUMPY_UINT64 = type(_NUMPY.dtype('uint64'))
    _NUMPY_INT64 = type(_NUMPY.dtype('int64'))
    _NUMPY_FLOAT32 = type(_NUMPY.dtype('float32'))
    _NUMPY_FLOAT64 = type(_NUMPY.dtype('float64'))
    _NUMPY_DATETIME64_NS = type(_NUMPY.dtype('datetime64[ns]'))
    _NUMPY_OBJECT = type(_NUMPY.dtype('object'))
    import pandas
    _PANDAS = pandas
    _PANDAS_NA = pandas.NA
    try:
        import pyarrow
        _PYARROW = pyarrow
    except ImportError:
        _PYARROW = None


cdef str _fqn(type obj):
    return obj.__module__ + '.' + obj.__qualname__


cdef object _check_is_pandas_dataframe(object data):
    if not isinstance(data, _PANDAS.DataFrame):
        raise TypeError(
            f'Bad argument `data`: Expected {_fqn(_PANDAS.DataFrame)}, ' +
            f'not an object of type {_fqn(type(data))}.')


cdef ssize_t _pandas_resolve_table_name(
        qdb_pystr_buf* b,
        object data,
        list tagged_cols,
        object table_name,
        object table_name_col,
        size_t col_count,
        line_sender_table_name* name_out) except -2:
    """
    Return a tuple-pair of:
      * int column index
      * object
    
    If the column index is -1, then `name_out` is set and either the returned
    object is None or a bytes object to track ownership of data in `name_out`.

    Alternatively, if the returned column index > 0, then `name_out` is not set
    and the column index relates to which pandas column contains the table name
    on a per-row basis. In such case, the object is always None.

    This method validates input and may raise.
    """
    cdef size_t col_index = 0
    cdef TaggedEntry entry
    if table_name is not None:
        if table_name_col is not None:
            raise ValueError(
                'Can specify only one of `table_name` or `table_name_col`.')
        if isinstance(table_name, str):
            try:
                str_to_table_name(b, table_name, name_out)
                return -1  # Magic value for "no column index".
            except IngressError as ie:
                raise ValueError(f'Bad argument `table_name`: {ie}')
        else:
            raise TypeError('Bad argument `table_name`: Must be str.')
    elif table_name_col is not None:
        if isinstance(table_name_col, str):
            _pandas_get_loc(data, table_name_col, 'table_name_col', &col_index)
        elif isinstance(table_name_col, int):
            _bind_col_index(
                'table_name_col', table_name_col, col_count, &col_index)
        else:
            raise TypeError(
                'Bad argument `table_name_col`: ' +
                'must be a column name (str) or index (int).')
        _pandas_check_column_is_str(
            data,
            col_index,
            'Bad argument `table_name_col`: ',
            table_name_col)
        entry = tagged_cols[col_index]
        entry.meta_target = meta_target_t.meta_target_table
        name_out.len = 0
        name_out.buf = NULL
        return col_index
    else:
        raise ValueError(
            'Must specify at least one of `table_name` or `table_name_col`.')


cdef bint _bind_col_index(
        str arg_name, int col_num, size_t col_count,
        size_t* col_index) except False:
    """
    Validate that `col_index` is in bounds for `col_count`.
    This function also converts negative indicies (e.g. -1 for last column) to
    positive indicies.
    """
    cdef bint bad = False
    cdef int orig_col_num = col_num
    if col_num < 0:
        col_num += col_count  # Try convert negative offsets to positive ones.
    if col_num < 0:
        bad = True
    if (not bad) and (<size_t>col_num >= col_count):
        bad = True
    if bad:
        raise IndexError(
            f'Bad argument `{arg_name}`: {orig_col_num} index out of range')
    col_index[0] = <size_t>col_num
    return True


cdef object _pandas_column_is_str(object data, int col_index):
    """
    Return True if the column at `col_index` is a string column.
    """
    # NB: Returning `object` rather than `bint` to allow for exceptions.
    cdef str col_kind
    cdef object col
    col_kind = data.dtypes[col_index].kind
    if col_kind == 'S':  # string, string[pyarrow]
        return True
    elif col_kind == 'O':  # object
        if len(data.index) == 0:
            return True
        else:
            # We only check the first element and hope for the rest.
            # We also accept None as a null value.
            col = data.iloc[0, col_index]
            return (col is None) or isinstance(col, str)
    else:
        return False


cdef object _pandas_check_column_is_str(
        object data, size_t col_index, str err_msg_prefix, object col_name):
    cdef str col_kind
    col_kind = data.dtypes[col_index].kind
    if col_kind in 'SO':
        if not _pandas_column_is_str(data, col_index):
            raise TypeError(
                err_msg_prefix +
                'Found non-string value ' +
                f'in column {col_name!r}.')
    else:
        raise TypeError(
            err_msg_prefix + 
            f'Bad dtype `{data.dtypes[col_index]}` for the ' +
            f'{col_name!r} column: Must be a strings column.')


# Int values in order for sorting (as needed for API's sequential coupling).
cdef enum meta_target_t:
    meta_target_table = <int>col_target_t.col_target_table
    meta_target_symbol = <int>col_target_t.col_target_symbol
    meta_target_field = <int>col_target_t.col_target_column_bool
    meta_target_at = <int>col_target_t.col_target_at


@cython.internal
cdef class TaggedEntry:
    """Python object representing a column whilst parsing .pandas arguments."""

    cdef size_t orig_index
    cdef str name
    cdef object dtype
    cdef object series
    cdef meta_target_t meta_target

    def __init__(
            self,
            size_t orig_index,
            str name,
            object dtype,
            object series,
            meta_target_t meta_target):
        self.orig_index = orig_index
        self.name = name
        self.dtype = dtype
        self.series = series
        self.meta_target = meta_target


cdef bint _pandas_resolve_symbols(
        object data,
        list tagged_cols,
        ssize_t table_name_col,
        ssize_t at_col,
        object symbols,
        size_t col_count) except False:
    cdef size_t col_index = 0
    cdef object symbol
    cdef TaggedEntry entry
    if symbols is False:
        return 0
    elif symbols is True:
        for col_index in range(col_count):
            if _pandas_column_is_str(data, col_index):
                tagged_cols[col_index].meta_target = \
                    meta_target_t.meta_target_symbol
        return True
    else:
        if not isinstance(symbols, (tuple, list)):
            raise TypeError(
                f'Bad argument `symbols`: Must be a bool or a tuple or list '+
                'of column names (str) or indices (int).')
        for symbol in symbols:
            if isinstance(symbol, str):
                _pandas_get_loc(data, symbol, 'symbols', &col_index)
            elif isinstance(symbol, int):
                _bind_col_index('symbol', symbol, col_count, &col_index) 
            else:
                raise TypeError(
                    f'Bad argument `symbols`: Elements must ' +
                    'be a column name (str) or index (int).')
            if (table_name_col >= 0) and (col_index == <size_t>table_name_col):
                raise ValueError(
                    f'Bad argument `symbols`: Cannot use the same column ' +
                    f'{symbol!r} as both the table_name and as a symbol.')
            if (at_col >= 0) and (col_index == <size_t>at_col):
                raise ValueError(
                    f'Bad argument `symbols`: Cannot use the `at` column ' +
                    f'({data.columns[at_col]!r}) as a symbol column.')
            _pandas_check_column_is_str(
                data,
                col_index,
                'Bad element in argument `symbols`: ',
                symbol)
            entry = tagged_cols[col_index]
            entry.meta_target = meta_target_t.meta_target_symbol
        return True


cdef bint _pandas_get_loc(
        object data, str col_name, str arg_name,
        size_t* col_index_out) except False:
    """
    Return the column index for `col_name`.
    """
    try:
        col_index_out[0] = data.columns.get_loc(col_name)
        return True
    except KeyError:
        raise KeyError(
            f'Bad argument `{arg_name}`: ' +
            f'Column {col_name!r} not found in the dataframe.')


# The -1 value is safe to use as a sentinel because the TimestampNanos type
# already validates that the value is >= 0.
cdef int64_t _AT_SET_BY_COLUMN = -1


cdef ssize_t _pandas_resolve_at(
        object data,
        list tagged_cols,
        object at,
        size_t col_count,
        int64_t* at_value_out) except -2:
    cdef size_t col_index
    cdef object dtype
    cdef TaggedEntry entry
    if at is None:
        at_value_out[0] = 0  # Special value for `at_now`.
        return -1
    elif isinstance(at, TimestampNanos):
        at_value_out[0] = at._value
        return -1
    elif isinstance(at, datetime):
        at_value_out[0] = datetime_to_nanos(at)
        return -1
    elif isinstance(at, str):
        _pandas_get_loc(data, at, 'at', &col_index)
    elif isinstance(at, int):
        _bind_col_index('at', at, col_count, &col_index)
    else:
        raise TypeError(
            f'Bad argument `at`: Unsupported type {_fqn(type(at))}. ' +
            'Must be one of: None, TimestampNanos, datetime, ' +
            'int (column index), str (colum name)')
    dtype = data.dtypes[col_index]
    if _pandas_is_supported_datetime(dtype):
        at_value_out[0] = _AT_SET_BY_COLUMN
        entry = tagged_cols[col_index]
        entry.meta_target = meta_target_t.meta_target_at
        return col_index
    else:
        raise TypeError(
            f'Bad argument `at`: Bad dtype `{dtype}` ' +
            f'for the {at!r} column: Must be a datetime64[ns] column.')


cdef object _pandas_is_supported_datetime(object dtype):
    # We currently only accept datetime64[ns] columns.
    return (
        (dtype.kind == 'M') and
        (dtype.itemsize == 8) and
        (dtype.byteorder == '=') and
        (dtype.alignment == 8) and
        (not dtype.hasobject))


# cdef int64_t _pandas_get_timestamp_cell(
#         dtype_t* dtype,
#         Py_buffer* col_buffer,
#         size_t row_index) except -1:
#     # Note: Type is pre-validated by `_pandas_is_supported_datetime`.
#     cdef const void* cell = _pandas_get_cell(col_buffer, row_index)
#     cdef int64_t res = (<int64_t*>cell)[0]
#     if res < 0:
#         # TODO [amunra]: Improve error messaging. Add column name.
#         raise ValueError(  
#             f'Bad value: Negative timestamp, got {res}')
#     return res


# cdef bint _pandas_row_table_name(
#         line_sender_buffer* impl,
#         qdb_pystr_buf* b,
#         dtype_t* dtypes,
#         Py_buffer* col_buffers,
#         ssize_t name_col,
#         size_t row_index,
#         line_sender_table_name c_table_name) except False:
#     cdef line_sender_error* err = NULL
#     cdef bint is_null = False
#     cdef line_sender_utf8 utf8
#     if name_col >= 0:
#         _pandas_get_str_cell(
#             b,
#             &dtypes[<size_t>name_col],
#             &col_buffers[<size_t>name_col],
#             row_index,
#             &is_null,
#             &utf8)
#         if is_null:
#             # TODO [amunra]: Improve error messaging. Add column name.
#             raise ValueError(
#                 f'Bad value: `None` table name value at row {row_index}.')
#         if not line_sender_table_name_init(
#                 &c_table_name, utf8.len, utf8.buf, &err):
#             raise c_err_to_py(err)
#     if not line_sender_buffer_table(impl, c_table_name, &err):
#         raise c_err_to_py(err)
#     return True

    
# cdef bint _pandas_row_symbols(
#         line_sender_buffer* impl,
#         qdb_pystr_buf* b,
#         dtype_t* dtypes,
#         Py_buffer* col_buffers,
#         size_t row_index,
#         const column_name_vec* symbol_names,
#         const size_t_vec* symbol_indices) except False:
#     cdef line_sender_error* err = NULL
#     cdef size_t sym_index
#     cdef size_t col_index
#     cdef dtype_t* dtype
#     cdef Py_buffer* col_buffer
#     cdef line_sender_column_name col_name
#     cdef line_sender_utf8 symbol
#     cdef bint is_null = False
#     for sym_index in range(symbol_indices.size):
#         col_name = symbol_names.d[sym_index]
#         col_index = symbol_indices.d[sym_index]
#         col_buffer = &col_buffers[col_index]
#         dtype = &dtypes[col_index]
#         _pandas_get_str_cell(
#             b,
#             dtype,
#             col_buffer,
#             row_index,
#             &is_null,
#             &symbol)
#         if not is_null:
#             if not line_sender_buffer_symbol(impl, col_name, symbol, &err):
#                 raise c_err_to_py(err)
#     return True


# cdef bint _pandas_row_at(
#         line_sender_buffer* impl,
#         dtype_t* dtypes,
#         Py_buffer* col_buffers,
#         size_t row_index,
#         ssize_t at_col,
#         int64_t at_value) except False:
#     cdef line_sender_error* err = NULL
#     cdef Py_buffer* col_buffer
#     cdef dtype_t* dtype
#     if at_col >= 0:
#         col_buffer = &col_buffers[<size_t>at_col]
#         dtype = &dtypes[<size_t>at_col]
#         at_value = _pandas_get_timestamp_cell(dtype, col_buffer, row_index)
#     if at_value > 0:
#         if not line_sender_buffer_at(impl, at_value, &err):
#             raise c_err_to_py(err)
#     else:
#         if not line_sender_buffer_at_now(impl, &err):
#             raise c_err_to_py(err)
#     return True


cdef bint _pandas_alloc_chunks(size_t n_chunks, col_t* col_out) except False:
    col_out.chunks.n_chunks = n_chunks
    col_out.chunks.chunks = <ArrowArray*>calloc(
        col_out.chunks.n_chunks + 1,  # See `_pandas_col_advance` on why +1.
        sizeof(ArrowArray))
    if col_out.chunks.chunks == NULL:
        raise MemoryError()
    return True


cdef void _pandas_free_mapped_arrow(ArrowArray* arr):
    free(arr.buffers)
    arr.buffers = NULL
    arr.release = NULL


cdef bint _pandas_series_as_pybuf(
        TaggedEntry entry, col_t* col_out) except False:
    import sys; sys.stderr.write('_pandas_series_as_pybuf :: (A)\n')
    cdef object nparr = entry.series.to_numpy()
    cdef ArrowArray* mapped
    sys.stderr.write('_pandas_series_as_pybuf :: (C)\n')
    if not PyObject_CheckBuffer(nparr):
        sys.stderr.write('_pandas_series_as_pybuf :: (D)\n')
        raise TypeError(
            f'Bad column {entry.name!r}: Expected a buffer, got ' +
            f'{entry.series!r} ({_fqn(type(entry.series))})')
    sys.stderr.write('_pandas_series_as_pybuf :: (E)\n')
    col_out.tag = col_access_tag_t.numpy
    sys.stderr.write('_pandas_series_as_pybuf :: (F)\n')
    try:
        sys.stderr.write('_pandas_series_as_pybuf :: (G)\n')
        # Note! We don't need to support numpy strides since Pandas doesn't.
        # Also note that this guarantees a 1D buffer.
        PyObject_GetBuffer(nparr, &col_out.pybuf, PyBUF_SIMPLE)
        sys.stderr.write('_pandas_series_as_pybuf :: (H)\n')
    except BufferError as be:
        sys.stderr.write('_pandas_series_as_pybuf :: (I)\n')
        raise TypeError(
            f'Bad column {entry.name!r}: Expected a buffer, got ' +
            f'{entry.series!r} ({_fqn(type(entry.series))})') from be
    sys.stderr.write('_pandas_series_as_pybuf :: (J)\n')
    _pandas_alloc_chunks(1, col_out)
    mapped = &col_out.chunks.chunks[0]

    # Total number of elements.
    mapped.length = (
        <int64_t>col_out.pybuf.len // <int64_t>col_out.pybuf.itemsize)
    mapped.null_count = 0
    mapped.offset = 0
    mapped.n_buffers = 2
    mapped.n_children = 0
    mapped.buffers = <const void**>calloc(2, sizeof(const void*))
    mapped.buffers[0] = NULL
    mapped.buffers[1] = <const void*>col_out.pybuf.buf
    mapped.children = NULL
    mapped.dictionary = NULL
    mapped.release = _pandas_free_mapped_arrow  # to cleanup allocated array.
    return True


cdef bint _pandas_series_as_arrow(
        TaggedEntry entry,
        col_t* col_out,
        col_source_t np_fallback) except False:
    cdef object array
    cdef list chunks
    cdef size_t n_chunks
    cdef size_t chunk_index
    if _PYARROW is None:
        col_out.source = np_fallback
        _pandas_series_as_pybuf(entry, col_out)
        return True

    col_out.tag = col_access_tag_t.arrow
    array = _PYARROW.Array.from_pandas(entry.series)
    if isinstance(array, _PYARROW.ChunkedArray):
        chunks = array.chunks
    else:
        chunks = [array]

    n_chunks = len(chunks)
    _pandas_alloc_chunks(n_chunks, col_out)

    for chunk_index in range(n_chunks):
        array = chunks[chunk_index]
        if chunk_index == 0:
            chunks[chunk_index]._export_to_c(
                <uintptr_t>&col_out.chunks.chunks[chunk_index],
                <uintptr_t>&col_out.arrow_schema)
        else:
            chunks[chunk_index]._export_to_c(
                <uintptr_t>&col_out.chunks.chunks[chunk_index])
    return True
    

cdef const char* _ARROW_FMT_INT8 = "c"
cdef const char* _ARROW_FMT_INT16 = "s"
cdef const char* _ARROW_FMT_INT32 = "i"
cdef const char* _ARROW_FMT_INT64 = "l"


cdef bint _pandas_category_series_as_arrow(
        TaggedEntry entry, col_t* col_out) except False:
    cdef const char* format
    col_out.source = col_source_t.col_source_nulls  # placeholder value.
    _pandas_series_as_arrow(entry, col_out, col_source_t.col_source_str_pyobj)
    if col_out.source == col_source_t.col_source_str_pyobj:
        return True  # PyArrow wasn't imported.
    format = col_out.arrow_schema.format
    if strncmp(format, _ARROW_FMT_INT8, 1) == 0:
        col_out.source = col_source_t.col_source_str_i8_cat
    elif strncmp(format, _ARROW_FMT_INT16, 1) == 0:
        col_out.source = col_source_t.col_source_str_i16_cat
    elif strncmp(format, _ARROW_FMT_INT32, 1) == 0:
        col_out.source = col_source_t.col_source_str_i32_cat
    elif strncmp(format, _ARROW_FMT_INT64, 1) == 0:
        col_out.source = col_source_t.col_source_str_i64_cat
    else:
        raise TypeError(
            f'Bad column {entry.name!r}: Expected an arrow category index ' +
            f'format, got {(<bytes>format).decode("utf-8")!r}')
    return True


cdef inline bint _pandas_is_float_nan(obj):
    return PyFloat_CheckExact(obj) and isnan(PyFloat_AS_DOUBLE(obj))


cdef inline bint _pandas_is_null_pyobj(object obj):
    return (obj is None) or (obj is _PANDAS_NA) or _pandas_is_float_nan(obj)


cdef bint _pandas_series_sniff_pyobj(
        TaggedEntry entry, col_t* col_out) except False:
    """
    Deduct the type of the object column.
    Object columns can contain pretty much anything, but they usually don't.
    We make an educated guess by finding the first non-null value in the column.
    """
    cdef object obj
    cdef size_t el_index
    cdef size_t n_elements = len(entry.series)
    cdef PyObject* obj_arr = <PyObject*>(col_out.pybuf.buf)
    import sys; sys.stderr.write('_pandas_series_sniff_pyobj :: (A)\n')
    _pandas_series_as_pybuf(entry, col_out)
    sys.stderr.write('_pandas_series_sniff_pyobj :: (B)\n')
    for el_index in range(n_elements):
        # TODO: Check there's no pointless INCREF/DECREF going on here.
        obj = <object>&(obj_arr[el_index])
        sys.stderr.write(f'_pandas_series_sniff_pyobj :: (C) obj: {obj!r}\n')
        if _pandas_is_null_pyobj(obj):
            continue
        elif PyBool_Check(obj):
            col_out.source = col_source_t.col_source_bool_pyobj
        elif PyLong_Check(obj):
            col_out.source = col_source_t.col_source_int_pyobj
        elif PyFloat_Check(obj):
            col_out.source = col_source_t.col_source_float_pyobj
        elif PyUnicode_Check(obj):
            col_out.source = col_source_t.col_source_str_pyobj
        elif isinstance(obj, bytes):
            raise ValueError(
                f'Bad column {entry.name!r}: ' +
                'Unsupported object column containing bytes.' +
                'If this is a string column, decode it first. ' +
                'See: https://stackoverflow.com/questions/40389764/')
        else:
            raise TypeError(
                f'Bad column {entry.name!r}: ' +
                f'Unsupported object column containing an {_fqn(type(obj))}')
        sys.stderr.write('_pandas_series_sniff_pyobj :: (D)\n')
        return True
    sys.stderr.write('_pandas_series_sniff_pyobj :: (E)\n')

    # We've hit an object column that exclusively has null values.
    # We will just skip this column.
    col_out.source = col_source_t.col_source_nulls
    sys.stderr.write('_pandas_series_sniff_pyobj :: (F)\n')
    return True
    

cdef bint _pandas_resolve_source_and_buffers(
        TaggedEntry entry, col_t* col_out) except False:
    cdef object dtype = entry.dtype
    if isinstance(dtype, _NUMPY_BOOL):
        col_out.source = col_source_t.col_source_bool_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.BooleanDtype):
        col_out.source = col_source_t.col_source_bool_arrow
        _pandas_series_as_arrow(
                entry, col_out, col_source_t.col_source_bool_pyobj)
    elif isinstance(dtype, _NUMPY_UINT8):
        col_out.source = col_source_t.col_source_u8_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_INT8):
        col_out.source = col_source_t.col_source_i8_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_UINT16):
        col_out.source = col_source_t.col_source_u16_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_INT16):
        col_out.source = col_source_t.col_source_i16_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_UINT32):
        col_out.source = col_source_t.col_source_u32_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_INT32):
        col_out.source = col_source_t.col_source_i32_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_UINT64):
        col_out.source = col_source_t.col_source_u64_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_INT64):
        col_out.source = col_source_t.col_source_i64_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.UInt8Dtype):
        col_out.source = col_source_t.col_source_u8_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int8Dtype):
        col_out.source = col_source_t.col_source_i8_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt16Dtype):
        col_out.source = col_source_t.col_source_u16_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int16Dtype):
        col_out.source = col_source_t.col_source_i16_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt32Dtype):
        col_out.source = col_source_t.col_source_u32_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int32Dtype):
        col_out.source = col_source_t.col_source_i32_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt64Dtype):
        col_out.source = col_source_t.col_source_u64_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int64Dtype):
        col_out.source = col_source_t.col_source_i64_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _NUMPY_FLOAT32):
        col_out.source = col_source_t.col_source_f32_numpy
        _pandas_series_as_pybuf(
            entry, col_out)
    elif isinstance(dtype, _NUMPY_FLOAT64):
        col_out.source = col_source_t.col_source_f64_numpy
        _pandas_series_as_pybuf(
            entry, col_out)
    elif isinstance(dtype, _PANDAS.Float32Dtype):
        col_out.source = col_source_t.col_source_f32_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_float_pyobj)
    elif isinstance(dtype, _PANDAS.Float64Dtype):
        col_out.source = col_source_t.col_source_f64_arrow
        _pandas_series_as_arrow(
            entry, col_out, col_source_t.col_source_float_pyobj)
    elif isinstance(dtype, _PANDAS.StringDtype):
        if dtype.storage == 'pyarrow':
            col_out.source = col_source_t.col_source_str_arrow
            _pandas_series_as_arrow(
                entry, col_out, col_source_t.col_source_str_pyobj)
        elif dtype.storage == 'python':
            col_out.source = col_source_t.col_source_str_pyobj
            _pandas_series_as_pybuf(entry, col_out)
        else:
            raise ValueError(
                f'Unknown string dtype storage: f{dtype.storage} ' +
                f'for column {entry.name} of dtype {dtype}.')
    elif isinstance(dtype, _PANDAS.CategoricalDtype):
        _pandas_category_series_as_arrow(entry, col_out)
    elif isinstance(dtype, _NUMPY_DATETIME64_NS):
        col_out.source = col_source_t.col_source_dt64ns_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.DatetimeTZDtype):
        col_out.source = col_source_t.col_source_dt64ns_tz_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY_OBJECT):
        import sys; sys.stderr.write(f'_pandas_resolve_source_and_buffers :: (A)\n')
        _pandas_series_sniff_pyobj(entry, col_out)
        import sys; sys.stderr.write(f'_pandas_resolve_source_and_buffers :: (B)\n')
    else:
        raise ValueError(
            f'Unsupported dtype {dtype} for column {entry.name}. ' +
            'Raise an issue if you think it should be supported: ' +
            'https://github.com/questdb/py-questdb-client/issues.')
    return True


cdef bint _pandas_resolve_target(
        TaggedEntry entry, col_t* col_out) except False:
    cdef col_target_t target
    cdef set target_sources
    if entry.meta_target in _DIRECT_META_TARGETS:
        col_out.target = <col_target_t><int>entry.meta_target
        return True
    for target in _FIELD_TARGETS:
        target_sources = _FIELD_TARGETS[target]
        if col_out.source in target_sources:
            col_out.target = target
            return True
    raise ValueError(
        f'Could not map column source type (code {col_out.source} for ' +
        f'{entry.dtype!r}) {entry.name}.')


cdef void _pandas_init_cursor(col_t* col_out):
    col_out.cursor.chunk = col_out.chunks.chunks
    col_out.cursor.chunk_index = 0
    col_out.cursor.offset = col_out.cursor.chunk.offset


cdef bint _pandas_resolve_col(
        qdb_pystr_buf* b,
        size_t index,
        TaggedEntry entry,
        col_t* col_out) except False:
    col_out.orig_index = entry.orig_index

    # Since we don't need to send the column names for 'table' and 'at' columns,
    # we don't need to validate and encode them as column names.
    import sys; sys.stderr.write(f'_pandas_resolve_col :: (A) name: {entry.name}, dtype: {entry.dtype}\n')
    if ((entry.meta_target != meta_target_t.meta_target_table) and
            (entry.meta_target != meta_target_t.meta_target_at)):
        sys.stderr.write('_pandas_resolve_col :: (B)\n')
        str_to_column_name(b, entry.name, &col_out.name)
        sys.stderr.write('_pandas_resolve_col :: (C)\n')

    sys.stderr.write('_pandas_resolve_col :: (D)\n')
    _pandas_resolve_source_and_buffers(entry, col_out)
    sys.stderr.write('_pandas_resolve_col :: (E)\n')
    _pandas_resolve_target(entry, col_out)
    sys.stderr.write('_pandas_resolve_col :: (F)\n')
    if col_out.source not in _TARGET_TO_SOURCE[col_out.target]:
        sys.stderr.write('_pandas_resolve_col :: (G)\n')
        raise ValueError(
            f'Bad value: Column {entry.name!r} ({entry.dtype}) is not ' +
            f'supported as a {_TARGET_NAMES[col_out.target]} column.')
    sys.stderr.write('_pandas_resolve_col :: (H)\n')
    col_out.dispatch_code = <col_dispatch_code_t>(
        <int>col_out.source + <int>col_out.target)
    sys.stderr.write('_pandas_resolve_col :: (I)\n')
    _pandas_init_cursor(col_out)
    sys.stderr.write('_pandas_resolve_col :: (J)\n')
    return True



cdef bint _pandas_resolve_cols(
        qdb_pystr_buf*b, list tagged_cols, col_t_arr* cols_out) except False:
    cdef size_t index
    cdef size_t len_tagged_cols = len(tagged_cols)
    for index in range(len_tagged_cols):
        _pandas_resolve_col(b, index, tagged_cols[index], &cols_out.d[index])
    return True


cdef bint _pandas_resolve_args(
        object data,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        qdb_pystr_buf* b,
        size_t col_count,
        line_sender_table_name* c_table_name_out,
        int64_t* at_value_out,
        col_t_arr* cols_out) except False:
    cdef ssize_t name_col
    cdef ssize_t at_col

    import sys; sys.stderr.write('_pandas_resolve_args :: (A)\n')
    # List of Lists with col index, col name, series object, sorting key.
    cdef list tagged_cols = [
        TaggedEntry(
            index,
            name,
            data.dtypes[index],
            series,
            meta_target_t.meta_target_field)  # Later resolved to a target.
        for index, (name, series) in enumerate(data.items())]
    sys.stderr.write('_pandas_resolve_args :: (A)\n')
    name_col = _pandas_resolve_table_name(
        b,
        data,
        tagged_cols,
        table_name,
        table_name_col,
        col_count,
        c_table_name_out)
    sys.stderr.write('_pandas_resolve_args :: (B)\n')
    at_col = _pandas_resolve_at(data, tagged_cols, at, col_count, at_value_out)
    sys.stderr.write('_pandas_resolve_args :: (C)\n')
    _pandas_resolve_symbols(
        data, tagged_cols, name_col, at_col, symbols, col_count)
    sys.stderr.write('_pandas_resolve_args :: (D)\n')

    # Sort with table name is first, then the symbols, then fields, then at.
    # Note: Python 3.6+ guarantees stable sort.
    tagged_cols.sort(key=lambda x: (<TaggedEntry>x).meta_target)
    sys.stderr.write('_pandas_resolve_args :: (E)\n')
    _pandas_resolve_cols(b, tagged_cols, cols_out)
    sys.stderr.write('_pandas_resolve_args :: (F)\n')
    return True


cdef bint _pandas_serialize_cell_table__str_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_table__str_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_table__str_i8_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_table__str_i16_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_table__str_i32_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_table__str_i64_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_symbol__str_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_symbol__str_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_symbol__str_i8_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_symbol__str_i16_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_symbol__str_i32_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_symbol__str_i64_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_bool__bool_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_bool__bool_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_bool__bool_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__int_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u8_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i8_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u16_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i16_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u32_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i32_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u64_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i64_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u8_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i8_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u16_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i16_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u32_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i32_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__u64_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_i64__i64_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_f64__float_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_f64__f32_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_f64__f64_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_f64__f32_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_f64__f64_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_str__str_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_str__str_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_str__str_i8_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_str__str_i16_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_str__str_i32_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_str__str_i64_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_ts__dt64ns_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell_column_ts__dt64ns_tz_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    raise ValueError('nyi')


cdef bint _pandas_serialize_cell(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    cdef col_dispatch_code_t dc = col.dispatch_code
    # TODO: Check that this `if/elif` gets compiled into a C switch statement.
    if dc == col_dispatch_code_t.col_dispatch_code_skip_nulls:
        pass  # We skip a null column. Nothing to do.
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_pyobj:
        _pandas_serialize_cell_table__str_pyobj(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_arrow:
        _pandas_serialize_cell_table__str_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i8_cat:
        _pandas_serialize_cell_table__str_i8_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i16_cat:
        _pandas_serialize_cell_table__str_i16_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i32_cat:
        _pandas_serialize_cell_table__str_i32_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i64_cat:
        _pandas_serialize_cell_table__str_i64_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_pyobj:
        _pandas_serialize_cell_symbol__str_pyobj(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_arrow:
        _pandas_serialize_cell_symbol__str_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i8_cat:
        _pandas_serialize_cell_symbol__str_i8_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i16_cat:
        _pandas_serialize_cell_symbol__str_i16_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i32_cat:
        _pandas_serialize_cell_symbol__str_i32_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i64_cat:
        _pandas_serialize_cell_symbol__str_i64_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_pyobj:
        _pandas_serialize_cell_column_bool__bool_pyobj(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_numpy:
        _pandas_serialize_cell_column_bool__bool_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_arrow:
        _pandas_serialize_cell_column_bool__bool_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__int_pyobj:
        _pandas_serialize_cell_column_i64__int_pyobj(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u8_numpy:
        _pandas_serialize_cell_column_i64__u8_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i8_numpy:
        _pandas_serialize_cell_column_i64__i8_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u16_numpy:
        _pandas_serialize_cell_column_i64__u16_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i16_numpy:
        _pandas_serialize_cell_column_i64__i16_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u32_numpy:
        _pandas_serialize_cell_column_i64__u32_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i32_numpy:
        _pandas_serialize_cell_column_i64__i32_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u64_numpy:
        _pandas_serialize_cell_column_i64__u64_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i64_numpy:
        _pandas_serialize_cell_column_i64__i64_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u8_arrow:
        _pandas_serialize_cell_column_i64__u8_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i8_arrow:
        _pandas_serialize_cell_column_i64__i8_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u16_arrow:
        _pandas_serialize_cell_column_i64__u16_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i16_arrow:
        _pandas_serialize_cell_column_i64__i16_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u32_arrow:
        _pandas_serialize_cell_column_i64__u32_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i32_arrow:
        _pandas_serialize_cell_column_i64__i32_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u64_arrow:
        _pandas_serialize_cell_column_i64__u64_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i64_arrow:
        _pandas_serialize_cell_column_i64__i64_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__float_pyobj:
        _pandas_serialize_cell_column_f64__float_pyobj(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f32_numpy:
        _pandas_serialize_cell_column_f64__f32_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f64_numpy:
        _pandas_serialize_cell_column_f64__f64_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f32_arrow:
        _pandas_serialize_cell_column_f64__f32_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f64_arrow:
        _pandas_serialize_cell_column_f64__f64_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_pyobj:
        _pandas_serialize_cell_column_str__str_pyobj(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_arrow:
        _pandas_serialize_cell_column_str__str_arrow(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i8_cat:
        _pandas_serialize_cell_column_str__str_i8_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i16_cat:
        _pandas_serialize_cell_column_str__str_i16_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i32_cat:
        _pandas_serialize_cell_column_str__str_i32_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i64_cat:
        _pandas_serialize_cell_column_str__str_i64_cat(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_ts__dt64ns_numpy:
        _pandas_serialize_cell_column_ts__dt64ns_numpy(impl, b, col, row_index)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_ts__dt64ns_tz_numpy:
        _pandas_serialize_cell_column_ts__dt64ns_tz_numpy(
            impl, b, col, row_index)
    else:
        raise RuntimeError(f"Unknown column dispatch code: {dc}")
    return True


cdef void _pandas_col_advance(col_t* col):
    # Branchless version of:
    #     cdef bint new_chunk = cursor.offset == <size_t>cursor.chunk.length
    #     if new_chunk == 0:
    #         cursor.chunk_index += 1
    #         cursor.chunk += 1  # pointer advance
    #
    #     if new_chunk:
    #         cursor.offset = cursor.chunk.offset
    #     else:
    #         cursor.offset += 1
    #
    # (Checked with Godbolt, GCC -O3 code was rather "jumpy")
    cdef col_cursor_t* cursor = &col.cursor
    cdef size_t new_chunk  # disguised bint. Either 0 or 1.
    cursor.offset += 1
    new_chunk = cursor.offset == <size_t>cursor.chunk.length
    cursor.chunk_index += new_chunk
    cursor.chunk += new_chunk
    # Note: We get away with this because we've allocated one extra blank chunk.
    # This ensures that accessing `cursor.chunk.offset` doesn't segfault.
    cursor.offset = (
        (new_chunk * cursor.chunk.offset) +
        ((not new_chunk) * cursor.offset))


cdef bint _pandas(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        object data,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        bint sort) except False:
    cdef size_t col_count
    cdef line_sender_table_name c_table_name
    cdef int64_t at_value = _AT_SET_BY_COLUMN
    cdef col_t_arr cols = col_t_arr_blank()
    cdef qdb_pystr_pos str_buf_marker
    cdef size_t row_count
    cdef line_sender_error* err = NULL
    cdef size_t col_index
    cdef col_t* col

    _pandas_may_import_deps()
    try:
        import sys; sys.stderr.write(' :: :: (A)\n')
        qdb_pystr_buf_clear(b)
        sys.stderr.write(' :: :: (B)\n')
        _check_is_pandas_dataframe(data)
        sys.stderr.write(' :: :: (C)\n')
        col_count = len(data.columns)
        sys.stderr.write(' :: :: (D)\n')
        cols = col_t_arr_new(col_count)
        sys.stderr.write(' :: :: (E)\n')
        _pandas_resolve_args(
            data,
            table_name,
            table_name_col,
            symbols,
            at,
            b,
            col_count,
            &c_table_name,
            &at_value,
            &cols)
        sys.stderr.write(' :: :: (F)\n')

        # We've used the str buffer up to a point for the headers.
        # Instead of clearing it (which would clear the headers' memory)
        # we will truncate (rewind) back to this position.
        str_buf_marker = qdb_pystr_buf_tell(b)
        sys.stderr.write(' :: :: (G)\n')

        # import sys
        # sys.stderr.write('_pandas :: (A) ' +
        #     f'name_col: {name_col}, ' +
        #     f'symbol_indices: {size_t_vec_str(&symbol_indices)}, ' +
        #     f'at_col: {at_col}, ' +
        #     f'at_value: {at_value}, ' +
        #     f'field_indices: {size_t_vec_str(&field_indices)}' +
        #     '\n')
        row_count = len(data)
        line_sender_buffer_clear_marker(impl)
        for row_index in range(row_count):
            qdb_pystr_buf_truncate(b, str_buf_marker)
            try:
                if not line_sender_buffer_set_marker(impl, &err):
                    raise c_err_to_py(err)

                # Fixed table-name.
                if c_table_name.buf != NULL:
                    if not line_sender_buffer_table(impl, c_table_name, &err):
                        raise c_err_to_py(err)

                # Serialize columns cells.
                # Note: Columns are sorted: table name, symbols, fields, at.
                for col_index in range(col_count):
                    col = &cols.d[col_index]
                    _pandas_serialize_cell(impl, b, col, row_index)
                    _pandas_col_advance(col)

                # Fixed "at" value (not from a column).
                if at_value == 0:
                    if not line_sender_buffer_at_now(impl, &err):
                        raise c_err_to_py(err)
                elif at_value > 0:
                    if not line_sender_buffer_at(impl, at_value, &err):
                        raise c_err_to_py(err)
            except:
                if not line_sender_buffer_rewind_to_marker(impl, &err):
                    raise c_err_to_py(err)
                raise
    finally:
        line_sender_buffer_clear_marker(impl)
        col_t_arr_release(&cols)
        qdb_pystr_buf_clear(b)
    return True
