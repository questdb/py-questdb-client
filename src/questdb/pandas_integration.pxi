# See: pandas_integration.md for technical overview.

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
    # Note: Hundreds digit set to 1 if GIL is required.
    col_source_nulls =                0
    col_source_bool_pyobj =      101100
    col_source_bool_numpy =      102000
    col_source_bool_arrow =      103000
    col_source_int_pyobj =       201100
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
    col_source_float_pyobj =     301100
    col_source_f32_numpy =       302000
    col_source_f64_numpy =       303000
    col_source_f32_arrow =       304000
    col_source_f64_arrow =       305000
    col_source_str_pyobj =       401100
    col_source_str_arrow =       402000
    col_source_str_i8_cat =      403000
    col_source_str_i16_cat =     404000
    col_source_str_i32_cat =     405000
    col_source_dt64ns_numpy =    501000
    col_source_dt64ns_tz_arrow = 502000


cdef bint col_source_needs_gil(col_source_t source):
    # Check if hundreds digit is 1.
    return <int>source // 100 % 10 == 1


cdef set _STR_SOURCES = {
    col_source_t.col_source_str_pyobj,
    col_source_t.col_source_str_arrow,
    col_source_t.col_source_str_i8_cat,
    col_source_t.col_source_str_i16_cat,
    col_source_t.col_source_str_i32_cat,
}


cdef dict _PYOBJ_SOURCE_DESCR = {
    col_source_t.col_source_bool_pyobj: "bool",
    col_source_t.col_source_int_pyobj: "int",
    col_source_t.col_source_float_pyobj: "float",
    col_source_t.col_source_str_pyobj: "str",
}


cdef dict _TARGET_TO_SOURCES = {
    col_target_t.col_target_skip: {
        col_source_t.col_source_nulls,
    },
    col_target_t.col_target_table: {
        col_source_t.col_source_str_pyobj,
        col_source_t.col_source_str_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
    },
    col_target_t.col_target_symbol: {
        col_source_t.col_source_str_pyobj,
        col_source_t.col_source_str_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
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
    },
    col_target_t.col_target_column_ts: {
        col_source_t.col_source_dt64ns_numpy,
        col_source_t.col_source_dt64ns_tz_arrow,
    },
    col_target_t.col_target_at: {
        col_source_t.col_source_dt64ns_numpy,
        col_source_t.col_source_dt64ns_tz_arrow,
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

    col_dispatch_code_column_ts__dt64ns_numpy = \
        col_target_t.col_target_column_ts + col_source_t.col_source_dt64ns_numpy
    col_dispatch_code_column_ts__dt64ns_tz_arrow = \
        col_target_t.col_target_column_ts + \
        col_source_t.col_source_dt64ns_tz_arrow

    col_dispatch_code_at__dt64ns_numpy = \
        col_target_t.col_target_at + col_source_t.col_source_dt64ns_numpy
    col_dispatch_code_at__dt64ns_tz_arrow = \
        col_target_t.col_target_at + col_source_t.col_source_dt64ns_tz_arrow


# Int values in order for sorting (as needed for API's sequential coupling).
cdef enum meta_target_t:
    meta_target_table = <int>col_target_t.col_target_table
    meta_target_symbol = <int>col_target_t.col_target_symbol
    meta_target_field = <int>col_target_t.col_target_column_bool
    meta_target_at = <int>col_target_t.col_target_at


cdef struct col_t:
    meta_target_t meta_target
    size_t orig_index
    line_sender_column_name name
    Py_buffer pybuf
    ArrowSchema arrow_schema  # Schema of first chunk.
    col_chunks_t chunks
    col_cursor_t cursor
    col_source_t source
    col_target_t target
    col_dispatch_code_t dispatch_code  # source + target. Determines serializer.


cdef void col_t_release(col_t* col):
    """
    Release a (possibly) initialized column.

    col_t objects are `calloc`ed, so uninitialized (or partially) initialized
    objects will have their pointers and other values set to 0.
    """
    cdef size_t chunk_index
    cdef ArrowArray* chunk

    if Py_buffer_obj_is_set(&col.pybuf):
        PyBuffer_Release(&col.pybuf)  # Note: Sets `col.pybuf.obj` to NULL.

    for chunk_index in range(col.chunks.n_chunks):
        chunk = &col.chunks.chunks[chunk_index]
        if chunk.release != NULL:
            chunk.release(chunk)
        memset(chunk, 0, sizeof(ArrowArray))

    if col.arrow_schema.release != NULL:
        col.arrow_schema.release(&col.arrow_schema)


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


cdef object _check_is_pandas_dataframe(object data):
    if not isinstance(data, _PANDAS.DataFrame):
        raise TypeError(
            f'Bad argument `data`: Expected {_fqn(_PANDAS.DataFrame)}, ' +
            f'not an object of type {_fqn(type(data))}.')


cdef ssize_t _pandas_resolve_table_name(
        qdb_pystr_buf* b,
        object data,
        list pandas_cols,
        col_t_arr* cols,
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
    cdef PandasCol pandas_col
    cdef col_t* col
    if table_name is not None:
        if table_name_col is not None:
            raise ValueError(
                'Can specify only one of `table_name` or `table_name_col`.')
        if isinstance(table_name, str):
            try:
                str_to_table_name_copy(b, <PyObject*>table_name, name_out)
                return -1  # Magic value for "no column index".
            except IngressError as ie:
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    f'Bad argument `table_name`: {ie}')
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
        pandas_col = pandas_cols[col_index]
        col = &cols.d[col_index]
        _pandas_check_column_is_str(
            'Bad argument `table_name_col`: ',
            pandas_col,
            col.source)
        col.meta_target = meta_target_t.meta_target_table
        name_out.len = 0
        name_out.buf = NULL
        return col_index
    else:
        raise ValueError(
            'Must specify at least one of `table_name` or `table_name_col`.')


cdef void_int _bind_col_index(
        str arg_name, int col_num, size_t col_count,
        size_t* col_index) except -1:
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


cdef void_int _pandas_check_column_is_str(
        str err_msg_prefix,
        PandasCol pandas_col,
        col_source_t source) except -1:
    cdef str inferred_descr = ""
    if not source in _STR_SOURCES:
        if isinstance(pandas_col.dtype, _NUMPY_OBJECT):
            inferred_descr = f' (inferred type: {_PYOBJ_SOURCE_DESCR[source]})'
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            err_msg_prefix + 
            f'Bad dtype `{pandas_col.dtype}`{inferred_descr} for the ' +
            f'{pandas_col.name!r} column: Must be a strings column.')


@cython.internal
cdef class PandasCol:
    """Python object representing a column whilst parsing .pandas arguments."""
    cdef str name
    cdef object dtype
    cdef object series

    def __init__(
            self,
            str name,
            object dtype,
            object series):
        self.name = name
        self.dtype = dtype
        self.series = series


cdef void_int _pandas_resolve_symbols(
        object data,
        list pandas_cols,
        col_t_arr* cols,
        ssize_t table_name_col,
        ssize_t at_col,
        object symbols) except -1:
    cdef size_t col_index = 0
    cdef object symbol
    cdef PandasCol pandas_col
    cdef col_t* col
    if symbols == 'auto':
        for col_index in range(cols.size):
            pandas_col = pandas_cols[col_index]
            col = &cols.d[col_index]
            if col.meta_target == meta_target_t.meta_target_field:
                if isinstance(pandas_col.dtype, _PANDAS.CategoricalDtype):
                    col.meta_target = meta_target_t.meta_target_symbol
    elif symbols is False:
        pass
    elif symbols is True:
        for col_index in range(cols.size):
            col = &cols.d[col_index]
            if col.source in _STR_SOURCES:
                pandas_col = pandas_cols[col_index]
                if col.meta_target == meta_target_t.meta_target_field:
                    col.meta_target = meta_target_t.meta_target_symbol
    else:
        if not isinstance(symbols, (tuple, list)):
            raise TypeError(
                f'Bad argument `symbols`: Must be a bool or a tuple or list '+
                'of column names (str) or indices (int).')
        for symbol in symbols:
            if isinstance(symbol, str):
                _pandas_get_loc(data, symbol, 'symbols', &col_index)
            elif isinstance(symbol, int):
                _bind_col_index('symbol', symbol, cols.size, &col_index) 
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
            pandas_col = pandas_cols[col_index]
            col = &cols.d[col_index]
            _pandas_check_column_is_str(
                'Bad argument `symbols`: ',
                pandas_col,
                col.source)
            col.meta_target = meta_target_t.meta_target_symbol


cdef void_int _pandas_get_loc(
        object data, str col_name, str arg_name,
        size_t* col_index_out) except -1:
    """
    Return the column index for `col_name`.
    """
    try:
        col_index_out[0] = data.columns.get_loc(col_name)
    except KeyError:
        raise KeyError(
            f'Bad argument `{arg_name}`: ' +
            f'Column {col_name!r} not found in the dataframe.')


# The -1 value is safe to use as a sentinel because the TimestampNanos type
# already validates that the value is >= 0.
cdef int64_t _AT_IS_SET_BY_COLUMN = -1


cdef str _SUPPORTED_DATETIMES = 'datetime64[ns] or datetime64[ns, tz]'


cdef object _pandas_is_supported_datetime(object dtype):
    if (isinstance(dtype, _NUMPY_DATETIME64_NS) and
            (str(dtype) == 'datetime64[ns]')):
        return True
    if isinstance(dtype, _PANDAS.DatetimeTZDtype):
        return dtype.unit == 'ns'
    return False


cdef ssize_t _pandas_resolve_at(
        object data,
        col_t_arr* cols,
        object at,
        size_t col_count,
        int64_t* at_value_out) except -2:
    cdef size_t col_index
    cdef object dtype
    cdef PandasCol pandas_col
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
        at_value_out[0] = _AT_IS_SET_BY_COLUMN
        col = &cols.d[col_index]
        col.meta_target = meta_target_t.meta_target_at
        return col_index
    else:
        raise TypeError(
            f'Bad argument `at`: Bad dtype `{dtype}` ' +
            f'for the {at!r} column: Must be a {_SUPPORTED_DATETIMES} column.')


cdef void_int _pandas_alloc_chunks(
        size_t n_chunks, col_t* col) except -1:
    col.chunks.n_chunks = n_chunks
    col.chunks.chunks = <ArrowArray*>calloc(
        col.chunks.n_chunks + 1,  # See `_pandas_col_advance` on why +1.
        sizeof(ArrowArray))
    if col.chunks.chunks == NULL:
        raise MemoryError()


cdef void _pandas_free_mapped_arrow(ArrowArray* arr):
    free(arr.buffers)
    arr.buffers = NULL
    arr.release = NULL


cdef void_int _pandas_series_as_pybuf(
        PandasCol pandas_col, col_t* col, str fallback_dtype=None) except -1:
    cdef object nparr = pandas_col.series.to_numpy(dtype=fallback_dtype)
    cdef ArrowArray* mapped
    cdef int get_buf_ret
    if not PyObject_CheckBuffer(nparr):
        raise TypeError(
            f'Bad column {pandas_col.name!r}: Expected a buffer, got ' +
            f'{pandas_col.series!r} ({_fqn(type(pandas_col.series))})')
    try:
        # Note! We don't need to support numpy strides since Pandas doesn't.
        # Also note that this guarantees a 1D buffer.
        get_buf_ret = PyObject_GetBuffer(nparr, &col.pybuf, PyBUF_SIMPLE)

    except BufferError as be:
        raise TypeError(
            f'Bad column {pandas_col.name!r}: Expected a buffer, got ' +
            f'{pandas_col.series!r} ({_fqn(type(pandas_col.series))})') from be
    _pandas_alloc_chunks(1, col)
    mapped = &col.chunks.chunks[0]

    # Total number of elements.
    mapped.length = (
        <int64_t>col.pybuf.len // <int64_t>col.pybuf.itemsize)
    mapped.null_count = 0
    mapped.offset = 0
    mapped.n_buffers = 2
    mapped.n_children = 0
    mapped.buffers = <const void**>calloc(2, sizeof(const void*))
    mapped.buffers[0] = NULL
    mapped.buffers[1] = <const void*>col.pybuf.buf
    mapped.children = NULL
    mapped.dictionary = NULL
    mapped.release = _pandas_free_mapped_arrow  # to cleanup allocated array.


cdef void_int _pandas_series_as_arrow(
        PandasCol pandas_col,
        col_t* col,
        col_source_t np_fallback,
        str fallback_dtype=None) except -1:
    cdef object array
    cdef list chunks
    cdef size_t n_chunks
    cdef size_t chunk_index
    if _PYARROW is None:
        col.source = np_fallback
        _pandas_series_as_pybuf(pandas_col, col, fallback_dtype)
        return 0

    array = _PYARROW.Array.from_pandas(pandas_col.series)
    if isinstance(array, _PYARROW.ChunkedArray):
        chunks = array.chunks
    else:
        chunks = [array]

    n_chunks = len(chunks)
    _pandas_alloc_chunks(n_chunks, col)

    for chunk_index in range(n_chunks):
        array = chunks[chunk_index]
        if chunk_index == 0:
            chunks[chunk_index]._export_to_c(
                <uintptr_t>&col.chunks.chunks[chunk_index],
                <uintptr_t>&col.arrow_schema)
        else:
            chunks[chunk_index]._export_to_c(
                <uintptr_t>&col.chunks.chunks[chunk_index])
    

cdef const char* _ARROW_FMT_INT8 = "c"
cdef const char* _ARROW_FMT_INT16 = "s"
cdef const char* _ARROW_FMT_INT32 = "i"
cdef const char* _ARROW_FMT_SML_STR = "u"


cdef void_int _pandas_category_series_as_arrow(
        PandasCol pandas_col, col_t* col) except -1:
    cdef const char* format
    col.source = col_source_t.col_source_nulls  # placeholder value.
    _pandas_series_as_arrow(pandas_col, col, col_source_t.col_source_str_pyobj)
    if col.source == col_source_t.col_source_str_pyobj:
        return 0  # PyArrow wasn't imported.
    format = col.arrow_schema.format
    if strncmp(format, _ARROW_FMT_INT8, 1) == 0:
        col.source = col_source_t.col_source_str_i8_cat
    elif strncmp(format, _ARROW_FMT_INT16, 1) == 0:
        col.source = col_source_t.col_source_str_i16_cat
    elif strncmp(format, _ARROW_FMT_INT32, 1) == 0:
        col.source = col_source_t.col_source_str_i32_cat
    else:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Bad column {pandas_col.name!r}: ' +
            'Expected an arrow category index ' +
            f'format, got {(<bytes>format).decode("utf-8")!r}.')
    
    format = col.arrow_schema.dictionary.format
    if strncmp(format, _ARROW_FMT_SML_STR, 1) != 0:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Bad column {pandas_col.name!r}: ' +
            'Expected a category of strings, ' +
            f'got a category of {pandas_col.series.dtype.categories.dtype}.')


cdef inline bint _pandas_is_float_nan(PyObject* obj):
    return PyFloat_CheckExact(obj) and isnan(PyFloat_AS_DOUBLE(obj))


cdef inline bint _pandas_is_null_pyobj(PyObject* obj):
    return (
        (obj == Py_None) or
        (obj == <PyObject*>_PANDAS_NA) or
        _pandas_is_float_nan(obj))


cdef void_int _pandas_series_sniff_pyobj(
        PandasCol pandas_col, col_t* col) except -1:
    """
    Deduct the type of the object column.
    Object columns can contain pretty much anything, but they usually don't.
    We make an educated guess by finding the first non-null value in the column.
    """
    cdef size_t el_index
    cdef size_t n_elements = len(pandas_col.series)
    cdef PyObject** obj_arr
    cdef PyObject* obj
    _pandas_series_as_pybuf(pandas_col, col)
    obj_arr = <PyObject**>(col.pybuf.buf)
    for el_index in range(n_elements):
        obj = obj_arr[el_index]
        if not _pandas_is_null_pyobj(obj):
            if PyBool_Check(obj):
                col.source = col_source_t.col_source_bool_pyobj
            elif PyLong_CheckExact(obj):
                col.source = col_source_t.col_source_int_pyobj
            elif PyFloat_CheckExact(obj):
                col.source = col_source_t.col_source_float_pyobj
            elif PyUnicode_CheckExact(obj):
                col.source = col_source_t.col_source_str_pyobj
            elif PyBytes_CheckExact(obj):
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    f'Bad column {pandas_col.name!r}: ' +
                    'Unsupported object column containing bytes.' +
                    'If this is a string column, decode it first. ' +
                    'See: https://stackoverflow.com/questions/40389764/')
            else:
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    f'Bad column {pandas_col.name!r}: ' +
                    f'Unsupported object column containing an object of type ' +
                    _fqn(type(<object>obj)) + '.')
            return 0

    # We haven't returned yet, so we've hit an object column that
    # exclusively has null values. We will just skip this column.
    col.source = col_source_t.col_source_nulls
    

cdef void_int _pandas_resolve_source_and_buffers(
        PandasCol pandas_col, col_t* col) except -1:
    cdef object dtype = pandas_col.dtype
    if isinstance(dtype, _NUMPY_BOOL):
        col.source = col_source_t.col_source_bool_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _PANDAS.BooleanDtype):
        col.source = col_source_t.col_source_bool_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_bool_pyobj)
    elif isinstance(dtype, _NUMPY_UINT8):
        col.source = col_source_t.col_source_u8_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT8):
        col.source = col_source_t.col_source_i8_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT16):
        col.source = col_source_t.col_source_u16_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT16):
        col.source = col_source_t.col_source_i16_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT32):
        col.source = col_source_t.col_source_u32_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT32):
        col.source = col_source_t.col_source_i32_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT64):
        col.source = col_source_t.col_source_u64_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT64):
        col.source = col_source_t.col_source_i64_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _PANDAS.UInt8Dtype):
        col.source = col_source_t.col_source_u8_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int8Dtype):
        col.source = col_source_t.col_source_i8_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt16Dtype):
        col.source = col_source_t.col_source_u16_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int16Dtype):
        col.source = col_source_t.col_source_i16_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt32Dtype):
        col.source = col_source_t.col_source_u32_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int32Dtype):
        col.source = col_source_t.col_source_i32_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt64Dtype):
        col.source = col_source_t.col_source_u64_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _PANDAS.Int64Dtype):
        col.source = col_source_t.col_source_i64_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_int_pyobj)
    elif isinstance(dtype, _NUMPY_FLOAT32):
        col.source = col_source_t.col_source_f32_numpy
        _pandas_series_as_pybuf(
            pandas_col, col)
    elif isinstance(dtype, _NUMPY_FLOAT64):
        col.source = col_source_t.col_source_f64_numpy
        _pandas_series_as_pybuf(
            pandas_col, col)
    elif isinstance(dtype, _PANDAS.Float32Dtype):
        col.source = col_source_t.col_source_f32_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_float_pyobj)
    elif isinstance(dtype, _PANDAS.Float64Dtype):
        col.source = col_source_t.col_source_f64_arrow
        _pandas_series_as_arrow(
            pandas_col, col, col_source_t.col_source_float_pyobj)
    elif isinstance(dtype, _PANDAS.StringDtype):
        if dtype.storage == 'pyarrow':
            col.source = col_source_t.col_source_str_arrow
            _pandas_series_as_arrow(
                pandas_col, col, col_source_t.col_source_str_pyobj)
        elif dtype.storage == 'python':
            col.source = col_source_t.col_source_str_pyobj
            _pandas_series_as_pybuf(pandas_col, col)
        else:
            raise IngressError(
                IngressErrorCode.BadDataFrame,
                f'Unknown string dtype storage: f{dtype.storage} ' +
                f'for column {pandas_col.name} of dtype {dtype}.')
    elif isinstance(dtype, _PANDAS.CategoricalDtype):
        _pandas_category_series_as_arrow(pandas_col, col)
    elif (isinstance(dtype, _NUMPY_DATETIME64_NS) and
            _pandas_is_supported_datetime(dtype)):
        col.source = col_source_t.col_source_dt64ns_numpy
        _pandas_series_as_pybuf(pandas_col, col)
    elif (isinstance(dtype, _PANDAS.DatetimeTZDtype) and
            _pandas_is_supported_datetime(dtype)):
        col.source = col_source_t.col_source_dt64ns_tz_arrow
        _pandas_series_as_arrow(
            pandas_col,
            col,
            col_source_t.col_source_dt64ns_numpy,
            'datetime64[ns]')
    elif isinstance(dtype, _NUMPY_OBJECT):
        _pandas_series_sniff_pyobj(pandas_col, col)
    else:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Unsupported dtype {dtype} for column {pandas_col.name}. ' +
            'Raise an issue if you think it should be supported: ' +
            'https://github.com/questdb/py-questdb-client/issues.')


cdef void_int _pandas_resolve_target(
        PandasCol pandas_col, col_t* col) except -1:
    cdef col_target_t target
    cdef set target_sources
    if col.meta_target in _DIRECT_META_TARGETS:
        col.target = <col_target_t><int>col.meta_target
        return 0
    for target in _FIELD_TARGETS:
        target_sources = _TARGET_TO_SOURCES[target]
        if col.source in target_sources:
            col.target = target
            return 0
    raise IngressError(
        IngressErrorCode.BadDataFrame,
        f'Could not map column source type (code {col.source} for ' +
        f'column {pandas_col.name!r} ' +
        f' ({pandas_col.dtype}) to any ILP type.')


cdef void _pandas_init_cursor(col_t* col):
    col.cursor.chunk = col.chunks.chunks
    col.cursor.chunk_index = 0
    col.cursor.offset = col.cursor.chunk.offset


cdef void_int _pandas_resolve_col(
        qdb_pystr_buf* b,
        size_t index,
        PandasCol pandas_col,
        col_t* col) except -1:
    # The target is resolved in stages:
    # * We first assign all columns to be fields.
    # * Then, depending on argument parsing some/none of the columns
    #   obtain a meta-target of "table", "symbol" or "at".
    # * Finally, based on the source, any remaining "meta_target_field"
    #   columns are converted to the appropriate target.
    #   See: _pandas_resolve_col_targets_and_dc(..).
    col.meta_target = meta_target_t.meta_target_field
    col.orig_index = index  # We will sort columns later.
    _pandas_resolve_source_and_buffers(pandas_col, col)
    _pandas_init_cursor(col)


cdef void_int _pandas_resolve_cols(
        qdb_pystr_buf* b,
        list pandas_cols,
        col_t_arr* cols,
        bint* any_cols_need_gil_out) except -1:
    cdef size_t index
    cdef size_t len_pandas_cols = len(pandas_cols)
    cdef col_t* col
    any_cols_need_gil_out[0] = False
    for index in range(len_pandas_cols):
        col = &cols.d[index]
        _pandas_resolve_col(b, index, pandas_cols[index], col)
        if col_source_needs_gil(col.source):
            any_cols_need_gil_out[0] = True


cdef void_int _pandas_resolve_cols_target_name_and_dc(
        qdb_pystr_buf* b,
        list pandas_cols,
        col_t_arr* cols) except -1:
    cdef size_t index
    cdef col_t* col
    cdef PandasCol pandas_col
    for index in range(cols.size):
        col = &cols.d[index]
        pandas_col = pandas_cols[index]
        _pandas_resolve_target(pandas_col, col)
        if col.source not in _TARGET_TO_SOURCES[col.target]:
            raise ValueError(
                f'Bad value: Column {pandas_col.name!r} ' +
                f'({pandas_col.dtype}) is not ' +
                f'supported as a {_TARGET_NAMES[col.target]} column.')
        col.dispatch_code = <col_dispatch_code_t>(
            <int>col.source + <int>col.target)

        # Since we don't need to send the column names for 'table' and
        # 'at' columns, we don't need to validate and encode them as
        # column names. This allows unsupported names for these columns.
        if ((col.meta_target != meta_target_t.meta_target_table) and
                (col.meta_target != meta_target_t.meta_target_at)):
            str_to_column_name_copy(b, pandas_col.name, &col.name)


cdef int _pandas_compare_cols(const void* lhs, const void* rhs) nogil:
    cdef col_t* lhs_col = <col_t*>lhs
    cdef col_t* rhs_col = <col_t*>rhs
    cdef int source_diff = lhs_col.meta_target - rhs_col.meta_target
    if source_diff != 0:
        return source_diff
    return <int>lhs_col.orig_index - <int>rhs_col.orig_index


cdef void_int _pandas_resolve_args(
        object data,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        qdb_pystr_buf* b,
        size_t col_count,
        line_sender_table_name* c_table_name_out,
        int64_t* at_value_out,
        col_t_arr* cols,
        bint* any_cols_need_gil_out) except -1:
    cdef ssize_t name_col
    cdef ssize_t at_col

    cdef list pandas_cols = [
        PandasCol(name, data.dtypes[index], series)
        for index, (name, series) in enumerate(data.items())]
    _pandas_resolve_cols(b, pandas_cols, cols, any_cols_need_gil_out)
    name_col = _pandas_resolve_table_name(
        b,
        data,
        pandas_cols,
        cols,
        table_name,
        table_name_col,
        col_count,
        c_table_name_out)
    at_col = _pandas_resolve_at(data, cols, at, col_count, at_value_out)
    _pandas_resolve_symbols(
        data, pandas_cols, cols, name_col, at_col, symbols)
    _pandas_resolve_cols_target_name_and_dc(b, pandas_cols, cols)
    qsort(cols.d, col_count, sizeof(col_t), _pandas_compare_cols)


cdef void _ensure_has_gil(PyThreadState** gs):
    if gs[0] != NULL:
        PyEval_RestoreThread(gs[0])
        gs[0] = NULL


cdef inline bint _pandas_arrow_get_bool(col_cursor_t* cursor):
    return (
        (<uint8_t*>cursor.chunk.buffers[1])[cursor.offset // 8] &
        (1 << (cursor.offset % 8)))


cdef inline bint _pandas_arrow_is_valid(col_cursor_t* cursor):
    return (
        cursor.chunk.null_count == 0 or
        (
            (<uint8_t*>cursor.chunk.buffers[0])[cursor.offset // 8] &
            (1 << (cursor.offset % 8))))


cdef inline void _pandas_arrow_get_cat_value(
        col_cursor_t* cursor, 
        size_t key,
        size_t* len_out,
        const char** buf_out):
    cdef int32_t* value_index_access
    cdef int32_t value_begin
    cdef uint8_t* value_char_access
    value_index_access = <int32_t*>cursor.chunk.dictionary.buffers[1]
    value_begin = value_index_access[key]
    len_out[0] = value_index_access[key + 1] - value_begin
    value_char_access = <uint8_t*>cursor.chunk.dictionary.buffers[2]
    buf_out[0] = <const char*>&value_char_access[value_begin]


cdef inline bint _pandas_arrow_get_cat_i8(
        col_cursor_t* cursor, size_t* len_out, const char** buf_out):
    cdef bint valid = _pandas_arrow_is_valid(cursor)
    cdef int8_t* key_access
    cdef int8_t key
    if valid:
        key_access = <int8_t*>cursor.chunk.buffers[1]
        key = key_access[cursor.offset]
        _pandas_arrow_get_cat_value(cursor, <size_t>key, len_out, buf_out)
    return valid


cdef inline bint _pandas_arrow_get_cat_i16(
        col_cursor_t* cursor, size_t* len_out, const char** buf_out):
    cdef bint valid = _pandas_arrow_is_valid(cursor)
    cdef int16_t* key_access
    cdef int16_t key
    if valid:
        key_access = <int16_t*>cursor.chunk.buffers[1]
        key = key_access[cursor.offset]
        _pandas_arrow_get_cat_value(cursor, <size_t>key, len_out, buf_out)
    return valid


cdef inline bint _pandas_arrow_get_cat_i32(
        col_cursor_t* cursor, size_t* len_out, const char** buf_out):
    cdef bint valid = _pandas_arrow_is_valid(cursor)
    cdef int32_t* key_access
    cdef int32_t key
    if valid:
        key_access = <int32_t*>cursor.chunk.buffers[1]
        key = key_access[cursor.offset]
        _pandas_arrow_get_cat_value(cursor, <size_t>key, len_out, buf_out)
    return valid


cdef inline bint _pandas_arrow_str(
        col_cursor_t* cursor,
        size_t* len_out,
        const char** buf_out):
    cdef int32_t* index_access
    cdef uint8_t* char_access
    cdef int32_t begin
    cdef bint valid = _pandas_arrow_is_valid(cursor)
    if valid:
        index_access = <int32_t*>cursor.chunk.buffers[1]
        char_access = <uint8_t*>cursor.chunk.buffers[2]
        begin = index_access[cursor.offset]
        len_out[0] = index_access[cursor.offset + 1] - begin
        buf_out[0] = <const char*>&char_access[begin]
    return valid


cdef inline void_int _pandas_cell_str_pyobj_to_utf8(
        qdb_pystr_buf* b,
        col_cursor_t* cursor,
        bint* valid_out,
        line_sender_utf8* utf8_out) except -1: 
    cdef PyObject** access = <PyObject**>cursor.chunk.buffers[1]
    cdef PyObject* cell = access[cursor.offset]
    if PyUnicode_CheckExact(cell):
        str_to_utf8(b, cell, utf8_out)
        valid_out[0] = True
    elif _pandas_is_null_pyobj(cell):
        valid_out[0] = False
    else:
        raise ValueError(
            'Expected a string, ' +
            f'got an object of type {_fqn(type(<object>cell))}.')


cdef void_int _pandas_serialize_cell_table__str_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef line_sender_table_name c_table_name
    if not PyUnicode_CheckExact(cell):
        if _pandas_is_null_pyobj(cell):
            raise ValueError('Expected a table name, got a null value')
        else:
            raise ValueError(
                'Expected a table name (str object), ' +
                f'got an object of type {_fqn(type(<object>cell))}.')
    str_to_table_name(b, cell, &c_table_name)
    if not line_sender_buffer_table(impl, c_table_name, &err):
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_table__str_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* buf
    cdef line_sender_table_name c_table_name
    if _pandas_arrow_str(&col.cursor, &c_len, &buf):
        if not line_sender_table_name_init(&c_table_name, c_len, buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(impl, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _pandas_serialize_cell_table__str_i8_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* c_buf
    cdef line_sender_table_name c_table_name
    if _pandas_arrow_get_cat_i8(&col.cursor, &c_len, &c_buf):
        if not line_sender_table_name_init(&c_table_name, c_len, c_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(impl, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _pandas_serialize_cell_table__str_i16_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* c_buf
    cdef line_sender_table_name c_table_name
    if _pandas_arrow_get_cat_i16(&col.cursor, &c_len, &c_buf):
        if not line_sender_table_name_init(&c_table_name, c_len, c_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(impl, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _pandas_serialize_cell_table__str_i32_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* c_buf
    cdef line_sender_table_name c_table_name
    if _pandas_arrow_get_cat_i32(&col.cursor, &c_len, &c_buf):
        if not line_sender_table_name_init(&c_table_name, c_len, c_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(impl, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _pandas_serialize_cell_symbol__str_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = False
    cdef line_sender_utf8 utf8
    _pandas_cell_str_pyobj_to_utf8(b, &col.cursor, &valid, &utf8)
    if valid and not line_sender_buffer_symbol(impl, col.name, utf8, &err):
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_symbol__str_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_str(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_symbol__str_i8_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_get_cat_i8(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_symbol__str_i16_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_get_cat_i16(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_symbol__str_i32_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_get_cat_i32(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_bool__bool_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    if PyBool_Check(cell):
        if not line_sender_buffer_column_bool(
                impl, col.name, cell == Py_True, &err):
            raise c_err_to_py(err)
    elif _pandas_is_null_pyobj(cell):
        raise ValueError('Cannot insert null values into a boolean column.')
    else:
        raise ValueError(
            'Expected an object of type bool, got a ' +
            _fqn(type(<object>cell)) + '.')


cdef void_int _pandas_serialize_cell_column_bool__bool_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint8_t* access = <uint8_t*>col.cursor.chunk.buffers[1]
    cdef uint8_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_bool(impl, col.name, not not cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_bool__bool_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef bint value
    if valid:
        value = _pandas_arrow_get_bool(&col.cursor)
        if not line_sender_buffer_column_bool(impl, col.name, value, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Cannot insert null values into a boolean column.')


cdef void_int _pandas_serialize_cell_column_i64__int_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef int64_t value
    if PyLong_CheckExact(cell):
        value = PyLong_AsLongLong(cell)
        if not line_sender_buffer_column_i64(impl, col.name, value, &err):
            raise c_err_to_py(err)
    elif _pandas_is_null_pyobj(cell):
        pass
    else:
        raise ValueError(
            'Expected an object of type int, got an object of type ' +
            _fqn(type(<object>cell)) + '.')


cdef void_int _pandas_serialize_cell_column_i64__u8_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint8_t* access = <uint8_t*>col.cursor.chunk.buffers[1]
    cdef uint8_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i8_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int8_t* access = <int8_t*>col.cursor.chunk.buffers[1]
    cdef int8_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u16_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint16_t* access = <uint16_t*>col.cursor.chunk.buffers[1]
    cdef uint16_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i16_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int16_t* access = <int16_t*>col.cursor.chunk.buffers[1]
    cdef int16_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u32_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint32_t* access = <uint32_t*>col.cursor.chunk.buffers[1]
    cdef uint32_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i32_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int32_t* access = <int32_t*>col.cursor.chunk.buffers[1]
    cdef int32_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u64_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint64_t* access = <uint64_t*>col.cursor.chunk.buffers[1]
    cdef uint64_t cell = access[col.cursor.offset]
    if cell > <uint64_t>INT64_MAX:
        _ensure_has_gil(gs)
        raise OverflowError('uint64 value too large for int64 column type.')
    if not line_sender_buffer_column_i64(impl, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i64_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int64_t* access = <int64_t*>col.cursor.chunk.buffers[1]
    cdef int64_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(impl, col.name, cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u8_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef uint8_t* access
    if valid:
        access = <uint8_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i8_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef int8_t* access
    if valid:
        access = <int8_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u16_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef uint16_t* access
    if valid:
        access = <uint16_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i16_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef int16_t* access
    if valid:
        access = <int16_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u32_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef uint32_t* access
    if valid:
        access = <uint32_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i32_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef int32_t* access
    if valid:
        access = <int32_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__u64_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef uint64_t* access
    cdef uint64_t cell
    if valid:
        access = <uint64_t*>col.cursor.chunk.buffers[1]
        cell = access[col.cursor.offset]
        if cell > <uint64_t>INT64_MAX:
            _ensure_has_gil(gs)
            raise OverflowError('uint64 value too large for int64 column type.')
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                <int64_t>cell,
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_i64__i64_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef int64_t* access
    if valid:
        access = <int64_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                impl,
                col.name,
                access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_f64__float_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef double value
    if PyFloat_CheckExact(cell):
        value = PyFloat_AS_DOUBLE(cell)
        if not line_sender_buffer_column_f64(impl, col.name, value, &err):
            raise c_err_to_py(err)
    elif _pandas_is_null_pyobj(cell):
        pass
    else:
        raise ValueError(
            'Expected an object of type float, got an object of type ' +
            _fqn(type(<object>cell)) + '.')


cdef void_int _pandas_serialize_cell_column_f64__f32_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    # Note: This is the C `float` type, not the Python `float` type.
    cdef float* access = <float*>col.cursor.chunk.buffers[1]
    cdef float cell = access[col.cursor.offset]
    if not line_sender_buffer_column_f64(impl, col.name, <double>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_f64__f64_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef double* access = <double*>col.cursor.chunk.buffers[1]
    cdef double cell = access[col.cursor.offset]
    if not line_sender_buffer_column_f64(impl, col.name, cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_f64__f32_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef float* access
    if valid:
        access = <float*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_f64(
                impl,
                col.name,
                <double>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_f64__f64_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef double* access
    if valid:
        access = <double*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_f64(
                impl,
                col.name,
                access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_str__str_pyobj(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = False
    cdef line_sender_utf8 utf8
    _pandas_cell_str_pyobj_to_utf8(b, &col.cursor,  &valid, &utf8)
    if valid and not line_sender_buffer_column_str(impl, col.name, utf8, &err):
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_str__str_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_str(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_str__str_i8_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_get_cat_i8(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_str__str_i16_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_get_cat_i16(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_str__str_i32_cat(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _pandas_arrow_get_cat_i32(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(impl, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_ts__dt64ns_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int64_t* access = <int64_t*>col.cursor.chunk.buffers[1]
    cdef int64_t cell = access[col.cursor.offset]
    cell //= 1000  # Convert from nanoseconds to microseconds.
    if not line_sender_buffer_column_ts(impl, col.name, cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_column_ts__dt64ns_tz_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef int64_t cell
    cdef int64_t* access
    if valid:
        access = <int64_t*>col.cursor.chunk.buffers[1]
        cell = access[col.cursor.offset]
        cell //= 1000  # Convert from nanoseconds to microseconds.
        if not line_sender_buffer_column_ts(impl, col.name, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_at_dt64ns_numpy(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int64_t* access = <int64_t*>col.cursor.chunk.buffers[1]
    cdef int64_t cell = access[col.cursor.offset]
    if cell == 0:
        if not line_sender_buffer_at_now(impl, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        # Note: impl will validate against negative numbers.
        if not line_sender_buffer_at(impl, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell_at_dt64ns_tz_arrow(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _pandas_arrow_is_valid(&col.cursor)
    cdef int64_t* access
    cdef int64_t cell
    if valid:
        access = <int64_t*>col.cursor.chunk.buffers[1]
        cell = access[col.cursor.offset]
    else:
        cell = 0

    if cell == 0:
        if not line_sender_buffer_at_now(impl, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        # Note: impl will validate against negative numbers.
        if not line_sender_buffer_at(impl, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _pandas_serialize_cell(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef col_dispatch_code_t dc = col.dispatch_code
    # Note!: Code below will generate a `switch` statement.
    # Ensure this happens! Don't break the `dc == ...` pattern.
    if dc == col_dispatch_code_t.col_dispatch_code_skip_nulls:
        pass  # We skip a null column. Nothing to do.
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_pyobj:
        _pandas_serialize_cell_table__str_pyobj(impl, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_arrow:
        _pandas_serialize_cell_table__str_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i8_cat:
        _pandas_serialize_cell_table__str_i8_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i16_cat:
        _pandas_serialize_cell_table__str_i16_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i32_cat:
        _pandas_serialize_cell_table__str_i32_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_pyobj:
        _pandas_serialize_cell_symbol__str_pyobj(impl, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_arrow:
        _pandas_serialize_cell_symbol__str_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i8_cat:
        _pandas_serialize_cell_symbol__str_i8_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i16_cat:
        _pandas_serialize_cell_symbol__str_i16_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i32_cat:
        _pandas_serialize_cell_symbol__str_i32_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_pyobj:
        _pandas_serialize_cell_column_bool__bool_pyobj(impl, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_numpy:
        _pandas_serialize_cell_column_bool__bool_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_arrow:
        _pandas_serialize_cell_column_bool__bool_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__int_pyobj:
        _pandas_serialize_cell_column_i64__int_pyobj(impl, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u8_numpy:
        _pandas_serialize_cell_column_i64__u8_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i8_numpy:
        _pandas_serialize_cell_column_i64__i8_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u16_numpy:
        _pandas_serialize_cell_column_i64__u16_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i16_numpy:
        _pandas_serialize_cell_column_i64__i16_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u32_numpy:
        _pandas_serialize_cell_column_i64__u32_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i32_numpy:
        _pandas_serialize_cell_column_i64__i32_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u64_numpy:
        _pandas_serialize_cell_column_i64__u64_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i64_numpy:
        _pandas_serialize_cell_column_i64__i64_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u8_arrow:
        _pandas_serialize_cell_column_i64__u8_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i8_arrow:
        _pandas_serialize_cell_column_i64__i8_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u16_arrow:
        _pandas_serialize_cell_column_i64__u16_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i16_arrow:
        _pandas_serialize_cell_column_i64__i16_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u32_arrow:
        _pandas_serialize_cell_column_i64__u32_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i32_arrow:
        _pandas_serialize_cell_column_i64__i32_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u64_arrow:
        _pandas_serialize_cell_column_i64__u64_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i64_arrow:
        _pandas_serialize_cell_column_i64__i64_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__float_pyobj:
        _pandas_serialize_cell_column_f64__float_pyobj(impl, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f32_numpy:
        _pandas_serialize_cell_column_f64__f32_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f64_numpy:
        _pandas_serialize_cell_column_f64__f64_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f32_arrow:
        _pandas_serialize_cell_column_f64__f32_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f64_arrow:
        _pandas_serialize_cell_column_f64__f64_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_pyobj:
        _pandas_serialize_cell_column_str__str_pyobj(impl, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_arrow:
        _pandas_serialize_cell_column_str__str_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i8_cat:
        _pandas_serialize_cell_column_str__str_i8_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i16_cat:
        _pandas_serialize_cell_column_str__str_i16_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i32_cat:
        _pandas_serialize_cell_column_str__str_i32_cat(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_ts__dt64ns_numpy:
        _pandas_serialize_cell_column_ts__dt64ns_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_ts__dt64ns_tz_arrow:
        _pandas_serialize_cell_column_ts__dt64ns_tz_arrow(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_at__dt64ns_numpy:
        _pandas_serialize_cell_at_dt64ns_numpy(impl, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_at__dt64ns_tz_arrow:
        _pandas_serialize_cell_at_dt64ns_tz_arrow(impl, b, col, gs)
    else:
        _ensure_has_gil(gs)
        raise RuntimeError(f"Unknown column dispatch code: {dc}")
    # See earlier note about switch statement generation.
    # Don't add complex conditions above!


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


# Every how many cells to release and re-acquire the Python GIL.
#
# We've done some perf testing with some mixed column dtypes.
# On a modern CPU we're doing over 8 million pandas cells per second.
# By default, `sys.getswitchinterval()` is 0.005 seconds.
# To accomodate this, we'd need to release the GIL every 40,000 cells.
# This will be divided by the column count to get the row gil blip interval.
cdef size_t _CELL_GIL_BLIP_INTERVAL = 40000


cdef void_int _pandas(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        object data,
        object table_name,
        object table_name_col,
        object symbols,
        object at,
        bint sort) except -1:
    cdef size_t col_count
    cdef line_sender_table_name c_table_name
    cdef int64_t at_value = _AT_IS_SET_BY_COLUMN
    cdef col_t_arr cols = col_t_arr_blank()
    cdef bint any_cols_need_gil = False
    cdef qdb_pystr_pos str_buf_marker
    cdef size_t row_count
    cdef line_sender_error* err = NULL
    cdef size_t row_index
    cdef size_t col_index
    cdef col_t* col
    cdef size_t row_gil_blip_interval
    cdef PyThreadState* gs = NULL  # GIL state. NULL means we have the GIL.
    cdef bint was_serializing_cell = False

    _pandas_may_import_deps()
    try:
        qdb_pystr_buf_clear(b)
        _check_is_pandas_dataframe(data)
        col_count = len(data.columns)
        cols = col_t_arr_new(col_count)
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
            &cols,
            &any_cols_need_gil)

        # We've used the str buffer up to a point for the headers.
        # Instead of clearing it (which would clear the headers' memory)
        # we will truncate (rewind) back to this position.
        str_buf_marker = qdb_pystr_buf_tell(b)

        row_count = len(data)
        line_sender_buffer_clear_marker(impl)

        # On error, undo all added lines.
        if not line_sender_buffer_set_marker(impl, &err):
            raise c_err_to_py(err)

        row_gil_blip_interval = _CELL_GIL_BLIP_INTERVAL // col_count
        if row_gil_blip_interval < 400:  # ceiling reached at 100 columns
            row_gil_blip_interval = 400
        try:
            # Don't move this logic up! We need the GIL to execute a `try`.
            # Also we can't have any other `try` blocks between here and the
            # `finally` block.
            if not any_cols_need_gil:
                gs = PyEval_SaveThread()

            for row_index in range(row_count):
                if (gs == NULL) and (row_index % row_gil_blip_interval == 0):
                    # Release and re-acquire the GIL every so often.
                    # This is to allow other python threads to run.
                    # If we hold the GIL for too long, we can starve other
                    # threads, for example timing out network activity.
                    gs = PyEval_SaveThread()
                    _ensure_has_gil(&gs)

                qdb_pystr_buf_truncate(b, str_buf_marker)

                # Table-name from `table_name` arg in Python.
                if c_table_name.buf != NULL:
                    if not line_sender_buffer_table(impl, c_table_name, &err):
                        _ensure_has_gil(&gs)
                        raise c_err_to_py(err)

                # Serialize columns cells.
                # Note: Columns are sorted: table name, symbols, fields, at.
                was_serializing_cell = True
                for col_index in range(col_count):
                    col = &cols.d[col_index]
                    _pandas_serialize_cell(impl, b, col, &gs)  # may raise
                    _pandas_col_advance(col)
                was_serializing_cell = False

                # Fixed "at" value (not from a column).
                if at_value == 0:
                    if not line_sender_buffer_at_now(impl, &err):
                        _ensure_has_gil(&gs)
                        raise c_err_to_py(err)
                elif at_value > 0:
                    if not line_sender_buffer_at(impl, at_value, &err):
                        _ensure_has_gil(&gs)
                        raise c_err_to_py(err)
        except Exception as e:
            # It would be an internal bug for this to raise.
            if not line_sender_buffer_rewind_to_marker(impl, &err):
                raise c_err_to_py(err)

            if was_serializing_cell:
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    'Failed to serialize value of column ' +
                    repr(data.columns[col.orig_index]) +
                    f' at row index {row_index} (' +
                    repr(data.iloc[row_index, col.orig_index]) +
                    f'): {e}  [dc={<int>col.dispatch_code}]') from e
            else:
                raise
    finally:
        _ensure_has_gil(&gs)  # Note: We need the GIL for cleanup.
        line_sender_buffer_clear_marker(impl)
        col_t_arr_release(&cols)
        qdb_pystr_buf_clear(b)
