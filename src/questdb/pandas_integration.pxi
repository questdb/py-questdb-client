include "size_t_vec.pxi"
include "column_name_vec.pxi"

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
    table = 1
    symbol = 2
    column_bool = 3
    column_i64 = 4
    column_f64 = 5
    column_str = 6
    column_ts = 7
    at = 8


cdef dict _TARGET_NAMES = {
    col_target_t.table: "table name",
    col_target_t.symbol: "symbol",
    col_target_t.column_bool: "boolean",
    col_target_t.column_i64: "integer",
    col_target_t.column_f64: "float",
    col_target_t.column_str: "string",
    col_target_t.column_ts: "timestamp",
    col_target_t.at: "designated timestamp",
}


cdef enum col_source_t:
    bool_pyobj =      101000
    bool_numpy =      102000
    bool_arrow =      103000
    int_pyobj =       201000
    u8_numpy =        202000
    i8_numpy =        203000
    u16_numpy =       204000
    i16_numpy =       205000
    u32_numpy =       206000
    i32_numpy =       207000
    u64_numpy =       208000
    i64_numpy =       209000
    u8_arrow =        210000
    i8_arrow =        211000
    u16_arrow =       212000
    i16_arrow =       213000
    u32_arrow =       214000
    i32_arrow =       215000
    u64_arrow =       216000
    i64_arrow =       217000
    float_pyobj =     301000
    f32_numpy =       302000
    f64_numpy =       303000
    f32_arrow =       304000
    f64_arrow =       305000
    str_pyobj =       401000
    str_arrow =       402000
    str_i8_cat =      403000
    str_i16_cat =     404000
    str_i32_cat =     405000
    str_i64_cat =     406000
    dt64ns_numpy =    501000
    dt64ns_tz_numpy = 502000


cdef dict _TARGET_TO_SOURCE = {
    col_target_t.table: {
        col_source_t.str_pyobj,
        col_source_t.str_arrow,
    },
    col_target_t.symbol: {
        col_source_t.str_pyobj,
        col_source_t.str_arrow,
    },
    col_target_t.column_bool: {
        col_source_t.bool_pyobj,
        col_source_t.bool_numpy,
        col_source_t.bool_arrow,
    },
    col_target_t.column_i64: {
        col_source_t.int_pyobj,
        col_source_t.u8_numpy,
        col_source_t.i8_numpy,
        col_source_t.u16_numpy,
        col_source_t.i16_numpy,
        col_source_t.u32_numpy,
        col_source_t.i32_numpy,
        col_source_t.u64_numpy,
        col_source_t.i64_numpy,
    },
    col_target_t.column_f64: {
        col_source_t.f32_num,
        col_source_t.f64_num,
    },
    col_target_t.column_str: {
        col_source_t.str_pyobj,
        col_source_t.str_arrow,
    },
    col_target_t.column_ts: {
        col_source_t.dt64_numpy,
    },
    col_target_t.at: {
        col_source_t.dt64_numpy
    },
}


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
    int dispatch_code  # source + target. Determines cell encoder function.


cdef void col_t_release(col_t* col):
    # We can use `col.tag` to determine what to release.
    # Arrow chunks might be "synthetic" borrowing from `col.pybuf`.
    # TODO: Implement cleanup, PyBuffer_Release, etc.
    # TODO: Do we need to call Arrow's cleanup functions when done?
    pass


cdef struct col_t_arr:
    size_t capacity
    size_t size  # Number of elements we've populated. Needed for cleanup.
    col_t* d


cdef col_t_arr col_t_arr_blank():
    cdef col_t_arr arr
    arr.capacity = 0
    arr.size = 0
    arr.d = NULL
    return arr


cdef col_t_arr col_t_arr_new(size_t capacity):
    cdef col_t_arr arr
    arr.capacity = capacity
    arr.size = 0
    arr.d = <col_t*>calloc(capacity, sizeof(col_t))
    return arr


cdef void col_t_arr_release(col_t_arr* arr):
    if arr.d:
        for i in range(arr.size):
            col_t_release(&arr.d[i])
        free(arr.d)
        arr.size = 0
        arr.capacity = 0
        arr.d = NULL


cdef object _NUMPY = None  # module object
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
    if _NUMPY is not None:
        return
    import numpy
    _NUMPY = numpy
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
            f'Bad argument `data`: Expected {_PANDAS.DataFrame}, ' +
            f'not an object of type {type(data)}.')


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
        tagged_cols[col_index][3] = -2  # Sort column to front as table name.
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


@cython.internal
cdef class TaggedEntry:
    """Python object representing a column whilst parsing .pandas arguments."""

    cdef size_t orig_index
    cdef str name
    cdef object dtype
    cdef object series
    cdef int meta_target

    def __init__(
            self,
            size_t orig_index,
            str name,
            object dtype,
            object series,
            int meta_target):
        self.orig_index = orig_index
        self.name = name
        self.dtype = dtype
        self.series = series
        self.meta_target = meta_target


cdef int _META_TARGET_FIELD = 0
cdef int _META_TARGET_TABLE_NAME = -2
cdef int _META_TARGET_SYMBOL = -1
cdef int _META_TARGET_AT = 1


cdef bint _pandas_resolve_symbols(
        object data,
        list tagged_cols,
        ssize_t table_name_col,
        ssize_t at_col,
        object symbols,
        size_t col_count) except False:
    cdef size_t col_index = 0
    cdef object symbol
    if symbols is False:
        return 0
    elif symbols is True:
        for col_index in range(col_count):
            if _pandas_column_is_str(data, col_index):
                tagged_cols[col_index].meta_target = _META_TARGET_SYMBOL
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
            tagged_cols[col_index].meta_target = _META_TARGET_SYMBOL
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
            f'Bad argument `at`: Unsupported type {type(at)}. ' +
            'Must be one of: None, TimestampNanos, datetime, ' +
            'int (column index), str (colum name)')
    dtype = data.dtypes[col_index]
    if _pandas_is_supported_datetime(dtype):
        at_value_out[0] = _AT_SET_BY_COLUMN
        tagged_cols[col_index].meta_target = _META_TARGET_AT
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


cdef char _str_to_char(str field, str s) except 0:
    cdef int res
    if len(s) != 1:
        raise ValueError(
            f'dtype.{field}: Expected a single character, got {s!r}')
    res = ord(s)
    if res <= 0 or res > 127:  # Check if ASCII, excluding the nul-termintor.
        raise ValueError(
            f'dtype.{field}: Character out of ASCII range, got {s!r}')
    return <char>res


cdef bint _pandas_parse_dtype(object np_dtype, dtype_t* dtype_out) except False:
    """
    Parse a numpy dtype and return a dtype_t.
    """
    dtype_out.alignment = getattr(np_dtype, 'alignment' , 0)
    dtype_out.kind = _str_to_char('kind', np_dtype.kind)
    dtype_out.itemsize = getattr(np_dtype, 'itemsize', 0)
    dtype_out.byteorder = _str_to_char(
        'byteorder', getattr(np_dtype, 'byteorder', '='))
    dtype_out.hasobject = getattr(np_dtype, 'hasobject', False)
    return True


cdef bint _pandas_resolve_dtypes(
        object data, size_t col_count, dtype_t* dtypes_out) except False:
    cdef size_t col_index
    for col_index in range(col_count):
        _pandas_parse_dtype(data.dtypes[col_index], &dtypes_out[col_index])
    return True


cdef bint _pandas_resolve_col_buffers(
        object data, size_t col_count, const dtype_t* dtypes,
        Py_buffer* col_buffers, size_t* set_buf_count) except False:
    """
    Map pandas columns to array of col_buffers.
    """
    # Note: By calling "to_numpy" we are throwing away what might be an Arrow.
    # This is particularly expensive for string columns.
    # If you want to use Arrow (i.e. your data comes from Parquet) please ask
    # for the feature in our issue tracker.
    cdef size_t col_index
    cdef object nparr
    cdef Py_buffer* view
    cdef const dtype_t* dtype
    for col_index in range(col_count):
        nparr = data.iloc[:, col_index].to_numpy()
        view = &col_buffers[col_index]
        dtype = &dtypes[col_index]
        if not PyObject_CheckBuffer(nparr):
            raise TypeError(
                f'Bad column: Expected a numpy array, got {nparr!r}')
        PyObject_GetBuffer(nparr, view, PyBUF_STRIDES)
        # TODO [amunra]: We should check that the buffer metadata and the dtype match. We currently risk a segfault.
        set_buf_count[0] += 1   # Set to avoid wrongly calling PyBuffer_Release.
    return True


cdef inline const void* _pandas_get_cell(
        Py_buffer* col_buffer, size_t row_index):
    return col_buffer.buf + (<ssize_t>row_index * col_buffer.strides[0])


cdef char _PANDAS_DTYPE_KIND_OBJECT = <char>79  # 'O'
cdef char _PANDAS_DTYPE_KIND_DATETIME = <char>77  # 'M'


cdef bint _pandas_get_str_cell(
        qdb_pystr_buf* b,
        dtype_t* dtype,
        Py_buffer* col_buffer,
        size_t row_index,
        bint* is_null_out,
        line_sender_utf8* utf8_out) except False:
    cdef const void* cell = _pandas_get_cell(col_buffer, row_index)
    cdef object obj
    if dtype.kind == _PANDAS_DTYPE_KIND_OBJECT:
        # TODO [amunra]: Check in the generated .C code that it doesn't produce an INCREF.
        # TODO: Improve error messaging. Error message should include the column name.
        obj = <object>((<PyObject**>cell)[0])
        if (obj is None) or (obj is _PANDAS_NA):
            is_null_out[0] = True
        else:
            is_null_out[0] = False
            try:
                str_to_utf8(b, obj, utf8_out)
            except TypeError as e:
                raise TypeError(
                    'Bad column: Expected a string, ' +
                    f'got {obj!r} ({type(obj)!r})') from e
    else:
        raise TypeError(
            f'NOT YET IMPLEMENTED. Kind: {dtype.kind}')  # TODO [amunra]: Implement and test.
    return True


cdef int64_t _pandas_get_timestamp_cell(
        dtype_t* dtype,
        Py_buffer* col_buffer,
        size_t row_index) except -1:
    # Note: Type is pre-validated by `_pandas_is_supported_datetime`.
    cdef const void* cell = _pandas_get_cell(col_buffer, row_index)
    cdef int64_t res = (<int64_t*>cell)[0]
    if res < 0:
        # TODO [amunra]: Improve error messaging. Add column name.
        raise ValueError(  
            f'Bad value: Negative timestamp, got {res}')
    return res


cdef bint _pandas_row_table_name(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        dtype_t* dtypes,
        Py_buffer* col_buffers,
        ssize_t name_col,
        size_t row_index,
        line_sender_table_name c_table_name) except False:
    cdef line_sender_error* err = NULL
    cdef bint is_null = False
    cdef line_sender_utf8 utf8
    if name_col >= 0:
        _pandas_get_str_cell(
            b,
            &dtypes[<size_t>name_col],
            &col_buffers[<size_t>name_col],
            row_index,
            &is_null,
            &utf8)
        if is_null:
            # TODO [amunra]: Improve error messaging. Add column name.
            raise ValueError(
                f'Bad value: `None` table name value at row {row_index}.')
        if not line_sender_table_name_init(
                &c_table_name, utf8.len, utf8.buf, &err):
            raise c_err_to_py(err)
    if not line_sender_buffer_table(impl, c_table_name, &err):
        raise c_err_to_py(err)
    return True

    
cdef bint _pandas_row_symbols(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        dtype_t* dtypes,
        Py_buffer* col_buffers,
        size_t row_index,
        const column_name_vec* symbol_names,
        const size_t_vec* symbol_indices) except False:
    cdef line_sender_error* err = NULL
    cdef size_t sym_index
    cdef size_t col_index
    cdef dtype_t* dtype
    cdef Py_buffer* col_buffer
    cdef line_sender_column_name col_name
    cdef line_sender_utf8 symbol
    cdef bint is_null = False
    for sym_index in range(symbol_indices.size):
        col_name = symbol_names.d[sym_index]
        col_index = symbol_indices.d[sym_index]
        col_buffer = &col_buffers[col_index]
        dtype = &dtypes[col_index]
        _pandas_get_str_cell(
            b,
            dtype,
            col_buffer,
            row_index,
            &is_null,
            &symbol)
        if not is_null:
            if not line_sender_buffer_symbol(impl, col_name, symbol, &err):
                raise c_err_to_py(err)
    return True


cdef bint _pandas_row_at(
        line_sender_buffer* impl,
        dtype_t* dtypes,
        Py_buffer* col_buffers,
        size_t row_index,
        ssize_t at_col,
        int64_t at_value) except False:
    cdef line_sender_error* err = NULL
    cdef Py_buffer* col_buffer
    cdef dtype_t* dtype
    if at_col >= 0:
        col_buffer = &col_buffers[<size_t>at_col]
        dtype = &dtypes[<size_t>at_col]
        at_value = _pandas_get_timestamp_cell(dtype, col_buffer, row_index)
    if at_value > 0:
        if not line_sender_buffer_at(impl, at_value, &err):
            raise c_err_to_py(err)
    else:
        if not line_sender_buffer_at_now(impl, &err):
            raise c_err_to_py(err)
    return True


cdef int _pandas_series_as_pybuf(
        TaggedEntry entry, col_t* col_out) except -1:
    cdef object nparr = entry.series.to_numpy()
    if not PyObject_CheckBuffer(nparr):
        raise TypeError(
            f'Bad column {entry.name!r}: Expected a buffer, got ' +
            f'{entry.series!r} ({type(entry.series)!r})')
    col_out.tag = col_access_tag_t.numpy
    return PyObject_GetBuffer(nparr, &col_out.pybuf, PyBUF_STRIDES)
    # TODO: Wrap exception from `PyObject_GetBuffer` informatively.
    # Then change to `except False` and `bint` return type.
    # Then manually map the `pybuf` to the arrow `col.chunks` even if we're
    # not using arrow. This allows is to give us a single consistent iterating
    # device.
    # TODO: Set stride.


cdef bint _pandas_series_as_arrow(
        TaggedEntry entry,
        col_t* col_out,
        col_source_t np_fallback) except False:
    cdef object array
    cdef list chunks
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

    col_out.chunks.n_chunks = len(chunks)
    col_out.chunks.chunks = <ArrowArray*>calloc(
        col_out.chunks.n_chunks + 1,  # See `_pandas_col_advance` on why +1.
        sizeof(ArrowArray))

    for chunk_index in range(len(chunks)):
        array = chunks[chunk_index]
        if chunk_index == 0:
            chunks[chunk_index]._export_to_c(
                &col_out.chunks.chunks[chunk_index],
                &col_out.arrow_schema)
        else:
            chunks[chunk_index]._export_to_c(
                &col_out.chunks.chunks[chunk_index])
    return True
    

cdef bint _pandas_category_series_as_arrow(
        TaggedEntry entry, col_t* col_out) except False:
    col_out.source = <col_source_t>0  # place holder value
    _pandas_series_as_arrow(entry, col_out, col_source_t.str_pyobj)
    if col_out.source == col_source_t.str_pyobj:
        return True
    


cdef bint _pandas_resolve_source_and_buffers(
        TaggedEntry entry, col_t* col_out) except False:
    cdef object dtype = entry.dtype
    if isinstance(dtype, _PANDAS.dtype('bool')):
        col_out.source = col_source_t.bool_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.BooleanDtype):
        col_out.source = col_source_t.bool_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.bool_pyobj)
    elif isinstance(dtype, _NUMPY.dtype('uint8')):
        col_out.source = col_source_t.u8_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('int8')):
        col_out.source = col_source_t.i8_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('uint16')):
        col_out.source = col_source_t.u16_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('int16')):
        col_out.source = col_source_t.i16_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('uint32')):
        col_out.source = col_source_t.u32_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('int32')):
        col_out.source = col_source_t.i32_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('uint64')):
        col_out.source = col_source_t.u64_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('int64')):
        col_out.source = col_source_t.i64_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.UInt8Dtype):
        col_out.source = col_source_t.u8_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.Int8Dtype):
        col_out.source = col_source_t.i8_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt16Dtype):
        col_out.source = col_source_t.u16_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.Int16Dtype):
        col_out.source = col_source_t.i16_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt32Dtype):
        col_out.source = col_source_t.u32_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.Int32Dtype):
        col_out.source = col_source_t.i32_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.UInt64Dtype):
        col_out.source = col_source_t.u64_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _PANDAS.Int64Dtype):
        col_out.source = col_source_t.i64_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.int_pyobj)
    elif isinstance(dtype, _NUMPY.dtype('float32')):
        col_out.source = col_source_t.f32_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('float64')):
        col_out.source = col_source_t.f64_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.Float32Dtype):
        col_out.source = col_source_t.f32_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.float_pyobj)
    elif isinstance(dtype, _PANDAS.Float64Dtype):
        col_out.source = col_source_t.f64_arrow
        _pandas_series_as_arrow(entry, col_out, col_source_t.float_pyobj)
    elif isinstance(dtype, _PANDAS.StringDtype):
        if dtype.storage == 'pyarrow':
            col_out.source = col_source_t.str_arrow
            _pandas_series_as_arrow(entry, col_out, col_source_t.str_pyobj)
        elif dtype.storage == 'python':
            col_out.source = col_source_t.str_pyobj
            _pandas_series_as_pybuf(entry, col_out)
        else:
            raise ValueError(
                f'Unknown string dtype storage: f{dtype.storage} ' +
                f'for column {entry.name} of dtype {dtype}.')
    elif isinstance(dtype, _PANDAS.CategoricalDtype):
        _pandas_category_series_as_arrow(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('datetime64[ns]')):
        col_out.source = col_source_t.dt64ns_numpy
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _PANDAS.DatetimeTZDtype):
        col_out.source = col_source_t.dt64ns_tz_numpy
        col_out.tz_offset_ns = _pandas_get_tz_offset_ns(dtype.tz)
        _pandas_series_as_pybuf(entry, col_out)
    elif isinstance(dtype, _NUMPY.dtype('object')):
        # We need to sniff the objects
        # TODO: bool_pyobj, int_pyobj, str_pyobj

    
    
    raise ValueError('nyi')  # TODO [amunra]: Implement and test.


cdef int _pandas_resolve_target(
        TaggedEntry entry, col_source_t source) except -1:
    raise ValueError('nyi')


cdef bint _pandas_resolve_col(
        qdb_pystr_buf* b,
        size_t index,
        TaggedEntry entry,
        col_t* col_out) except False:
    col_out.orig_index = entry.orig_index

    # Since we don't need to send the column names for 'table' and 'at' columns,
    # we don't need to validate and encode them as column names.
    if ((entry.meta_target != _META_TARGET_TABLE_NAME) and
            (entry.meta_target != _META_TARGET_AT)):
        str_to_column_name(b, entry.name, &col_out.name)

    _pandas_resolve_source_and_buffers(entry, col_out)
    col_out.target = <col_target_t>_pandas_resolve_target(entry, col_out.source)
    if col_out.source not in _TARGET_TO_SOURCE[col_out.target]:
        raise ValueError(
            f'Bad value: Column {entry.name!r} ({entry.dtype}) is not ' +
            f'supported as a {_TARGET_NAMES[col_out.target]} column.')
    col_out.dispatch_code = <int>col_out.source + <int>col_out.target



cdef bint _pandas_resolve_cols(
        qdb_pystr_buf*b, list tagged_cols, col_t_arr* cols_out) except False:
    cdef size_t index
    for index in range(len(tagged_cols)):
        _pandas_resolve_col(b, index, tagged_cols[index], &cols_out.d[index])
        cols_out.size += 1
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

    # List of Lists with col index, col name, series object, sorting key.
    cdef list tagged_cols = [
        TaggedEntry(
            index,
            name,
            data.dtypes[index]
            series,
            _META_TARGET_FIELD)  # FIELD is a `bool`, `f64`, `string` or `ts`.
        for index, (name, series) in data.items()]
    name_col = _pandas_resolve_table_name(
        b,
        data, tagged_cols, table_name, table_name_col, col_count, c_table_name_out)
    at_col = _pandas_resolve_at(data, tagged_cols, at, col_count, at_value_out)
    _pandas_resolve_symbols(
        data, tagged_cols, name_col, at_col, symbols, col_count)

    # Sort with table name is first, then the symbols, then fields, then at.
    # Note: Python 3.6+ guarantees stable sort.
    tagged_cols.sort(key=lambda x: x.meta_target)
    _pandas_resolve_cols(b, tagged_cols, cols_out)

    return True


    # cdef dtype_t* dtypes = NULL
    # cdef size_t set_buf_count = 0
    # cdef Py_buffer* col_buffers = NULL
    # dtypes = <dtype_t*>calloc(col_count, sizeof(dtype_t))
    # _pandas_resolve_dtypes(data, col_count, dtypes)
    # col_buffers = <Py_buffer*>calloc(col_count, sizeof(Py_buffer))
    # _pandas_resolve_col_buffers(
    #     data, col_count, dtypes, col_buffers, &set_buf_count)


cdef bint _pandas_serialize_cell(
        line_sender_buffer* impl,
        qdb_pystr_buf* b,
        col_t* col,
        size_t row_index) except False:
    pass


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
            &cols)

        # We've used the str buffer up to a point for the headers.
        # Instead of clearing it (which would clear the headers' memory)
        # we will truncate (rewind) back to this position.
        str_buf_marker = qdb_pystr_buf_tell(b)

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
        return True
    finally:
        line_sender_buffer_clear_marker(impl)
        col_t_arr_release(&cols)
        qdb_pystr_buf_clear(b)
