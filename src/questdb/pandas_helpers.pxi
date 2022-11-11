

cdef struct c_size_t_vec:
    size_t capacity
    size_t size
    size_t* d

ctypedef c_size_t_vec size_t_vec

cdef size_t_vec size_t_vec_new():
    cdef size_t_vec vec
    vec.capacity = 0
    vec.size = 0
    vec.d = <size_t*>NULL
    return vec

cdef void size_t_vec_free(size_t_vec* vec):
    if vec.d:
        free(vec.d)
        vec.d = NULL

cdef str size_t_vec_str(size_t_vec* vec):
    return 'size_t_vec' + str([vec.d[i] for i in range(vec.size)])

cdef void size_t_vec_push(size_t_vec* vec, size_t value):
    if vec.capacity == 0:
        vec.capacity = 8
        vec.d = <size_t*>malloc(vec.capacity * sizeof(size_t))
        if vec.d == NULL:
            abort()
    elif vec.size == vec.capacity:
        vec.capacity = vec.capacity * 2
        vec.d = <size_t*>realloc(vec.d, vec.capacity * sizeof(size_t))
        if not vec.d:
            abort()
    vec.d[vec.size] = value
    vec.size += 1


cdef struct c_column_name_vec:
    size_t capacity
    size_t size
    line_sender_column_name* d

ctypedef c_column_name_vec column_name_vec

cdef column_name_vec column_name_vec_new():
    cdef column_name_vec vec
    vec.capacity = 0
    vec.size = 0
    vec.d = <line_sender_column_name*>NULL
    return vec

cdef void column_name_vec_free(column_name_vec* vec):
    if vec.d:
        free(vec.d)
        vec.d = NULL

cdef void column_name_vec_push(
        column_name_vec* vec, line_sender_column_name value):
    if vec.capacity == 0:
        vec.capacity = 8
        vec.d = <line_sender_column_name*>malloc(
            vec.capacity * sizeof(line_sender_column_name))
        if vec.d == NULL:
            abort()
    elif vec.size == vec.capacity:
        vec.capacity = vec.capacity * 2
        vec.d = <line_sender_column_name*>realloc(
            vec.d,
            vec.capacity * sizeof(line_sender_column_name))
        if not vec.d:
            abort()
    vec.d[vec.size] = value
    vec.size += 1


cdef enum col_type_t:
    COL_TYPE_INT = 0
    COL_TYPE_FLOAT = 1
    COL_TYPE_STRING = 2
    COL_TYPE_BOOL = 3
    COL_TYPE_DATE = 4
    COL_TYPE_TIME = 5
    COL_TYPE_DATETIME = 6
    COL_TYPE_BLOB = 7
    COL_TYPE_NULL = 8

# Inspired by a Py_buffer.
# See: https://docs.python.org/3/c-api/buffer.html
# This is simpler as we discard Python-specifics and it's one-dimensional only,
# i.e. `ndim` is always 1.
# If this stuff makes no sense to you:
# http://jakevdp.github.io/blog/2014/05/05/introduction-to-the-python-buffer-protocol/
# and https://www.youtube.com/watch?v=10smLBD0kXg
cdef struct column_buf_t:
    col_type_t dtype  # internal enum value of supported pandas type.
    void* d           # start of the buffer (pointer to element 0)
    ssize_t nbytes    # size of the buffer in bytes
    ssize_t count     # number of elements in the buffer (aka shape[0])
    ssize_t itemsize  # element size in bytes (!=nbytes/count due to strides)
    ssize_t stride    # stride in bytes between elements
    # NB: We don't support suboffsets.


cdef object _PANDAS_NA = None


cdef object _pandas_may_set_na_type():
    global _PANDAS_NA
    if _PANDAS_NA is None:
        import pandas as pd
        _PANDAS_NA = pd.NA


cdef ssize_t _pandas_resolve_table_name(
        qdb_pystr_buf* b,
        object data,
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
        return col_index
    else:
        raise ValueError(
            'Must specify at least one of `table_name` or `table_name_col`.')


cdef int _pandas_resolve_fields(
        int name_col,
        const size_t_vec* symbol_indices,
        int at_col,
        size_t col_count,
        size_t_vec* field_indices_out) except -1:
    """
    Populate vec of field column indices via `field_indices_out`.
    Returns the length of the list.
    The vec will contain all columns which are not the table name column,
    symbols or the at timestamp column.
    """
    # We rely on `symbol_indices` being sorted.
    cdef size_t col_index = 0
    cdef size_t sym_index = 0
    cdef size_t sym_len = symbol_indices.size
    while col_index < col_count:
        if (name_col >= 0) and (col_index == <size_t>name_col):
            col_index += 1
            continue
        if (at_col >= 0) and (col_index == <size_t>at_col):
            col_index += 1
            continue
        while sym_index < sym_len and symbol_indices.d[sym_index] < col_index:
            sym_index += 1
        if sym_index < sym_len and symbol_indices.d[sym_index] == col_index:
            col_index += 1
            continue
        size_t_vec_push(field_indices_out, col_index)
        col_index += 1
    return 0


cdef bint _pandas_resolve_col_names(
        qdb_pystr_buf* b,
        object data,
        const size_t_vec* symbol_indices,
        const size_t_vec* field_indices,
        column_name_vec* symbol_names_out,
        column_name_vec* field_names_out) except False:
    cdef line_sender_column_name c_name
    cdef size_t col_index
    for col_index in range(symbol_indices.size):
        col_index = symbol_indices.d[col_index]
        str_to_column_name(b, data.columns[col_index], &c_name)
        column_name_vec_push(symbol_names_out, c_name)
    for col_index in range(field_indices.size):
        col_index = field_indices.d[col_index]
        str_to_column_name(b, data.columns[col_index], &c_name)
        column_name_vec_push(field_names_out, c_name)
    return True


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


cdef int _pandas_resolve_symbols(
        object data,
        ssize_t table_name_col,
        ssize_t at_col,
        object symbols,
        size_t col_count,
        size_t_vec* symbol_indices_out) except -1:
    """
    Populate vec of symbol column indices via `symbol_indices_out`.
    Returns the length of the vec.
    """
    cdef size_t col_index = 0
    cdef object symbol
    if symbols is False:
        return 0
    elif symbols is True:
        for col_index in range(col_count):
            if _pandas_column_is_str(data, col_index):
                size_t_vec_push(symbol_indices_out, col_index)
        return 0
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
            size_t_vec_push(symbol_indices_out, col_index)
        return 0


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


cdef ssize_t _pandas_resolve_at(
        object data,
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
        at_value_out[0] = 0
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


cdef struct dtype_t:
    # A ripoff of a subset of PyArray_Descr as we were able to extract from numpy.dtype.
    # See: https://numpy.org/doc/stable/reference/generated/numpy.dtype.html?highlight=dtype#numpy.dtype
    # See: https://numpy.org/doc/stable/reference/c-api/types-and-structures.html#c.PyArray_Descr
    int alignment
    char kind
    int itemsize
    char byteorder
    bint hasobject


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
