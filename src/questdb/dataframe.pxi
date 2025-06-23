# See: dataframe.md for technical overview.

# Auto-flush settings.
# The individual `interval`, `row_count` and `byte_count`
# settings are set to `-1` when disabled.
# If `.enabled`, then at least one of the settings are `!= -1`.
cdef struct auto_flush_mode_t:
    bint enabled
    int64_t interval  
    int64_t row_count
    int64_t byte_count


cdef struct auto_flush_t:
    line_sender* sender
    auto_flush_mode_t mode
    int64_t* last_flush_ms


cdef auto_flush_t auto_flush_blank() noexcept nogil:
    cdef auto_flush_t af
    af.sender = NULL
    af.mode.enabled = False
    af.mode.interval = -1
    af.mode.row_count = -1
    af.mode.byte_count = -1
    af.last_flush_ms = NULL
    return af

cdef bint should_auto_flush(
            const auto_flush_mode_t* af_mode,
            line_sender_buffer* ls_buf,
            int64_t last_flush_ms):
    if not af_mode.enabled:
        return False

    # Check `auto_flush_rows` breach.
    if (af_mode.row_count != -1) and \
        (<int64_t>line_sender_buffer_row_count(ls_buf) >= af_mode.row_count):
        return True

    # Check `auto_flush_bytes` breach.
    if (af_mode.byte_count != -1) and \
        (<int64_t>line_sender_buffer_size(ls_buf) >= af_mode.byte_count):
        return True

    # Check for interval breach.
    if (af_mode.interval != -1) and \
        (((line_sender_now_micros() / 1000) - last_flush_ms) >= af_mode.interval):
        return True

    return False


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
    col_target_column_arr_f64 = 8
    col_target_at = 9


cdef dict _TARGET_NAMES = {
    col_target_t.col_target_skip: "skipped",
    col_target_t.col_target_table: "table name",
    col_target_t.col_target_symbol: "symbol",
    col_target_t.col_target_column_bool: "boolean",
    col_target_t.col_target_column_i64: "integer",
    col_target_t.col_target_column_f64: "float",
    col_target_t.col_target_column_str: "string",
    col_target_t.col_target_column_ts: "timestamp",
    col_target_t.col_target_column_arr_f64: "array",
    col_target_t.col_target_at: "designated timestamp",
}


cdef enum col_source_t:
    # Note: Hundreds digit set to 1 if GIL is required.
    col_source_nulls =                       0
    col_source_bool_pyobj =             101100
    col_source_bool_numpy =             102000
    col_source_bool_arrow =             103000
    col_source_int_pyobj =              201100
    col_source_u8_numpy =               202000
    col_source_i8_numpy =               203000
    col_source_u16_numpy =              204000
    col_source_i16_numpy =              205000
    col_source_u32_numpy =              206000
    col_source_i32_numpy =              207000
    col_source_u64_numpy =              208000
    col_source_i64_numpy =              209000
    col_source_u8_arrow =               210000
    col_source_i8_arrow =               211000
    col_source_u16_arrow =              212000
    col_source_i16_arrow =              213000
    col_source_u32_arrow =              214000
    col_source_i32_arrow =              215000
    col_source_u64_arrow =              216000
    col_source_i64_arrow =              217000
    col_source_float_pyobj =            301100
    col_source_f32_numpy =              302000
    col_source_f64_numpy =              303000
    col_source_f32_arrow =              304000
    col_source_f64_arrow =              305000
    col_source_str_pyobj =              401100
    col_source_str_utf8_arrow =         402000
    col_source_str_i8_cat =             403000
    col_source_str_i16_cat =            404000
    col_source_str_i32_cat =            405000
    col_source_str_lrg_utf8_arrow =     406000
    col_source_dt64ns_numpy =           501000
    col_source_dt64ns_tz_arrow =        502000
    col_source_arr_f64_numpyobj =       601100


cdef bint col_source_needs_gil(col_source_t source) noexcept nogil:
    # Check if hundreds digit is 1.
    return <int>source // 100 % 10 == 1


cdef set _STR_SOURCES = {
    col_source_t.col_source_str_pyobj,
    col_source_t.col_source_str_utf8_arrow,
    col_source_t.col_source_str_lrg_utf8_arrow,
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
        col_source_t.col_source_str_utf8_arrow,
        col_source_t.col_source_str_lrg_utf8_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
    },
    col_target_t.col_target_symbol: {
        col_source_t.col_source_str_pyobj,
        col_source_t.col_source_str_utf8_arrow,
        col_source_t.col_source_str_lrg_utf8_arrow,
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
        col_source_t.col_source_str_utf8_arrow,
        col_source_t.col_source_str_lrg_utf8_arrow,
        col_source_t.col_source_str_i8_cat,
        col_source_t.col_source_str_i16_cat,
        col_source_t.col_source_str_i32_cat,
    },
    col_target_t.col_target_column_ts: {
        col_source_t.col_source_dt64ns_numpy,
        col_source_t.col_source_dt64ns_tz_arrow,
    },
    col_target_t.col_target_column_arr_f64: {
        col_source_t.col_source_arr_f64_numpyobj,
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
    col_target_t.col_target_column_ts,
    col_target_t.col_target_column_arr_f64)


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
    col_dispatch_code_table__str_utf8_arrow = \
        col_target_t.col_target_table + col_source_t.col_source_str_utf8_arrow
    col_dispatch_code_table__str_lrg_utf8_arrow = \
        col_target_t.col_target_table + col_source_t.col_source_str_lrg_utf8_arrow
    col_dispatch_code_table__str_i8_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i8_cat
    col_dispatch_code_table__str_i16_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i16_cat
    col_dispatch_code_table__str_i32_cat = \
        col_target_t.col_target_table + col_source_t.col_source_str_i32_cat

    col_dispatch_code_symbol__str_pyobj = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_pyobj
    col_dispatch_code_symbol__str_utf8_arrow = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_utf8_arrow
    col_dispatch_code_symbol__str_lrg_utf8_arrow = \
        col_target_t.col_target_symbol + col_source_t.col_source_str_lrg_utf8_arrow
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
    col_dispatch_code_column_str__str_utf8_arrow = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_utf8_arrow
    col_dispatch_code_column_str__str_lrg_utf8_arrow = \
        col_target_t.col_target_column_str + col_source_t.col_source_str_lrg_utf8_arrow
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

    col_dispatch_code_column_arr_f64__arr_f64_numpyobj = \
        col_target_t.col_target_column_arr_f64 + col_source_t.col_source_arr_f64_numpyobj


# Int values in order for sorting (as needed for API's sequential coupling).
cdef enum meta_target_t:
    meta_target_table = <int>col_target_t.col_target_table
    meta_target_symbol = <int>col_target_t.col_target_symbol
    meta_target_field = <int>col_target_t.col_target_column_bool
    meta_target_at = <int>col_target_t.col_target_at


cdef struct col_setup_t:
    col_chunks_t chunks
    size_t orig_index
    Py_buffer pybuf
    ArrowSchema arrow_schema  # Schema of first chunk.
    col_source_t source
    meta_target_t meta_target
    col_target_t target


cdef struct col_t:
    col_dispatch_code_t dispatch_code  # source + target. Determines serializer.
    line_sender_column_name name
    col_cursor_t cursor
    col_setup_t* setup  # Grouping to reduce size of struct.


cdef void col_t_release(col_t* col) noexcept:
    """
    Release a (possibly) initialized column.

    col_t objects are `calloc`ed, so uninitialized (or partially) initialized
    objects will have their pointers and other values set to 0.
    """
    cdef size_t chunk_index
    cdef ArrowArray* chunk

    if Py_buffer_obj_is_set(&col.setup.pybuf):
        PyBuffer_Release(&col.setup.pybuf)  # Note: Sets `.pybuf.obj` to NULL.

    for chunk_index in range(col.setup.chunks.n_chunks):
        chunk = &col.setup.chunks.chunks[chunk_index]
        if chunk.release != NULL:
            chunk.release(chunk)
        memset(chunk, 0, sizeof(ArrowArray))

    if col.setup.arrow_schema.release != NULL:
        col.setup.arrow_schema.release(&col.setup.arrow_schema)

    free(col.setup.chunks.chunks)
    col.setup.chunks.chunks = NULL
    col.setup.chunks.n_chunks = 0

    free(col.setup)
    col.setup = NULL


# Calloc'd array of col_t.
cdef struct col_t_arr:
    size_t size
    col_t* d


cdef col_t_arr col_t_arr_blank() noexcept nogil:
    cdef col_t_arr arr
    arr.size = 0
    arr.d = NULL
    return arr


cdef col_t_arr col_t_arr_new(size_t size) noexcept nogil:
    cdef col_t_arr arr
    cdef size_t index
    arr.size = size
    arr.d = <col_t*>calloc(size, sizeof(col_t))
    for index in range(size):
        arr.d[index].setup = <col_setup_t*>calloc(1, sizeof(col_setup_t))
    return arr


cdef void col_t_arr_release(col_t_arr* arr) noexcept:
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

cdef int64_t _NAT = INT64_MIN  # pandas NaT


cdef object _dataframe_may_import_deps():
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
    try:
        import pandas
        import numpy
        import pyarrow
    except ImportError as ie:
        raise ImportError(
            'Missing dependencies: `pandas`, `numpy` and `pyarrow` must all ' +
            'be installed to use the `.dataframe()` method. ' +
            'See: https://py-questdb-client.readthedocs.io/' +
            'en/latest/installation.html.') from ie
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
    _PANDAS = pandas
    _PANDAS_NA = pandas.NA
    _PYARROW = pyarrow


cdef object _dataframe_check_is_dataframe(object df):
    if not isinstance(df, _PANDAS.DataFrame):
        raise IngressError(
            IngressErrorCode.InvalidApiCall,
            f'Bad argument `df`: Expected {_fqn(_PANDAS.DataFrame)}, ' +
            f'not an object of type {_fqn(type(df))}.')


cdef ssize_t _dataframe_resolve_table_name(
        qdb_pystr_buf* b,
        object df,
        list pandas_cols,
        col_t_arr* cols,
        object table_name,
        object table_name_col,
        size_t col_count,
        line_sender_table_name* name_out) except -2:
    """
    Resolve the table name string or column.

    Returns -1 if the table name is a string, otherwise the column index.
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
                raise ValueError(
                    f'Bad argument `table_name`: {ie}')
        else:
            raise TypeError('Bad argument `table_name`: Must be str.')
    elif table_name_col is not None:
        if isinstance(table_name_col, str):
            _dataframe_get_loc(df, table_name_col, 'table_name_col', &col_index)
        elif isinstance(table_name_col, int):
            _bind_col_index(
                'table_name_col', table_name_col, col_count, &col_index)
        else:
            raise TypeError(
                'Bad argument `table_name_col`: ' +
                'must be a column name (str) or index (int).')
        pandas_col = pandas_cols[col_index]
        col = &cols.d[col_index]
        _dataframe_check_column_is_str(
            'Bad argument `table_name_col`: ',
            pandas_col,
            col.setup.source)
        col.setup.meta_target = meta_target_t.meta_target_table
        name_out.len = 0
        name_out.buf = NULL
        return col_index
    elif df.index.name:
        if not isinstance(df.index.name, str):
            raise TypeError(
                'Bad dataframe index name as table name: Expected str, ' +
                f'not an object of type {_fqn(type(df.index.name))}.')

        # If the index has a name, use that as the table name.
        try:
            str_to_table_name_copy(b, <PyObject*>df.index.name, name_out)
            return -1  # Magic value for "no column index".
        except IngressError as ie:
            raise ValueError(
                f'Bad dataframe index name as table name: {ie}')
    else:
        raise ValueError(
            'Must specify at least one of `table_name` or `table_name_col`, ' +
            'or set the dataframe index name (df.index.name = \'tbl_name\').')


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


cdef void_int _dataframe_check_column_is_str(
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
    """Python object representing a column to parse .dataframe() arguments."""
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

cdef void_int _dataframe_resolve_symbols(
        object df,
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
            if col.setup.meta_target == meta_target_t.meta_target_field:
                if isinstance(pandas_col.dtype, _PANDAS.CategoricalDtype):
                    col.setup.meta_target = meta_target_t.meta_target_symbol
    elif symbols is False:
        pass
    elif symbols is True:
        for col_index in range(cols.size):
            col = &cols.d[col_index]
            if col.setup.source in _STR_SOURCES:
                pandas_col = pandas_cols[col_index]
                if col.setup.meta_target == meta_target_t.meta_target_field:
                    col.setup.meta_target = meta_target_t.meta_target_symbol
    else:
        if not isinstance(symbols, (tuple, list)):
            raise TypeError(
                f'Bad argument `symbols`: Must be a bool or a tuple or list '+
                'of column names (str) or indices (int).')
        for symbol in symbols:
            if isinstance(symbol, str):
                _dataframe_get_loc(df, symbol, 'symbols', &col_index)
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
                    f'({df.columns[at_col]!r}) as a symbol column.')
            pandas_col = pandas_cols[col_index]
            col = &cols.d[col_index]
            _dataframe_check_column_is_str(
                'Bad argument `symbols`: ',
                pandas_col,
                col.setup.source)
            col.setup.meta_target = meta_target_t.meta_target_symbol


cdef void_int _dataframe_get_loc(
        object df, str col_name, str arg_name,
        size_t* col_index_out) except -1:
    """
    Return the column index for `col_name`.
    """
    try:
        col_index_out[0] = df.columns.get_loc(col_name)
    except KeyError:
        raise KeyError(
            f'Bad argument `{arg_name}`: ' +
            f'Column {col_name!r} not found in the dataframe.')


# The values -2 and -1 are safe to use as a sentinel because the TimestampNanos
# type already validates that the value is >= 0.
cdef int64_t _AT_IS_SERVER_NOW = -2
cdef int64_t _AT_IS_SET_BY_COLUMN = -1


cdef str _SUPPORTED_DATETIMES = 'datetime64[ns] or datetime64[ns, tz]'


cdef object _dataframe_is_supported_datetime(object dtype):
    if (isinstance(dtype, _NUMPY_DATETIME64_NS) and
            (str(dtype) == 'datetime64[ns]')):
        return True
    if isinstance(dtype, _PANDAS.DatetimeTZDtype):
        return dtype.unit == 'ns'
    return False


cdef ssize_t _dataframe_resolve_at(
        object df,
        col_t_arr* cols,
        object at,
        size_t col_count,
        int64_t* at_value_out) except -2:
    cdef size_t col_index
    cdef object dtype
    cdef PandasCol pandas_col
    cdef TimestampNanos at_nanos
    if at is None:
        at_value_out[0] = _AT_IS_SERVER_NOW
        return -1
    elif isinstance(at, TimestampNanos):
        at_nanos = at
        at_value_out[0] = at_nanos._value
        return -1
    elif isinstance(at, cp_datetime):
        if at.timestamp() < 0:
            raise ValueError(
                'Bad argument `at`: Cannot use a datetime before the ' +
                'Unix epoch (1970-01-01 00:00:00).')
        at_value_out[0] = datetime_to_nanos(at)
        return -1
    elif isinstance(at, str):
        _dataframe_get_loc(df, at, 'at', &col_index)
    elif isinstance(at, int):
        _bind_col_index('at', at, col_count, &col_index)
    else:
        raise TypeError(
            f'Bad argument `at`: Unsupported type {_fqn(type(at))}. ' +
            'Must be one of: None, TimestampNanos, datetime, ' +
            'int (column index), str (colum name)')
    dtype = df.dtypes.iloc[col_index]
    if _dataframe_is_supported_datetime(dtype):
        at_value_out[0] = _AT_IS_SET_BY_COLUMN
        col = &cols.d[col_index]
        col.setup.meta_target = meta_target_t.meta_target_at
        return col_index
    else:
        raise TypeError(
            f'Bad argument `at`: Bad dtype `{dtype}` ' +
            f'for the {at!r} column: Must be a {_SUPPORTED_DATETIMES} column.')


cdef void_int _dataframe_alloc_chunks(
        size_t n_chunks, col_t* col) except -1:
    col.setup.chunks.n_chunks = n_chunks
    col.setup.chunks.chunks = <ArrowArray*>calloc(
        col.setup.chunks.n_chunks + 1,  # See `_dataframe_col_advance` on why +1.
        sizeof(ArrowArray))
    if col.setup.chunks.chunks == NULL:
        raise MemoryError()


cdef void _dataframe_free_mapped_arrow(ArrowArray* arr) noexcept nogil:
    free(arr.buffers)
    arr.buffers = NULL
    arr.release = NULL


cdef void_int _dataframe_series_as_pybuf(
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
        get_buf_ret = PyObject_GetBuffer(nparr, &col.setup.pybuf, PyBUF_SIMPLE)
    except ValueError as ve:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Bad column {pandas_col.name!r}: {ve}') from ve
    except BufferError as be:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Bad column {pandas_col.name!r}: Expected a buffer, got ' +
            f'{pandas_col.series!r} ({_fqn(type(pandas_col.series))})') from be
    _dataframe_alloc_chunks(1, col)
    mapped = &col.setup.chunks.chunks[0]

    # Total number of elements.
    mapped.length = (
        <int64_t>col.setup.pybuf.len // <int64_t>col.setup.pybuf.itemsize)
    mapped.null_count = 0
    mapped.offset = 0
    mapped.n_buffers = 2
    mapped.n_children = 0
    mapped.buffers = <const void**>calloc(2, sizeof(const void*))
    mapped.buffers[0] = NULL
    mapped.buffers[1] = <const void*>col.setup.pybuf.buf
    mapped.children = NULL
    mapped.dictionary = NULL
    mapped.release = _dataframe_free_mapped_arrow  # to cleanup allocated array.

cdef void_int _dataframe_series_as_arrow(
        PandasCol pandas_col,
        col_t* col) except -1:
    cdef object array
    cdef list chunks
    cdef size_t n_chunks
    cdef size_t chunk_index
    array = _PYARROW.Array.from_pandas(pandas_col.series)
    if isinstance(array, _PYARROW.ChunkedArray):
        chunks = array.chunks
    else:
        chunks = [array]

    n_chunks = len(chunks)
    _dataframe_alloc_chunks(n_chunks, col)

    for chunk_index in range(n_chunks):
        array = chunks[chunk_index]
        if chunk_index == 0:
            chunks[chunk_index]._export_to_c(
                <uintptr_t>&col.setup.chunks.chunks[chunk_index],
                <uintptr_t>&col.setup.arrow_schema)
        else:
            chunks[chunk_index]._export_to_c(
                <uintptr_t>&col.setup.chunks.chunks[chunk_index])
    

cdef const char* _ARROW_FMT_INT8 = "c"
cdef const char* _ARROW_FMT_INT16 = "s"
cdef const char* _ARROW_FMT_INT32 = "i"
cdef const char* _ARROW_FMT_UTF8_STRING = 'u'
cdef const char* _ARROW_FMT_LRG_UTF8_STRING = 'U'


cdef void_int _dataframe_category_series_as_arrow(
        PandasCol pandas_col, col_t* col) except -1:
    cdef const char* format
    _dataframe_series_as_arrow(pandas_col, col)
    format = col.setup.arrow_schema.format
    if strncmp(format, _ARROW_FMT_INT8, 1) == 0:
        col.setup.source = col_source_t.col_source_str_i8_cat
    elif strncmp(format, _ARROW_FMT_INT16, 1) == 0:
        col.setup.source = col_source_t.col_source_str_i16_cat
    elif strncmp(format, _ARROW_FMT_INT32, 1) == 0:
        col.setup.source = col_source_t.col_source_str_i32_cat
    else:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Bad column {pandas_col.name!r}: ' +
            'Unsupported arrow category index type. ' +
            f'Got {(<bytes>format).decode("utf-8")!r}.')

    format = col.setup.arrow_schema.dictionary.format
    if (strncmp(format, _ARROW_FMT_UTF8_STRING, 1) != 0):
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Bad column {pandas_col.name!r}: ' +
            'Expected a category of strings, ' +
            f'got a category of {pandas_col.series.dtype.categories.dtype}.')


cdef inline bint _dataframe_is_float_nan(PyObject* obj) noexcept:
    return PyFloat_CheckExact(obj) and isnan(PyFloat_AS_DOUBLE(obj))


cdef inline bint _dataframe_is_null_pyobj(PyObject* obj) noexcept:
    return (
        (obj == Py_None) or
        (obj == <PyObject*>_PANDAS_NA) or
        _dataframe_is_float_nan(obj))

# noinspection PyUnreachableCode
cdef void_int _dataframe_series_sniff_pyobj(
        PandasCol pandas_col, col_t* col) except -1:
    """
    Deduct the type of the object column.
    Object columns can contain pretty much anything, but they usually don't.
    We make an educated guess by finding the first non-null value in the column.
    """
    # To access elements.
    cdef size_t el_index
    cdef size_t n_elements = len(pandas_col.series)
    cdef PyObject** obj_arr
    cdef PyObject* obj

    # To access elements which are themselves arrays.
    cdef PyArrayObject* arr
    cdef npy_int arr_type
    cdef cnp.dtype arr_descr  # A cython defn for `PyArray_Descr*`
    cdef str arr_type_name

    _dataframe_series_as_pybuf(pandas_col, col)
    obj_arr = <PyObject**>(col.setup.pybuf.buf)
    for el_index in range(n_elements):
        obj = obj_arr[el_index]
        if not _dataframe_is_null_pyobj(obj):
            if PyBool_Check(obj):
                col.setup.source = col_source_t.col_source_bool_pyobj
            elif PyLong_CheckExact(obj):
                col.setup.source = col_source_t.col_source_int_pyobj
            elif PyFloat_CheckExact(obj):
                col.setup.source = col_source_t.col_source_float_pyobj
            elif PyUnicode_CheckExact(obj):
                col.setup.source = col_source_t.col_source_str_pyobj
            elif PyArray_CheckExact(obj):
                arr = <PyArrayObject*>obj
                arr_type = PyArray_TYPE(arr)
                if arr_type == NPY_DOUBLE:
                    col.setup.source = col_source_t.col_source_arr_f64_numpyobj
                else:
                    arr_type_name = '??unknown??'
                    arr_descr = cnp.PyArray_DescrFromType(arr_type)
                    if arr_descr is not None:
                        arr_type_name = arr_descr.name.decode('ascii')
                    raise IngressError(
                        IngressErrorCode.BadDataFrame,
                        f'Bad column {pandas_col.name!r}: ' +
                        'Unsupported object column containing a numpy array ' +
                        f'of an unsupported element type {arr_type_name}.')
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
    col.setup.source = col_source_t.col_source_nulls
    

cdef void_int _dataframe_resolve_source_and_buffers(
        PandasCol pandas_col, col_t* col) except -1:
    cdef object dtype = pandas_col.dtype
    if isinstance(dtype, _NUMPY_BOOL):
        col.setup.source = col_source_t.col_source_bool_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _PANDAS.BooleanDtype):
        col.setup.source = col_source_t.col_source_bool_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT8):
        col.setup.source = col_source_t.col_source_u8_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT8):
        col.setup.source = col_source_t.col_source_i8_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT16):
        col.setup.source = col_source_t.col_source_u16_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT16):
        col.setup.source = col_source_t.col_source_i16_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT32):
        col.setup.source = col_source_t.col_source_u32_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT32):
        col.setup.source = col_source_t.col_source_i32_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_UINT64):
        col.setup.source = col_source_t.col_source_u64_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_INT64):
        col.setup.source = col_source_t.col_source_i64_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _PANDAS.UInt8Dtype):
        col.setup.source = col_source_t.col_source_u8_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.Int8Dtype):
        col.setup.source = col_source_t.col_source_i8_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.UInt16Dtype):
        col.setup.source = col_source_t.col_source_u16_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.Int16Dtype):
        col.setup.source = col_source_t.col_source_i16_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.UInt32Dtype):
        col.setup.source = col_source_t.col_source_u32_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.Int32Dtype):
        col.setup.source = col_source_t.col_source_i32_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.UInt64Dtype):
        col.setup.source = col_source_t.col_source_u64_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.Int64Dtype):
        col.setup.source = col_source_t.col_source_i64_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _NUMPY_FLOAT32):
        col.setup.source = col_source_t.col_source_f32_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _NUMPY_FLOAT64):
        col.setup.source = col_source_t.col_source_f64_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif isinstance(dtype, _PANDAS.Float32Dtype):
        col.setup.source = col_source_t.col_source_f32_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.Float64Dtype):
        col.setup.source = col_source_t.col_source_f64_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _PANDAS.StringDtype):
        if dtype.storage == 'pyarrow':
            _dataframe_series_as_arrow(pandas_col, col)
            if strncmp(col.setup.arrow_schema.format, _ARROW_FMT_UTF8_STRING, 1) == 0:
                col.setup.source = col_source_t.col_source_str_utf8_arrow
            elif strncmp(col.setup.arrow_schema.format, _ARROW_FMT_LRG_UTF8_STRING, 1) == 0:
                col.setup.source = col_source_t.col_source_str_lrg_utf8_arrow
            else:
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    f'Unknown string dtype storage: {dtype.storage} ' +
                    f'for column {pandas_col.name} of dtype {dtype}. ' +
                    f'Format specifier: ' + repr(bytes(col.setup.arrow_schema.format).decode('latin-1')))
        elif dtype.storage == 'python':
            col.setup.source = col_source_t.col_source_str_pyobj
            _dataframe_series_as_pybuf(pandas_col, col)
        else:
            raise IngressError(
                IngressErrorCode.BadDataFrame,
                f'Unknown string dtype storage: f{dtype.storage} ' +
                f'for column {pandas_col.name} of dtype {dtype}.')
    elif isinstance(dtype, _PANDAS.CategoricalDtype):
        _dataframe_category_series_as_arrow(pandas_col, col)
    elif (isinstance(dtype, _NUMPY_DATETIME64_NS) and
            _dataframe_is_supported_datetime(dtype)):
        col.setup.source = col_source_t.col_source_dt64ns_numpy
        _dataframe_series_as_pybuf(pandas_col, col)
    elif (isinstance(dtype, _PANDAS.DatetimeTZDtype) and
            _dataframe_is_supported_datetime(dtype)):
        col.setup.source = col_source_t.col_source_dt64ns_tz_arrow
        _dataframe_series_as_arrow(pandas_col, col)
    elif isinstance(dtype, _NUMPY_OBJECT):
        _dataframe_series_sniff_pyobj(pandas_col, col)
    else:
        raise IngressError(
            IngressErrorCode.BadDataFrame,
            f'Unsupported dtype {dtype} for column {pandas_col.name!r}. ' +
            'Raise an issue if you think it should be supported: ' +
            'https://github.com/questdb/py-questdb-client/issues.')


cdef void_int _dataframe_resolve_target(
        PandasCol pandas_col, col_t* col) except -1:
    cdef col_target_t target
    cdef set target_sources
    if col.setup.meta_target in _DIRECT_META_TARGETS:
        col.setup.target = <col_target_t><int>col.setup.meta_target
        return 0
    for target in _FIELD_TARGETS:
        target_sources = _TARGET_TO_SOURCES[target]
        if col.setup.source in target_sources:
            col.setup.target = target
            return 0
    raise IngressError(
        IngressErrorCode.BadDataFrame,
        f'Could not map column source type (code {col.setup.source} for ' +
        f'column {pandas_col.name!r} ' +
        f' ({pandas_col.dtype}) to any ILP type.')


cdef void _dataframe_init_cursor(col_t* col) noexcept nogil:
    col.cursor.chunk = col.setup.chunks.chunks
    col.cursor.chunk_index = 0
    col.cursor.offset = col.cursor.chunk.offset


cdef void_int _dataframe_resolve_cols(
        qdb_pystr_buf* b,
        list pandas_cols,
        col_t_arr* cols,
        bint* any_cols_need_gil_out) except -1:
    cdef size_t index
    cdef size_t len_dataframe_cols = len(pandas_cols)
    cdef PandasCol pandas_col
    cdef col_t* col
    any_cols_need_gil_out[0] = False
    for index in range(len_dataframe_cols):
        pandas_col = pandas_cols[index]
        col = &cols.d[index]

        # The target is resolved in stages:
        # * We first assign all column `.meta_target`s to be fields.
        # * Then, depending on argument parsing some/none of the columns
        #   obtain a meta-target of "table", "symbol" or "at".
        # * Finally, based on the source, any remaining "meta_target_field"
        #   columns are converted to the appropriate target.
        #   See: _dataframe_resolve_col_targets_and_dc(..).
        col.setup.meta_target = meta_target_t.meta_target_field

        # We will sort columns later. The index will be used to achieve a stable
        # sort among columns with the same `.meta_target`.
        col.setup.orig_index = index

        _dataframe_resolve_source_and_buffers(pandas_col, col)
        _dataframe_init_cursor(col)
        if col_source_needs_gil(col.setup.source):
            any_cols_need_gil_out[0] = True


cdef void_int _dataframe_resolve_cols_target_name_and_dc(
        qdb_pystr_buf* b,
        list pandas_cols,
        col_t_arr* cols) except -1:
    cdef size_t index
    cdef col_t* col
    cdef PandasCol pandas_col
    for index in range(cols.size):
        col = &cols.d[index]
        pandas_col = pandas_cols[index]
        _dataframe_resolve_target(pandas_col, col)
        if col.setup.source not in _TARGET_TO_SOURCES[col.setup.target]:
            raise ValueError(
                f'Bad value: Column {pandas_col.name!r} ' +
                f'({pandas_col.dtype}) is not ' +
                f'supported as a {_TARGET_NAMES[col.setup.target]} column.')
        col.dispatch_code = <col_dispatch_code_t>(
            <int>col.setup.source + <int>col.setup.target)

        # Since we don't need to send the column names for 'table' and
        # 'at' columns, we don't need to validate and encode them as
        # column names. This allows unsupported names for these columns.
        if ((col.setup.meta_target != meta_target_t.meta_target_table) and
                (col.setup.meta_target != meta_target_t.meta_target_at)):
            str_to_column_name_copy(b, pandas_col.name, &col.name)


cdef int _dataframe_compare_cols(const void* lhs, const void* rhs) noexcept nogil:
    cdef col_t* lhs_col = <col_t*>lhs
    cdef col_t* rhs_col = <col_t*>rhs
    cdef int source_diff = lhs_col.setup.meta_target - rhs_col.setup.meta_target
    if source_diff != 0:
        return source_diff
    return <int>lhs_col.setup.orig_index - <int>rhs_col.setup.orig_index

# noinspection PyUnreachableCode
cdef void_int _dataframe_resolve_args(
        object df,
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
        PandasCol(name, df.dtypes.iloc[index], series)
        for index, (name, series) in enumerate(df.items())]
    _dataframe_resolve_cols(b, pandas_cols, cols, any_cols_need_gil_out)
    name_col = _dataframe_resolve_table_name(
        b,
        df,
        pandas_cols,
        cols,
        table_name,
        table_name_col,
        col_count,
        c_table_name_out)
    at_col = _dataframe_resolve_at(df, cols, at, col_count, at_value_out)
    _dataframe_resolve_symbols(df, pandas_cols, cols, name_col, at_col, symbols)
    _dataframe_resolve_cols_target_name_and_dc(b, pandas_cols, cols)
    qsort(cols.d, col_count, sizeof(col_t), _dataframe_compare_cols)


cdef inline bint _dataframe_arrow_get_bool(col_cursor_t* cursor) noexcept nogil:
    return (
        (<uint8_t*>cursor.chunk.buffers[1])[cursor.offset // 8] &
        (1 << (cursor.offset % 8)))


cdef inline bint _dataframe_arrow_is_valid(col_cursor_t* cursor) noexcept nogil:
    """Check if the value is set according to the validity bitmap."""
    return (
        cursor.chunk.null_count == 0 or
        (
            (<uint8_t*>cursor.chunk.buffers[0])[cursor.offset // 8] &
            (1 << (cursor.offset % 8))))


cdef inline void _dataframe_arrow_get_cat_value(
        col_cursor_t* cursor, 
        size_t key,
        size_t* len_out,
        const char** buf_out) noexcept nogil:
    cdef int32_t* value_index_access
    cdef int32_t value_begin
    cdef uint8_t* value_char_access
    value_index_access = <int32_t*>cursor.chunk.dictionary.buffers[1]
    value_begin = value_index_access[key]
    len_out[0] = value_index_access[key + 1] - value_begin
    value_char_access = <uint8_t*>cursor.chunk.dictionary.buffers[2]
    buf_out[0] = <const char*>&value_char_access[value_begin]


cdef inline bint _dataframe_arrow_get_cat_i8(
        col_cursor_t* cursor, size_t* len_out, const char** buf_out) noexcept nogil:
    cdef bint valid = _dataframe_arrow_is_valid(cursor)
    cdef int8_t* key_access
    cdef int8_t key
    if valid:
        key_access = <int8_t*>cursor.chunk.buffers[1]
        key = key_access[cursor.offset]
        _dataframe_arrow_get_cat_value(cursor, <size_t>key, len_out, buf_out)
    return valid


cdef inline bint _dataframe_arrow_get_cat_i16(
        col_cursor_t* cursor, size_t* len_out, const char** buf_out) noexcept nogil:
    cdef bint valid = _dataframe_arrow_is_valid(cursor)
    cdef int16_t* key_access
    cdef int16_t key
    if valid:
        key_access = <int16_t*>cursor.chunk.buffers[1]
        key = key_access[cursor.offset]
        _dataframe_arrow_get_cat_value(cursor, <size_t>key, len_out, buf_out)
    return valid


cdef inline bint _dataframe_arrow_get_cat_i32(
        col_cursor_t* cursor, size_t* len_out, const char** buf_out) noexcept nogil:
    cdef bint valid = _dataframe_arrow_is_valid(cursor)
    cdef int32_t* key_access
    cdef int32_t key
    if valid:
        key_access = <int32_t*>cursor.chunk.buffers[1]
        key = key_access[cursor.offset]
        _dataframe_arrow_get_cat_value(cursor, <size_t>key, len_out, buf_out)
    return valid


cdef inline bint _dataframe_arrow_str_utf8(
        col_cursor_t* cursor,
        size_t* len_out,
        const char** buf_out) noexcept nogil:
    cdef int32_t* index_access
    cdef uint8_t* char_access
    cdef int32_t begin
    cdef bint valid = _dataframe_arrow_is_valid(cursor)
    if valid:
        index_access = <int32_t*>cursor.chunk.buffers[1]
        char_access = <uint8_t*>cursor.chunk.buffers[2]
        begin = index_access[cursor.offset]
        len_out[0] = index_access[cursor.offset + 1] - begin
        buf_out[0] = <const char*>&char_access[begin]
    return valid

cdef inline bint _dataframe_arrow_str_utf8_lrg(
        col_cursor_t* cursor,
        size_t* len_out,
        const char** buf_out) noexcept nogil:
    cdef int64_t* index_access
    cdef uint8_t* char_access
    cdef int64_t begin
    cdef bint valid = _dataframe_arrow_is_valid(cursor)
    if valid:
        index_access = <int64_t*>cursor.chunk.buffers[1]
        char_access = <uint8_t*>cursor.chunk.buffers[2]
        begin = index_access[cursor.offset]
        len_out[0] = index_access[cursor.offset + 1] - begin
        buf_out[0] = <const char*>&char_access[begin]
    return valid


cdef inline void_int _dataframe_cell_str_pyobj_to_utf8(
        qdb_pystr_buf* b,
        col_cursor_t* cursor,
        bint* valid_out,
        line_sender_utf8* utf8_out) except -1: 
    cdef PyObject** access = <PyObject**>cursor.chunk.buffers[1]
    cdef PyObject* cell = access[cursor.offset]
    if PyUnicode_CheckExact(cell):
        str_to_utf8(b, cell, utf8_out)
        valid_out[0] = True
    elif _dataframe_is_null_pyobj(cell):
        valid_out[0] = False
    else:
        raise ValueError(
            'Expected a string, ' +
            f'got an object of type {_fqn(type(<object>cell))}.')


cdef void_int _dataframe_serialize_cell_table__str_pyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef line_sender_table_name c_table_name
    if not PyUnicode_CheckExact(cell):
        if _dataframe_is_null_pyobj(cell):
            raise ValueError('Expected a table name, got a null value')
        else:
            raise ValueError(
                'Expected a table name (str object), ' +
                f'got an object of type {_fqn(type(<object>cell))}.')
    str_to_table_name(b, cell, &c_table_name)
    if not line_sender_buffer_table(ls_buf, c_table_name, &err):
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_table__str_utf8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* buf
    cdef line_sender_table_name c_table_name
    if _dataframe_arrow_str_utf8(&col.cursor, &c_len, &buf):
        if not line_sender_table_name_init(&c_table_name, c_len, buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(ls_buf, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')

cdef void_int _dataframe_serialize_cell_table__str_lrg_utf8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* buf
    cdef line_sender_table_name c_table_name
    if _dataframe_arrow_str_utf8_lrg(&col.cursor, &c_len, &buf):
        if not line_sender_table_name_init(&c_table_name, c_len, buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(ls_buf, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _dataframe_serialize_cell_table__str_i8_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* c_buf
    cdef line_sender_table_name c_table_name
    if _dataframe_arrow_get_cat_i8(&col.cursor, &c_len, &c_buf):
        if not line_sender_table_name_init(&c_table_name, c_len, c_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(ls_buf, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _dataframe_serialize_cell_table__str_i16_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* c_buf
    cdef line_sender_table_name c_table_name
    if _dataframe_arrow_get_cat_i16(&col.cursor, &c_len, &c_buf):
        if not line_sender_table_name_init(&c_table_name, c_len, c_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(ls_buf, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _dataframe_serialize_cell_table__str_i32_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef size_t c_len
    cdef const char* c_buf
    cdef line_sender_table_name c_table_name
    if _dataframe_arrow_get_cat_i32(&col.cursor, &c_len, &c_buf):
        if not line_sender_table_name_init(&c_table_name, c_len, c_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
        if not line_sender_buffer_table(ls_buf, c_table_name, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Table name cannot be null')


cdef void_int _dataframe_serialize_cell_symbol__str_pyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = False
    cdef line_sender_utf8 utf8
    _dataframe_cell_str_pyobj_to_utf8(b, &col.cursor, &valid, &utf8)
    if valid and not line_sender_buffer_symbol(ls_buf, col.name, utf8, &err):
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_symbol__str_utf8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_str_utf8(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)

cdef void_int _dataframe_serialize_cell_symbol__str_lrg_utf8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_str_utf8_lrg(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_symbol__str_i8_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_get_cat_i8(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_symbol__str_i16_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_get_cat_i16(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_symbol__str_i32_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_get_cat_i32(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_symbol(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_bool__bool_pyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    if PyBool_Check(cell):
        if not line_sender_buffer_column_bool(
                ls_buf, col.name, cell == Py_True, &err):
            raise c_err_to_py(err)
    elif _dataframe_is_null_pyobj(cell):
        raise ValueError('Cannot insert null values into a boolean column.')
    else:
        raise ValueError(
            'Expected an object of type bool, got a ' +
            _fqn(type(<object>cell)) + '.')


cdef void_int _dataframe_serialize_cell_column_bool__bool_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint8_t* access = <uint8_t*>col.cursor.chunk.buffers[1]
    cdef uint8_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_bool(ls_buf, col.name, not not cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_bool__bool_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef bint value
    if valid:
        value = _dataframe_arrow_get_bool(&col.cursor)
        if not line_sender_buffer_column_bool(ls_buf, col.name, value, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        _ensure_has_gil(gs)
        raise ValueError('Cannot insert null values into a boolean column.')


cdef void_int _dataframe_serialize_cell_column_i64__int_pyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef int64_t value
    if PyLong_CheckExact(cell):
        value = PyLong_AsLongLong(cell)
        if not line_sender_buffer_column_i64(ls_buf, col.name, value, &err):
            raise c_err_to_py(err)
    elif _dataframe_is_null_pyobj(cell):
        pass
    else:
        raise ValueError(
            'Expected an object of type int, got an object of type ' +
            _fqn(type(<object>cell)) + '.')


cdef void_int _dataframe_serialize_cell_column_i64__u8_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint8_t* access = <uint8_t*>col.cursor.chunk.buffers[1]
    cdef uint8_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i8_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int8_t* access = <int8_t*>col.cursor.chunk.buffers[1]
    cdef int8_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u16_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint16_t* access = <uint16_t*>col.cursor.chunk.buffers[1]
    cdef uint16_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i16_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int16_t* access = <int16_t*>col.cursor.chunk.buffers[1]
    cdef int16_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u32_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint32_t* access = <uint32_t*>col.cursor.chunk.buffers[1]
    cdef uint32_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i32_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int32_t* access = <int32_t*>col.cursor.chunk.buffers[1]
    cdef int32_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u64_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef uint64_t* access = <uint64_t*>col.cursor.chunk.buffers[1]
    cdef uint64_t cell = access[col.cursor.offset]
    if cell > <uint64_t>INT64_MAX:
        _ensure_has_gil(gs)
        raise OverflowError('uint64 value too large for int64 column type.')
    if not line_sender_buffer_column_i64(ls_buf, col.name, <int64_t>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i64_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int64_t* access = <int64_t*>col.cursor.chunk.buffers[1]
    cdef int64_t cell = access[col.cursor.offset]
    if not line_sender_buffer_column_i64(ls_buf, col.name, cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef uint8_t* access
    if valid:
        access = <uint8_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef int8_t* access
    if valid:
        access = <int8_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u16_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef uint16_t* access
    if valid:
        access = <uint16_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i16_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef int16_t* access
    if valid:
        access = <int16_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u32_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef uint32_t* access
    if valid:
        access = <uint32_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i32_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef int32_t* access
    if valid:
        access = <int32_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__u64_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef uint64_t* access
    cdef uint64_t cell
    if valid:
        access = <uint64_t*>col.cursor.chunk.buffers[1]
        cell = access[col.cursor.offset]
        if cell > <uint64_t>INT64_MAX:
            _ensure_has_gil(gs)
            raise OverflowError('uint64 value too large for int64 column type.')
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                <int64_t>cell,
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_i64__i64_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef int64_t* access
    if valid:
        access = <int64_t*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_i64(
                ls_buf,
                col.name,
                access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_f64__float_pyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef double value
    if PyFloat_CheckExact(cell):
        value = PyFloat_AS_DOUBLE(cell)
        if not line_sender_buffer_column_f64(ls_buf, col.name, value, &err):
            raise c_err_to_py(err)
    elif _dataframe_is_null_pyobj(cell):
        pass
    else:
        raise ValueError(
            'Expected an object of type float, got an object of type ' +
            _fqn(type(<object>cell)) + '.')


cdef void_int _dataframe_serialize_cell_column_f64__f32_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    # Note: This is the C `float` type, not the Python `float` type.
    cdef float* access = <float*>col.cursor.chunk.buffers[1]
    cdef float cell = access[col.cursor.offset]
    if not line_sender_buffer_column_f64(ls_buf, col.name, <double>cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_f64__f64_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef double* access = <double*>col.cursor.chunk.buffers[1]
    cdef double cell = access[col.cursor.offset]
    if not line_sender_buffer_column_f64(ls_buf, col.name, cell, &err):
        _ensure_has_gil(gs)
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_f64__f32_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef float* access
    if valid:
        access = <float*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_f64(
                ls_buf,
                col.name,
                <double>access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_f64__f64_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef double* access
    if valid:
        access = <double*>col.cursor.chunk.buffers[1]
        if not line_sender_buffer_column_f64(
                ls_buf,
                col.name,
                access[col.cursor.offset],
                &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_str__str_pyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = False
    cdef line_sender_utf8 utf8
    _dataframe_cell_str_pyobj_to_utf8(b, &col.cursor,  &valid, &utf8)
    if valid and not line_sender_buffer_column_str(
            ls_buf, col.name, utf8, &err):
        raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_str__str_utf8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_str_utf8(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)

cdef void_int _dataframe_serialize_cell_column_str__str_lrg_utf8_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_str_utf8_lrg(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_str__str_i8_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_get_cat_i8(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_str__str_i16_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_get_cat_i16(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_str__str_i32_cat(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef line_sender_utf8 utf8
    if _dataframe_arrow_get_cat_i32(&col.cursor, &utf8.len, &utf8.buf):
        if not line_sender_buffer_column_str(ls_buf, col.name, utf8, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_column_ts__dt64ns_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int64_t* access = <int64_t*>col.cursor.chunk.buffers[1]
    cdef int64_t cell = access[col.cursor.offset]
    if cell != _NAT:
        if not line_sender_buffer_column_ts_nanos(ls_buf, col.name, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)

cdef void_int _dataframe_serialize_cell_column_arr_f64__arr_f64_numpyobj(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col) except -1:
    cdef PyObject** access = <PyObject**>col.cursor.chunk.buffers[1]
    cdef PyObject* cell = access[col.cursor.offset]
    cdef PyArrayObject* arr = <PyArrayObject*> cell
    cdef npy_int arr_type = PyArray_TYPE(arr)
    cdef cnp.dtype arr_descr
    if arr_type != NPY_DOUBLE:
        arr_descr = cnp.PyArray_DescrFromType(arr_type)
        raise IngressError(
            IngressErrorCode.ArrayWriteToBufferError,
            f'Only float64 numpy arrays are supported, got dtype: {arr_descr}')
    cdef:
        size_t rank = PyArray_NDIM(arr)
        const double* data_ptr = <const double *> PyArray_DATA(arr)
        line_sender_error * err = NULL

    if PyArray_FLAGS(arr) & NPY_ARRAY_C_CONTIGUOUS != 0:
        if not line_sender_buffer_column_f64_arr_c_major(
                ls_buf,
                col.name,
                rank,
                <const size_t *> PyArray_DIMS(arr),
                data_ptr,
                PyArray_SIZE(arr),
                &err):
            raise c_err_to_py(err)
    else:
        if not line_sender_buffer_column_f64_arr_byte_strides(
                ls_buf,
                col.name,
                rank,
                <const size_t*> PyArray_DIMS(arr),
                <const ssize_t*> PyArray_STRIDES(arr), # N.B.: Strides expressed as byte jumps
                data_ptr,
                PyArray_SIZE(arr),
                &err):
            raise c_err_to_py(err)

cdef void_int _dataframe_serialize_cell_column_ts__dt64ns_tz_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef int64_t cell
    cdef int64_t* access
    if valid:
        access = <int64_t*>col.cursor.chunk.buffers[1]
        cell = access[col.cursor.offset]
        if not line_sender_buffer_column_ts_nanos(ls_buf, col.name, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_at_dt64ns_numpy(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef int64_t* access = <int64_t*>col.cursor.chunk.buffers[1]
    cdef int64_t cell = access[col.cursor.offset]
    if cell == _NAT:
        if not line_sender_buffer_at_now(ls_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        # Note: ls_buf will validate against negative numbers.
        if not line_sender_buffer_at_nanos(ls_buf, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell_at_dt64ns_tz_arrow(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef line_sender_error* err = NULL
    cdef bint valid = _dataframe_arrow_is_valid(&col.cursor)
    cdef int64_t* access
    cdef int64_t cell
    if valid:
        access = <int64_t*>col.cursor.chunk.buffers[1]
        cell = access[col.cursor.offset]
        # Note: ls_buf will validate against negative numbers.
        if not line_sender_buffer_at_nanos(ls_buf, cell, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)
    else:
        if not line_sender_buffer_at_now(ls_buf, &err):
            _ensure_has_gil(gs)
            raise c_err_to_py(err)


cdef void_int _dataframe_serialize_cell(
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        col_t* col,
        PyThreadState** gs) except -1:
    cdef col_dispatch_code_t dc = col.dispatch_code
    # Note!: Code below will generate a `switch` statement.
    # Ensure this happens! Don't break the `dc == ...` pattern.
    if dc == col_dispatch_code_t.col_dispatch_code_skip_nulls:
        pass  # We skip a null column. Nothing to do.
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_pyobj:
        _dataframe_serialize_cell_table__str_pyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_utf8_arrow:
        _dataframe_serialize_cell_table__str_utf8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_lrg_utf8_arrow:
        _dataframe_serialize_cell_table__str_lrg_utf8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i8_cat:
        _dataframe_serialize_cell_table__str_i8_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i16_cat:
        _dataframe_serialize_cell_table__str_i16_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_table__str_i32_cat:
        _dataframe_serialize_cell_table__str_i32_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_pyobj:
        _dataframe_serialize_cell_symbol__str_pyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_utf8_arrow:
        _dataframe_serialize_cell_symbol__str_utf8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_lrg_utf8_arrow:
        _dataframe_serialize_cell_symbol__str_lrg_utf8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i8_cat:
        _dataframe_serialize_cell_symbol__str_i8_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i16_cat:
        _dataframe_serialize_cell_symbol__str_i16_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_symbol__str_i32_cat:
        _dataframe_serialize_cell_symbol__str_i32_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_pyobj:
        _dataframe_serialize_cell_column_bool__bool_pyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_numpy:
        _dataframe_serialize_cell_column_bool__bool_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_bool__bool_arrow:
        _dataframe_serialize_cell_column_bool__bool_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__int_pyobj:
        _dataframe_serialize_cell_column_i64__int_pyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u8_numpy:
        _dataframe_serialize_cell_column_i64__u8_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i8_numpy:
        _dataframe_serialize_cell_column_i64__i8_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u16_numpy:
        _dataframe_serialize_cell_column_i64__u16_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i16_numpy:
        _dataframe_serialize_cell_column_i64__i16_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u32_numpy:
        _dataframe_serialize_cell_column_i64__u32_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i32_numpy:
        _dataframe_serialize_cell_column_i64__i32_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u64_numpy:
        _dataframe_serialize_cell_column_i64__u64_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i64_numpy:
        _dataframe_serialize_cell_column_i64__i64_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u8_arrow:
        _dataframe_serialize_cell_column_i64__u8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i8_arrow:
        _dataframe_serialize_cell_column_i64__i8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u16_arrow:
        _dataframe_serialize_cell_column_i64__u16_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i16_arrow:
        _dataframe_serialize_cell_column_i64__i16_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u32_arrow:
        _dataframe_serialize_cell_column_i64__u32_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i32_arrow:
        _dataframe_serialize_cell_column_i64__i32_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__u64_arrow:
        _dataframe_serialize_cell_column_i64__u64_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_i64__i64_arrow:
        _dataframe_serialize_cell_column_i64__i64_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__float_pyobj:
        _dataframe_serialize_cell_column_f64__float_pyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f32_numpy:
        _dataframe_serialize_cell_column_f64__f32_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f64_numpy:
        _dataframe_serialize_cell_column_f64__f64_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f32_arrow:
        _dataframe_serialize_cell_column_f64__f32_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_f64__f64_arrow:
        _dataframe_serialize_cell_column_f64__f64_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_pyobj:
        _dataframe_serialize_cell_column_str__str_pyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_utf8_arrow:
        _dataframe_serialize_cell_column_str__str_utf8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_lrg_utf8_arrow:
        _dataframe_serialize_cell_column_str__str_lrg_utf8_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i8_cat:
        _dataframe_serialize_cell_column_str__str_i8_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i16_cat:
        _dataframe_serialize_cell_column_str__str_i16_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_str__str_i32_cat:
        _dataframe_serialize_cell_column_str__str_i32_cat(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_ts__dt64ns_numpy:
        _dataframe_serialize_cell_column_ts__dt64ns_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_arr_f64__arr_f64_numpyobj:
        _dataframe_serialize_cell_column_arr_f64__arr_f64_numpyobj(ls_buf, b, col)
    elif dc == col_dispatch_code_t.col_dispatch_code_column_ts__dt64ns_tz_arrow:
        _dataframe_serialize_cell_column_ts__dt64ns_tz_arrow(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_at__dt64ns_numpy:
        _dataframe_serialize_cell_at_dt64ns_numpy(ls_buf, b, col, gs)
    elif dc == col_dispatch_code_t.col_dispatch_code_at__dt64ns_tz_arrow:
        _dataframe_serialize_cell_at_dt64ns_tz_arrow(ls_buf, b, col, gs)
    else:
        _ensure_has_gil(gs)
        raise RuntimeError(f"Unknown column dispatch code: {dc}")
    # See earlier note about switch statement generation.
    # Don't add complex conditions above!


cdef void _dataframe_col_advance(col_t* col) noexcept nogil:
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


cdef void_int _dataframe_handle_auto_flush(
            const auto_flush_t* af,
            line_sender_buffer* ls_buf,
            PyThreadState** gs) except -1:
    cdef line_sender_error* flush_err
    cdef line_sender_error* marker_err
    cdef bint flush_ok
    cdef bint marker_ok
    if (af.sender == NULL) or (not should_auto_flush(&af.mode, ls_buf, af.last_flush_ms[0])):
        return 0

    # Always temporarily release GIL during a flush.
    had_gil = _ensure_doesnt_have_gil(gs)
    flush_ok = line_sender_flush(af.sender, ls_buf, &flush_err)
    if flush_ok:
        af.last_flush_ms[0] = line_sender_now_micros() // 1000
    else:
        # To avoid flush reattempt on Sender.__exit__.
        line_sender_buffer_clear(ls_buf)

    # Flushing will have cleared the marker: We need to set it again
    # We need this also on error due to our error handling logic which will
    # try to rewind the buffer on error and fail if the marker is unset.
    marker_ok = line_sender_buffer_set_marker(ls_buf, &marker_err)

    if had_gil or (not flush_ok) or (not marker_ok):
        _ensure_has_gil(gs)

    if not flush_ok:
        raise c_err_to_py_fmt(flush_err, _FLUSH_FMT)

    # The flush error takes precedence over the marker error.
    if not marker_ok:
        raise c_err_to_py(marker_err)


# Every how many cells to release and re-acquire the Python GIL.
#
# We've done some perf testing with some mixed column dtypes.
# On a modern CPU we're doing over 8 million pandas cells per second.
# By default, `sys.getswitchinterval()` is 0.005 seconds.
# To accomodate this, we'd need to release the GIL every 40,000 cells.
# This will be divided by the column count to get the row gil blip interval.
cdef size_t _CELL_GIL_BLIP_INTERVAL = 40000


cdef void_int _dataframe(
        auto_flush_t af,
        line_sender_buffer* ls_buf,
        qdb_pystr_buf* b,
        object df,
        object table_name,
        object table_name_col,
        object symbols,
        object at) except -1:
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
    cdef bint had_gil
    cdef bint was_serializing_cell = False

    _dataframe_may_import_deps()
    _dataframe_check_is_dataframe(df)
    row_count = len(df)
    col_count = len(df.columns)
    if (col_count == 0) or (row_count == 0):
        return 0  # Nothing to do.

    try:
        qdb_pystr_buf_clear(b)
        cols = col_t_arr_new(col_count)
        _dataframe_resolve_args(
            df,
            table_name,
            table_name_col,
            symbols,
            at if not isinstance(at, ServerTimestampType) else None,
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
        line_sender_buffer_clear_marker(ls_buf)

        # On error, undo all added lines.
        if not line_sender_buffer_set_marker(ls_buf, &err):
            raise c_err_to_py(err)

        row_gil_blip_interval = _CELL_GIL_BLIP_INTERVAL // col_count
        if row_gil_blip_interval < 400:  # ceiling reached at 100 columns
            row_gil_blip_interval = 400
        try:
            # Don't move this logic up! We need the GIL to execute a `try`.
            # Also we can't have any other `try` blocks between here and the
            # `finally` block.
            if not any_cols_need_gil:
                _ensure_doesnt_have_gil(&gs)

            for row_index in range(row_count):
                if (gs == NULL) and (row_index % row_gil_blip_interval == 0):
                    # Release and re-acquire the GIL every so often.
                    # This is to allow other python threads to run.
                    # If we hold the GIL for too long, we can starve other
                    # threads, for example timing out network activity.
                    _ensure_doesnt_have_gil(&gs)
                    _ensure_has_gil(&gs)

                qdb_pystr_buf_truncate(b, str_buf_marker)

                # Table-name from `table_name` arg in Python.
                if c_table_name.buf != NULL:
                    if not line_sender_buffer_table(ls_buf, c_table_name, &err):
                        _ensure_has_gil(&gs)
                        raise c_err_to_py(err)

                # Serialize columns cells.
                # Note: Columns are sorted: table name, symbols, fields, at.
                was_serializing_cell = True
                for col_index in range(col_count):
                    col = &cols.d[col_index]
                    _dataframe_serialize_cell(ls_buf, b, col, &gs)  # may raise
                    _dataframe_col_advance(col)
                was_serializing_cell = False

                # Fixed "at" value (not from a column).
                if at_value == _AT_IS_SERVER_NOW:
                    if not line_sender_buffer_at_now(ls_buf, &err):
                        _ensure_has_gil(&gs)
                        raise c_err_to_py(err)
                elif at_value >= 0:
                    if not line_sender_buffer_at_nanos(ls_buf, at_value, &err):
                        _ensure_has_gil(&gs)
                        raise c_err_to_py(err)

                was_auto_flush = True
                _dataframe_handle_auto_flush(&af, ls_buf, &gs)
                was_auto_flush = False
        except Exception as e:
            # It would be an internal bug for this to raise.
            if not line_sender_buffer_rewind_to_marker(ls_buf, &err):
                raise c_err_to_py(err)

            if (isinstance(e, IngressError) and
                    (e.code == IngressErrorCode.InvalidApiCall) and not was_auto_flush):
                # TODO: This should be allowed by the database.
                # It currently isn't so we have to raise an error.
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    f'Bad dataframe row at index {row_index}: ' +
                    'All values are nulls. '+
                    'Ensure at least one column is not null.') from e
            elif was_serializing_cell:
                raise IngressError(
                    IngressErrorCode.BadDataFrame,
                    'Failed to serialize value of column ' +
                    repr(df.columns[col.setup.orig_index]) +
                    f' at row index {row_index} (' +
                    repr(df.iloc[row_index, col.setup.orig_index]) +
                    f'): {e}  [dc={<int>col.dispatch_code}]') from e
            else:
                raise
    except Exception as e:
        if not isinstance(e, IngressError):
            raise IngressError(
                IngressErrorCode.InvalidApiCall,
                str(e)) from e
        else:
            raise
    finally:
        _ensure_has_gil(&gs)  # Note: We need the GIL for cleanup.
        line_sender_buffer_clear_marker(ls_buf)
        col_t_arr_release(&cols)
        qdb_pystr_buf_clear(b)
