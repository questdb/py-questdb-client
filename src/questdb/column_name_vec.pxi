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
