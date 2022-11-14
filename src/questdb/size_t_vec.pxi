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
