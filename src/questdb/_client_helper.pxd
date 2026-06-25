cdef extern from "_client_helper.inc":
    bint Py_buffer_obj_is_set(Py_buffer* buf)