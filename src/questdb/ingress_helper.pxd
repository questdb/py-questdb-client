cdef extern from "ingress_helper.h":
    bint Py_buffer_obj_is_set(Py_buffer* buf)