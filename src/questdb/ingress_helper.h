#pragma once
// This file is included into `ingress.c`.


// Cython idiosyncrasy workaround.
// If we do this in Cython it treats `buf.obj` as
// a ref-counted `object` instead of a `PyObject*`,
// so we can't check it for NULL.
inline int Py_buffer_obj_is_set(Py_buffer* buf)
{
    return buf->obj != NULL;
}