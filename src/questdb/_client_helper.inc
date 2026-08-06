#pragma once
// This file is included into `_client.c`.


// Cython idiosyncrasy workaround.
// If we do this in Cython it treats `buf.obj` as
// a ref-counted `object` instead of a `PyObject*`,
// so we can't check it for NULL.
// Since `Py_buffer` is a Cython built-in we can't actually
// just re-define it in `extra_cpython.pxd`.
static int Py_buffer_obj_is_set(Py_buffer* buf)
{
    return buf->obj != NULL;
}

// Nonzero while the interpreter is shutting down. Callbacks arriving from
// native threads must not attempt to acquire the GIL past this point.
static int qdb_py_is_finalizing(void)
{
#if PY_VERSION_HEX >= 0x030D0000
    return Py_IsFinalizing();
#else
    return _Py_IsFinalizing();
#endif
}
