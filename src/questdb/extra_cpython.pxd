# Custom definitions that aren't provided in the standard `cpython` module.

from libc.stdint cimport uint8_t, uint16_t, uint32_t
from cpython.object cimport PyObject

cdef extern from "Python.h":
    cdef PyObject* Py_None
    cdef PyObject* Py_True

    ctypedef uint8_t Py_UCS1  # unicodeobject.h
    ctypedef uint16_t Py_UCS2
    ctypedef uint32_t Py_UCS4

    ctypedef unsigned int uint

    cdef enum PyUnicode_Kind:
        PyUnicode_1BYTE_KIND
        PyUnicode_2BYTE_KIND
        PyUnicode_4BYTE_KIND

    # Note: Returning an `object` rather than `PyObject` as the function
    # returns a new reference rather than borrowing an existing one.
    object PyUnicode_FromKindAndData(
        int kind, const void* buffer, Py_ssize_t size)

    # Ditto, see comment on why not returning a `PyObject` above.
    str PyUnicode_FromStringAndSize(
        const char* u, Py_ssize_t size)

    # Must be called before accessing data or is compact check.
    int PyUnicode_READY(PyObject* o) except -1

    # Is UCS1 and ascii (and therefore valid UTF-8).
    bint PyUnicode_IS_COMPACT_ASCII(PyObject* o)

    # Get length.
    Py_ssize_t PyUnicode_GET_LENGTH(PyObject* o)

    # Zero-copy access to string buffer.
    int PyUnicode_KIND(PyObject* o)
    Py_UCS1* PyUnicode_1BYTE_DATA(PyObject* o)
    Py_UCS2* PyUnicode_2BYTE_DATA(PyObject* o)
    Py_UCS4* PyUnicode_4BYTE_DATA(PyObject* o)

    Py_ssize_t PyBytes_GET_SIZE(object o)

    bint PyBytes_CheckExact(PyObject* o)

    char* PyBytes_AsString(object o)

    bint PyUnicode_CheckExact(PyObject* o)

    bint PyBool_Check(PyObject* o)

    bint PyLong_CheckExact(PyObject* o)

    bint PyFloat_CheckExact(PyObject* o)

    double PyFloat_AS_DOUBLE(PyObject* o)

    long long PyLong_AsLongLong(PyObject* o) except? -1

    PyObject* PyErr_Occurred()

    ctypedef struct PyThreadState:
        pass

    PyThreadState* PyEval_SaveThread()

    void PyEval_RestoreThread(PyThreadState* tstate)
