# We'd _love_ to use the defns from
# https://github.com/numpy/numpy/blob/main/numpy/__init__.pxd
# unfortunately these usually take `object` instead of `PyObject*`.
# Annoyingly, this means that they can incur extra incref/decref
# operations that we most certainly want to avoid for perf reasons.

from cpython.object cimport PyObject
from numpy cimport (
    # Constants
    NPY_DOUBLE,  # N.B.: From `#include <numpy/npy_common.h>`: `#define NPY_FLOAT64 NPY_DOUBLE`

    # Types
    PyArrayObject,
    PyArray_Descr,
    npy_intp,
    npy_int,
    dtype,

    # Functions
    PyArray_DescrFromType,  # returns a `dtype` Python/C object.
)

cdef extern from "numpy/arrayobject.h":
    # PyArrayObject
    npy_intp PyArray_NBYTES(PyArrayObject*) nogil
    npy_intp* PyArray_STRIDES(PyArrayObject*) nogil
    npy_intp* PyArray_DIMS(PyArrayObject*) nogil
    npy_int PyArray_TYPE(PyArrayObject* arr) nogil
    void* PyArray_DATA(PyArrayObject*) nogil
    char* PyArray_BYTES(PyArrayObject*) nogil
    npy_intp* PyArray_DIMS(PyArrayObject*) nogil
    npy_intp* PyArray_STRIDES(PyArrayObject*) nogil
    npy_intp PyArray_DIM(PyArrayObject*, size_t) nogil
    npy_intp PyArray_STRIDE(PyArrayObject*, size_t) nogil
    int PyArray_NDIM(PyArrayObject*) nogil
