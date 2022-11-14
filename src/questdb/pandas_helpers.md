# Pandas Integration High-level Overview

## Goal

We want to access data in a pandas dataframe from Cython efficiently.
To do this, we need to access its raw memory to traverse it efficiently.
The data held by a dataframe is organized in a columnar fashion.
Each column is a Series object in Python.
Each series object can be backed up by either a Numpy data-structure or
by an Arrow data-structure.

## Accessing raw Numpy data
To access Numpy data we take the series, call its `.to_numpy()` method
and then access the numpy data as a `Py_buffer`.
* https://docs.python.org/3/c-api/buffer.html
* http://jakevdp.github.io/blog/2014/05/05/introduction-to-the-python-buffer-protocol/

## Accessing raw Arrow data
To access Arrow data we first need to unpack each chunk of data at the
Python level giving us `pyarrow` wrapper Array objects.
Each Arrow object in `pyarrow` has a `._export_to_c(..)` python method where we
can pass a Python ints with the addresses to a pre-allocated `ArrowArray` and
`ArrowSchema` C structures.
* https://arrow.apache.org/docs/python/integration/python_java.html
    (Ignore the Java part, we just use the same approach for Python to C.)
* https://arrow.apache.org/docs/format/CDataInterface.html
* https://arrow.apache.org/docs/format/Columnar.html#format-columnar

## Consolidating data access
Now that we've obtained all the pointers we can traverse through the data
without the aid of the Python interpreter (until we hit a Python string in a
Numpy array that is).

The trouble is, though, that we're dealing with so many potential column types
numpy strides, arrow dictionaries and nullables that we risk having an
unmaintainable spaghetti mess of conditionals, special cases and downright
untestability.

To tame this and maintain one's sanity we need to remember that we
don't need to support every type, data-structure et cetera that pandas, numpy
and arrow can throw at us: Instead we approach this by only accepting
one-dimensional arrays that support our basic ILP supported types _only_.

We can also further simplify iteration via the introduction of a cursor:
a struct that is a mishmash of the simplified subsets of arrow and py buffers
that we we actually care about.

## Cherry-picking `Py_buffer` and `ArrowArray` features

First, off the bat, we can exclude supporting some of these structs' fields:

### `Py_buffer`
_Always one single `Py_buffer` per column. Not chunked._

* `void *buf`: Points to the start of our data.                   **`[NEEDED]`**
* `PyObject *obj`: No need to access Py object again.             **`[IGNORED]`**
* `int readonly`: We never write.                                 **`[IGNORED]`**
* `Py_ssize_t len`: We already have the row-count.                **`[IGNORED]`**
* `Py_ssize_t itemsize`: It's enough to know our stride.          **`[IGNORED]`**
* `int ndim`: We only support 1-D data.                           **`[VALIDATED]`**
* `Py_ssize_t *shape`: We only support 1-D data.                  **`[IGNORED]`**
* `Py_ssize_t *strides`: We only need the first value             **`[SIMPLIFIED]`**
* `Py_ssize_t *suboffsets`: Numpy shouldn't be using this.        **`[VALIDATED]`**
* `void *internal`: Says on the tin.                              **`[IGNORED]`**

### `ArrowArray`
_Multiple of these `ArrowArray` structs per column. Chunked._

* `int64_t length`: We need it for the length of the chunk.   **`[NEEDED]`**
* `int64_t null_count`: Needed as if == 0, null col may be NULL.  **`[NEEDED]`**
* `int64_t offset`: Needed to determine number of skipped rows.  **`[NEEDED]`**
* `int64_t n_buffers`: A function of the type, not needed.  **`[NEEDED]`**
* `int64_t n_children`:
* `const void** buffers`: Data, e.g. buffers[0] is validity bitvec.  **`[NEEDED]`**
* `ArrowArray** children`: Needed only for strings where:  **`[NEEDED]`**
    * `buffers[0]` is nulls bitvec
    * `buffers[1]` is int32 offsets buffer
    * `children[0]` is ArrowArray of int8
    * See: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
* `ArrowArray* dictionary`: Needed to support Pandas categories. **`[INITIALLY OUT OF SCOPE]`**
    * Given the complexity of supporting this feature
      (and it being less common in use) we instead validate that it's not set.
      See: https://pandas.pydata.org/docs/reference/api/pandas.CategoricalDtype.html

## Mapping Datatypes

We can now start to remind ourselves of the destination data-types that we
actually need to support, and see how these map from source column data-types
in both of Numpy and Arrow.

We need to extract:
* booleans
* 64-bit signed integers
* 64-bit floats
* UTF-8 string buffers
* Nanosecond-precision UTC unix epoch 64-bit signed int timestamps

### Booleans

**Numpy**



TO BE CONTINUED ....