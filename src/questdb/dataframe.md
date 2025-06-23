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
that we actually care about.

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
* `int64_t n_buffers`: A function of the type, not needed.  **`[IGNORED]`**
* `int64_t n_children`: A function of the type, not needed.  **`[IGNORED]`**
* `const void** buffers`: Data, e.g. buffers[0] is validity bitvec.  **`[NEEDED]`**
* `ArrowArray** children`: Needed only for strings where:  **`[NEEDED]`**
    * `buffers[0]` is nulls bitvec
    * `buffers[1]` is int32 offsets buffer
    * `children[0]` is ArrowArray of int8
    * See: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
* `ArrowArray* dictionary`: Needed to support Pandas categories.
    * This ends up being an array of strings, whilst the index is kept in the
      parent `buffers[1]` with `buffers[0]` (possibly) as the validity bitmap.

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

```python
import pandas as pd
import pyarrow as pa
import datetime as dt
```

### Booleans

```python
>>> df = pd.DataFrame({
...    'bool_col': [True, False, False, True],
...    'obj_bool_col': [True, False, None, False],
...    'nullable_bool_col': pd.array(
...       [True, False, None, False], dtype="boolean")})
```

#### Numpy-native representation.
```python
>>> df.dtypes['bool_col']
dtype('bool')
>>> type(df.dtypes['bool_col']).mro()
[<class 'numpy.dtype[bool_]'>, <class 'numpy.dtype'>, <class 'object'>]
>>> df.bool_col.to_numpy().dtype
dtype('bool')
```

#### Bools as Python objects
```python
>>> df.obj_bool_col
0     True
1    False
2     None
3    False
Name: obj_bool_col, dtype: object
```

It's unclear if this should be supported or not. We might want to and error out
as soon as we encounter either a `None` or a `pandas.NA` object.

```python
>>> df.obj_bool_col.astype('bool')
0     True
1    False
2    False
3    False
Name: obj_bool_col, dtype: bool
```

Lastly, we have what appears to be an Arrow-backed representation.
```python
>>> df.dtypes['nullable_bool_col']
BooleanDtype
>>> type(df.dtypes['nullable_bool_col']).mro()
[<class 'pandas.core.arrays.boolean.BooleanDtype'>, <class 'pandas.core.dtypes.dtypes.BaseMaskedDtype'>, <class 'pandas.core.dtypes.base.ExtensionDtype'>, <class 'object'>]
```

We can convert it and then access its contents:
```
>>> arr1 = pa.Array.from_pandas(df.nullable_bool_col)
>>> arr1
<pyarrow.lib.BooleanArray object at 0x7f3ae82b0dc0>
[
  true,
  false,
  null,
  false
]
>>> arr1._export_to_c(.... pointer_refs to ArrowArray and ArrowSchema)
```

This last type is represented as two bitmaps.
See: https://docs.rs/arrow-array/26.0.0/src/arrow_array/array/boolean_array.rs.html#107

We want to support this representation, but skip out on nulls.
We want to error out as soon as we see a `null`.

### 64-bit signed integers

From Numpy's side, we've got a fair few to deal with:
https://numpy.org/doc/stable/user/basics.types.html

This is all your usual signed/unsigned integers with 8, 16, 32 and 64 bit width.

The good news is that the default _is_ `int64`:

```python
>>> df = pd.DataFrame({'n': [1, 2, 3, 4, 5]})
>>> df.n
0    1
1    2
2    3
3    4
4    5
Name: n, dtype: int64

>>> df.dtypes['n']
dtype('int64')

>>> type(df.dtypes['n']).mro()
[<class 'numpy.dtype[int64]'>, <class 'numpy.dtype'>, <class 'object'>]
```

Some of these are going to be in bounds, others out of bounds of 64-bit signed:
Validation needed.

Pandas also provides its own (arrow-based) nullable integers.

```python
>>> df2 = pd.DataFrame({'nullable_n': pd.array([1, 2, None], dtype=pd.Int64Dtype())})
>>> df2.nullable_n
0       1
1       2
2    <NA>
Name: nullable_n, dtype: Int64
>>> type(df2.dtypes['nullable_n']).mro()
[<class 'pandas.core.arrays.integer.Int64Dtype'>, <class 'pandas.core.arrays.integer.IntegerDtype'>, <class 'pandas.core.arrays.numeric.NumericDtype'>, <class 'pandas.core.dtypes.dtypes.BaseMaskedDtype'>, <class 'pandas.core.dtypes.base.ExtensionDtype'>, <class 'object'>]
```

We also need to validate against potential byte-order issues as we're not going
to support this until someone asks:
https://pandas.pydata.org/pandas-docs/version/0.19.1/gotchas.html#byte-ordering-issues

```python
>>> df3 = pd.DataFrame({'big_e': np.array([1, 2, 3, 4]).astype('>u4')})
>>> df3.big_e
0    1
1    2
2    3
3    4
Name: big_e, dtype: uint32
>>> type(df3.dtypes['big_e']).mro()
[<class 'numpy.dtype[uint32]'>, <class 'numpy.dtype'>, <class 'object'>]
>>> df3.dtypes['big_e'].byteorder
'>'
```


### 64-bit floats

32-bit and 64-bit floats. They all support nullability. We will disallow 16-bit
floats.

64-bit is default.

```python
>>> df = pd.DataFrame({'a': [None, 1.0, 1.5, 2.0], 'b': pd.Series([None, 1.0, 1.5, 2.0], dtype='float32'), 'c': pd.Series([None, 1.0, 1.5, 2.0], dtype='float64')})
>>> df
     a    b    c
0  NaN  NaN  NaN
1  1.0  1.0  1.0
2  1.5  1.5  1.5
3  2.0  2.0  2.0
>>> df.a
0    NaN
1    1.0
2    1.5
3    2.0
Name: a, dtype: float64
>>> df.b
0    NaN
1    1.0
2    1.5
3    2.0
Name: b, dtype: float32
>>> df.c
0    NaN
1    1.0
2    1.5
3    2.0
Name: c, dtype: float64
```

#### Arrow floats

Pandas also has arrow-compatible floats.
These have an additional bitvector to represent nulls.



#### 16-bit floats

16-bit floats _do exist_ in Pandas, but we will disallow them:

```python
>>> df = pd.DataFrame({'a': pd.Series([1.0, 1.5, 2.0], dtype='float16')})
>>> df
     a
0  1.0
1  1.5
2  2.0
>>> df.a
0    1.0
1    1.5
2    2.0
Name: a, dtype: float16
```

### UTF-8 string buffers

Strings are.. hard. Strings in dataframes are harder.

#### Python Strings

Numpy usually holds strings as Python objects.

```python
>>> df = pd.DataFrame({'a': [
...     'Strings', 'in', 'Pandas', 'are', 'objects', 'by', 'default']})
>>> df.dtypes['a']
dtype('O')
>>> type(df.dtypes['a']).mro()
[<class 'numpy.dtype[object_]'>, <class 'numpy.dtype'>, <class 'object'>]
```

Ouch.

Python string objects internally hold buffers that, depending on need are
encoded as one of UCS-1, UCS-2 or UCS-4. These are variable-length arrays of
codepoints. One codepoint per array element.

In UCS-1 that's 1-byte elements - effectively `uint8_t`, so the highest code
point is `2 ** 8 - 1 == 255`, or in other words:

```python
>>> chr(255)
'ÿ'
```

If a string contains a codepoint with a numeric value higher than this, it would
need UCS-2 or UCS-4. Such representations are backed by `uint16_t` or `uint32_t`
arrays.

For example, the codepoint for a lobster is 129438.

```python
>>> ord('🦞')
129438
```

We _could_ ask Python to convert strings to UTF-8 for us,

```python
>>> '🦞'.encode('utf-8')
b'\xf0\x9f\xa6\x9e'
```

but this would require invoking the Python interpreter and the creation of a
gargantuan amount of little temporary objects.

This is such a common use case that we do the encoding in a supporting Rust
library. See `rpyutils/src/pystr_to_utf8.rs` in the source tree.

It accumulates strings in a address-stable buffer (internally a `Vec<String>`)
and allows us to borrow its memory.

As a side-note, we should also be ready to handle nulls here:

```python
>>> df = pd.DataFrame({'a': ['interspersed', None, 'in', None, 'data']})
>>> type(df.a[1])
<class 'NoneType'>
```

#### Fixed-length strings

Numpy also has some fixed-length strings via two datatypes:
* `S`: Bytes
* `U`: Unicode

```python
>>> df = pd.DataFrame({
...     'a': np.array(['fixed', 'len', 'strings'], dtype='S'),
...     'b': np.array(['example', 'with', 'unicode 🦞'], dtype='U')})
>>> df
            a          b
0    b'fixed'    example
1      b'len'       with
2  b'strings'  unicode 🦞
```

It doesn't really matter much though. Their Pandas datatype is actually just
`'O'` (object).

```python
>>> df.dtypes['a']
dtype('O')
>>> df.dtypes['b']
dtype('O')
>>> type(df.dtypes['b'])
<class 'numpy.dtype[object_]'>
```

We should:
* reject the first one (because in Python3 bytes aren't strings) - We lack the powers to guess which text encoding was used. It's usually `latin-1`, but was it?
  ```python
  >>> type(df.a[0])
  <class 'bytes'>
  ```
* Accept the second one without further optimisations:
  ```python
  >>> type(df.b[0])
  <class 'str'>
  ```

#### Pandas `string[object]` dtype

Since the `'O'` dtype could hold anything (not just strings), Pandas introduced a new column type that ensures the column only holds strings.

```python
>>> df = pd.DataFrame({'a': pd.Series(['another', None, 'str', 'example'], dtype='string')})
>>> df
         a
0  another
1     <NA>
2      str
3  example
>>> df.dtypes['a']
string[python]
>>> type(df.dtypes['a']).mro()
[<class 'pandas.core.arrays.string_.StringDtype'>, <class 'pandas.core.dtypes.base.StorageExtensionDtype'>, <class 'pandas.core.dtypes.base.ExtensionDtype'>, <class 'object'>]
```

Note that by default the storage is still Python objects (sigh),
so our Rust-based conversion will come handy here as well.

Note however that we need to handle nulls not as `None` objects,
but as `pandas.NA` objects.

```python
>>> df.a[1]
<NA>
```

At other times, we end up with `nan` python float objects to represent nulls.
_Yay!_.

#### Arrow-backed Strings

Finally - as we would expect when obtaining a frame from something like Parquet - there's string columns in UTF-8-native format backed by Arrow.

_note the different `dtype`:_

```python
df = pd.DataFrame({'a': pd.Series(['arrow', None, 'str', 'example'], dtype='string[pyarrow]')})
```

```
>>> df = pd.DataFrame({'a': pd.Series(['arrow', None, 'str', 'example'], dtype='string[pyarrow]')})
>>> df
         a
0    arrow
1     <NA>
2      str
3  example
>>> df.dtypes['a']
string[pyarrow]
>>> type(df.dtypes['a']).mro()
[<class 'pandas.core.arrays.string_.StringDtype'>, <class 'pandas.core.dtypes.base.StorageExtensionDtype'>, <class 'pandas.core.dtypes.base.ExtensionDtype'>, <class 'object'>]
```

Note that these strings will always have indices based on `int32_t`.

Arrow also has a `pyarrow.large_string()` type, but
pandas doesn't support it.

#### Symbol-like Categorical Data

Pandas supports categories. These are backed by Arrow.

```python
>>> df = pd.DataFrame({'a': pd.Series(
...     ['symbol', 'like', 'type', 'symbol', 'like', 'like', 'like', None],
...     dtype='category')})
>>> df
        a
0  symbol
1    like
2    type
3  symbol
4    like
5    like
6    like
7     NaN
>>> df.dtypes['a']
CategoricalDtype(categories=['like', 'symbol', 'type'], ordered=False)
>>> type(df.dtypes['a']).mro()
[<class 'pandas.core.dtypes.dtypes.CategoricalDtype'>, <class 'pandas.core.dtypes.dtypes.PandasExtensionDtype'>, <class 'pandas.core.dtypes.base.ExtensionDtype'>, <class 'object'>]
```

This is how it's represented:

```python
>>> pa.Array.from_pandas(df.a)
<pyarrow.lib.DictionaryArray object at 0x7f7a965fee30>

-- dictionary:
  [
    "like",
    "symbol",
    "type"
  ]
-- indices:
  [
    1,
    0,
    2,
    1,
    0,
    0,
    0,
    null
  ]
```

For this, we need the `dictionary` field in the `ArrowArray` struct.

What's also neat is that we know the categories in advance _before_ running the
encoding. This means we can build up our `line_sender_utf8` objects in advance,
though they are all UTF-8 buffers already so.. little gain.


### Nanosecond-precision UTC unix epoch 64-bit signed int timestamps

#### Timezone-free timestamp

```python
>>> n1 = pd.Timestamp(dt.datetime.now())
>>> n2 = pd.Timestamp(dt.datetime.now())
>>> df = pd.DataFrame({'a': [n1, n2]})
>>> df
                           a
0 2022-11-15 17:47:23.131445
1 2022-11-15 17:47:26.943899
```

The data is held as nanos since unix epoch as a 64-bit int.
```python
>>> df.dtypes['a']
dtype('<M8[ns]')
>>> type(df.dtypes['a']).mro()
[<class 'numpy.dtype[datetime64]'>, <class 'numpy.dtype'>, <class 'object'>]
```

This matches our own designated timestamp representation and we just need to convert to micros for the rest of the columns.

Null values _are_ supported.

```python
>>> df = pd.DataFrame({'a': [n1, n2, None]})
>>> df
                           a
0 2022-11-15 17:47:23.131445
1 2022-11-15 17:47:26.943899
2                        NaT
```

Unclear what the sentinel value for `NaT` is yet, but we want to map it internally to 0 for the designated timestamp and to recognise it
and skip the column otherwise.

#### Additionally, we can also have datetimes with a timezone

```python
>>> ts = pd.Timestamp(
...    year=2020, month=1, day=1, hour=12, minute=0, second=0,
...    tz=zoneinfo.ZoneInfo('America/Los_Angeles'))
>>> df = pd.DataFrame({'a': [ts]})
>>> df.dtypes['a']
datetime64[ns, America/Los_Angeles]
>>> type(_)
<class 'pandas.core.dtypes.dtypes.DatetimeTZDtype'>
>>> df.dtypes['a'].tz
zoneinfo.ZoneInfo(key='America/Los_Angeles')
```

The good news here is that the timestamp is still held as UTC (regardless of
timezone), so no timezone conversion logic is required here.

```python
>>> pa.Array.from_pandas(df.a)
<pyarrow.lib.TimestampArray object at 0x7ff63914c4c0>
[
  2020-01-01 20:00:00.000000000
]
```

**Note**: We need PyArrow to access the buffer, or we need to convert to
`datetime64[ns]`.


## Strided Numpy Arrays

Numpy arrays need not be contiguous. In Pandas, however, we
need not worry about this.

If we construct a `(4, 3)`-shaped 2D numpy array

```python
>>> import numpy as np
>>> a1 = np.array([[1, 10, 100], [2, 20, 200], [3, 30, 300], [4, 40, 400]])
>>> a1
array([[  1,  10, 100],
       [  2,  20, 200],
       [  3,  30, 300],
       [  4,  40, 400]])
>>> a1.dtype
dtype('int64')
```

and then select it's second column

```python
>>> a2 = a1[:, 1]
>>> a2
array([10, 20, 30, 40])
```

We encounter a non-contiguous array.

```python
>>> a2.data
<memory at 0x7faefaaac4c0>
>>> a2.data.contiguous
False
>>> a2.data.strides
(24,)
```

If we then wrap up the array in a dataframe and convert the series back to numpy

```python
>>> df = pd.DataFrame({'a': a2})
>>> df
    a
0  10
1  20
2  30
3  40
>>> df.a
0    10
1    20
2    30
3    40
Name: a, dtype: int64
>>> a3 = df.a.to_numpy()
```

We see that we get a new object back, and that the new object actually _is_
contiguous.

```python
>>> id(a2)
140389455034672
>>> id(a3)
140388032511696
>>> a3.data
<memory at 0x7faea2c17880>
>>> a3.data.contiguous
True
```

For this reason, supporting strides is not necessary.


## Unified Cursor

TO BE CONTINUED
