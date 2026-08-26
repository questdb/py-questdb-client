#!/usr/bin/env python3
"""A hand-rolled Arrow producer and the malformed shapes built on it.

The producer lives here rather than in ``test/test.py`` because the
malformed shapes have to run somewhere the interpreter is allowed to
die. Each shape in ``FORGED_CASES`` is a count, length or offset that a
pre-scan taking the producer at its word reads an array past the end
of; several of them end the process on a signal when it does. So the
test that covers them runs this module as a child and reads the return
code. That child needs the producer and nothing else the suite imports,
which is why this file stops at ctypes, struct and the client.

Run as a script it builds one named case, sends it at the port it is
given, requires the pre-flight refusal, and prints ``MARKER``::

    QUESTDB_FORGED_ARROW_CASE=null_batch_child
    QUESTDB_FORGED_ARROW_PORT=9009 python3 forged_arrow.py
"""
import sys

sys.dont_write_bytecode = True
import ctypes
import os
import struct

import patch_path  # noqa: F401  -- puts `src` on the path when asked to

import questdb._client as qi


class _RawArrowStream:
    """A hand-rolled ``__arrow_c_stream__`` producer, built straight
    onto the Arrow C data interface with ctypes.

    ``QuestDB.dataframe()`` accepts any object carrying that method, so
    the shapes it has to cope with are not only the ones pyarrow and
    polars emit. pyarrow normalises a few of them away on export -- it
    writes a counted ``null_count`` even where the array was built
    without one, and it puts a slice's start on each column rather than
    on the batch -- so the shapes below are reachable only from a
    producer written by hand, which is exactly what nanoarrow, DuckDB
    and arro3 are.

    Everything the stream hands out stays owned by this object and is
    freed with it; the release callbacks only clear the pointer the
    consumer is told to clear.
    """

    _ARRAY_FIELDS = [
        ('length', ctypes.c_int64),
        ('null_count', ctypes.c_int64),
        ('offset', ctypes.c_int64),
        ('n_buffers', ctypes.c_int64),
        ('n_children', ctypes.c_int64),
        ('buffers', ctypes.POINTER(ctypes.c_void_p)),
        ('children', ctypes.c_void_p),
        ('dictionary', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p)]
    _SCHEMA_FIELDS = [
        ('format', ctypes.c_char_p),
        ('name', ctypes.c_char_p),
        ('metadata', ctypes.c_char_p),
        ('flags', ctypes.c_int64),
        ('n_children', ctypes.c_int64),
        ('children', ctypes.c_void_p),
        ('dictionary', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p)]
    _STREAM_FIELDS = [
        ('get_schema', ctypes.c_void_p),
        ('get_next', ctypes.c_void_p),
        ('get_last_error', ctypes.c_void_p),
        ('release', ctypes.c_void_p),
        ('private_data', ctypes.c_void_p)]

    class Array(ctypes.Structure):
        pass

    class Schema(ctypes.Structure):
        pass

    class Stream(ctypes.Structure):
        pass

    def __init__(self, columns, row_count, batch_offset=0,
                 schema_n_children=None, batch_n_children=None,
                 null_schema_children=False, null_batch_children=False,
                 null_schema_child=None, null_batch_child=None):
        """`columns` is a list of dicts, one per column, each naming its
        Arrow `format`, its `name`, the `data` bytes of its value
        buffer, and optionally `metadata`, `null_count`, `validity`,
        `offset` and `length`. `row_count` and `batch_offset` are the
        struct-level length and start.

        The remaining arguments forge the top-level struct's own shape,
        which a producer states rather than the consumer measuring it.
        `schema_n_children` and `batch_n_children` each replace the
        declared column count while `children[]` keeps the length the
        column list gives it, so a count larger than the list is a claim
        about an array that was never allocated. `null_schema_children`
        and `null_batch_children` hand over a NULL array with the count
        left as it was, and `null_schema_child` / `null_batch_child`
        take an index and blank that one slot. Left alone, every one of
        them keeps the struct consistent, which is what every other
        caller of this class wants.
        """
        self._keep = []
        self._released = False
        self._schema = self._build_schema(
            columns, schema_n_children, null_schema_children,
            null_schema_child)
        self._array = self._build_array(
            columns, row_count, batch_offset, batch_n_children,
            null_batch_children, null_batch_child)
        self._stream = self._build_stream()

    # -- construction --------------------------------------------------

    def _buffer_array(self, blobs):
        """A `void*[]` over the given buffers; None becomes NULL."""
        out = (ctypes.c_void_p * len(blobs))()
        for i, blob in enumerate(blobs):
            if blob is None:
                out[i] = None
            else:
                held = ctypes.create_string_buffer(blob, len(blob))
                self._keep.append(held)
                out[i] = ctypes.cast(held, ctypes.c_void_p)
        self._keep.append(out)
        return out

    @staticmethod
    def _packed_metadata(pairs):
        """Arrow's packed metadata blob: a pair count, then each key and
        each value as a little-endian length followed by its bytes."""
        if not pairs:
            return None
        out = struct.pack('<i', len(pairs))
        for key, value in pairs.items():
            out += struct.pack('<i', len(key)) + key
            out += struct.pack('<i', len(value)) + value
        return out

    def _release_stub(self, struct_type):
        """A release callback that clears the caller's pointer. The
        memory itself belongs to this object."""
        proto = ctypes.CFUNCTYPE(None, ctypes.POINTER(struct_type))

        def release(ptr):
            ptr.contents.release = None

        held = proto(release)
        self._keep.append(held)
        return ctypes.cast(held, ctypes.c_void_p)

    def _build_schema(self, columns, n_children=None,
                      null_children=False, null_child=None):
        children = (ctypes.POINTER(self.Schema) * len(columns))()
        for i, column in enumerate(columns):
            child = self.Schema()
            child.format = column['format']
            child.name = column['name']
            child.metadata = self._packed_metadata(column.get('metadata'))
            child.flags = 2  # ARROW_FLAG_NULLABLE
            child.n_children = 0
            child.children = None
            child.dictionary = None
            child.release = self._release_stub(self.Schema)
            child.private_data = None
            self._keep.append(child)
            children[i] = ctypes.pointer(child)
        if null_child is not None:
            children[null_child] = None
        self._keep.append(children)
        top = self.Schema()
        top.format = b'+s'
        top.name = None
        top.metadata = None
        top.flags = 0
        top.n_children = (
            len(columns) if n_children is None else n_children)
        top.children = (
            None if null_children
            else ctypes.cast(children, ctypes.c_void_p))
        top.dictionary = None
        top.release = self._release_stub(self.Schema)
        top.private_data = None
        self._keep.append(top)
        return top

    def _build_array(self, columns, row_count, batch_offset,
                     n_children=None, null_children=False,
                     null_child=None):
        children = (ctypes.POINTER(self.Array) * len(columns))()
        for i, column in enumerate(columns):
            child = self.Array()
            child.length = column.get('length', row_count + batch_offset)
            child.null_count = column.get('null_count', 0)
            child.offset = column.get('offset', 0)
            child.n_buffers = column.get('n_buffers', 2)
            child.n_children = 0
            child.buffers = self._buffer_array(
                [column.get('validity'), column['data']])
            child.children = None
            child.dictionary = None
            child.release = self._release_stub(self.Array)
            child.private_data = None
            self._keep.append(child)
            children[i] = ctypes.pointer(child)
        if null_child is not None:
            children[null_child] = None
        self._keep.append(children)
        top = self.Array()
        top.length = row_count
        top.null_count = 0
        top.offset = batch_offset
        top.n_buffers = 1
        top.n_children = (
            len(columns) if n_children is None else n_children)
        top.buffers = self._buffer_array([None])
        top.children = (
            None if null_children
            else ctypes.cast(children, ctypes.c_void_p))
        top.dictionary = None
        top.release = self._release_stub(self.Array)
        top.private_data = None
        self._keep.append(top)
        return top

    def _build_stream(self):
        stream_ptr = ctypes.POINTER(self.Stream)
        get_schema_t = ctypes.CFUNCTYPE(
            ctypes.c_int, stream_ptr, ctypes.POINTER(self.Schema))
        get_next_t = ctypes.CFUNCTYPE(
            ctypes.c_int, stream_ptr, ctypes.POINTER(self.Array))
        last_error_t = ctypes.CFUNCTYPE(ctypes.c_char_p, stream_ptr)
        release_t = ctypes.CFUNCTYPE(None, stream_ptr)
        source = self

        def get_schema(_stream, out):
            ctypes.memmove(
                out, ctypes.byref(source._schema),
                ctypes.sizeof(source.Schema))
            return 0

        def get_next(_stream, out):
            # One batch, then the end-of-stream marker: a released
            # array with `length` left at zero.
            if source._released:
                out.contents.release = None
                out.contents.length = 0
                return 0
            source._released = True
            ctypes.memmove(
                out, ctypes.byref(source._array),
                ctypes.sizeof(source.Array))
            return 0

        def get_last_error(_stream):
            return None

        def release(ptr):
            ptr.contents.release = None

        callbacks = [
            get_schema_t(get_schema), get_next_t(get_next),
            last_error_t(get_last_error), release_t(release)]
        self._keep.extend(callbacks)
        stream = self.Stream()
        (stream.get_schema, stream.get_next,
         stream.get_last_error, stream.release) = [
            ctypes.cast(cb, ctypes.c_void_p) for cb in callbacks]
        stream.private_data = None
        self._keep.append(stream)
        return stream

    # -- the interface `QuestDB.dataframe()` dispatches on --------------

    def __arrow_c_stream__(self, requested_schema=None):
        self._released = False
        new_capsule = ctypes.pythonapi.PyCapsule_New
        new_capsule.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
        new_capsule.restype = ctypes.py_object
        return new_capsule(
            ctypes.addressof(self._stream), b'arrow_array_stream', None)


_RawArrowStream.Array._fields_ = _RawArrowStream._ARRAY_FIELDS
_RawArrowStream.Schema._fields_ = _RawArrowStream._SCHEMA_FIELDS
_RawArrowStream.Stream._fields_ = _RawArrowStream._STREAM_FIELDS


#: Printed by a child that saw the refusal it was sent to provoke. The
#: parent requires it, so an early `sys.exit(0)` cannot pass for one.
MARKER = 'FORGED-ARROW-REFUSED-OK'

#: Case name -> (builder, the fragment its refusal has to carry).
FORGED_CASES = {}

_STAMP = struct.pack('<q', 1735689600000000)
_GEOHASH_CLAIM = {b'questdb.column_type': b'geohash',
                  b'questdb.geohash_bits': b'5'}


def _case(name, fragment):
    """Register a forged shape under `name`, refused in words carrying
    `fragment`."""
    def register(build):
        assert name not in FORGED_CASES, name
        FORGED_CASES[name] = (build, fragment)
        return build
    return register


def columns(rows, geohash=True, **column):
    """A two-column batch: one integer column and the designated
    timestamp, each with `rows` slots of value buffer behind it.

    Anything in `column` -- a forged `length` or `offset` -- is put on
    both, which keeps the timestamp column's own numbers honest against
    the same slice. Only the integer column carries a GEOHASH claim, and
    only a claimed column reaches the row-shape checks at all.
    """
    return [
        dict(format=b'i', name=b'gh',
             data=struct.pack('<%di' % rows, *([1] * rows)),
             metadata=_GEOHASH_CLAIM if geohash else None, **column),
        dict(format=b'tsu:UTC', name=b'ts', data=_STAMP * rows, **column)]


# -- counts and child pointers -----------------------------------------
#
# Read before the claim is looked for and before the zero-row return, so
# none of these shapes needs a GEOHASH column to be turned away, and two
# of them carry no rows at all.

@_case('schema_count_absurd', 'is not a column count this client reads')
def _schema_count_absurd():
    """A count both structs agree on, far past any array either of them
    allocated. Agreement is what a plain equality check cannot see
    through, which is why the count answers to a bound of its own."""
    return _RawArrowStream(
        columns(1, geohash=False), 1,
        schema_n_children=1 << 40, batch_n_children=1 << 40)


@_case('schema_count_cap_plus_one', 'is not a column count this client reads')
def _schema_count_cap_plus_one():
    return _RawArrowStream(
        columns(1, geohash=False), 1,
        schema_n_children=4096, batch_n_children=4096)


@_case('schema_count_negative', 'is not a column count this client reads')
def _schema_count_negative():
    return _RawArrowStream(
        columns(1, geohash=False), 1,
        schema_n_children=-1, batch_n_children=-1)


@_case('batch_count_negative', 'the batch carries -1 columns')
def _batch_count_negative():
    return _RawArrowStream(
        columns(1, geohash=False), 1, batch_n_children=-1)


@_case('count_disagreement', 'the batch carries 3 columns')
def _count_disagreement():
    return _RawArrowStream(
        columns(1, geohash=False), 1, batch_n_children=3)


@_case('zero_column_schema_disagreement', "against the schema's 0")
def _zero_column_schema_disagreement():
    """A zero-column schema is a shape the scan has nothing to do with,
    and the batch's own count still has to agree with it."""
    return _RawArrowStream(
        columns(1, geohash=False), 1, schema_n_children=0)


@_case('zero_row_count_disagreement', 'the batch carries 3 columns')
def _zero_row_count_disagreement():
    return _RawArrowStream(
        columns(1, geohash=False), 0, batch_n_children=3)


@_case('null_schema_children', 'without the array holding them')
def _null_schema_children():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_schema_children=True)


@_case('null_batch_children', 'without the array holding them')
def _null_batch_children():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_batch_children=True)


@_case('null_schema_child', 'column 0 of the batch arrived as a null pointer')
def _null_schema_child():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_schema_child=0)


@_case('null_batch_child', 'column 1 of the batch arrived as a null pointer')
def _null_batch_child():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_batch_child=1)


@_case('null_batch_children_zero_rows', 'without the array holding them')
def _null_batch_children_zero_rows():
    """A batch with no rows still has its struct read, so an empty one
    cannot carry a malformed shape past the checks."""
    return _RawArrowStream(
        columns(1, geohash=False), 0, null_batch_children=True)


@_case('null_schema_child_zero_rows',
       'column 0 of the batch arrived as a null pointer')
def _null_schema_child_zero_rows():
    return _RawArrowStream(
        columns(1, geohash=False), 0, null_schema_child=0)


# -- row shapes --------------------------------------------------------
#
# Read only once a GEOHASH is claimed, so that a malformed stream
# carrying none of them keeps the importer's own words.

@_case('batch_length_overflow', 'the batch reports')
def _batch_length_overflow():
    """The review's own reproduction: `batch.offset + batch.length`
    leaves `int64_t` and lands as a large negative, which an unbounded
    slice check reads as a batch comfortably inside its column."""
    return _RawArrowStream(columns(1, length=1), (1 << 63) - 1, 1)


@_case('batch_length_cap_plus_one', 'the batch reports 16777217 rows')
def _batch_length_cap_plus_one():
    return _RawArrowStream(columns(1, length=1), 16777217, 0)


@_case('batch_offset_cap_plus_one', 'the batch starts at row 16777217')
def _batch_offset_cap_plus_one():
    return _RawArrowStream(columns(1, length=1), 1, 16777217)


@_case('batch_offset_negative', 'the batch starts at row -1')
def _batch_offset_negative():
    return _RawArrowStream(columns(1, length=1), 1, -1)


@_case('column_length_cap_plus_one', 'the column reports 16777217 rows')
def _column_length_cap_plus_one():
    return _RawArrowStream(columns(1, length=16777217), 1, 0)


@_case('column_length_negative', 'the column reports -1 rows')
def _column_length_negative():
    return _RawArrowStream(columns(1, length=-1), 1, 0)


@_case('column_offset_cap_plus_one', 'from row 16777217')
def _column_offset_cap_plus_one():
    return _RawArrowStream(columns(1, length=1, offset=16777217), 1, 0)


@_case('column_offset_negative', 'without readable value buffers')
def _column_offset_negative():
    return _RawArrowStream(columns(1, length=1, offset=-1), 1, 0)


@_case('slice_past_column', 'the column holds 2 rows and the batch asks')
def _slice_past_column():
    """One row past the column: `batch.offset + batch.length` comes to
    `col.length + 1`. Its twin, one row shorter, is sent for real by
    `test_a_batch_ending_exactly_at_the_column_end_is_accepted`."""
    return _RawArrowStream(columns(3, length=2), 2, 1)


def accepted_slice():
    """The shape `slice_past_column` is one row longer than: a batch
    ending exactly where its columns do. Built here so the two differ in
    a single number."""
    return _RawArrowStream(columns(3, length=3), 2, 1)


def run_case(name, port):
    """Build the named shape, send it, and require the pre-flight
    refusal. Raises rather than returning anything on disagreement."""
    build, fragment = FORGED_CASES[name]
    stream = build()
    conf = (f'ws::addr=127.0.0.1:{port};lazy_connect=true;'
            f'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
    client = qi.QuestDB.from_conf(conf)
    try:
        try:
            client.dataframe(stream, table_name='forged', at='ts')
        except qi.QuestDBError as exc:
            if exc.code is not qi.QuestDBErrorCode.BadDataFrame:
                raise AssertionError(
                    f'{name}: refused as {exc.code!r}, expected '
                    f'BadDataFrame: {exc}') from None
            if fragment not in str(exc):
                raise AssertionError(
                    f'{name}: refused as {exc!s}, which does not carry '
                    f'{fragment!r}') from None
        else:
            raise AssertionError(f'{name}: the forged stream was accepted')
    finally:
        client.close()
    print(MARKER)


def main():
    name = os.environ.get('QUESTDB_FORGED_ARROW_CASE')
    port = os.environ.get('QUESTDB_FORGED_ARROW_PORT')
    if not name or not port:
        raise SystemExit(
            'QUESTDB_FORGED_ARROW_CASE and QUESTDB_FORGED_ARROW_PORT name '
            'the shape to build and the server to send it at.')
    if name not in FORGED_CASES:
        raise SystemExit(f'no forged Arrow case named {name!r}')
    run_case(name, int(port))
    return 0


if __name__ == '__main__':
    sys.exit(main())
