#!/usr/bin/env python3
"""A hand-rolled Arrow producer and the malformed shapes built on it.

The producer lives here rather than in ``test/test.py`` so every malformed
shape can run in an isolated interpreter. The cases exercise structural values
that the guarded Python/C-ABI path can check before the known panicking
arrow-rs operations it reaches. Producer-owned pointer arrays, strings and
buffers still have to be valid, correctly allocated and stable for the lifetime
required by the Arrow C Data Interface. The parent reads the child return code
to guard against regressions that abort the interpreter. That child needs the
producer and nothing else the suite imports, which is why this file stops at
ctypes, struct and the client.

Run as a script it builds one named case, sends it at the port it is
given, requires the registered success or native refusal, and prints
``MARKER``::

    TEST_QUESTDB_PATCH_PATH=1 QUESTDB_FORGED_ARROW_CASE=null_batch_child \
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
                 null_schema_child=None, null_batch_child=None,
                 poison_schema_children=False,
                 poison_batch_children=False):
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
            null_schema_child, poison_schema_children)
        self._array = self._build_array(
            columns, row_count, batch_offset, batch_n_children,
            null_batch_children, null_batch_child,
            poison_batch_children)
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

    def _build_schema_node(self, column):
        node = self.Schema()
        node.format = column['format']
        node.name = column.get('name')
        node.metadata = column.get(
            'metadata_blob', self._packed_metadata(column.get('metadata')))
        node.flags = 2  # ARROW_FLAG_NULLABLE
        child_defs = column.get('children', [])
        if child_defs:
            children = (ctypes.POINTER(self.Schema) * len(child_defs))()
            for i, child_def in enumerate(child_defs):
                child = self._build_schema_node(child_def)
                children[i] = ctypes.pointer(child)
            self._keep.append(children)
            node.n_children = column.get('schema_n_children', len(child_defs))
            node.children = (
                None if column.get('null_schema_children')
                else ctypes.cast(children, ctypes.c_void_p))
        else:
            node.n_children = column.get('schema_n_children', 0)
            node.children = (
                1 if column.get('poison_schema_children') else None)
        node.dictionary = None
        node.release = self._release_stub(self.Schema)
        node.private_data = None
        self._keep.append(node)
        return node

    def _build_schema(self, columns, n_children=None,
                      null_children=False, null_child=None,
                      poison_children=False):
        children = (ctypes.POINTER(self.Schema) * len(columns))()
        for i, column in enumerate(columns):
            child = self._build_schema_node(column)
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
            1 if poison_children else (
                None if null_children
                else ctypes.cast(children, ctypes.c_void_p)))
        top.dictionary = None
        top.release = self._release_stub(self.Schema)
        top.private_data = None
        self._keep.append(top)
        return top

    def _build_array_node(self, column, default_length):
        node = self.Array()
        node.length = column.get('length', default_length)
        node.null_count = column.get('null_count', 0)
        node.offset = column.get('offset', 0)
        buffer_defs = column.get('buffers')
        if buffer_defs is None:
            if column['format'] == b'+s':
                buffer_defs = [column.get('validity')]
            else:
                buffer_defs = [column.get('validity'), column.get('data', b'')]
        buffers = self._buffer_array(buffer_defs)
        node.n_buffers = column.get('n_buffers', len(buffer_defs))
        node.buffers = buffers
        child_defs = column.get('children', [])
        if child_defs:
            children = (ctypes.POINTER(self.Array) * len(child_defs))()
            for i, child_def in enumerate(child_defs):
                child = self._build_array_node(child_def, node.length)
                children[i] = ctypes.pointer(child)
            self._keep.append(children)
            node.n_children = column.get('array_n_children', len(child_defs))
            node.children = (
                None if column.get('null_array_children')
                else ctypes.cast(children, ctypes.c_void_p))
        else:
            node.n_children = column.get('array_n_children', 0)
            node.children = (
                1 if column.get('poison_array_children') else None)
        node.dictionary = None
        node.release = self._release_stub(self.Array)
        node.private_data = None
        self._keep.append(node)
        return node

    def _build_array(self, columns, row_count, batch_offset,
                     n_children=None, null_children=False,
                     null_child=None, poison_children=False):
        children = (ctypes.POINTER(self.Array) * len(columns))()
        for i, column in enumerate(columns):
            child = self._build_array_node(column, row_count + batch_offset)
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
            1 if poison_children else (
                None if null_children
                else ctypes.cast(children, ctypes.c_void_p)))
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


#: Printed by a child that observed its declared success/refusal outcome. The
#: parent requires it, so an early `sys.exit(0)` cannot pass for one.
MARKER = 'FORGED-ARROW-OUTCOME-OK'

#: Case name -> builder for one malformed Arrow stream.
FORGED_CASES = {}
FORGED_EXPECTATIONS = {}

_STAMP_MICROS = 1735689600000000
_GEOHASH_CLAIM = {b'questdb.column_type': b'geohash',
                  b'questdb.geohash_bits': b'5'}


def _case(name, *, outcome='ArrowIngest', message=None, at='ts',
          wire=False, row_count=None, wire_contains=()):
    """Register an ABI-conforming hand-built shape and its full outcome."""
    def register(build):
        assert name not in FORGED_CASES, name
        FORGED_CASES[name] = build
        FORGED_EXPECTATIONS[name] = {
            'outcome': outcome,
            'message': message,
            'at': at,
            'wire': wire,
            'row_count': row_count,
            'wire_contains': tuple(wire_contains),
        }
        return build
    return register


def columns(rows, geohash=True, **column):
    """A two-column batch: one integer column and the designated
    timestamp, each with `rows` slots of value buffer behind it.

    Anything in `column` -- a forged `length` or `offset` -- is put on
    both, which keeps the timestamp column's own numbers honest against
    the same slice. The integer column keeps a GEOHASH claim so these
    streams also follow the production path that originally exposed the
    bug; native structural validation itself applies to every column.
    """
    return [
        dict(format=b'i', name=b'gh',
             data=struct.pack('<%di' % rows, *range(101, 101 + rows)),
             metadata=_GEOHASH_CLAIM if geohash else None, **column),
        dict(
            format=b'tsu:UTC', name=b'ts',
            data=struct.pack(
                '<%dq' % rows,
                *(_STAMP_MICROS + i * 1_000_000 for i in range(rows))),
            **column)]


# -- counts and child pointers -----------------------------------------
#
# Native validation reads these before Arrow import, independently of field
# metadata and row count. Two of the cases therefore carry no rows at all.

@_case(
    'root_column_count_cap_plus_one',
    message='Arrow schema root: root column count 4096 exceeds 4095')
def _root_column_count_cap_plus_one():
    return _RawArrowStream(
        columns(1, geohash=False), 1,
        schema_n_children=4096, batch_n_children=4096,
        poison_schema_children=True, poison_batch_children=True)


@_case(
    'schema_count_negative',
    message='Arrow schema root: n_children -1 is negative')
def _schema_count_negative():
    return _RawArrowStream(
        columns(1, geohash=False), 1,
        schema_n_children=-1, batch_n_children=-1)


@_case(
    'batch_count_negative',
    message='Arrow array root: n_children -1 is negative')
def _batch_count_negative():
    return _RawArrowStream(
        columns(1, geohash=False), 1, batch_n_children=-1)


@_case(
    'count_disagreement',
    message=('Arrow array root: n_children 3 disagrees with '
             'schema n_children 2'))
def _count_disagreement():
    return _RawArrowStream(
        columns(1, geohash=False), 1, batch_n_children=3)


@_case(
    'zero_column_schema_disagreement',
    message=('Arrow array root: n_children 2 disagrees with '
             'schema n_children 0'))
def _zero_column_schema_disagreement():
    """A zero-column schema is a shape the scan has nothing to do with,
    and the batch's own count still has to agree with it."""
    return _RawArrowStream(
        columns(1, geohash=False), 1, schema_n_children=0)


@_case(
    'zero_row_count_disagreement',
    message=('Arrow array root: n_children 4 disagrees with '
             'schema n_children 2'))
def _zero_row_count_disagreement():
    return _RawArrowStream(
        columns(1, geohash=False), 0, batch_n_children=4)


@_case(
    'null_schema_children',
    message=('Arrow schema root: declares 2 children but children '
             'pointer is NULL'))
def _null_schema_children():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_schema_children=True)


@_case(
    'null_batch_children',
    message=('Arrow array root: length 1 declares 2 children but '
             'children pointer is NULL'))
def _null_batch_children():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_batch_children=True)


@_case(
    'null_schema_child',
    message='Arrow schema root.children[0]: child pointer is NULL')
def _null_schema_child():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_schema_child=0)


@_case(
    'null_batch_child',
    message=('Arrow array root.children[1]: array or schema child '
             'pointer is NULL'))
def _null_batch_child():
    return _RawArrowStream(
        columns(1, geohash=False), 1, null_batch_child=1)


@_case(
    'null_batch_children_zero_rows',
    message=('Arrow array root: length 0 declares 2 children but '
             'children pointer is NULL'))
def _null_batch_children_zero_rows():
    """A batch with no rows still has its struct read, so an empty one
    cannot carry a malformed shape past the checks."""
    return _RawArrowStream(
        columns(1, geohash=False), 0, null_batch_children=True)


@_case(
    'null_schema_child_zero_rows',
    message='Arrow schema root.children[1]: child pointer is NULL')
def _null_schema_child_zero_rows():
    return _RawArrowStream(
        columns(1, geohash=False), 0, null_schema_child=1)


# -- unsupported nested columns and bounded metadata -------------------

def _nested_struct_stream(root_offset):
    leaf = {
        'format': b'i',
        'name': b'value',
        'length': 2,
        'data': struct.pack('<ii', 11, 22),
    }
    nested = {
        'format': b'+s',
        'name': b'nested',
        'length': 2,
        'buffers': [None],
        'children': [leaf],
    }
    return _RawArrowStream([nested], 1, root_offset)


@_case(
    'nested_struct_root_offset_zero',
    outcome='ArrowUnsupportedColumnKind',
    message='Arrow schema root.children[0]: Struct columns are not supported',
    at='server')
def _nested_struct_root_offset_zero():
    return _nested_struct_stream(0)


@_case(
    'nested_struct_root_offset_one',
    outcome='ArrowUnsupportedColumnKind',
    message='Arrow schema root.children[0]: Struct columns are not supported',
    at='server')
def _nested_struct_root_offset_one():
    return _nested_struct_stream(1)


@_case(
    'nested_struct_narrowed_to_zero_rows',
    outcome='ArrowUnsupportedColumnKind',
    message='Arrow schema root.children[0]: Struct columns are not supported',
    at='server')
def _nested_struct_narrowed_to_zero_rows():
    stream = _nested_struct_stream(0)
    stream._array.length = 0
    return stream


@_case(
    'three_level_nested_struct',
    outcome='ArrowUnsupportedColumnKind',
    message='Arrow schema root.children[0]: Struct columns are not supported',
    at='server')
def _three_level_nested_struct():
    leaf = {
        'format': b'i', 'name': b'value', 'length': 2,
        'data': struct.pack('<ii', 11, 22)}
    inner = {
        'format': b'+s', 'name': b'inner', 'length': 2,
        'buffers': [None], 'children': [leaf]}
    outer = {
        'format': b'+s', 'name': b'outer', 'length': 2,
        'buffers': [None], 'children': [inner]}
    return _RawArrowStream([outer], 1)


@_case(
    'fixed_size_list_short_child',
    message=('Arrow array root.children[0]: FixedSizeList slice offset 0 '
             '+ length 2 with size 2 ends at 4, beyond child 0 length 3'),
    at='server')
def _fixed_size_list_short_child():
    values = {
        'format': b'g', 'name': b'item', 'length': 3,
        'data': struct.pack('<ddd', 1.0, 2.0, 3.0)}
    fixed = {
        'format': b'+w:2', 'name': b'values', 'length': 2, 'offset': 0,
        'buffers': [None], 'children': [values]}
    return _RawArrowStream([fixed], 1, 1)


@_case(
    'list_missing_child',
    message=('Arrow schema root.children[0]: format requires exactly 1 '
             'normal child(ren) but declares 0'),
    at='server')
def _list_missing_child():
    return _RawArrowStream([
        {'format': b'+l', 'name': b'values', 'length': 1,
         'buffers': [None, struct.pack('<ii', 0, 0)]}
    ], 1)


@_case(
    'fixed_layout_buffer_count_below_exact',
    message='Arrow array root.children[0]: declares 1 buffers but Int32 requires exactly 2',
    at='server')
def _fixed_layout_buffer_count_below_exact():
    return _RawArrowStream([
        {'format': b'i', 'name': b'value', 'length': 1,
         'buffers': [None]}
    ], 1)


@_case(
    'fixed_layout_buffer_count_above_exact',
    message='Arrow array root.children[0]: declares 3 buffers but Int32 requires exactly 2',
    at='server')
def _fixed_layout_buffer_count_above_exact():
    return _RawArrowStream([
        {'format': b'i', 'name': b'value', 'length': 1,
         'buffers': [None, struct.pack('<i', 7), b'extra']}
    ], 1)


@_case(
    'view_buffer_count_below_minimum',
    message=('Arrow array root.children[0]: view layout requires 3..=16 '
             'buffers but declares 2'),
    at='server')
def _view_buffer_count_below_minimum():
    return _RawArrowStream([
        {'format': b'vu', 'name': b'value', 'length': 0,
         'buffers': [None, None]}
    ], 0)


@_case(
    'view_buffer_count_above_maximum',
    message=('Arrow array root.children[0]: view layout requires 3..=16 '
             'buffers but declares 17'),
    at='server')
def _view_buffer_count_above_maximum():
    return _RawArrowStream([
        {'format': b'vu', 'name': b'value', 'length': 0,
         'buffers': [None] * 17}
    ], 0)


@_case(
    'view_null_variadic_lengths_slot',
    message=('Arrow array root.children[0]: view variadic-lengths buffer '
             '(slot 3) is NULL'),
    at='server')
def _view_null_variadic_lengths_slot():
    return _RawArrowStream([
        {'format': b'vu', 'name': b'value', 'length': 1,
         'buffers': [None, b'\x00' * 16, b'x', None]}
    ], 1)


@_case(
    'utf8_null_offset_slot',
    message=('Arrow array root.children[0]: variable-width offset buffer '
             '(slot 1) is NULL'),
    at='server')
def _utf8_null_offset_slot():
    return _RawArrowStream([
        {'format': b'u', 'name': b'value', 'length': 1,
         'buffers': [None, None, b'x']}
    ], 1)


@_case(
    'metadata_oversized_key_length',
    message=('Arrow schema root.children[0]: metadata blob exceeds '
             '1048576 bytes'),
    at='server')
def _metadata_oversized_key_length():
    column = dict(
        format=b'i', name=b'value', data=struct.pack('<i', 1),
        metadata_blob=struct.pack('<ii', 1, 0x7fffffff))
    return _RawArrowStream([column], 1)


@_case(
    'metadata_oversized_value_length',
    message=('Arrow schema root.children[0]: metadata blob exceeds '
             '1048576 bytes'),
    at='server')
def _metadata_oversized_value_length():
    column = dict(
        format=b'i', name=b'value', data=struct.pack('<i', 1),
        metadata_blob=struct.pack('<iii', 1, 0, 0x7fffffff))
    return _RawArrowStream([column], 1)


def _metadata_stream(blob):
    return _RawArrowStream([
        dict(format=b'i', name=b'value', data=struct.pack('<i', 1),
             metadata_blob=blob)
    ], 1)


@_case(
    'metadata_negative_entry_count',
    message=('Arrow schema root.children[0]: metadata entry count -1 '
             'is negative'),
    at='server')
def _metadata_negative_entry_count():
    return _metadata_stream(struct.pack('<i', -1))


@_case(
    'metadata_entry_count_cap_plus_one',
    message=('Arrow schema root.children[0]: metadata declares 65537 '
             'entries, above maximum 65536'),
    at='server')
def _metadata_entry_count_cap_plus_one():
    return _metadata_stream(struct.pack('<i', 65537))


@_case(
    'metadata_negative_key_length',
    message=('Arrow schema root.children[0]: metadata entry 0 key '
             'length -2 is negative'),
    at='server')
def _metadata_negative_key_length():
    return _metadata_stream(struct.pack('<ii', 1, -2))


@_case(
    'metadata_negative_value_length',
    message=('Arrow schema root.children[0]: metadata entry 0 value '
             'length -3 is negative'),
    at='server')
def _metadata_negative_value_length():
    return _metadata_stream(struct.pack('<iii', 1, 0, -3))


# -- row shapes --------------------------------------------------------
#
# These exercise native length and offset validation. The GEOHASH metadata
# preserves the production route that first exposed the malformed slices;
# it does not gate the structural checks.

@_case(
    'batch_length_cap_plus_one',
    message='Arrow array root: length 16777217 exceeds 16777216')
def _batch_length_cap_plus_one():
    return _RawArrowStream(columns(1, length=1), 16777217, 0)


@_case(
    'batch_offset_cap_plus_one',
    message='Arrow array root: offset 16777217 exceeds 16777216')
def _batch_offset_cap_plus_one():
    return _RawArrowStream(columns(1, length=1), 1, 16777217)


@_case(
    'batch_offset_negative',
    message='Arrow array root: offset -1 is negative')
def _batch_offset_negative():
    return _RawArrowStream(columns(1, length=1), 1, -1)


@_case(
    'column_length_cap_plus_one',
    message=('Arrow array root.children[1]: length 16777217 exceeds '
             '16777216'))
def _column_length_cap_plus_one():
    return _RawArrowStream(columns(1, length=16777217), 1, 0)


@_case(
    'column_length_negative',
    message='Arrow array root.children[1]: length -1 is negative')
def _column_length_negative():
    return _RawArrowStream(columns(1, length=-1), 1, 0)


@_case(
    'column_offset_cap_plus_one',
    message=('Arrow array root.children[1]: offset 16777217 exceeds '
             '16777216'))
def _column_offset_cap_plus_one():
    return _RawArrowStream(columns(1, length=1, offset=16777217), 1, 0)


@_case(
    'column_offset_negative',
    message='Arrow array root.children[1]: offset -1 is negative')
def _column_offset_negative():
    return _RawArrowStream(columns(1, length=1, offset=-1), 1, 0)


@_case(
    'slice_past_column',
    message=('Arrow array root: Struct slice offset 1 + length 2 ends '
             'at 3, beyond child 0 length 2'))
def _slice_past_column():
    """One row past the column: `batch.offset + batch.length` comes to
    `col.length + 1`. Its twin, one row shorter, is sent for real by
    `test_a_batch_ending_exactly_at_the_column_end_is_accepted`."""
    return _RawArrowStream(columns(3, length=2), 2, 1)


@_case(
    'slice_at_column_end',
    outcome='success',
    at='ts',
    wire=True,
    row_count=2,
    wire_contains=(struct.pack('<qq', 102, 103),))
def accepted_slice():
    """The shape `slice_past_column` is one row longer than: a batch
    ending exactly where its columns do. Built here so the two differ in
    a single number."""
    return _RawArrowStream(columns(3, geohash=False, length=3), 2, 1)


def run_case(name, port):
    """Build the named shape and require its declared native outcome."""
    build = FORGED_CASES[name]
    expected = FORGED_EXPECTATIONS[name]
    stream = build()
    conf = (f'ws::addr=127.0.0.1:{port};lazy_connect=true;'
            f'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
    client = qi.QuestDB.from_conf(conf)
    try:
        try:
            at = qi.ServerTimestamp if expected['at'] == 'server' else expected['at']
            client.dataframe(stream, table_name='forged', at=at)
        except qi.QuestDBError as exc:
            if expected['outcome'] == 'success':
                raise AssertionError(
                    f'{name}: expected success, got {exc.code!r}: {exc}') from None
            expected_code = getattr(qi.QuestDBErrorCode, expected['outcome'])
            if exc.code is not expected_code:
                raise AssertionError(
                    f'{name}: refused as {exc.code!r}, expected '
                    f'{expected["outcome"]}: {exc}') from None
            if (expected['message'] is not None
                    and expected['message'] not in str(exc)):
                raise AssertionError(
                    f'{name}: expected diagnostic {expected["message"]!r}, '
                    f'got: {exc}') from None
        else:
            if expected['outcome'] != 'success':
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
