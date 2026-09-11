"""Decode a captured QWP1 frame far enough to name its column types.

A column's *wire type* is the only place the client's decisions about a
column actually land, so every test and grid that asks "what did this
frame really send?" answers it from here rather than from the client's
own opinion of itself.
"""


#: QWP wire type tags, from `questdb-rs/src/ingress/column_sender/wire.rs`.
#: A frame's type byte is the only place the client's decisions about a
#: column actually land, so a grid that stores names rather than hex is
#: a grid a person can read a diff of.
WIRE_TYPES = {
    0x01: 'BOOLEAN',
    0x02: 'BYTE',
    0x03: 'SHORT',
    0x04: 'INT',
    0x05: 'LONG',
    0x06: 'FLOAT',
    0x07: 'DOUBLE',
    0x09: 'SYMBOL',
    0x0A: 'TIMESTAMP',
    0x0B: 'DATE',
    0x0C: 'UUID',
    0x0D: 'LONG256',
    0x0E: 'GEOHASH',
    0x0F: 'VARCHAR',
    0x10: 'TIMESTAMP_NANOS',
    0x11: 'DOUBLE_ARRAY',
    0x13: 'DECIMAL64',
    0x14: 'DECIMAL128',
    0x15: 'DECIMAL256',
    0x16: 'CHAR',
    0x17: 'BINARY',
    0x18: 'IPV4',
}


def read_varint(payload, pos):
    value = 0
    shift = 0
    while True:
        if pos >= len(payload):
            raise AssertionError('truncated QWP varint')
        byte = payload[pos]
        pos += 1
        value |= (byte & 0x7f) << shift
        if byte & 0x80 == 0:
            return value, pos
        shift += 7
        if shift >= 64:
            raise AssertionError('oversize QWP varint')


def _first_table_start(payload):
    """Position of the first table block, past the frame header and the
    delta-symbol-dictionary prefix pooled row frames carry."""
    if len(payload) < 12 or payload[:4] != b'QWP1':
        raise AssertionError('not a QWP1 frame')
    if int.from_bytes(payload[6:8], 'little') < 1:
        raise AssertionError('QWP1 frame contains no table')
    pos = 12
    _delta_start, pos = read_varint(payload, pos)
    delta_count, pos = read_varint(payload, pos)
    for _ in range(delta_count):
        entry_len, pos = read_varint(payload, pos)
        pos += entry_len
        if pos > len(payload):
            raise AssertionError('truncated QWP symbol dictionary')
    return pos


def first_table_row_count(payload):
    """Decode the first table's row count from a captured QWP1 frame."""
    pos = _first_table_start(payload)
    table_name_len, pos = read_varint(payload, pos)
    pos += table_name_len
    if pos > len(payload):
        raise AssertionError('truncated QWP table name')
    row_count, _ = read_varint(payload, pos)
    return row_count


def first_table_column_types(payload):
    """The first table's ``(column name, type tag)`` pairs.

    The designated timestamp column carries an empty name.
    """
    pos = _first_table_start(payload)
    table_name_len, pos = read_varint(payload, pos)
    pos += table_name_len
    _row_count, pos = read_varint(payload, pos)
    column_count, pos = read_varint(payload, pos)
    columns = []
    for _ in range(column_count):
        name_len, pos = read_varint(payload, pos)
        name = payload[pos:pos + name_len]
        pos += name_len
        if pos >= len(payload):
            raise AssertionError('truncated QWP column schema')
        columns.append((name.decode('utf-8'), payload[pos]))
        pos += 1
    return columns


def type_name(tag):
    """A readable name for a wire tag, so a stored grid is diffable by a
    human rather than by hex."""
    return WIRE_TYPES.get(tag, f'0x{tag:02X}')
