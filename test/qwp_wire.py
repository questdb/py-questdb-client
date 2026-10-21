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


#: Frame-header flag (byte 5) saying a delta symbol dictionary follows the
#: header, from `questdb-rs/src/ingress/column_sender/wire.rs`. WebSocket
#: frames set it; UDP datagrams do not.
QWP_FLAG_DELTA_SYMBOL_DICT = 0x08


def _read_symbol_dict_delta(payload):
    """``(delta_start, entries, end)`` of a QWP1 frame's delta symbol
    dictionary, with ``end`` the position just past it, or
    ``(0, [], 12)`` when the header's flag announces none."""
    if len(payload) < 12 or payload[:4] != b'QWP1':
        raise AssertionError('not a QWP1 frame')
    pos = 12
    if not payload[5] & QWP_FLAG_DELTA_SYMBOL_DICT:
        return 0, [], pos
    delta_start, pos = read_varint(payload, pos)
    delta_count, pos = read_varint(payload, pos)
    entries = []
    for _ in range(delta_count):
        entry_len, pos = read_varint(payload, pos)
        if pos + entry_len > len(payload):
            raise AssertionError('truncated QWP symbol dictionary')
        entries.append(bytes(payload[pos:pos + entry_len]))
        pos += entry_len
    return delta_start, entries, pos


def _first_table_start(payload):
    """Position of the first table block, past the frame header and, when
    the header's flag announces one, the delta symbol dictionary."""
    if len(payload) < 12 or payload[:4] != b'QWP1':
        raise AssertionError('not a QWP1 frame')
    if int.from_bytes(payload[6:8], 'little') < 1:
        raise AssertionError('QWP1 frame contains no table')
    return _read_symbol_dict_delta(payload)[2]


def extend_symbol_dict(dictionary, payload):
    """Add a QWP1 frame's delta symbol dictionary to `dictionary`, the
    connection's symbol values in id order.

    A frame refers to ids that earlier frames on the same connection
    introduced, so call this on every frame of the connection in order,
    before decoding the frame's tables. An entry for an id already in
    `dictionary`, as a reconnect's catch-up frame re-sends, must match it.
    """
    delta_start, entries, _ = _read_symbol_dict_delta(payload)
    for offset, entry in enumerate(entries):
        symbol_id = delta_start + offset
        value = entry.decode('utf-8')
        if symbol_id < len(dictionary):
            if dictionary[symbol_id] != value:
                raise AssertionError(
                    f'QWP symbol id {symbol_id} re-sent as {value!r}, '
                    f'was {dictionary[symbol_id]!r}')
        elif symbol_id == len(dictionary):
            dictionary.append(value)
        else:
            raise AssertionError(
                f'QWP symbol id {symbol_id} skips the ids from '
                f'{len(dictionary)}')


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
    return _first_table_schema(payload)[0]


def only_table_column_data(payload):
    """The column data of a frame that holds exactly one table: every
    byte after its schema, in column order."""
    if int.from_bytes(payload[6:8], 'little') != 1:
        raise AssertionError('QWP1 frame does not hold exactly one table')
    return payload[_first_table_schema(payload)[1]:]


def table_values(payload, symbol_dict=None):
    """Every table block of a QWP1 frame, in frame order, as
    ``(table name, {column name: [values]})``.

    Decodes only LONG, VARCHAR and SYMBOL columns without a null bitmap,
    which is what the row-splitting tests send. SYMBOL values resolve
    through `symbol_dict`, the connection's dictionary as
    `extend_symbol_dict` builds it.
    """
    pos = _first_table_start(payload)
    tables = []
    for _ in range(int.from_bytes(payload[6:8], 'little')):
        name_len, pos = read_varint(payload, pos)
        table_name = payload[pos:pos + name_len].decode('utf-8')
        pos += name_len
        row_count, pos = read_varint(payload, pos)
        column_count, pos = read_varint(payload, pos)
        schema = []
        for _ in range(column_count):
            name_len, pos = read_varint(payload, pos)
            schema.append(
                (payload[pos:pos + name_len].decode('utf-8'),
                 payload[pos + name_len]))
            pos += name_len + 1
        columns = {}
        for name, tag in schema:
            if payload[pos] != 0:
                raise AssertionError(f'column {name!r} has a null bitmap')
            pos += 1
            if tag == 0x05:
                values = [
                    int.from_bytes(
                        payload[pos + 8 * i:pos + 8 * i + 8], 'little',
                        signed=True)
                    for i in range(row_count)]
                pos += 8 * row_count
            elif tag == 0x0F:
                offsets = [
                    int.from_bytes(payload[pos + 4 * i:pos + 4 * i + 4],
                                   'little')
                    for i in range(row_count + 1)]
                pos += 4 * (row_count + 1)
                data = payload[pos:pos + offsets[-1]]
                values = [
                    data[offsets[i]:offsets[i + 1]].decode('utf-8')
                    for i in range(row_count)]
                pos += offsets[-1]
            elif tag == 0x09:
                if symbol_dict is None:
                    raise AssertionError(
                        f'column {name!r} is a SYMBOL column; pass the '
                        'connection symbol dictionary')
                values = []
                for _ in range(row_count):
                    symbol_id, pos = read_varint(payload, pos)
                    if symbol_id >= len(symbol_dict):
                        raise AssertionError(
                            f'QWP symbol id {symbol_id} is not in the '
                            'connection symbol dictionary')
                    values.append(symbol_dict[symbol_id])
            else:
                raise AssertionError(
                    f'column {name!r} of type {type_name(tag)} is not decoded')
            columns[name] = values
        tables.append((table_name, columns))
    if pos != len(payload):
        raise AssertionError('trailing bytes after the last QWP table')
    return tables


def _first_table_schema(payload):
    """The first table's ``(column name, type tag)`` pairs, and the
    position its column data starts at."""
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
    return columns, pos


def type_name(tag):
    """A readable name for a wire tag, so a stored grid is diffable by a
    human rather than by hex."""
    return WIRE_TYPES.get(tag, f'0x{tag:02X}')
