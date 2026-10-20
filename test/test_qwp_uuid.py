"""UUID byte-order and binary-type contracts of the bundled native client."""

import os
import time
import unittest
import uuid

import patch_path
import questdb._client as qi
from qwp_ws_ack_server import QwpAckServer

try:
    import pandas as pd
    import pyarrow as pa
except ImportError:
    pd = pa = None


UUIDS = [uuid.UUID('00112233-4455-6677-8899-aabbccddeeff'), None,
         uuid.UUID('123e4567-e89b-12d3-a456-426614174000'), uuid.UUID(int=0)]
UUID_BYTES = [u.bytes if u is not None else None for u in UUIDS]


def _inputs():
    # Object UUIDs use Python's manual planner; the other inputs use the
    # Arrow capsule path, with each supported way of claiming UUID bytes.
    yield 'object', pd.DataFrame({'u': UUIDS}), None
    if hasattr(pa, 'uuid'):
        yield 'extension', pa.table({'u': pa.ExtensionArray.from_storage(
            pa.uuid(), pa.array(UUID_BYTES, type=pa.binary(16)))}), None
    field = pa.field('u', pa.binary(16),
                     metadata={'questdb.column_type': 'uuid'})
    yield 'metadata', pa.Table.from_arrays(
        [pa.array(UUID_BYTES, type=field.type)], schema=pa.schema([field])), None
    for dtype in (pa.binary(16), pa.binary()):
        yield str(dtype), pa.table({'u': pa.array(UUID_BYTES, type=dtype)}), {'u': 'uuid'}


@unittest.skipIf(pd is None or pa is None, 'pandas/pyarrow not installed')
class TestQwpUuid(unittest.TestCase):
    def _capture(self, frame, overrides=None):
        with QwpAckServer(record_payloads=True) as server:
            with qi.QuestDB.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;') as client:
                client.dataframe(frame, table_name='uuids', at=qi.ServerTimestamp,
                                 schema_overrides=overrides)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        return b''.join(stats['binary_payloads'])

    def test_uuid_inputs_use_standard_bytes(self):
        for name, frame, overrides in _inputs():
            with self.subTest(source=name):
                payload = self._capture(frame, overrides)
                # The native API converts canonical bytes to wire order.
                # Supplying already-reversed bytes would reverse them twice.
                for value in UUIDS[:3]:
                    if value is not None:
                        self.assertIn(value.bytes[::-1], payload)
                        self.assertNotIn(value.bytes, payload)

    def test_unmarked_fixed_size_binary_policy_and_byte_order(self):
        for width in (8, 12, 16, 32):
            for manual in (False, True):
                with self.subTest(width=width, manual=manual):
                    value = bytes(range(width))
                    array = pa.array([value, None], type=pa.binary(width))
                    frame = (pd.DataFrame({
                        'seq': [0, 1],
                        'b': pd.Series(array, dtype=pd.ArrowDtype(array.type)),
                    }) if manual else pa.table({'b': array}))
                    # Width 8 is indistinguishable from exported polars
                    # Object handles. Pandas also refuses unlabelled
                    # widths 16/32, whose UUID/LONG256 metadata may have
                    # been lost. Native Arrow retains its field metadata
                    # and may send those unlabelled widths as BINARY.
                    if width == 8 or (manual and width in (16, 32)):
                        with self.assertRaises(qi.QuestDBError) as raised:
                            self._capture(frame)
                        expected_code = (
                            qi.QuestDBErrorCode.ArrowUnsupportedColumnKind
                            if width == 8 else qi.QuestDBErrorCode.BadDataFrame)
                        self.assertEqual(raised.exception.code, expected_code)
                        self.assertFalse(raised.exception.in_doubt)
                        continue
                    payload = self._capture(frame)
                    self.assertIn(value, payload)
                    self.assertNotIn(value[::-1], payload)

    def test_long256_override_preserves_little_endian_bytes(self):
        value = bytes(range(32))
        for dtype in (pa.binary(32), pa.binary()):
            with self.subTest(dtype=dtype):
                payload = self._capture(
                    pa.table({'b': pa.array([value, None], type=dtype)}),
                    {'b': 'long256'})
                self.assertIn(value, payload)

    def test_uuid_override_rejects_wrong_width(self):
        with self.assertRaises(qi.QuestDBError) as raised:
            self._capture(pa.table({'u': [b'too short']}), {'u': 'uuid'})
        self.assertFalse(raised.exception.in_doubt)


@unittest.skipUnless(
    os.environ.get('QDB_HTTP_ADDR') and pd is not None and pa is not None,
    'set QDB_HTTP_ADDR=host:port for a running QuestDB with pandas/pyarrow')
class TestQwpUuidLive(unittest.TestCase):
    def test_ingress_query_binds_and_both_result_formats(self):
        conf = f'ws::addr={os.environ["QDB_HTTP_ADDR"]};'
        for name, frame, overrides in _inputs():
            with self.subTest(source=name):
                table = 'uuid_' + uuid.uuid4().hex
                with qi.QuestDB.from_conf(conf) as client:
                    try:
                        client.dataframe(frame, table_name=table,
                                         at=qi.ServerTimestamp,
                                         schema_overrides=overrides)
                        deadline = time.monotonic() + 10
                        while True:
                            text = client.query(
                                f'SELECT cast(u AS STRING) u FROM {table}').to_arrow()
                            if text.num_rows == len(UUIDS):
                                break
                            self.assertLess(time.monotonic(), deadline)
                            time.sleep(0.05)
                        # Check server-side text, so symmetric bugs in ingest
                        # and query conversion cannot cancel each other out.
                        self.assertEqual(text.column('u').to_pylist(),
                                         [str(u) if u else None for u in UUIDS])
                        result = client.query(f'SELECT u FROM {table}').to_pandas()
                        self.assertEqual(result['u'].tolist(), UUIDS)
                        arrow = client.query(f'SELECT u FROM {table}').to_arrow()
                        column = arrow.column('u').combine_chunks()
                        if isinstance(column.type, pa.BaseExtensionType):
                            column = column.storage
                        self.assertEqual(column.to_pylist(), UUID_BYTES)
                        count = client.query(
                            f'SELECT count() FROM {table} WHERE u = $1',
                            [UUIDS[0]]).to_arrow()
                        self.assertEqual(count.column(0).to_pylist(), [1])
                    finally:
                        client.execute(f'DROP TABLE IF EXISTS {table}')
