#!/usr/bin/env python3
"""Claim grid: backings x claim kinds x planners, decoded off the wire.

``df.attrs['questdb']`` is how a frame read out of QuestDB says what its
columns *were*, so writing it back puts the same types in the same
columns. Whether that works is not a question about the client's
intentions -- it is a question about the byte on the wire, and the two
DataFrame planners answer it separately.

Two review rounds in a row produced a defect in this subsystem, and both
times reasoning about it went wrong where measuring would not have. So
this measures: every (backing, claim, planner) cell is driven through a
real ``QwpAckServer``, the frame it produces is decoded, and the wire
type and the diagnostic-notice count are recorded. Neither planner is
asked what it thinks it did.

Cells are stored in ``claim_matrix_expected.json`` and diffed on re-run,
so a submodule bump or a planner change that moves one column's type
shows up as a diff rather than as a silent re-type.

Usage::

    python3 test/claim_matrix.py            # run and diff
    python3 test/claim_matrix.py --update   # rewrite the expected table
    python3 test/claim_matrix.py --show KIND   # print one kind's rows
"""

import argparse
import contextlib
import ipaddress
import json
import logging
import pathlib
import sys
import uuid

import patch_path  # noqa: F401  (sys.path fix-up)

import questdb._client as qi
import qwp_wire
from qwp_ws_ack_server import QwpAckServer

EXPECTED_PATH = pathlib.Path(__file__).parent / 'claim_matrix_expected.json'

UUID_VALUE = uuid.UUID('12345678-1234-5678-1234-567812345678')
LONG256_BYTES = bytes(range(32))
IPV4_VALUE = ipaddress.IPv4Address('192.0.2.1')


# ---------------------------------------------------------------------
# Axis 1: how the column is backed
# ---------------------------------------------------------------------
#
# A backing is a (name, builder) pair. The builder returns the column's
# values in one concrete storage, which is what decides which planner
# the frame takes and what the claim has to work with.

def backings(pd, pa, np):
    """Every storage a claimed column plausibly arrives in.

    ``to_pandas()`` hands back the first three, one per ``dtype_backend``;
    the rest are what a caller builds by hand or gets from polars.

    ``str/pandas`` is the value in its text spelling: what a cast to
    VARCHAR reads back as, and what pandas 3 turns a column of Python
    strings into. Its dtype is ``StringDtype(storage='pyarrow')``, the
    one dtype the client lets through to the Arrow capsule path while
    holding no Arrow type itself -- so it is where a claim meets a
    column that cannot carry it on that path.

    Each entry pairs its builder with the storage its name promises,
    because ``pd.DataFrame`` converts some of what it is handed and
    `build_frame` holds every backing to its own description. A row
    measuring something other than what it is named is a row nobody is
    reading.
    """
    def is_arrow(dtype):
        return isinstance(dtype, pd.ArrowDtype)

    return {
        'object/py': (
            lambda values: pd.Series(values['py'], dtype=object),
            lambda dtype: dtype == object),
        'str/pandas': (
            lambda values: pd.array(
                values['text'], dtype=pd.StringDtype('pyarrow')),
            lambda dtype: isinstance(dtype, pd.StringDtype)),
        'arrow/native': (
            lambda values: pd.array(
                values['arrow'], dtype=pd.ArrowDtype(values['arrow'].type)),
            is_arrow),
        'arrow/binary': (
            lambda values: pd.array(
                pa.array(values['raw'], pa.binary()),
                dtype=pd.ArrowDtype(pa.binary())),
            is_arrow),
        'arrow/fsb': (
            lambda values: pd.array(
                pa.array(values['raw'], pa.binary(values['width']))
                if values['width'] else pa.array(values['raw'], pa.binary()),
                dtype=pd.ArrowDtype(
                    pa.binary(values['width']) if values['width']
                    else pa.binary())),
            is_arrow),
        'numpy/int': (
            lambda values: np.array(
                values['ints'], dtype=values['int_dtype']),
            lambda dtype: getattr(dtype, 'kind', '') in 'iu'),
        'arrow/int': (
            lambda values: pd.array(
                pa.array(values['ints'], values['arrow_int']),
                dtype=pd.ArrowDtype(values['arrow_int'])),
            is_arrow),
    }


# ---------------------------------------------------------------------
# Axis 2: what the claim says
# ---------------------------------------------------------------------

def claim_kinds():
    """Every ``kind`` the claim vocabulary accepts, plus the two shapes
    that are not a kind: no claim at all, and a kind no column can be."""
    return [
        None,
        {'kind': 'uuid'},
        {'kind': 'long256'},
        {'kind': 'ipv4'},
        {'kind': 'char'},
        {'kind': 'geohash', 'precision_bits': 20},
        {'kind': 'geohash', 'precision_bits': 60},
        {'kind': 'binary'},
        {'kind': 'not_a_kind'},
    ]


def claim_label(claim):
    if claim is None:
        return 'none'
    if 'precision_bits' in claim:
        return f"{claim['kind']}({claim['precision_bits']}b)"
    return claim['kind']


# ---------------------------------------------------------------------
# Axis 3: the planner
# ---------------------------------------------------------------------
#
# The Arrow capsule path takes a frame whose every column is
# Arrow-backed; anything else falls to the NumPy planner. A frame is
# steered onto one or the other by what its *other* column is made of,
# so the same claimed column can be measured on both.

PLANNERS = ('arrow', 'numpy')


def value_set(pa, np, kind):
    """One column's worth of values, in every storage the backings need."""
    if kind == 'uuid':
        # `pa.uuid()` (pyarrow 18+) is the route the changelog points
        # users at, so it is what 'native' means here when it exists.
        native = (pa.array([UUID_VALUE.bytes], pa.uuid())
                  if hasattr(pa, 'uuid')
                  else pa.array([UUID_VALUE.bytes], pa.binary(16)))
        return {
            'py': [UUID_VALUE],
            'text': [str(UUID_VALUE)],
            'arrow': native,
            'raw': [UUID_VALUE.bytes],
            'width': 16,
            'ints': [7],
            'int_dtype': np.int64,
            'arrow_int': pa.int64(),
        }
    if kind == 'long256':
        return {
            'py': [LONG256_BYTES],
            'text': ['0x' + LONG256_BYTES[::-1].hex()],
            'arrow': pa.array([LONG256_BYTES], pa.binary(32)),
            'raw': [LONG256_BYTES],
            'width': 32,
            'ints': [7],
            'int_dtype': np.int64,
            'arrow_int': pa.int64(),
        }
    if kind == 'ipv4':
        return {
            'py': [IPV4_VALUE],
            'text': [str(IPV4_VALUE)],
            'arrow': pa.array([0xC0000201], pa.uint32()),
            'raw': [b'\xc0\x00\x02\x01'],
            'width': 4,
            'ints': [0xC0000201],
            'int_dtype': np.uint32,
            'arrow_int': pa.uint32(),
        }
    if kind == 'char':
        # A CHAR column is one UTF-16 code unit, and egress hands it
        # back as `pa.uint16()` -- `system_test.py` pins that. The
        # shapes here are the ones a read-modify-write actually
        # produces, so `py`, `arrow` and the integer pair all carry the
        # code unit rather than the character it prints as. `text` is
        # the one shape that does not: a string column is what a caller
        # reaches for by hand, and it cannot carry the claim.
        return {
            'py': [ord('x')],
            'text': ['x'],
            'arrow': pa.array([ord('x')], pa.uint16()),
            'raw': [b'x\x00'],
            'width': 2,
            'ints': [ord('x')],
            'int_dtype': np.uint16,
            'arrow_int': pa.uint16(),
        }
    if kind == 'geohash':
        return {
            'py': [7],
            'text': ['7'],
            'arrow': pa.array([7], pa.int32()),
            'raw': [b'\x07\x00\x00\x00'],
            'width': 4,
            'ints': [7],
            'int_dtype': np.int32,
            'arrow_int': pa.int32(),
        }
    raise AssertionError(kind)


#: Which value set each claim is measured against. A claim is only
#: interesting over a column that could plausibly carry it, so the
#: source kind names the values and the claim names what is asked of
#: them -- including asking for something else entirely.
SOURCE_KINDS = ('uuid', 'long256', 'ipv4', 'char', 'geohash')


def build_frame(pd, pa, np, source_kind, backing_name, claim, planner):
    values = value_set(pa, np, source_kind)
    build, holds = backings(pd, pa, np)[backing_name]
    column = build(values)
    if planner == 'arrow':
        # Every column Arrow-backed, including the designated timestamp,
        # so the frame reaches the capsule path.
        stamps = pd.array(
            pa.array([0], pa.timestamp('ns')),
            dtype=pd.ArrowDtype(pa.timestamp('ns')))
    else:
        # A plain NumPy datetime column is enough to send the whole
        # frame to the NumPy planner.
        stamps = pd.to_datetime([0], unit='s')
    frame = pd.DataFrame({'c': column, 'ts': stamps})
    if not holds(frame['c'].dtype):
        raise AssertionError(
            f'the {backing_name} backing produced a '
            f'{frame["c"].dtype} column')
    if claim is not None:
        frame.attrs['questdb'] = {
            'version': 1, 'columns': {'c': dict(claim)}}
    return frame


# ---------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------

def measure(frame):
    """Send `frame` and report what actually went out.

    Returns ``(wire type name, notice count, error)``. An error is a
    result too: a claim the planner refuses outright is a different
    answer from one it drops with a log notice, and the two planners have
    disagreed on exactly that before.
    """
    with _questdb_log_notices() as notices:
        try:
            payload = send(frame)
        except Exception as exc:
            return None, len(notices), _short(exc)
    types = dict(qwp_wire.first_table_column_types(payload))
    return qwp_wire.type_name(types['c']), len(notices), None


@contextlib.contextmanager
def _questdb_log_notices():
    """Capture claim diagnostics without letting them fill grid output."""
    logger = logging.getLogger('questdb')
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            if (record.levelno >= logging.WARNING
                    and record.getMessage().startswith('questdb: column')):
                records.append(record)

    handler = Capture()
    old_level = logger.level
    old_propagate = logger.propagate
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.propagate = old_propagate
        logger.setLevel(old_level)


def _describe(record):
    """One cell's answer, for the disagreement report."""
    if record is None:
        return 'absent'
    return (f'{record["wire_type"] or "refused"}'
            f'/{record["notices"]}n')


def _short(exc):
    return ' '.join(f'{type(exc).__name__}: {exc}'.split())[:160]


def send(frame):
    with QwpAckServer(record_payloads=True) as server:
        conf = (f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
        with qi.QuestDB.from_conf(conf) as client:
            client.dataframe(frame, table_name='claim_grid', at='ts')
        stats = server.snapshot()
    if stats['errors']:
        raise AssertionError(f'mock server errors: {stats["errors"]}')
    return next(
        payload for payload in stats['binary_payloads']
        if int.from_bytes(payload[6:8], 'little') > 0)


def all_cells():
    for source_kind in SOURCE_KINDS:
        for backing_name in sorted(backings(None, None, None)):
            for claim in claim_kinds():
                for planner in PLANNERS:
                    yield source_kind, backing_name, claim, planner


def run_grid(pd, pa, np, only_kind=None):
    results = {}
    total = 0
    for source_kind, backing_name, claim, planner in all_cells():
        if only_kind and source_kind != only_kind:
            continue
        key = (f'{source_kind} | {backing_name} | '
               f'{claim_label(claim)} | {planner}')
        try:
            frame = build_frame(
                pd, pa, np, source_kind, backing_name, claim, planner)
        except Exception as exc:
            results[key] = {
                'wire_type': None,
                'notices': 0,
                'error': f'unbuildable: {_short(exc)}',
            }
            continue
        wire_type, notices, error = measure(frame)
        results[key] = {
            'wire_type': wire_type,
            'notices': notices,
            'error': error,
        }
        total += 1
        if total % 50 == 0:
            print(f'  {total} cells', file=sys.stderr, flush=True)
    return dict(sorted(results.items()))


def diff_tables(expected, actual):
    problems = []
    for key in sorted(set(expected) | set(actual)):
        want, got = expected.get(key), actual.get(key)
        if want is None:
            problems.append(f'NEW CELL  {key}: {got}')
        elif got is None:
            problems.append(f'GONE      {key}: was {want}')
        elif want != got:
            problems.append(
                f'CHANGED   {key}:\n'
                f'    expected {want}\n'
                f'    actual   {got}')
    return problems


def _answer(record):
    """The part of a cell that both planners have to agree on.

    Every field the record holds except the text of an error: the two
    planners phrase a refusal differently for the same reason, and
    holding them to one wording would report a disagreement on every
    refused cell. Whether there *was* an error counts, and so does the
    notice count -- a claim one planner drops with a notice and the
    other drops in silence sends the same column to the same type, and
    is still one frame given two answers.
    """
    if record is None:
        return None
    return (record['wire_type'], record['notices'], record['error'] is None)


def planner_disagreements(results):
    """Cells where the two planners answer the same frame differently.

    Not automatically wrong -- one documented divergence is inherited --
    but each one is a decision somebody made, and an undocumented new
    one is how a claim quietly changes a column's type.

    Read from both sides: a cell present under one planner and missing
    under the other is a disagreement too, and the more surprising kind.
    """
    out = []
    stems = []
    seen = set()
    for key in results:
        stem = key.rsplit(' | ', 1)[0]
        if stem not in seen:
            seen.add(stem)
            stems.append(stem)
    for stem in sorted(stems):
        arrow = results.get(f'{stem} | arrow')
        numpy_planner = results.get(f'{stem} | numpy')
        if _answer(arrow) != _answer(numpy_planner):
            out.append((stem, arrow, numpy_planner))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--show', metavar='SOURCE_KIND')
    args = parser.parse_args()

    try:
        import numpy as np
        import pandas as pd
        import pyarrow as pa
    except ImportError as exc:
        print(f'the claim grid needs pandas, pyarrow and numpy: {exc}',
              file=sys.stderr)
        return 2

    results = run_grid(pd, pa, np, only_kind=args.show)
    print(f'{len(results)} cells', file=sys.stderr)

    if args.show:
        for key, record in results.items():
            print(f'{key:60}  {record}')
        return 0

    disagreements = planner_disagreements(results)
    print(f'{len(disagreements)} planner disagreement(s)', file=sys.stderr)
    for key, arrow_record, numpy_record in disagreements:
        print(f'   {key}: arrow={_describe(arrow_record)} '
              f'numpy={_describe(numpy_record)}',
              file=sys.stderr)

    if args.update:
        EXPECTED_PATH.write_text(json.dumps(results, indent=1) + '\n')
        print(f'wrote {EXPECTED_PATH}', file=sys.stderr)
        return 0

    if not EXPECTED_PATH.exists():
        print(f'{EXPECTED_PATH} does not exist; run with --update',
              file=sys.stderr)
        return 2
    problems = diff_tables(json.loads(EXPECTED_PATH.read_text()), results)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(f'{len(problems)} cell(s) differ from the expected table',
              file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
