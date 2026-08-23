#!/usr/bin/env python3
"""Re-entrancy grid: every outer call x every way back into the client.

Five review rounds of this client each found its worst defect in the
previous round's repair, and every miss was the same shape: a guard was
added for the routes somebody had thought of, and the route nobody
thought of was a use-after-free or a hang. Reading found none of them.
Enumerating found all of them.

So this enumerates. One axis is the calls that run the caller's Python
while they hold native state; the other is every public method and
property of every class that Python can reach, taken from
``api_surface`` rather than from anyone's memory. Each cell runs in its
own subprocess under a watchdog, and its outcome is one of:

``refused``
    The re-entered call raised. The good answer for anything that would
    corrupt or reorder what the outer call is doing.
``clean``
    It returned, and the outer call still ran to completion. Also a good
    answer -- for reads, and for calls that touch nothing the outer one
    holds.
``unreachable``
    The combination cannot be built, with the reason recorded. A
    ``SenderTransaction`` over QWP, say, or a reader lease with no live
    server.
``HANG``
    The watchdog fired. Worse for a caller than any refusal.
``CRASH``
    The interpreter died. The thing this grid exists to find.

The whole grid is stored as data in ``reentrancy_matrix_expected.json``
and diffed on re-run, so a change that turns a refusal into a hang is a
diff rather than a discovery.

Usage::

    python3 test/reentrancy_matrix.py              # run and diff
    python3 test/reentrancy_matrix.py --update     # rewrite the expected table
    python3 test/reentrancy_matrix.py --guard      # re-run 'clean' cells under Guard Malloc
    python3 test/reentrancy_matrix.py --outer NAME # one row
    python3 test/reentrancy_matrix.py --cell OUTER REENTERED   # one cell, in process
"""

import argparse
import concurrent.futures
import datetime
import ipaddress
import json
import os
import pathlib
import subprocess
import sys
import traceback

import patch_path  # noqa: F401  (sys.path fix-up)

import api_surface
import questdb._client as qi
from qwp_ws_ack_server import QwpAckServer
from mock_server import HttpServer, Server

PROJ_ROOT = pathlib.Path(__file__).parent.parent
EXPECTED_PATH = pathlib.Path(__file__).parent / 'reentrancy_matrix_expected.json'

#: A cell that has not answered in this long is a hang. Generous: the
#: slowest legitimate cell opens a socket and sends a frame.
CELL_TIMEOUT_S = 25.0

#: Guard Malloc slows a process down by one to two orders of magnitude.
GUARD_TIMEOUT_S = 120.0

RESULT_PREFIX = 'CELL_RESULT '


# ---------------------------------------------------------------------
# Scenario context
# ---------------------------------------------------------------------

class Ctx:
    """The objects a cell can re-enter through.

    An outer scenario fills in whatever it has. A cell whose target
    class is missing here is ``unreachable`` for that scenario, with the
    absence recorded rather than skipped silently.
    """

    def __init__(self):
        self.db = None
        self.sender = None
        self.buffer = None
        self.txn = None
        self.lease = None
        self.reader_lease = None
        self.reader_unreachable = None
        self.closeables = []

    def target_for(self, cls_name):
        if cls_name == 'PooledReader' and self.reader_lease is None:
            self._try_reader_lease()
        return {
            'QuestDB': self.db,
            'Sender': self.sender,
            'Buffer': self.buffer,
            'SenderTransaction': self.txn,
            'PooledSender': self.lease,
            'PooledReader': self.reader_lease,
        }[cls_name]

    def _try_reader_lease(self):
        """A reader lease needs a real handshake with a read endpoint,
        which no offline fixture serves. Borrowing one anyway records
        why the row is unreachable instead of leaving it blank."""
        if self.db is None:
            self.reader_unreachable = 'no QuestDB in this scenario'
            return
        try:
            self.reader_lease = self.db.reader()
        except Exception as exc:
            self.reader_unreachable = (
                f'a reader lease needs a live read endpoint, and the '
                f'offline fixture is a sender-side mock: '
                f'{type(exc).__name__}')


# ---------------------------------------------------------------------
# Hostile values -- the three windows where the caller's Python runs
# ---------------------------------------------------------------------

def hostile_at(hook):
    """A designated timestamp whose conversion runs `hook`.

    ``at`` is written last, so the row is part-way through and its
    rewind marker is held when the hook runs. Works on every protocol.
    """

    class HostileTz(datetime.tzinfo):
        fired = False

        def utcoffset(self, dt):
            if not HostileTz.fired:
                HostileTz.fired = True
                hook()
            return datetime.timedelta(0)

        def dst(self, dt):
            return datetime.timedelta(0)

        def tzname(self, dt):
            return 'HOSTILE'

    return datetime.datetime(2020, 1, 1, tzinfo=HostileTz())


def hostile_ipv4(hook):
    """An IPV4 cell whose conversion to an int runs `hook`, in the
    middle of the row rather than at its end. QWP protocols only."""

    class HostileAddr(ipaddress.IPv4Address):
        fired = False

        def __int__(self):
            if not HostileAddr.fired:
                HostileAddr.fired = True
                hook()
            return 0x01020304

    return HostileAddr('192.0.2.1')


def hostile_frame(hook, pd):
    """A frame whose ``attrs`` read runs `hook`, during the plan build:
    before a byte is written, and with every native capture already
    taken."""

    class HostileFrame(pd.DataFrame):
        fired = False

        @property
        def attrs(self):
            if not HostileFrame.fired:
                HostileFrame.fired = True
                hook()
            return {}

        @attrs.setter
        def attrs(self, value):
            pass

    return HostileFrame({
        'v': [1, 2],
        'ts': pd.to_datetime([0, 1], unit='s')})


def plain_frame(pd):
    return pd.DataFrame({
        'v': [1, 2],
        'ts': pd.to_datetime([0, 1], unit='s')})


def hostile_arrow_stream(hook, pa):
    """An Arrow producer that runs `hook` between batches -- inside the
    send, with the connection open and the schema borrowed."""

    table = pa.table({
        'v': pa.array([1, 2], pa.int64()),
        'ts': pa.array(
            [0, 1_000_000_000], pa.timestamp('ns'))})

    class Producer:
        fired = False

        def __arrow_c_stream__(self, requested_schema=None):
            if not Producer.fired:
                Producer.fired = True
                hook()
            return table.__arrow_c_stream__(requested_schema)

    return Producer()


# ---------------------------------------------------------------------
# Outer scenarios
# ---------------------------------------------------------------------

OUTER_SCENARIOS = {}


def outer(name, note):
    def register(fn):
        fn.note = note
        OUTER_SCENARIOS[name] = fn
        return fn
    return register


def _ilp_sender(ctx, stack):
    server = Server()
    server.__enter__()
    stack.append(server)
    sender = qi.Sender.from_conf(f'tcp::addr=localhost:{server.port};')
    sender.establish()
    server.accept()
    ctx.sender = sender
    return sender


def _http_sender(ctx, stack):
    """Transactions are an ILP/HTTP feature, so the two transaction rows
    of the grid need this rather than the tcp sender."""
    server = HttpServer()
    server.__enter__()
    stack.append(server)
    sender = qi.Sender(
        qi.Protocol.Http, '127.0.0.1', server.port, auto_flush=False)
    sender.establish()
    ctx.sender = sender
    return sender


def _qwp_sender(ctx, stack):
    server = QwpAckServer()
    server.__enter__()
    stack.append(server)
    sender = qi.Sender.from_conf(f'ws::addr=127.0.0.1:{server.port};')
    sender.establish()
    ctx.sender = sender
    return sender


def _handle(ctx, stack):
    server = QwpAckServer()
    server.__enter__()
    stack.append(server)
    db = qi.QuestDB.from_conf(
        f'ws::addr=127.0.0.1:{server.port};'
        'sender_pool_min=0;sender_pool_max=4;'
        'query_pool_min=0;pool_reap=manual;')
    ctx.db = db
    return db


@outer('Sender.row/ilp', 'a hostile `at` conversion, mid-row, over tcp')
def _outer_sender_row_ilp(ctx, stack, hook, deps):
    sender = _ilp_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    sender.row('outer', columns={'v': 1}, at=qi.TimestampNanos(1))
    sender.row('hostile', columns={'v': 2}, at=hostile_at(hook))


@outer('Sender.row/qwp', 'a hostile IPV4 cell, mid-row, over ws')
def _outer_sender_row_qwp(ctx, stack, hook, deps):
    sender = _qwp_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    sender.row('outer', columns={'v': 1}, at=qi.ServerTimestamp)
    sender.row('hostile', columns={'ip': hostile_ipv4(hook)},
               at=qi.ServerTimestamp)


@outer('Sender.dataframe/ilp',
       'a hostile `attrs` read during the row-serializing plan build')
def _outer_sender_dataframe_ilp(ctx, stack, hook, deps):
    pd = deps.require('pandas')
    sender = _ilp_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    sender.dataframe(hostile_frame(hook, pd), table_name='t', at='ts')


@outer('Sender.dataframe/ws-plan',
       'a hostile `attrs` read during the direct columnar plan build')
def _outer_sender_dataframe_ws(ctx, stack, hook, deps):
    pd = deps.require('pandas')
    sender = _qwp_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    sender.dataframe(hostile_frame(hook, pd), table_name='t', at='ts')


@outer('Sender.dataframe/ws-stream',
       'an Arrow producer that runs the hook between batches, with the '
       'connection open')
def _outer_sender_dataframe_stream(ctx, stack, hook, deps):
    pa = deps.require('pyarrow')
    sender = _qwp_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    sender.dataframe(hostile_arrow_stream(hook, pa), table_name='t', at='ts')


@outer('Buffer.row', 'a hostile IPV4 cell in a caller-owned QWP buffer')
def _outer_buffer_row(ctx, stack, hook, deps):
    sender = _qwp_sender(ctx, stack)
    buffer = sender.new_buffer()
    ctx.buffer = buffer
    buffer.row('outer', columns={'v': 1}, at=qi.ServerTimestamp)
    buffer.row('hostile', columns={'ip': hostile_ipv4(hook)},
               at=qi.ServerTimestamp)


@outer('Buffer.dataframe', 'a hostile `attrs` read from a buffer load')
def _outer_buffer_dataframe(ctx, stack, hook, deps):
    pd = deps.require('pandas')
    sender = _qwp_sender(ctx, stack)
    buffer = sender.new_buffer()
    ctx.buffer = buffer
    buffer.dataframe(hostile_frame(hook, pd), table_name='t', at='ts')


@outer('SenderTransaction.row', 'a hostile `at` conversion inside a txn')
def _outer_txn_row(ctx, stack, hook, deps):
    sender = _http_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    with sender.transaction('t') as txn:
        ctx.txn = txn
        txn.row(columns={'v': 1}, at=qi.TimestampNanos(1))
        txn.row(columns={'v': 2}, at=hostile_at(hook))


@outer('SenderTransaction.dataframe', 'a hostile `attrs` read inside a txn')
def _outer_txn_dataframe(ctx, stack, hook, deps):
    pd = deps.require('pandas')
    sender = _http_sender(ctx, stack)
    ctx.buffer = sender.new_buffer()
    with sender.transaction('t') as txn:
        ctx.txn = txn
        txn.dataframe(hostile_frame(hook, pd), at='ts')


@outer('QuestDB.dataframe', 'a hostile `attrs` read during a pooled load')
def _outer_handle_dataframe(ctx, stack, hook, deps):
    pd = deps.require('pandas')
    db = _handle(ctx, stack)
    # A lease held open across the load, so the grid also covers what a
    # lease call does while the handle it came from is mid-frame.
    ctx.lease = db.sender()
    db.dataframe(hostile_frame(hook, pd), table_name='t', at='ts')


@outer('PooledSender.row',
       "a hostile IPV4 cell in a lease's row -- the cell that hung")
def _outer_lease_row(ctx, stack, hook, deps):
    db = _handle(ctx, stack)
    lease = db.sender()
    ctx.lease = lease
    lease.row('outer', columns={'v': 1}, at=qi.ServerTimestamp)
    lease.row('hostile', columns={'ip': hostile_ipv4(hook)},
              at=qi.ServerTimestamp)


@outer('PooledSender.dataframe', "a hostile `attrs` read from a lease's load")
def _outer_lease_dataframe(ctx, stack, hook, deps):
    pd = deps.require('pandas')
    db = _handle(ctx, stack)
    lease = db.sender()
    ctx.lease = lease
    lease.dataframe(hostile_frame(hook, pd), table_name='t', at='ts')


# ---------------------------------------------------------------------
# Re-entered calls
# ---------------------------------------------------------------------

class MissingDep(Exception):
    """A scenario needs a package this interpreter does not have."""


class Deps:
    def require(self, name):
        try:
            return __import__(name)
        except ImportError:
            raise MissingDep(f'{name} is not installed')


def reentry_args(member, ctx, deps):
    """Plausible arguments for re-entering `member`.

    Deliberately arguments a caller might really pass: the point is what
    the call does to the outer one, not how it validates its input.
    """
    _, name = member.split('.', 1)
    if name in ('row',):
        if member.startswith('SenderTransaction.'):
            return (), {'columns': {'v': 9}, 'at': qi.ServerTimestamp}
        return ('reentered',), {'columns': {'v': 9},
                                'at': qi.ServerTimestamp}
    if name == 'dataframe':
        pd = deps.require('pandas')
        frame = plain_frame(pd)
        if member.startswith('SenderTransaction.'):
            return (frame,), {'at': 'ts'}
        return (frame,), {'table_name': 'reentered', 'at': 'ts'}
    if name == 'transaction':
        return ('reentered',), {}
    if name in ('query', 'execute'):
        return ('SELECT 1',), {}
    if name == 'await_acked_fsn':
        return (0,), {'timeout_millis': 1}
    if name == 'wait':
        return (), {'timeout_millis': 1}
    if name == 'reserve':
        return (1024,), {}
    if name == '__exit__':
        return (None, None, None), {}
    if name == 'reap_idle':
        return (), {}
    return (), {}


# ---------------------------------------------------------------------
# Cell driver
# ---------------------------------------------------------------------

def run_cell(outer_name, member):
    """Run one cell in this process and return its record."""
    cls_name = member.split('.', 1)[0]
    kind = dict(
        (name, k) for name, _, _, k in api_surface.qualified_members()
    )[member]
    scenario = OUTER_SCENARIOS[outer_name]
    ctx = Ctx()
    deps = Deps()
    stack = []
    record = {
        'outer': outer_name,
        'reentered': member,
        'result': 'unreachable',
        'reason': 'the hook never ran',
        'outer_outcome': None,
    }
    hook_ran = [False]

    def hook():
        hook_ran[0] = True
        target = ctx.target_for(cls_name)
        if target is None:
            record['result'] = 'unreachable'
            record['reason'] = (
                ctx.reader_unreachable if cls_name == 'PooledReader'
                and ctx.reader_unreachable
                else f'no {cls_name} in this scenario')
            return
        try:
            args, kwargs = reentry_args(member, ctx, deps)
        except MissingDep as exc:
            record['result'] = 'unreachable'
            record['reason'] = str(exc)
            return
        try:
            if kind == 'property':
                getattr(target, member.split('.', 1)[1])
            else:
                getattr(target, member.split('.', 1)[1])(*args, **kwargs)
        except qi.QuestDBError as exc:
            record['result'] = 'refused'
            record['reason'] = _short(exc)
        except (TypeError, ValueError, OSError) as exc:
            # Not a guard: the call was let through and failed on its own
            # terms. Recorded separately so a refusal cannot hide here.
            record['result'] = 'clean'
            record['reason'] = f'raised {type(exc).__name__}: {_short(exc)}'
        else:
            record['result'] = 'clean'
            record['reason'] = ''

    try:
        try:
            scenario(ctx, stack, hook, deps)
        except MissingDep as exc:
            record['result'] = 'unreachable'
            record['reason'] = str(exc)
            record['outer_outcome'] = 'not run'
            return record
        except qi.QuestDBError as exc:
            record['outer_outcome'] = f'raised {_short(exc)}'
        except Exception as exc:
            record['outer_outcome'] = (
                f'raised {type(exc).__name__}: {_short(exc)}')
        else:
            record['outer_outcome'] = 'completed'
        if not hook_ran[0] and record['result'] == 'unreachable':
            record['reason'] = (
                'the outer call never reached the hook: '
                + str(record['outer_outcome']))
    finally:
        _teardown(ctx, stack)
    return record


def _short(exc):
    text = ' '.join(str(exc).split())
    return text[:140]


def _teardown(ctx, stack):
    for obj in (ctx.reader_lease, ctx.lease, ctx.sender, ctx.db):
        if obj is None:
            continue
        try:
            obj.close()
        except Exception:
            pass
    for entered in reversed(stack):
        try:
            entered.__exit__(None, None, None)
        except Exception:
            pass


# ---------------------------------------------------------------------
# Grid driver
# ---------------------------------------------------------------------

def all_cells():
    members = [name for name, _, _, _ in api_surface.qualified_members()]
    return [(outer_name, member)
            for outer_name in sorted(OUTER_SCENARIOS)
            for member in members]


def _child_env(guard):
    env = dict(os.environ)
    env['TEST_QUESTDB_PATCH_PATH'] = '1'
    if guard and sys.platform == 'darwin':
        # Guard Malloc puts every allocation on its own page and unmaps
        # it on free, so a read of freed native memory is a SIGSEGV here
        # rather than stale bytes that look like a working frame.
        env['DYLD_INSERT_LIBRARIES'] = '/usr/lib/libgmalloc.dylib'
        env['MALLOC_PROTECT_BEFORE'] = '1'
    return env


def run_cell_subprocess(outer_name, member, guard=False):
    timeout = GUARD_TIMEOUT_S if guard else CELL_TIMEOUT_S
    cmd = [sys.executable, str(pathlib.Path(__file__).resolve()),
           '--cell', outer_name, member]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            env=_child_env(guard), cwd=str(pathlib.Path(__file__).parent))
    except subprocess.TimeoutExpired:
        return {
            'outer': outer_name,
            'reentered': member,
            'result': 'HANG',
            'reason': f'no answer in {timeout:.0f}s',
            'outer_outcome': None,
        }
    for line in proc.stdout.splitlines():
        if line.startswith(RESULT_PREFIX):
            record = json.loads(line[len(RESULT_PREFIX):])
            if guard:
                record['guard_malloc'] = 'clean'
            return record
    return {
        'outer': outer_name,
        'reentered': member,
        'result': 'CRASH',
        'reason': _crash_reason(proc),
        'outer_outcome': None,
    }


def _crash_reason(proc):
    if proc.returncode < 0:
        detail = f'killed by signal {-proc.returncode}'
    else:
        detail = f'exit {proc.returncode}'
    tail = ' '.join((proc.stderr or '').split())[-200:]
    return f'{detail}: {tail}' if tail else detail


def run_grid(cells, guard=False, workers=None):
    workers = workers or min(8, (os.cpu_count() or 4))
    results = {}
    done = 0
    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        futures = {
            pool.submit(run_cell_subprocess, outer_name, member, guard):
                (outer_name, member)
            for outer_name, member in cells}
        for future in concurrent.futures.as_completed(futures):
            outer_name, member = futures[future]
            results[f'{outer_name} | {member}'] = future.result()
            done += 1
            if done % 25 == 0 or done == len(cells):
                print(f'  {done}/{len(cells)} cells', file=sys.stderr,
                      flush=True)
    return dict(sorted(results.items()))


def summarize(results):
    counts = {}
    for record in results.values():
        counts[record['result']] = counts.get(record['result'], 0) + 1
    return counts


def to_table(results):
    """The stored form: outcome and reason per cell, nothing volatile.

    ``outer_outcome`` is kept because a cell that is `clean` only because
    the outer call died first is not the same result as one where both
    ran, and the difference is exactly the kind of thing a repair
    silently changes.
    """
    return {
        key: {
            'result': record['result'],
            'reason': record['reason'],
            'outer_outcome': record['outer_outcome'],
        }
        for key, record in results.items()
    }


def diff_tables(expected, actual):
    problems = []
    for key in sorted(set(expected) | set(actual)):
        want = expected.get(key)
        got = actual.get(key)
        if want is None:
            problems.append(f'NEW CELL  {key}: {got["result"]} ({got["reason"]})')
        elif got is None:
            problems.append(f'GONE      {key}: was {want["result"]}')
        elif want != got:
            problems.append(
                f'CHANGED   {key}:\n'
                f'    expected {want["result"]}: {want["reason"]}\n'
                f'             outer {want["outer_outcome"]}\n'
                f'    actual   {got["result"]}: {got["reason"]}\n'
                f'             outer {got["outer_outcome"]}')
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cell', nargs=2, metavar=('OUTER', 'REENTERED'),
                        help='run one cell in this process')
    parser.add_argument('--outer', help='run one row of the grid')
    parser.add_argument('--update', action='store_true',
                        help='rewrite the expected table from this run')
    parser.add_argument('--guard', action='store_true',
                        help="re-run every 'clean' cell under Guard Malloc")
    parser.add_argument('--workers', type=int, default=None)
    args = parser.parse_args()

    if args.cell:
        outer_name, member = args.cell
        try:
            record = run_cell(outer_name, member)
        except Exception:
            traceback.print_exc()
            record = {
                'outer': outer_name,
                'reentered': member,
                'result': 'CRASH',
                'reason': 'the harness itself raised; see stderr',
                'outer_outcome': None,
            }
        print(RESULT_PREFIX + json.dumps(record), flush=True)
        return 0

    cells = all_cells()
    if args.outer:
        cells = [c for c in cells if c[0] == args.outer]
        if not cells:
            print(f'no such outer call: {args.outer}', file=sys.stderr)
            return 2

    print(f'running {len(cells)} cells '
          f'({len(OUTER_SCENARIOS)} outer x '
          f'{len(api_surface.qualified_members())} re-entered)',
          file=sys.stderr)
    results = run_grid(cells, workers=args.workers)
    counts = summarize(results)
    print(f'  {counts}', file=sys.stderr)

    exit_code = 0
    bad = {k: v for k, v in results.items()
           if v['result'] in ('HANG', 'CRASH')}
    for key, record in bad.items():
        print(f'!! {record["result"]}  {key}: {record["reason"]}',
              file=sys.stderr)
    if bad:
        exit_code = 1

    if args.guard and sys.platform == 'darwin':
        clean = [(v['outer'], v['reentered']) for v in results.values()
                 if v['result'] == 'clean']
        print(f're-running {len(clean)} clean cells under Guard Malloc',
              file=sys.stderr)
        guarded = run_grid(clean, guard=True, workers=args.workers)
        for key, record in guarded.items():
            if record['result'] in ('HANG', 'CRASH'):
                print(f'!! GUARD MALLOC {record["result"]}  {key}: '
                      f'{record["reason"]}', file=sys.stderr)
                exit_code = 1
    elif args.guard:
        print('Guard Malloc is a macOS facility; skipped on '
              f'{sys.platform}', file=sys.stderr)

    table = to_table(results)
    if args.update:
        EXPECTED_PATH.write_text(json.dumps(table, indent=1) + '\n')
        print(f'wrote {EXPECTED_PATH}', file=sys.stderr)
        return exit_code

    if not EXPECTED_PATH.exists():
        print(f'{EXPECTED_PATH} does not exist; run with --update',
              file=sys.stderr)
        return 2
    expected = json.loads(EXPECTED_PATH.read_text())
    if args.outer:
        expected = {k: v for k, v in expected.items()
                    if k.split(' | ')[0] == args.outer}
    problems = diff_tables(expected, table)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(f'{len(problems)} cell(s) differ from the expected table',
              file=sys.stderr)
        exit_code = 1
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
