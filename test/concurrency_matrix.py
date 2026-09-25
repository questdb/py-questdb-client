#!/usr/bin/env python3
"""Concurrency grid: every state another thread holds x every call.

`reentrancy_matrix.py` enumerates one axis: a call that runs the
caller's Python, and the way back in from inside it. Everything it
drives happens on one thread. That axis found the use-after-free and
the mid-row corruption it was built for.

It cannot see the other axis, and three blocking `close()` defects came
from there -- one of them reported twice before anyone reproduced it:

- a `close()` that stopped waiting handed the handle back, so a second
  thread's `close()` returned reporting success against a handle that
  was open and still lending;
- a `dataframe()` that was simply taking a while was counted as an
  unreturnable lease, and blamed as one;
- while a `close()` waited, every other thread was told the handle was
  closed -- a statement that stopped being true when the wait ran out.

None of those is re-entrancy. In each, one thread sits in a state and
another thread asks the handle a perfectly ordinary question. So this
enumerates that: one axis is the states a thread can be holding an
object in, the other is the same public surface, from
``api_surface``. `QuestDB` is the class this matters most for, because
it is the one whose docstring promises instances are safe to share
across threads.

A holder is parked, not raced. Each scenario stops a thread at a known
point -- inside the drain wait, inside a plan build, between Arrow
batches -- using the same hostile values the re-entrancy grid uses, and
keeps it there until the cell is done. So a cell is a fact about a
state, reproducible on re-run, rather than a sample of an interleaving.
Whether a state is reachable at all is a separate question the holder
answers once, on its own.

Each cell records two things:

``result``
    what the call on the asking thread did -- ``refused``, ``clean``,
    ``unreachable``, ``HANG`` or ``CRASH``, read exactly as in the
    re-entrancy grid.
``holder_outcome``
    what became of the parked thread after it was let go. A call that
    looks ``clean`` while breaking the thread it interrupted is the
    shape this column exists to show, and the re-entrancy grid's
    equivalent is uniformly ``completed`` where this one is not.

Usage::

    python3 test/concurrency_matrix.py               # run and diff
    python3 test/concurrency_matrix.py --update      # rewrite expected
    python3 test/concurrency_matrix.py --holder NAME # one row
    python3 test/concurrency_matrix.py --cell HOLDER CALLED
"""

import argparse
import concurrent.futures
import json
import os
import pathlib
import subprocess
import sys
import threading
import traceback

import patch_path  # noqa: F401  (sys.path fix-up)

import api_surface
import questdb._client as qi
from qwp_ws_ack_server import QwpAckServer

from reentrancy_matrix import (
    Ctx,
    Deps,
    MissingDep,
    hostile_arrow_stream,
    hostile_dt,
    hostile_frame,
    reentry_args,
    summarize,
)

EXPECTED_PATH = (pathlib.Path(__file__).parent
                 / 'concurrency_matrix_expected.json')

#: A cell that has not answered in this long is a hang. Generous: a
#: holder has to open a socket and reach its park before the cell
#: starts.
CELL_TIMEOUT_S = 40.0

#: How long a holder waits at its park, and how long we wait for it to
#: get there. Shorter than `CELL_TIMEOUT_S` so a scenario that never
#: parks is reported by the cell itself, naming the holder, rather than
#: as a watchdog kill that names nothing.
PARK_TIMEOUT_S = 15.0

#: `close()`'s own bound, shortened for every cell.
#:
#: Any cell may call `close()`, from any held state, and a held state
#: is by construction one the close has to wait for -- so the shipped
#: minute would outlast the watchdog and every such cell would be
#: reported as a hang that is really the bound doing its job. Set once
#: per cell rather than per holder, because which cells can reach the
#: wait is a question about the member axis, not the holder axis.
CLOSE_WAIT_LIMIT_S = 5.0

#: The bound the *holder's* close runs under, when a scenario parks a
#: thread inside one. Deliberately longer than any cell, and in force
#: only while that close starts: `close()` reads the bound once and
#: keeps the deadline, so raising it here and lowering it again before
#: the cell runs gives the holder a wait that cannot expire on its own
#: and the member a wait that always does.
#:
#: Two bounds rather than one, because a member call can itself be a
#: close. Under a single bound the two waits expire within
#: microseconds of each other, and whether the holder notices its own
#: deadline or the released lease first is settled by scheduling --
#: which makes `holder_outcome` a reading of machine load. Kept apart,
#: the holder is ended by the cell releasing what it waits for and
#: never by its own clock, so the cell records what the member did.
HOLDER_CLOSE_WAIT_LIMIT_S = 300.0

RESULT_PREFIX = 'CELL_RESULT '


def _short(exc):
    text = ' '.join(str(exc).split())
    return text[:140]


# ---------------------------------------------------------------------
# Parking a thread at a known point
# ---------------------------------------------------------------------

class Park:
    """A place a holder thread stops and stays until it is let go.

    Passed as the `hook` the hostile values in `reentrancy_matrix`
    already take, so the two grids stop threads in exactly the same
    places and a state named here means the same thing there.
    """

    def __init__(self, name):
        self.name = name
        self.reached = threading.Event()
        self.released = threading.Event()

    def hook(self):
        self.reached.set()
        # Bounded, so a cell that forgets to let the holder go is a
        # failure that names this park rather than a watchdog kill.
        self.released.wait(timeout=PARK_TIMEOUT_S)

    def await_parked(self):
        if not self.reached.wait(timeout=PARK_TIMEOUT_S):
            raise Unreachable(
                f'the holder never reached its park ({self.name}) in '
                f'{PARK_TIMEOUT_S:.0f}s')

    def let_go(self):
        self.released.set()


class Unreachable(Exception):
    """This state could not be set up, with the reason recorded."""


class Held:
    """The threads a holder scenario is running, and their parks."""

    def __init__(self):
        self.threads = []
        self.parks = []
        self.releases = []
        self.outcomes = {}

    def spawn(self, tag, fn):
        def run():
            try:
                fn()
                self.outcomes[tag] = 'completed'
            except qi.QuestDBError as exc:
                self.outcomes[tag] = f'raised {_short(exc)}'
            except BaseException as exc:  # noqa: BLE001 -- recorded, not handled
                self.outcomes[tag] = (
                    f'raised {type(exc).__name__}: {_short(exc)}')

        thread = threading.Thread(target=run, name=tag, daemon=True)
        self.threads.append((tag, thread))
        thread.start()
        return thread

    def park(self, name):
        park = Park(name)
        self.parks.append(park)
        return park

    def on_release(self, fn):
        """Something to undo once the cell has its answer.

        A thread waiting for a lease is only held there because the
        lease is out; handing it back is what lets the holder finish.
        Done after the call is scored, so what the call did is recorded
        against the state as it stood, and `holder_outcome` still says
        whether the holder came back.
        """
        self.releases.append(fn)

    def let_go_and_join(self):
        """Release every park, then wait for the holders.

        A holder still running when the cell is scored has not been
        interrupted -- it has not been let go yet -- so the outcome is
        only meaningful after this.
        """
        for park in self.parks:
            park.let_go()
        for release in self.releases:
            try:
                release()
            except Exception:
                pass
        stuck = []
        for tag, thread in self.threads:
            thread.join(timeout=PARK_TIMEOUT_S)
            if thread.is_alive():
                stuck.append(tag)
                self.outcomes[tag] = 'never returned'
        return stuck

    def summary(self):
        if not self.outcomes:
            return 'no holder ran'
        if len(self.outcomes) == 1:
            return next(iter(self.outcomes.values()))
        return '; '.join(f'{tag}={outcome}'
                         for tag, outcome in sorted(self.outcomes.items()))


# ---------------------------------------------------------------------
# Holder scenarios
# ---------------------------------------------------------------------

HOLDER_SCENARIOS = {}


def holder(name, note):
    def register(fn):
        fn.note = note
        HOLDER_SCENARIOS[name] = fn
        return fn
    return register


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


def _await_closing(db):
    """Block until a `close()` on another thread has published that the
    handle is closing.

    `reap_idle` reads the published handle under the lock that
    publishes it and refuses one that is closing, so it reports the
    state without changing it. Polling a state the client itself
    exposes is what makes the park a fact rather than a sleep.
    """
    deadline = threading.Event()
    for _ in range(int(PARK_TIMEOUT_S * 200)):
        try:
            db.reap_idle()
        except qi.QuestDBError as exc:
            if 'closing' in str(exc) or 'closed' in str(exc):
                return
        deadline.wait(timeout=0.005)
    raise Unreachable(
        f'no close() published a closing handle in {PARK_TIMEOUT_S:.0f}s')


@holder('QuestDB.close/lease-wait',
        'a second thread inside close(), waiting for a lease this one '
        'holds and will not return')
def _hold_close_lease_wait(ctx, stack, held, deps):
    db = _handle(ctx, stack)
    ctx.lease = db.sender()
    qi._debug_set_close_lease_wait_limit_s(HOLDER_CLOSE_WAIT_LIMIT_S)
    held.spawn('close', db.close)
    _await_closing(db)
    qi._debug_set_close_lease_wait_limit_s(CLOSE_WAIT_LIMIT_S)
    held.on_release(ctx.lease.close)


@holder('QuestDB.close/call-wait',
        'a second thread inside close(), waiting for a dataframe() a '
        'third thread is part-way through')
def _hold_close_call_wait(ctx, stack, held, deps):
    pd = deps.require('pandas')
    db = _handle(ctx, stack)
    park = held.park('dataframe plan build')
    frame = hostile_frame(park.hook, pd)
    held.spawn('dataframe',
               lambda: db.dataframe(frame, table_name='held', at='ts'))
    park.await_parked()
    qi._debug_set_close_lease_wait_limit_s(HOLDER_CLOSE_WAIT_LIMIT_S)
    held.spawn('close', db.close)
    _await_closing(db)
    qi._debug_set_close_lease_wait_limit_s(CLOSE_WAIT_LIMIT_S)


@holder('QuestDB.dataframe/in-flight',
        'a second thread part-way through dataframe(), holding an '
        'active use of the handle')
def _hold_handle_dataframe(ctx, stack, held, deps):
    pd = deps.require('pandas')
    db = _handle(ctx, stack)
    park = held.park('dataframe plan build')
    frame = hostile_frame(park.hook, pd)
    held.spawn('dataframe',
               lambda: db.dataframe(frame, table_name='held', at='ts'))
    park.await_parked()


@holder('QuestDB.dataframe/mid-stream',
        'a second thread between Arrow batches, with the connection '
        'open and the schema borrowed')
def _hold_handle_dataframe_stream(ctx, stack, held, deps):
    pa = deps.require('pyarrow')
    db = _handle(ctx, stack)
    park = held.park('between Arrow batches')
    producer = hostile_arrow_stream(park.hook, pa)
    held.spawn('dataframe',
               lambda: db.dataframe(producer, table_name='held', at='ts'))
    park.await_parked()


@holder('QuestDB.close/done',
        'a second thread has closed the handle and returned')
def _hold_close_done(ctx, stack, held, deps):
    db = _handle(ctx, stack)
    held.spawn('close', db.close)
    stuck = held.let_go_and_join()
    if stuck:
        raise Unreachable(f'the close never returned: {stuck}')
    if held.outcomes.get('close') != 'completed':
        raise Unreachable(
            f'the close did not complete: {held.outcomes.get("close")}')


#: A lease has thread affinity -- one lease per thread, on the thread
#: that created it -- so the two `PooledSender` rows below are a
#: configuration the client does not support. They are here because
#: `QuestDB.close()` has to wait for a lease however wrongly it is
#: being used, and because an unsupported call should still be refused
#: rather than corrupt something. Read those rows as "what happens",
#: not as "what is promised": a `clean` there is not a guarantee.
_LEASE_AFFINITY_NOTE = (
    'a lease is documented as belonging to one thread, so this state '
    'is unsupported use; the rows say what happens, not what is '
    'promised')


@holder('PooledSender.row/in-flight',
        'a second thread part-way through a row on a lease this one '
        'can also see -- ' + _LEASE_AFFINITY_NOTE)
def _hold_lease_row(ctx, stack, held, deps):
    db = _handle(ctx, stack)
    lease = db.sender()
    ctx.lease = lease
    ctx.buffer = None
    park = held.park('mid-row value conversion')
    value = hostile_dt(park.hook)
    held.spawn(
        'row',
        lambda: lease.row('held', columns={'v': 1, 'dt': value},
                          at=qi.ServerTimestamp))
    park.await_parked()


@holder('PooledSender.dataframe/in-flight',
        'a second thread part-way through dataframe() on a lease this '
        'one can also see -- ' + _LEASE_AFFINITY_NOTE)
def _hold_lease_dataframe(ctx, stack, held, deps):
    pd = deps.require('pandas')
    db = _handle(ctx, stack)
    lease = db.sender()
    ctx.lease = lease
    park = held.park('dataframe plan build')
    frame = hostile_frame(park.hook, pd)
    held.spawn('dataframe',
               lambda: lease.dataframe(frame, table_name='held', at='ts'))
    park.await_parked()


# ---------------------------------------------------------------------
# Cell driver
# ---------------------------------------------------------------------

def run_cell(holder_name, member):
    """Run one cell in this process and return its record."""
    cls_name = member.split('.', 1)[0]
    kind = dict(
        (name, k) for name, _, _, k in api_surface.qualified_members()
    )[member]
    scenario = HOLDER_SCENARIOS[holder_name]
    ctx = Ctx()
    deps = Deps()
    held = Held()
    stack = []
    original_limit = qi._debug_close_lease_wait_limit_s()
    qi._debug_set_close_lease_wait_limit_s(CLOSE_WAIT_LIMIT_S)
    record = {
        'holder': holder_name,
        'called': member,
        'result': 'unreachable',
        'reason': 'the holder never parked',
        'holder_outcome': None,
    }
    try:
        try:
            scenario(ctx, stack, held, deps)
        except MissingDep as exc:
            record['reason'] = str(exc)
            record['holder_outcome'] = 'not run'
            return record
        except Unreachable as exc:
            record['reason'] = f'the state could not be held: {exc}'
            record['holder_outcome'] = held.summary()
            return record

        target = ctx.target_for(cls_name)
        if target is None:
            record['result'] = 'unreachable'
            record['reason'] = (
                ctx.unreachable.get(cls_name)
                or f'no {cls_name} in this scenario')
        else:
            try:
                args, kwargs = reentry_args(member, ctx, deps)
            except MissingDep as exc:
                record['result'] = 'unreachable'
                record['reason'] = str(exc)
            else:
                _score_call(record, target, member, kind, args, kwargs)

        stuck = held.let_go_and_join()
        record['holder_outcome'] = held.summary()
        if stuck:
            # The asking thread got an answer and the holder did not
            # come back. That is worse for a caller than any refusal,
            # so it is not allowed to hide behind a `clean` result.
            record['result'] = 'HANG'
            record['reason'] = (
                f'the call answered ({record["reason"]}) but the held '
                f'thread never returned: {stuck}')
    finally:
        qi._debug_set_close_lease_wait_limit_s(original_limit)
        _teardown(ctx, held, stack)
    return record


def _score_call(record, target, member, kind, args, kwargs):
    name = member.split('.', 1)[1]
    try:
        if kind == 'property':
            getattr(target, name)
        else:
            getattr(target, name)(*args, **kwargs)
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


def _teardown(ctx, held, stack):
    for park in held.parks:
        park.let_go()
    for release in held.releases:
        try:
            release()
        except Exception:
            pass
    for _tag, thread in held.threads:
        thread.join(timeout=PARK_TIMEOUT_S)
    for obj in (ctx.query_result, ctx.reader_lease, ctx.lease,
                ctx.sender, ctx.db):
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
    return [(holder_name, member)
            for holder_name in sorted(HOLDER_SCENARIOS)
            for member in members]


def run_cell_subprocess(holder_name, member):
    cmd = [sys.executable, str(pathlib.Path(__file__).resolve()),
           '--cell', holder_name, member]
    env = dict(os.environ)
    env['TEST_QUESTDB_PATCH_PATH'] = '1'
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=CELL_TIMEOUT_S,
            env=env, cwd=str(pathlib.Path(__file__).parent))
    except subprocess.TimeoutExpired:
        return {
            'holder': holder_name,
            'called': member,
            'result': 'HANG',
            'reason': f'no answer in {CELL_TIMEOUT_S:.0f}s',
            'holder_outcome': None,
        }
    for line in proc.stdout.splitlines():
        if line.startswith(RESULT_PREFIX):
            return json.loads(line[len(RESULT_PREFIX):])
    return {
        'holder': holder_name,
        'called': member,
        'result': 'CRASH',
        'reason': _crash_reason(proc),
        'holder_outcome': None,
    }


def _crash_reason(proc):
    if proc.returncode < 0:
        detail = f'killed by signal {-proc.returncode}'
    else:
        detail = f'exit {proc.returncode}'
    tail = ' '.join((proc.stderr or '').split())[-200:]
    return f'{detail}: {tail}' if tail else detail


def run_grid(cells, workers=None):
    workers = workers or min(8, (os.cpu_count() or 4))
    results = {}
    done = 0
    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        futures = {
            pool.submit(run_cell_subprocess, holder_name, member):
                (holder_name, member)
            for holder_name, member in cells}
        for future in concurrent.futures.as_completed(futures):
            holder_name, member = futures[future]
            results[f'{holder_name} | {member}'] = future.result()
            done += 1
            if done % 25 == 0 or done == len(cells):
                print(f'  {done}/{len(cells)} cells', file=sys.stderr,
                      flush=True)
    return dict(sorted(results.items()))


def to_table(results):
    return {
        key: {
            'result': record['result'],
            'reason': record['reason'],
            'holder_outcome': record['holder_outcome'],
        }
        for key, record in results.items()
    }


def diff_tables(expected, actual):
    problems = []
    for key in sorted(set(expected) | set(actual)):
        want = expected.get(key)
        got = actual.get(key)
        if want is None:
            problems.append(
                f'NEW CELL  {key}: {got["result"]} ({got["reason"]})')
        elif got is None:
            problems.append(f'GONE      {key}: was {want["result"]}')
        elif want != got:
            problems.append(
                f'CHANGED   {key}:\n'
                f'    expected {want["result"]}: {want["reason"]}\n'
                f'             holder {want["holder_outcome"]}\n'
                f'    actual   {got["result"]}: {got["reason"]}\n'
                f'             holder {got["holder_outcome"]}')
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cell', nargs=2, metavar=('HOLDER', 'CALLED'),
                        help='run one cell in this process')
    parser.add_argument('--holder', help='run one row of the grid')
    parser.add_argument('--update', action='store_true',
                        help='rewrite the expected table from this run')
    parser.add_argument('--workers', type=int, default=None)
    args = parser.parse_args()

    if args.cell:
        holder_name, member = args.cell
        try:
            record = run_cell(holder_name, member)
        except Exception:
            traceback.print_exc()
            record = {
                'holder': holder_name,
                'called': member,
                'result': 'CRASH',
                'reason': 'the harness itself raised; see stderr',
                'holder_outcome': None,
            }
        print(RESULT_PREFIX + json.dumps(record), flush=True)
        return 0

    cells = all_cells()
    if args.holder:
        cells = [c for c in cells if c[0] == args.holder]
        if not cells:
            print(f'no such held state: {args.holder}', file=sys.stderr)
            return 2

    print(f'running {len(cells)} cells '
          f'({len(HOLDER_SCENARIOS)} held x '
          f'{len(api_surface.qualified_members())} called)',
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
    if args.holder:
        expected = {k: v for k, v in expected.items()
                    if k.split(' | ')[0] == args.holder}
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
