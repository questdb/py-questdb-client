#!/usr/bin/env python3
"""Object cells whose own code replaces them in the frame mid-call.

A pandas object column is read through the frame's backing array. Code
that a cell runs while it is being read -- a ``__class__`` lookup inside
``isinstance``, a property -- can replace that cell in the array, and with
it the array's only reference to the cell. Each case here builds such a
cell in an array the frame wraps without copying, and sends the frame
through ``QuestDB.dataframe()``. The call has to return or raise a
``QuestDBError``; it must not take the interpreter down.

Each case is one site where the client reads a cell and then runs code
the cell controls before it is done with it: the UUID, IPv4 and datetime
columnar builders, the object-column sniff, and the ``Geohash`` advice the
sniff gives. The other object-column sites run no Python between reading a
cell and its last use, so no cell can replace itself there.

The parent runs each case in its own interpreter with
``PYTHONMALLOC=debug``, which overwrites freed memory, so a read of a
freed cell fails every time rather than by chance. Run as a script it
builds one named case, sends it at the port it is given, and prints
``MARKER`` once the cell's code has run and the call has finished::

    TEST_QUESTDB_PATCH_PATH=1 PYTHONMALLOC=debug \\
        QUESTDB_HOSTILE_CELL_CASE=uuid_builder \\
        QUESTDB_HOSTILE_CELL_PORT=9009 python3 hostile_cells.py
"""
import sys

sys.dont_write_bytecode = True
import datetime
import ipaddress
import os
import uuid

import patch_path  # noqa: F401  -- puts `src` on the path when asked to

import numpy as np
import pandas as pd

import questdb._client as qi

MARKER = 'HOSTILE_CELL_CASE_FINISHED'

N = 4


class _Column:
    """An object column over an array this module owns, so a cell can
    drop the array's reference to itself.

    A cell only acts once it is armed, which happens after the frame is
    built: pandas reads ``__class__`` while it infers the column's type.
    """

    def __init__(self):
        self.arr = np.empty(N, dtype=object)
        self.armed = False
        self.fired = False

    def drop(self, cell):
        """Replace `cell` in the array, once."""
        if not self.armed or self.fired:
            return
        for i in range(N):
            if self.arr[i] is cell:
                self.arr[i] = None
                self.fired = True
                return

    def frame(self):
        ts = (np.arange(N, dtype=np.int64) * 10**9).astype('datetime64[ns]')
        return pd.DataFrame({'c': self.arr, 'ts': ts}, copy=False)


CASES = {}


def case(fn):
    CASES[fn.__name__] = fn
    return fn


@case
def uuid_builder():
    """The first cell routes the column to the UUID builder, whose
    ``isinstance`` check reads the second cell's ``__class__``."""
    col = _Column()

    class PassesForUuid:
        int = 0x1234

        @property
        def __class__(self):
            col.drop(self)
            return uuid.UUID

    col.arr[0] = uuid.UUID(int=1)
    col.arr[1] = PassesForUuid()
    return col


@case
def ipv4_builder():
    """An ``IPv4Address`` subclass is checked against ``IPv4Interface``,
    which reads its ``__class__``, before the builder takes its value."""
    col = _Column()

    class Address(ipaddress.IPv4Address):
        @property
        def __class__(self):
            col.drop(self)
            return ipaddress.IPv4Address

    col.arr[0] = ipaddress.IPv4Address('10.0.0.1')
    col.arr[1] = Address(0x0A000002)
    return col


@case
def datetime_builder():
    """The first cell routes the column to the datetime builder, whose
    ``isinstance`` check reads the second cell's ``__class__`` and then
    names its type in the refusal."""
    col = _Column()

    class NotADatetime:
        @property
        def __class__(self):
            col.drop(self)
            return NotADatetime

    col.arr[0] = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    col.arr[1] = NotADatetime()
    return col


@case
def sniff():
    """The sniff checks the first cell against ``uuid.UUID``, which reads
    its ``__class__``, and then goes on checking the same cell."""
    col = _Column()

    class Unsupported:
        @property
        def __class__(self):
            col.drop(self)
            return Unsupported

    col.arr[0] = Unsupported()
    return col


@case
def geohash_advice():
    """A ``Geohash`` cell is refused with advice that names its
    precision, read from the cell."""
    col = _Column()

    class Geohash(qi.Geohash):
        @property
        def precision(self):
            col.drop(self)
            return 5

    col.arr[0] = Geohash(1, 5)
    return col


def main():
    name = os.environ.get('QUESTDB_HOSTILE_CELL_CASE')
    port = os.environ.get('QUESTDB_HOSTILE_CELL_PORT')
    if not name or not port:
        sys.stderr.write(
            'set QUESTDB_HOSTILE_CELL_CASE and QUESTDB_HOSTILE_CELL_PORT\n')
        return 2
    col = CASES[name]()
    df = col.frame()
    conf = (f'ws::addr=127.0.0.1:{port};lazy_connect=true;'
            'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
    with qi.QuestDB.from_conf(conf) as db:
        col.armed = True
        try:
            db.dataframe(df, table_name='t', at='ts')
        except qi.QuestDBError:
            pass
    if not col.fired:
        sys.stderr.write(f'{name}: the cell never replaced itself\n')
        return 3
    print(MARKER)
    return 0


if __name__ == '__main__':
    sys.exit(main())
