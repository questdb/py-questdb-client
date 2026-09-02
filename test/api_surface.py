"""The public API surface a caller can re-enter, by reflection.

Every list this repo's re-entrancy work has relied on has been wrong at
least once, and each time it was a list somebody remembered rather than
one the interpreter produced. So both axes come from here: which classes
count, and which of their members count.

Three consumers share it:

- ``reentrancy_matrix.py`` builds one grid axis from
  ``qualified_members``, so the matrix cannot under-count the API.
- ``test.py``'s classification meta-test walks the same names and
  requires each to be either exercised by the grid or on an explicit
  allow-list with a reason.
- ``test.py`` also holds ``NOT_GUARDED`` to its own rule: an excuse has
  to name a class that still exists, and has to say something.

A newly added method is therefore unclassified until someone says what
it does mid-call, and a newly exported class arrives guarded, so its
whole surface arrives unclassified. Either shows up as a test failure at
review time rather than as a crash a few rounds later.
"""

import enum
import inspect

import questdb._client as qi


#: Classes this client exports that a caller cannot re-enter, each with
#: the reason. An entry here is a claim somebody made and can be argued
#: with. Everything else the module exports is guarded, so a new class
#: arrives guarded and its methods arrive unclassified -- which is a
#: test failure naming them, rather than a door nobody listed.
NOT_GUARDED = {
    'Char':
        'an immutable value read back through one property; it holds '
        'no native state and nothing on it reaches the client.',
    'DateMillis': 'as Char, plus two alternative constructors.',
    'Geohash': 'as Char, plus one alternative constructor.',
    'Long256': 'as Char.',
    'TimestampMicros': 'as DateMillis.',
    'TimestampNanos': 'as DateMillis.',
    'ConnectionEvent':
        'a record handed to a connection listener. Its attributes are '
        'read off a snapshot taken before the callback runs, so reading '
        'one touches nothing the call that produced it still holds.',
    'SenderError': 'as ConnectionEvent, for an error handler.',
    'ServerInfo': 'as ConnectionEvent, returned by `QuestDB.server_info`.',
    'ServerTimestampType':
        'the type of the `ServerTimestamp` sentinel. It carries no '
        'state and its only use is identity.',
}


def guarded_classes():
    """The classes whose methods a caller can reach from inside another
    call on this client -- from a column value's conversion, a
    ``df.attrs`` read, an Arrow producer, or a ``types_mapper``.

    Read out of the module rather than listed: `__all__` is not the
    whole public surface (``Buffer`` arrives from
    ``Sender.new_buffer()`` and is not exported), so this walks what the
    module actually defines.

    Two kinds of class drop out by rule rather than by name. An
    exception is raised and caught, never called into. An enum's members
    are constants. Neither can hold native state, so neither can be a
    way back in, and no list of them has to be kept current.
    """
    out = []
    for name in dir(qi):
        if name.startswith('_') or name in NOT_GUARDED:
            continue
        cls = getattr(qi, name, None)
        if not inspect.isclass(cls):
            continue
        if getattr(cls, '__module__', None) != qi.__name__:
            continue
        if issubclass(cls, BaseException) or issubclass(cls, enum.Enum):
            continue
        out.append(cls)
    return tuple(out)


GUARDED_CLASSES = guarded_classes()

#: Dunders that are part of the callable surface rather than protocol
#: plumbing. ``__enter__`` / ``__exit__`` because a ``with`` block is how
#: most callers close things; ``__len__`` / ``__bytes__`` because they
#: read live native state; ``__arrow_c_stream__`` because it hands a
#: live cursor to a consumer that runs its own code against it.
PUBLIC_DUNDERS = frozenset({
    '__enter__',
    '__exit__',
    '__len__',
    '__bytes__',
    '__arrow_c_stream__',
})

#: Cython plumbing that is visible on a cdef class but is not API.
_NOT_API = frozenset({'__pyx_vtable__', '__setstate__'})

#: Alternative constructors. They build a new object rather than acting
#: on one, so re-entering them says nothing about the call they were
#: re-entered from.
_CONSTRUCTORS = frozenset({'from_conf', 'from_env'})


def public_methods(cls):
    """Every callable a user of ``cls`` can reach, sorted.

    Read-only ``@property`` attributes drop out on their own: accessed on
    the class they are descriptors, and a descriptor is not callable.
    """
    names = []
    for name in dir(cls):
        if name in _NOT_API or name in _CONSTRUCTORS:
            continue
        if name.startswith('_') and name not in PUBLIC_DUNDERS:
            continue
        if not callable(getattr(cls, name, None)):
            continue
        names.append(name)
    return sorted(names)


def public_properties(cls):
    """Every read-only attribute of ``cls`` that runs code to answer.

    A property read reaches the native layer exactly as a method call
    does, so the grid treats it as one more way back in.
    """
    names = []
    for name in dir(cls):
        if name in _NOT_API or name.startswith('_'):
            continue
        if callable(getattr(cls, name, None)):
            continue
        names.append(name)
    return sorted(names)


def qualified_members():
    """``[('Sender.row', qi.Sender, 'row', 'method'), ...]``.

    The whole re-entrant surface: every public method and every property
    read, on every guarded class.
    """
    out = []
    for cls in GUARDED_CLASSES:
        for name in public_methods(cls):
            out.append((f'{cls.__name__}.{name}', cls, name, 'method'))
        for name in public_properties(cls):
            out.append((f'{cls.__name__}.{name}', cls, name, 'property'))
    return sorted(out, key=lambda entry: entry[0])
