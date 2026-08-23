"""The public API surface of the guarded classes, by reflection.

Every list of methods this repo's re-entrancy work has relied on has been
wrong at least once, and each time it was a list somebody remembered
rather than one the interpreter produced. So the lists come from here.

Two consumers share it:

- ``reentrancy_matrix.py`` builds one grid axis from ``public_methods``,
  so the matrix cannot under-count the API.
- ``test.py``'s classification meta-test walks the same names and
  requires each to be either exercised by the grid or on an explicit
  allow-list with a reason.

A newly added method is therefore unclassified until someone says what
it does mid-call, and that shows up as a test failure at review time
rather than as a crash a few rounds later.
"""

import questdb._client as qi


#: The classes whose methods a caller can reach from inside another call
#: on this client -- from a column value's conversion, a ``df.attrs``
#: read, or an Arrow producer.
GUARDED_CLASSES = (
    qi.QuestDB,
    qi.Sender,
    qi.PooledSender,
    qi.PooledReader,
    qi.SenderTransaction,
    qi.Buffer,
)

#: Dunders that are part of the callable surface rather than protocol
#: plumbing. ``__enter__`` / ``__exit__`` because a ``with`` block is how
#: most callers close things; ``__len__`` / ``__bytes__`` because they
#: read live native state.
PUBLIC_DUNDERS = frozenset({
    '__enter__',
    '__exit__',
    '__len__',
    '__bytes__',
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
