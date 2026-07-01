################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2024 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

"""
Opt-in token persistence for :mod:`questdb.auth`.

By default :class:`~questdb.auth.OidcDeviceAuth` keeps its tokens in memory only,
so a restarted process must run the device flow again. A :class:`TokenStore`
persists the token state so the restarted process resumes from a saved refresh
token — one silent token-endpoint round-trip — instead of re-prompting.

:class:`FileTokenStore` is the default implementation: one plaintext JSON file
per identity, protected at rest by file permissions (``0600`` file, ``0700``
directory) rather than encryption — the same posture ``gcloud``, ``aws`` and
``gh`` take. Supply your own :class:`TokenStore` (backed by an OS keychain, a
KMS, or a vault) to encrypt the refresh token at rest.

The on-disk format (directory, file name, JSON schema, atomic-write and
lock-file protocols) is a deliberately **language-neutral contract** so the Java
QuestDB client and this one can share the same file. The Java client is the
reference implementation; this module mirrors it.
"""

from __future__ import annotations

import abc
import contextlib
import errno
import hashlib
import json
import math
import os
import socket
import stat
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ._errors import OidcConfigError, OidcError
from ._http import safe_urlparse

# Frozen cross-language contract. The schema version tags both the on-disk ``v``
# field and the canonical-string hash prefix (which also doubles as a domain
# tag), so a future format bump produces a different hash — hence a different
# file — rather than silently colliding with v1 entries. Derive the prefix from
# the version so the two can never drift apart.
_SCHEMA_VERSION = 1
_CANONICAL_PREFIX = f'questdb-oidc-token-v{_SCHEMA_VERSION}'

# Environment variable overriding the default token-store directory (the
# language-neutral analogue of the Java client's
# ``questdb.client.oidc.token.store.dir`` system property).
TOKEN_STORE_DIR_ENV = 'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR'

# Reject a token file larger than this; a real entry is a few KB even with a
# group-laden id token, so anything past this is corrupt or hostile and is not
# read into memory.
_MAX_FILE_BYTES = 1 << 20

# Wait this long (seconds) for the per-identity lock file before giving up and
# running without it (atomic replacement still guards integrity). Kept short
# because token() can take this lock on the latency-sensitive flush path: a real
# refresh round-trip is sub-second, so a peer not done within this budget is
# treated as too slow and we degrade to a lock-free refresh rather than stall.
_DEFAULT_LOCK_ACQUIRE_BUDGET = 3.0
# Treat a lock older than this (seconds) as abandoned by a crashed holder and
# steal it. Must stay comfortably above the longest a live holder can hold it
# (one refresh under the lock); OidcDeviceAuth caps its HTTP timeout at 120s and
# the worst-case hold is a few times that, so this 10-minute window stays safely
# above it.
_DEFAULT_LOCK_STALE = 600.0
_LOCK_POLL_SLICE = 0.05
# Floor on a configured ``lock_stale``. A lock may legitimately be held for the
# whole of one refresh under it: OidcDeviceAuth caps its HTTP timeout at 120s,
# applied per network leg, so the worst-case live hold is ~2x that (~240s) PLUS
# the save's two fsyncs (token file + directory) and scheduling slack. Set the
# floor above that whole envelope — not at the bare 240s network figure, which a
# ``lock_stale`` configured just past it (240.001) would slip under on a slow
# host — and reject a value at or below it. A shorter window would let
# ``_is_stale`` declare a LIVE holder's lock abandoned and a peer steal it
# mid-refresh, which the atomic steal cannot rescue (it guards two acquirers
# racing to break one *stale* lock, not a window so short a *live* lock reads as
# stale). The default (_DEFAULT_LOCK_STALE) sits comfortably above this.
_MIN_LOCK_STALE = 300.0

# Set once if the platform cannot enforce owner-only POSIX permissions on the
# token files (e.g. Windows), so the at-rest protection falls back to the
# directory's inherited ACL; warns the user once.
_warned_no_posix_perms = False
_warn_lock = threading.Lock()


def _warn_no_posix_perms_once() -> None:
    # Best-effort, once per process: the token store could not enforce 0600/0700,
    # so the persisted refresh token is protected only by the directory's
    # inherited ACL. ASCII-only, and never includes a path or token byte.
    global _warned_no_posix_perms
    with _warn_lock:
        if _warned_no_posix_perms:
            return
        _warned_no_posix_perms = True
    sys.stderr.write(
        'questdb client: the OIDC token store could not enforce owner-only '
        '(0600/0700) permissions on this filesystem; the persisted refresh '
        "token is protected only by the directory's default ACL. Back the "
        'store with an OS keychain for at-rest encryption.\n')


def _nonempty_str(value: Any) -> Optional[str]:
    """A persisted field as a non-empty ``str``, else ``None``.

    The file is attacker-writable, so a non-string (a JSON number/list from a
    hand-edited or hostile file) reads as absent rather than landing in a token
    field as a raw object, and an empty string — never a usable token — is
    likewise dropped.
    """
    return value if isinstance(value, str) and value else None


def _millis_to_seconds(value: Any) -> float:
    """An on-disk ``*_millis`` field as epoch/duration **seconds**, else ``0.0``.

    A non-numeric or non-finite value (a hostile file) reads as ``0.0``, which
    marks the entry expired so it falls through to a refresh rather than being
    served. ``bool`` is an ``int`` subclass but never a meaningful timestamp.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    try:
        result = float(value) / 1000.0
    except (OverflowError, ValueError):
        return 0.0
    # NaN / ±Inf are real floats (json.loads accepts bare NaN / Infinity), so the
    # divide above does not raise; map them to 0.0 (expired) per the contract
    # rather than let a non-finite timestamp reach the expiry math.
    if not math.isfinite(result):
        return 0.0
    return result


def _seconds_to_millis(value: Any) -> int:
    """Epoch/duration **seconds** as an on-disk ``*_millis`` int; the inverse of
    :func:`_millis_to_seconds`, mapping a non-finite value to ``0`` (expired).

    ``PersistedToken`` is public, so a direct caller of :meth:`FileTokenStore.save`
    could pass ``inf``/``nan`` — ``int(round(inf * 1000))`` raises ``OverflowError``
    and ``round(nan)`` raises ``ValueError``, escaping the store's ``OidcError``
    contract. Mapping them to ``0`` keeps ``save`` typed and makes a non-finite
    expiry read back as expired rather than valid-forever, exactly as the load
    side already does. ``bool`` is an ``int`` subclass but never a timestamp.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    try:
        seconds = float(value)
    except (OverflowError, ValueError):
        return 0  # e.g. an int too large to convert to float
    scaled = seconds * 1000.0
    # Check finiteness AFTER the *1000 scale, not before: NaN/±Inf (json.loads
    # accepts bare NaN/Infinity) AND a finite-but-huge value that overflows to inf
    # only once scaled (e.g. 1e306 * 1000) both land here. Checking `seconds`
    # alone would let the latter through, and int(round(inf)) then raises
    # OverflowError — escaping the store's OidcError contract (PersistedToken is
    # public, so a caller can pass such a value straight to save()).
    if not math.isfinite(scaled):
        return 0
    return int(round(scaled))


def _is_finite_number(value: Any) -> bool:
    """True if ``value`` is a finite real number (``int``/``float``).

    A non-numeric type, ``NaN``, ``±Inf``, or an ``int`` too large to convert to
    ``float`` (``math.isfinite`` raises ``OverflowError`` on it) all read as
    ``False``, so a caller can reject a non-finite duration before it reaches the
    lock-timing math. Mirrors ``_device._validate_positive_number``'s handling.
    """
    if not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (OverflowError, ValueError):
        return False


def _canonical_endpoint(url: str) -> str:
    """Canonicalise an endpoint URL for the cross-language store-key hash.

    ``scheme://host:port/path?query`` with the scheme and host lower-cased, the
    port always explicit (the device-flow default 443/80 when absent), a trailing
    slash stripped from the path, and the query preserved. A stable rendering that
    hashes to the same :class:`TokenStoreKey` across processes and language
    clients sharing this identity, and — crucially — that makes the SAME identity
    *distinctions* as the in-memory :attr:`OidcDeviceAuth.cache_key`
    (``_normalize_url`` + ``_normalize_scope``), so a token is never keyed one way
    in memory and a different way on disk. Mirrors the Java client's
    ``canonicalEndpoint``; keep the two in step (the on-disk hash is a
    cross-language contract). For the common case — no trailing slash, no query —
    this rendering is byte-for-byte unchanged, so cross-language sharing is
    unaffected there.
    """
    parts, explicit_port = safe_urlparse(url)
    scheme = (parts.scheme or '').lower()
    host = (parts.hostname or '').lower()
    # urllib strips the brackets off an IPv6 literal, which would make the
    # host:port boundary ambiguous ("::1:443") and, worse, diverge from the
    # bracketed authority form the Java client renders (URI.getHost() keeps the
    # brackets) — the two clients would then hash an IPv6 endpoint differently and
    # never share the file. Re-add them.
    if ':' in host:
        host = f'[{host}]'
    default_port = {'https': 443, 'http': 80}.get(scheme)
    port = explicit_port if explicit_port is not None else default_port
    # Strip a trailing slash (keeping at least '/'), so '…/token' and '…/token/'
    # are ONE identity — matching cache_key, which rstrip('/')s. Without this the
    # disk key splits one identity across two files on a trailing-slash spelling
    # difference, forcing a needless re-prompt after a restart.
    path = (parts.path or '/').rstrip('/') or '/'
    # Keep the query: a token endpoint that differs only by query string is a
    # different credential-routing target, so it must hash to a DIFFERENT file —
    # matching cache_key, which keeps the query. Dropping it (the old behaviour)
    # collided two distinct identities onto one file; _parse_and_verify then
    # compared only the query-stripped endpoint, so it couldn't tell them apart
    # and could serve one identity's token to the other.
    query = f'?{parts.query}' if parts.query else ''
    return f'{scheme}://{host}:{port}{path}{query}'


@dataclass(frozen=True)
class PersistedToken:
    """An immutable snapshot of the token state an
    :class:`~questdb.auth.OidcDeviceAuth` holds, passed to and from a
    :class:`TokenStore` so the device flow need not re-run after a process
    restart.

    ``expires_at`` is an absolute epoch-seconds value (not a monotonic reading),
    so it stays meaningful across a restart. ``token_ttl`` is the (clamped)
    lifetime that expiry was derived from. The token strings are kept out of
    ``repr`` so a credential can't leak into a log line or traceback.
    """

    access_token: Optional[str] = field(default=None, repr=False)
    id_token: Optional[str] = field(default=None, repr=False)
    refresh_token: Optional[str] = field(default=None, repr=False)
    expires_at: float = 0.0  # absolute epoch seconds; survives restart
    token_ttl: float = 0.0   # seconds; the lifetime expires_at was derived from


@dataclass(frozen=True)
class TokenStoreKey:
    """The non-secret identity a persisted token belongs to.

    The client id, the canonicalised token and device-authorization endpoints
    (see :func:`_canonical_endpoint`), the order-normalised scope (the
    space-joined sorted token set), the optional audience, whether the server
    expects groups encoded in the token, and the optional out-of-band issuer pin.
    A :class:`TokenStore` keys its entries by this so a token minted for one
    server / identity provider / scope / audience is never served to a process
    configured for another. The endpoint, scope and issuer fields must be passed
    already normalised — exactly as :class:`~questdb.auth.OidcDeviceAuth` builds
    them — so a directly-constructed key matches the same identity the auth
    object computes.

    :meth:`hash` is a stable lowercase-hex SHA-256 over a canonical,
    NUL-separated rendering of the fields — a file name (or opaque key) that is
    identical across client implementations (the Java client mirrors this), so
    several processes (and languages) sharing one identity address the same
    persisted entry. The fields are exposed (they are not secret) so a store can
    record and re-check them on load as a defence against a hash collision or a
    copied file.

    ``issuer`` participates in that on-load identity re-check (see
    :meth:`FileTokenStore.load` / ``_issuer_matches``) but **not** in
    :meth:`hash`: it is excluded from the file name so the cross-language
    addressing contract — and every existing token file — stays byte-identical,
    while a session pinned to a different issuer still never adopts another's
    token (two issuer-differing configs share a file but reject each other's
    contents on load). This mirrors the in-memory
    :attr:`~questdb.auth.OidcDeviceAuth.cache_key`, which also distinguishes the
    issuer, so the in-memory and on-disk identities agree on that axis.
    """

    client_id: str
    token_endpoint: str
    device_authorization_endpoint: str
    scope: str
    audience: Optional[str]
    groups_in_token: bool
    # Optional out-of-band issuer pin. Default keeps existing positional
    # construction (and the frozen cross-language file-name hash) unchanged.
    issuer: Optional[str] = None

    def hash(self) -> str:
        """A stable lowercase-hex SHA-256 of the canonical identity string."""
        # NUL-separate the fields so no field value can be confused with a
        # separator (an OAuth client id, url, scope or audience never contains a
        # NUL). The prefix tags the domain and schema version.
        #
        # `issuer` is deliberately NOT folded in here: the file name is a frozen
        # cross-language contract (the Java client mirrors it), so adding a field
        # would change every entry's hash and break addressing. Issuer isolation
        # is enforced on load instead (the in-file fingerprint re-check), which
        # keeps the file name stable while still never serving one issuer's token
        # to a session pinned to another.
        canonical = '\x00'.join((
            _CANONICAL_PREFIX,
            self.client_id or '',
            self.token_endpoint or '',
            self.device_authorization_endpoint or '',
            self.scope or '',
            self.audience or '',
            '1' if self.groups_in_token else '0',
        ))
        return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


class TokenStore(abc.ABC):
    """Persists the token state of an :class:`~questdb.auth.OidcDeviceAuth`.

    A restarted process resumes from a saved refresh token instead of running
    the interactive device flow again. Persistence is opt-in: an
    ``OidcDeviceAuth`` with no store keeps its tokens in memory only (the
    previous behaviour).

    The default implementation is :class:`FileTokenStore`. Supply your own to
    back persistence with an OS keychain, a secrets manager, or a vault — for
    example to encrypt the refresh token at rest, which the file store does not
    do.

    Calls are made while ``OidcDeviceAuth`` holds its own instance lock, so an
    implementation need not be thread-safe against concurrent calls from one
    ``OidcDeviceAuth`` instance; it does, however, share its backing storage
    with other processes (and other language clients), so it must keep a
    concurrent reader from observing a half-written entry. A store reports a
    failure by **raising**; ``OidcDeviceAuth`` treats persistence as best-effort
    and a raised failure as non-fatal — it warns to ``stderr`` and continues
    with the in-memory token, which is valid regardless of whether it could be
    saved.
    """

    @abc.abstractmethod
    def load(self, key: TokenStoreKey) -> Optional[PersistedToken]:
        """Load the persisted token for this identity, or ``None`` if there is
        none usable (no entry, an entry that does not match ``key``, or one that
        cannot be read as a valid token). A ``None`` return makes
        ``OidcDeviceAuth`` fall back to a refresh or an interactive sign-in, so
        an unreadable or stale entry is recoverable rather than fatal.

        **Security — the implementation MUST re-verify identity.**
        ``OidcDeviceAuth`` does not re-check the returned token against ``key``;
        it trusts ``load`` to only ever return an entry stored under the *same*
        identity. A store addressed solely by :meth:`TokenStoreKey.hash` must
        therefore also record the identity fields in the persisted payload and
        re-compare them on load (as :class:`FileTokenStore` does), so a hash
        collision, a copied secret, or a swapped backing entry cannot serve one
        identity's token to a session configured for another — returning a
        wrong-identity token here routes that credential onto the wire."""

    @abc.abstractmethod
    def save(self, key: TokenStoreKey, token: PersistedToken) -> None:
        """Persist (atomically replace) the token for this identity."""

    @abc.abstractmethod
    def clear(self, key: TokenStoreKey) -> None:
        """Remove any persisted entry for this identity. A no-op when nothing is
        stored. Called from :meth:`~questdb.auth.OidcDeviceAuth.clear`."""

    def in_lock(self, key: TokenStoreKey, action: Callable[[], Any]) -> Any:
        """Run ``action`` while holding a cross-process lock scoped to ``key``,
        so a refresh by another process sharing this identity is observed rather
        than raced, and return its result.

        The default runs ``action`` with no locking, which is correct for a
        single process or a non-rotating refresh token; :class:`FileTokenStore`
        overrides it with a lock-file protocol. **Most stores should NOT override
        this** — the no-op default is correct unless the backend is shared across
        processes *and* the IdP rotates the refresh token on each refresh. Note
        ``action`` re-enters this same store (it calls :meth:`load` and
        :meth:`save` on it). An implementation that cannot acquire the lock should
        run ``action`` anyway (degrade) rather than fail a sign-in.
        """
        return action()


class FileTokenStore(TokenStore):
    """The default :class:`TokenStore`: one plaintext JSON file per identity.

    The refresh token is protected at rest by file permissions (``0600`` file,
    ``0700`` directory) rather than encryption — matching ``gcloud``, ``aws`` and
    ``gh``; for encryption at rest supply a :class:`TokenStore` backed by an OS
    keychain or a secrets manager instead.

    The default location is ``${user.home}/.questdb/oidc-tokens/``, overridable
    with the ``QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR`` environment variable. The
    file name is ``<TokenStoreKey.hash()>.json``, so several identities coexist
    and the name leaks neither the endpoint nor the client id. The on-disk format
    is a language-neutral contract so other QuestDB clients can share the file.

    **Integrity (always).** :meth:`save` writes a sibling temp file then
    atomically renames it over the target, so a crash or an overlapping reader —
    in any process or language — sees the whole old or whole new file, never a
    torn credential.

    **Rotating refresh tokens.** :meth:`in_lock` serialises the
    read-refresh-write of a token refresh across processes with an
    ``O_CREAT|O_EXCL`` lock file (``<hash>.lock``) — not an OS advisory lock,
    which a Java ``FileLock`` and a Python ``flock`` cannot reliably share. It
    steals a stale lock left by a crashed holder, and degrades to running without
    the lock (integrity is still protected) rather than stall a sign-in if it
    cannot acquire one.

    The store never writes a token value into a log or an exception message; only
    file paths and OS error kinds may surface.
    """

    def __init__(
            self,
            directory: Any,
            *,
            lock_acquire_budget: float = _DEFAULT_LOCK_ACQUIRE_BUDGET,
            lock_stale: float = _DEFAULT_LOCK_STALE):
        """
        :param directory: the directory to hold the token files; created on first
            write with owner-only permissions.
        :param lock_acquire_budget: how long (seconds) :meth:`in_lock` waits to
            acquire a peer's lock before degrading to a lock-free refresh rather
            than stalling a sign-in.
        :param lock_stale: a lock older than this (seconds) is treated as
            abandoned by a crashed holder and stolen. It MUST exceed the longest
            a live holder can hold the lock (one refresh under the lock: up to
            ~240s of network — twice ``OidcDeviceAuth``'s 120s HTTP-timeout cap —
            plus the save's two fsyncs and scheduling slack), so a value at or
            below that envelope (the ``_MIN_LOCK_STALE`` floor) is rejected to
            keep a peer from stealing a live holder's lock mid-refresh; the
            default (600s) stays safely above it.
        """
        if not directory:
            raise OidcConfigError('the token store directory is required')
        # Require finite, positive timings. inf passes the bare `> 0` /
        # `> _MIN_LOCK_STALE` comparisons — and an infinite staleness window means
        # a crashed holder's lock is NEVER judged stale, so its identity degrades
        # to lock-free coordination forever — so reject non-finite up front
        # (nan already fails the comparisons, since `nan > x` is False). The
        # finiteness check is first so it short-circuits before comparing a huge
        # int (whose `math.isfinite` would itself raise).
        if not (_is_finite_number(lock_acquire_budget) and lock_acquire_budget > 0):
            raise OidcConfigError(
                'the token store lock_acquire_budget must be a positive, finite '
                'number of seconds')
        # A staleness window at or below the worst-case live hold makes a
        # freshly acquired lock look abandoned, so acquirers would steal each
        # other's LIVE locks. Require it finite and above _MIN_LOCK_STALE (and so,
        # transitively, positive).
        if not (_is_finite_number(lock_stale) and lock_stale > _MIN_LOCK_STALE):
            raise OidcConfigError(
                'the token store lock_stale must be a finite number above '
                f'{_MIN_LOCK_STALE:g} seconds (the worst-case time a live '
                'holder can hold the lock during a refresh); a shorter window '
                "would let a peer steal a live holder's lock mid-refresh.")
        self._directory = os.fspath(directory)
        self._lock_acquire_budget = lock_acquire_budget
        self._lock_stale = lock_stale

    @classmethod
    def at(cls, directory: Any) -> 'FileTokenStore':
        """A store rooted at the given directory."""
        return cls(directory)

    @classmethod
    def at_default_location(cls) -> 'FileTokenStore':
        """A store at ``$QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR`` if that environment
        variable is set, otherwise at ``${user.home}/.questdb/oidc-tokens/``."""
        override = os.environ.get(TOKEN_STORE_DIR_ENV)
        if override:
            return cls(override)
        home = os.path.expanduser('~')
        # expanduser returns the literal '~' when the home directory can't be
        # resolved (no HOME/USERPROFILE and no passwd entry, e.g. a distroless
        # container). Joining onto it would yield a RELATIVE path and silently
        # create a surprise directory named '~' under the cwd, so fail clearly
        # and point at the override instead.
        if not os.path.isabs(home):
            raise OidcConfigError(
                'could not resolve the home directory for the default OIDC '
                f'token-store location; set the {TOKEN_STORE_DIR_ENV} '
                'environment variable to an absolute path, or construct '
                'FileTokenStore(directory) explicitly.')
        return cls(os.path.join(home, '.questdb', 'oidc-tokens'))

    def load(self, key: TokenStoreKey) -> Optional[PersistedToken]:
        path = self._token_file(key)
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return None
        except OSError as e:
            # A path that is not a usable regular file — a symlink loop (ELOOP)
            # or a non-directory path component (ENOTDIR) — is "no usable
            # entry", not a fatal error: per load()'s contract, fall back to a
            # refresh / interactive sign-in rather than raise. A genuine I/O or
            # permission error (EACCES, EIO, ...) still surfaces as OidcError.
            if e.errno in (errno.ELOOP, errno.ENOTDIR):
                return None
            raise OidcError(
                f'could not read the OIDC token store file: {e}') from e
        # A directory (or other non-regular file) planted at the token path —
        # by another tool, or a hostile co-tenant with write access to the store
        # dir — is not a usable entry. Ignore it rather than fall through to
        # open(), which would raise IsADirectoryError and escape load()'s "an
        # unreadable entry returns None, not a fatal error" contract.
        if not stat.S_ISREG(st.st_mode):
            return None
        # An empty or implausibly large file is not a usable entry; ignore it
        # rather than read it into memory.
        if st.st_size <= 0 or st.st_size > _MAX_FILE_BYTES:
            return None
        try:
            with open(path, 'rb') as f:
                data = f.read(_MAX_FILE_BYTES + 1)
        except FileNotFoundError:
            return None
        except IsADirectoryError:
            # The regular file became a directory between the stat above and
            # this open (a TOCTOU); treat it as no usable entry, as above.
            return None
        except OSError as e:
            raise OidcError(
                f'could not read the OIDC token store file: {e}') from e
        if len(data) > _MAX_FILE_BYTES:
            return None
        return self._parse_and_verify(key, data)

    def save(self, key: TokenStoreKey, token: PersistedToken) -> None:
        content = self._serialize(key, token)
        try:
            self._ensure_directory()
            target = self._token_file(key)
            # mkstemp creates the temp file with 0600 (O_CREAT|O_EXCL, mode 0600)
            # on POSIX, so there is no world-readable window before the rename.
            fd, tmp = tempfile.mkstemp(
                prefix=key.hash(), suffix='.tmp', dir=self._directory)
            moved = False
            try:
                # Force the payload to disk before the rename, so a crash between
                # the write and the atomic rename cannot leave the target
                # pointing at unflushed (zero/partial) bytes.
                with os.fdopen(fd, 'wb') as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                # Atomic on POSIX (rename(2)) and on Windows.
                os.replace(tmp, target)
                moved = True
                # Persist the rename itself: without fsync-ing the directory a
                # host crash right after the rename can lose the new entry on
                # some filesystems. Best-effort (a lost entry only costs one
                # silent re-prompt), and a no-op where a directory fd can't be
                # fsynced (Windows).
                self._fsync_directory()
            finally:
                if not moved:
                    with contextlib.suppress(OSError):
                        os.remove(tmp)
        except OSError as e:
            raise OidcError(
                f'could not persist the OIDC token to the token store: '
                f'{e}') from e

    def clear(self, key: TokenStoreKey) -> None:
        try:
            os.remove(self._token_file(key))
        except FileNotFoundError:
            pass
        except OSError as e:
            raise OidcError(
                f'could not remove the OIDC token store file: {e}') from e

    def in_lock(self, key: TokenStoreKey, action: Callable[[], Any]) -> Any:
        lock = None
        held = False
        try:
            self._ensure_directory()
            lock = self._lock_file(key)
            held = self._acquire_lock(lock)
        except OSError:
            # Could not prepare the lock directory or file; run without the lock.
            # Atomic replacement still keeps every reader CONSISTENT (no torn
            # read), but a degraded lock-free refresh is no longer SERIALISED
            # against a peer, so for this one refresh two cross-process races are
            # unguarded: (1) a rotating-refresh-token race (two processes each
            # refresh and one rotation is lost), and (2) a clear-vs-save race —
            # because the clear()-generation re-check that normally guards a save
            # is process-local (it lives in this process's in-memory cache), a
            # save() here can re-create a file another process just clear()ed,
            # resurrecting a cleared token until the next clear(). Both are
            # best-effort by design; closing them across processes would need an
            # on-disk epoch that save re-checks under the lock.
            held = False
        try:
            return action()
        finally:
            if held:
                with contextlib.suppress(OSError):
                    # Best-effort release; a leftover lock goes stale and the
                    # next acquirer steals it.
                    os.remove(lock)

    # -- internals ----------------------------------------------------------

    def _token_file(self, key: TokenStoreKey) -> str:
        return os.path.join(self._directory, key.hash() + '.json')

    def _lock_file(self, key: TokenStoreKey) -> str:
        return os.path.join(self._directory, key.hash() + '.lock')

    def _ensure_directory(self) -> None:
        # Both os.path.isdir and os.chmod FOLLOW a symlink, so a symlink planted
        # at the store path — by anyone with write access to its parent dir —
        # would have us write the plaintext token files into, and chmod, the
        # link's TARGET (outside any directory we own): re-asserting 0700 would
        # then tighten the target, not close the exposure. lstat does not follow,
        # so use it to detect a symlinked leaf and refuse it rather than operate
        # through it. Only the final component is checked, so a symlinked PARENT
        # (e.g. the whole store relocated to another volume via
        # QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR) still works; a symlink AT the leaf
        # does not. Refusal is best-effort like every other store failure: the
        # sign-in still succeeds in memory, only persistence is skipped (with a
        # warning) — see OidcDeviceAuth._warn_persistence. This narrows but cannot
        # fully close the TOCTOU (a swap between lstat and the write needs precise
        # timing plus parent write access); it defeats a persistently-planted
        # symlink, the realistic case.
        try:
            leaf = os.lstat(self._directory)
        except FileNotFoundError:
            leaf = None
        except OSError as e:
            raise OidcError(
                f'could not access the OIDC token store directory: {e}') from e
        if leaf is not None and stat.S_ISLNK(leaf.st_mode):
            raise OidcError(
                'the OIDC token store path is a symbolic link; refusing to use '
                'it because the plaintext token files could be redirected '
                'outside the owner-only directory. Point the store at a real '
                f'directory, or set {TOKEN_STORE_DIR_ENV} to one.')
        if os.path.isdir(self._directory):
            # Re-assert owner-only permissions on a pre-existing (real) directory:
            # one left world/group-accessible by another tool, a permissive
            # umask, or a hostile local pre-create would otherwise expose the
            # token files. The symlink variant is handled above.
            self._restrict_to_owner()
            return
        os.makedirs(self._directory, mode=0o700, exist_ok=True)
        self._restrict_to_owner()

    def _restrict_to_owner(self) -> None:
        # Best-effort: the at-rest protection of the plaintext token files is
        # exactly these owner-only directory permissions. On a non-POSIX
        # filesystem (Windows) POSIX modes do not apply, so fall back to the
        # directory's inherited ACL and warn once.
        if os.name != 'posix':
            _warn_no_posix_perms_once()
            return
        try:
            os.chmod(self._directory, 0o700)
        except OSError:
            # The directory is not ours to chmod: keep the existing permissions.
            pass

    def _fsync_directory(self) -> None:
        # Flush the directory entry so an atomic rename into it survives a host
        # crash. POSIX only — a directory fd can't be opened/fsynced on Windows;
        # best-effort everywhere (a lost entry only costs a re-prompt).
        if os.name != 'posix':
            return
        try:
            dir_fd = os.open(self._directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    def _serialize(self, key: TokenStoreKey, token: PersistedToken) -> bytes:
        # A null value (an absent audience, or a token kind the grant did not
        # return) is omitted entirely rather than written as JSON null — the only
        # encoding under which a present value (a token equal to "null" included)
        # round-trips verbatim and an absent one reads back as null. Field order
        # follows the frozen schema for readability; key order is not semantic.
        obj = {
            'v': _SCHEMA_VERSION,
            'client_id': key.client_id,
            'token_endpoint': key.token_endpoint,
            'device_authorization_endpoint': key.device_authorization_endpoint,
            'scope': key.scope,
        }
        if key.audience is not None:
            obj['audience'] = key.audience
        # Persisted for the on-load identity re-check, not the file name (issuer
        # is excluded from TokenStoreKey.hash to keep the cross-language file
        # contract stable). Omitted when absent, like audience, so an entry
        # written without an issuer pin reads back as None and matches a None
        # key issuer — and an older/other client that doesn't write the field
        # interoperates unchanged.
        if key.issuer is not None:
            obj['issuer'] = key.issuer
        obj['groups_in_token'] = key.groups_in_token
        if token.access_token is not None:
            obj['access_token'] = token.access_token
        if token.id_token is not None:
            obj['id_token'] = token.id_token
        if token.refresh_token is not None:
            obj['refresh_token'] = token.refresh_token
        obj['expires_at_millis'] = _seconds_to_millis(token.expires_at)
        obj['token_ttl_millis'] = _seconds_to_millis(token.token_ttl)
        return json.dumps(obj, separators=(',', ':')).encode('utf-8')

    def _parse_and_verify(
            self, key: TokenStoreKey, data: bytes) -> Optional[PersistedToken]:
        try:
            obj = json.loads(data)
        except (ValueError, UnicodeDecodeError, RecursionError):
            # Corrupt, truncated, or deeply-nested file: treat as no usable
            # entry, fall back to refresh / interactive. RecursionError (the
            # attacker-writable file nests JSON deep enough to exhaust the
            # decoder's stack) is not a ValueError, so list it explicitly —
            # matching every other json.loads on untrusted input in this client
            # (_decode_jwt_claims, _http.get_json / post_form) — so a hostile
            # file makes load() return None rather than crash the caller.
            return None
        if not isinstance(obj, dict):
            return None
        # Schema and fingerprint must match the live identity; a mismatch is a
        # hash collision or a file copied from a different identity, so ignore it
        # rather than serve the wrong identity's token.
        if obj.get('v') != _SCHEMA_VERSION:
            return None
        if (obj.get('client_id') != key.client_id
                or obj.get('token_endpoint') != key.token_endpoint
                or obj.get('device_authorization_endpoint')
                != key.device_authorization_endpoint
                or obj.get('scope') != key.scope
                or not _audience_matches(key.audience, obj.get('audience'))
                or not _issuer_matches(key.issuer, obj.get('issuer'))
                or bool(obj.get('groups_in_token')) != key.groups_in_token):
            return None
        return PersistedToken(
            access_token=_nonempty_str(obj.get('access_token')),
            id_token=_nonempty_str(obj.get('id_token')),
            refresh_token=_nonempty_str(obj.get('refresh_token')),
            expires_at=_millis_to_seconds(obj.get('expires_at_millis')),
            token_ttl=_millis_to_seconds(obj.get('token_ttl_millis')))

    def _acquire_lock(self, lock: str) -> bool:
        deadline = time.monotonic() + self._lock_acquire_budget
        while True:
            try:
                self._create_lock_file(lock)
                return True
            except FileExistsError:
                if self._is_stale(lock):
                    self._steal_stale_lock(lock)
                    # Fall through to the bounded wait below rather than retry
                    # immediately: a steal contest between several acquirers (or
                    # a misconfigured tiny lock_stale) must not hot-spin.
                if time.monotonic() >= deadline:
                    # Give up and run without the lock rather than stall.
                    return False
                time.sleep(_LOCK_POLL_SLICE)
            except OSError:
                return False  # unexpected IO; degrade to no lock

    def _create_lock_file(self, lock: str) -> None:
        # The O_EXCL create IS the acquisition. Write the holder metadata through
        # this same fd — never via a second, non-exclusive open(): a concurrent
        # steal could replace the file between the create and a second open, so a
        # plain open('w') would truncate a peer's fresh lock (or resurrect one
        # just removed). Holder bytes are debugging-only; staleness is judged by
        # mtime, never by parsing them, so a write failure must not fail an
        # acquisition we already won.
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with contextlib.suppress(OSError):
                os.write(fd, self._holder_bytes())
        finally:
            os.close(fd)

    def _steal_stale_lock(self, lock: str) -> None:
        # Break a stale lock ATOMICALLY: rename it aside to a private path. Only
        # one racer can rename a given file away — the losers get an OSError (it
        # is already gone) and simply retry the O_EXCL create, which itself
        # admits a single winner. This replaces an unconditional os.remove(lock),
        # under which several acquirers could each delete a different generation
        # of the lock and ALL believe they won (two live holders at once). Then
        # re-judge staleness on the moved-aside file: if a peer recreated a fresh
        # lock between our staleness check and the rename, we moved that live lock
        # by mistake — restore it rather than strand the peer; only a genuinely
        # stale file is removed. This makes a single break the common outcome and
        # never blindly deletes a live lock; under pathological N-way concurrent
        # stealing a rare transient double-acquire can still slip through (no
        # file-only protocol prevents it without OS support), which stays
        # integrity-safe — the atomic write holds — and costs at most one extra
        # re-prompt, the same best-effort degradation as running lock-free.
        private = f'{lock}.stale.{os.getpid()}.{threading.get_ident()}'
        try:
            os.replace(lock, private)
        except OSError:
            return  # lost the steal race; the lock is already gone — retry create
        if self._is_stale(private):
            with contextlib.suppress(OSError):
                os.remove(private)
        else:
            # Moved a still-live lock by mistake; put it back. If the slot was
            # retaken meanwhile, our private copy is redundant — drop it.
            try:
                os.replace(private, lock)
            except OSError:
                with contextlib.suppress(OSError):
                    os.remove(private)

    def _holder_bytes(self) -> bytes:
        # pid@host plus a timestamp, to help debug a stuck lock; never parsed.
        try:
            return (f'{os.getpid()}@{socket.gethostname()} '
                    f'{time.time()}').encode('utf-8')
        except Exception:
            return b''  # metadata only — never fail acquisition over it

    def _is_stale(self, lock: str) -> bool:
        try:
            mtime = os.stat(lock).st_mtime
        except OSError:
            # Can't determine the age, so don't steal. (A pre-planted dangling
            # symlink at the lock path lands here forever, so that identity
            # degrades to lock-free coordination — integrity-safe, since the
            # atomic write still holds, and an attacker who can plant it already
            # has write access to the token directory.)
            return False
        elapsed = time.time() - mtime
        # Staleness rides the wall clock (st_mtime vs time.time()), unavoidable
        # for a lock that may be shared across hosts with no common monotonic
        # source: an NTP step still skews the age, and nothing file-only can fix
        # that. Guard the one anomaly we CAN detect locally — a future-dated mtime
        # (elapsed < 0), i.e. our clock currently reads BEHIND the lock's, whether
        # from a backward step here or a holder whose clock runs ahead. The age is
        # then untrustworthy and the lock may well be live, so treat it as fresh
        # (do not steal) rather than break a live holder's lock; a genuinely
        # abandoned lock is re-judged stale on a later poll once the clock catches
        # up to it.
        if elapsed < 0:
            return False
        return elapsed > self._lock_stale


def _audience_matches(key_audience: Optional[str], file_audience: Any) -> bool:
    # The file omits a null audience entirely, so an absent (or hand-edited JSON
    # null) field reads as None and matches a None key audience; a present
    # audience must be an exact string match. A non-string file value never
    # matches, so a hostile entry is rejected.
    if key_audience is None:
        return file_audience is None
    return isinstance(file_audience, str) and file_audience == key_audience


def _issuer_matches(key_issuer: Optional[str], file_issuer: Any) -> bool:
    # Same contract as _audience_matches, for the out-of-band issuer pin. The
    # file omits a null issuer entirely, so an absent (or hand-edited JSON null)
    # field reads as None and matches a None key issuer; a present issuer must be
    # an exact (already-normalised) string match, and a non-string file value
    # never matches. This is the on-load half of issuer isolation: issuer is part
    # of the identity re-check but NOT the file-name hash (see
    # TokenStoreKey.hash), so two configs differing only by issuer pin share a
    # file yet never adopt each other's token — a session pinned to one issuer
    # rejects a token persisted under another (and an un-pinned session rejects
    # an issuer-pinned token, and vice versa).
    if key_issuer is None:
        return file_issuer is None
    return isinstance(file_issuer, str) and file_issuer == key_issuer
