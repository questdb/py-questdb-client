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

"""Token state and cache backends for :mod:`questdb.auth`."""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import tempfile
import threading
from dataclasses import asdict, dataclass, replace
from typing import Dict, Optional, Union

from ._errors import OidcConfigError

# Refresh a little before the real expiry to absorb clock skew / latency.
DEFAULT_SKEW_SECONDS = 30


@dataclass
class TokenSet:
    """A set of tokens obtained from the IdP, plus their expiry."""

    access_token: Optional[str] = None
    id_token: Optional[str] = None
    refresh_token: Optional[str] = None
    expires_at: float = 0.0  # epoch seconds; 0 == unknown
    token_type: str = 'Bearer'
    scope: Optional[str] = None
    sub: Optional[str] = None
    issued_at: float = 0.0  # epoch seconds; 0 == unknown

    def is_valid(self, now: float, skew: float = DEFAULT_SKEW_SECONDS) -> bool:
        """True if the token is present and not within ``skew`` of expiry."""
        if self.expires_at <= 0:
            return False
        # Never let the early-refresh skew exceed half the token's own
        # lifetime, so a short-lived (< 2*skew) token isn't reported expired
        # the instant it is issued (which would refresh on every call).
        if self.issued_at:
            lifetime = self.expires_at - self.issued_at
            if lifetime > 0:
                skew = min(skew, lifetime / 2)
        return now < (self.expires_at - skew)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, object]) -> 'TokenSet':
        known = {f for f in cls.__dataclass_fields__}  # noqa: C416
        return cls(**{k: v for k, v in d.items() if k in known})


class TokenCache:
    """Interface for token caches."""

    def load(self, key: str) -> Optional[TokenSet]:  # pragma: no cover
        raise NotImplementedError

    def store(self, key: str, tokens: TokenSet) -> None:  # pragma: no cover
        raise NotImplementedError

    def clear(self, key: str) -> None:  # pragma: no cover
        raise NotImplementedError


# Module-global so that re-running a notebook cell (which constructs a fresh
# ``OidcDeviceAuth``) reuses the already-acquired token instead of re-prompting.
_MEMORY_STORE: Dict[str, TokenSet] = {}
_MEMORY_LOCK = threading.Lock()


class MemoryCache(TokenCache):
    """
    Process-global, in-memory cache (the default).

    Safest backend: nothing is written to disk. Tokens survive for the life
    of the Python process, so re-running cells is silent, but a kernel
    restart re-prompts once.
    """

    def load(self, key: str) -> Optional[TokenSet]:
        # Return a copy so callers can't mutate the cached entry in place
        # (the live token is refreshed/rotated independently).
        with _MEMORY_LOCK:
            tokens = _MEMORY_STORE.get(key)
        return replace(tokens) if tokens is not None else None

    def store(self, key: str, tokens: TokenSet) -> None:
        with _MEMORY_LOCK:
            _MEMORY_STORE[key] = replace(tokens)

    def clear(self, key: str) -> None:
        with _MEMORY_LOCK:
            _MEMORY_STORE.pop(key, None)


class NullCache(TokenCache):
    """Never persists anything; prompts every time."""

    def load(self, key: str) -> Optional[TokenSet]:
        return None

    def store(self, key: str, tokens: TokenSet) -> None:
        pass

    def clear(self, key: str) -> None:
        pass


# Cross-process file locking, used to serialize read-modify-write on the
# shared cache file. fcntl.flock (POSIX) also serializes across threads/
# instances in one process (locks are per open file description). Where no OS
# primitive is available it degrades to a best-effort no-op; the atomic
# os.replace still guarantees readers never see a torn file.
try:
    import fcntl

    def _lock_fd(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX)

    def _unlock_fd(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)
except ImportError:  # pragma: no cover - non-POSIX (e.g. Windows)
    try:
        import msvcrt

        def _lock_fd(fd: int) -> None:
            try:
                msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
            except OSError:
                pass

        def _unlock_fd(fd: int) -> None:
            try:
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
    except ImportError:  # pragma: no cover
        def _lock_fd(fd: int) -> None:
            pass

        def _unlock_fd(fd: int) -> None:
            pass


@contextlib.contextmanager
def _interprocess_lock(lock_path: pathlib.Path):
    """Best-effort exclusive lock via a sidecar lock file."""
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        _lock_fd(fd)
        try:
            yield
        finally:
            _unlock_fd(fd)
    finally:
        os.close(fd)


class FileCache(TokenCache):
    """
    Opt-in on-disk cache at ``~/.questdb/oidc-cache.json`` (mode ``600``).

    Survives kernel restarts and is shared across kernels on the same host.
    Security trade-off: a refresh token is stored at rest. The file is created
    owner-only (``0600``) from the start via an atomic temp-file replace, and a
    sidecar lock file serializes concurrent read-modify-writes across kernels
    so entries are not corrupted or lost.
    """

    def __init__(self, path: Optional[Union[str, os.PathLike]] = None):
        if path is None:
            path = pathlib.Path.home() / '.questdb' / 'oidc-cache.json'
        self.path = pathlib.Path(path)
        self._lock_path = self.path.with_name(self.path.name + '.lock')

    def _ensure_dir(self) -> None:
        parent = self.path.parent
        parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(parent, 0o700)
        except OSError:
            pass

    def _read_all(self) -> Dict[str, dict]:
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except (FileNotFoundError, ValueError, OSError):
            pass
        return {}

    def _write_all(self, data: Dict[str, dict]) -> None:
        # Atomic, owner-only replace. mkstemp creates the file mode 0600 with a
        # unique name, so concurrent writers never share a temp file and the
        # refresh token is never group/world-readable, even briefly.
        fd, tmp = tempfile.mkstemp(
            dir=str(self.path.parent), prefix='.oidc-', suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f)
            os.replace(tmp, self.path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise

    def load(self, key: str) -> Optional[TokenSet]:
        # Lock-free: the atomic replace guarantees a complete file is read.
        entry = self._read_all().get(key)
        if isinstance(entry, dict):
            try:
                return TokenSet.from_dict(entry)
            except TypeError:
                return None
        return None

    def store(self, key: str, tokens: TokenSet) -> None:
        self._ensure_dir()
        with _interprocess_lock(self._lock_path):
            data = self._read_all()
            data[key] = tokens.to_dict()
            self._write_all(data)

    def clear(self, key: str) -> None:
        self._ensure_dir()
        with _interprocess_lock(self._lock_path):
            data = self._read_all()
            if key in data:
                del data[key]
                self._write_all(data)


_CacheSpec = Union[str, None, TokenCache]


def make_cache(spec: _CacheSpec) -> TokenCache:
    """Resolve a cache spec (``"memory"`` / ``"file"`` / ``None`` / instance)."""
    if isinstance(spec, TokenCache):
        return spec
    if spec is None or spec == 'none':
        return NullCache()
    if spec == 'memory':
        return MemoryCache()
    if spec == 'file':
        return FileCache()
    raise OidcConfigError(
        f'Unknown cache backend {spec!r}; '
        "expected 'memory', 'file', None, or a TokenCache instance.")
