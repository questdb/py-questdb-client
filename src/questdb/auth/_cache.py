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

import threading
from dataclasses import dataclass, field, replace
from typing import Dict, Optional, Union

from ._errors import OidcConfigError

# Refresh a little before the real expiry to absorb clock skew / latency.
DEFAULT_SKEW_SECONDS = 30


@dataclass(frozen=True)
class TokenSet:
    """
    A set of tokens obtained from the IdP, plus their expiry.

    Immutable (``frozen``): the lock-free fast path in
    :class:`~questdb.auth._device.OidcDeviceAuth` reads a published ``TokenSet``
    without holding a lock, which is only safe because its fields never change
    after construction. Derive a modified copy with :func:`dataclasses.replace`
    rather than mutating in place. The three secret fields are kept out of
    ``repr`` so a token can't leak into a log line or traceback.
    """

    access_token: Optional[str] = field(default=None, repr=False)
    id_token: Optional[str] = field(default=None, repr=False)
    refresh_token: Optional[str] = field(default=None, repr=False)
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
# Per-key counter bumped on every clear(). store_if_current() uses it to drop a
# write from an acquisition that began before a concurrent clear() — including a
# clear() on a *different* OidcDeviceAuth that shares this process-global store,
# whose per-instance lock does not serialize against this one — so clear() can't
# be silently undone by an in-flight sign-in / refresh.
_MEMORY_GENERATION: Dict[str, int] = {}
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
            _MEMORY_GENERATION[key] = _MEMORY_GENERATION.get(key, 0) + 1

    def generation(self, key: str) -> int:
        """
        Current clear()-generation for ``key``.

        Captured before an acquisition's IdP round-trip and handed back to
        :meth:`store_if_current`, which drops the write if a ``clear()`` bumped
        the counter meanwhile (see :meth:`store_if_current`).
        """
        with _MEMORY_LOCK:
            return _MEMORY_GENERATION.get(key, 0)

    def store_if_current(
            self, key: str, tokens: TokenSet, generation: int) -> bool:
        """
        Store ``tokens`` only if no :meth:`clear` happened since ``generation``.

        If a concurrent ``clear()`` — on this or any other
        :class:`~questdb.auth.OidcDeviceAuth` sharing this process-global store —
        bumped the counter after ``generation`` was captured, the write is
        dropped (returns ``False``) so the just-cleared entry is not resurrected
        with a now-stale token. Returns ``True`` when the token was stored.
        """
        with _MEMORY_LOCK:
            if _MEMORY_GENERATION.get(key, 0) != generation:
                return False
            _MEMORY_STORE[key] = replace(tokens)
            return True


class NullCache(TokenCache):
    """Never persists anything; prompts every time."""

    def load(self, key: str) -> Optional[TokenSet]:
        return None

    def store(self, key: str, tokens: TokenSet) -> None:
        pass

    def clear(self, key: str) -> None:
        pass


_CacheSpec = Union[str, None, TokenCache]


def make_cache(spec: _CacheSpec) -> TokenCache:
    """Resolve a cache spec (``"memory"`` / ``None`` / a TokenCache instance)."""
    if isinstance(spec, TokenCache):
        return spec
    if spec is None or spec == 'none':
        return NullCache()
    if spec == 'memory':
        return MemoryCache()
    raise OidcConfigError(
        f'Unknown cache backend {spec!r}; '
        "expected 'memory', None, or a TokenCache instance.")
