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

"""Token state and the in-memory token cache for :mod:`questdb.auth`."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field, replace
from typing import Dict, Optional

# Refresh a little before the real expiry to absorb clock skew / latency.
DEFAULT_SKEW_SECONDS = 30


@dataclass(frozen=True)
class TokenSet:
    """
    IdP tokens plus their expiry.

    ``frozen`` because the lock-free fast path in
    :class:`~questdb.auth._device.OidcDeviceAuth` reads a published ``TokenSet``
    without a lock, which is safe only if its fields never change; use
    :func:`dataclasses.replace` for a modified copy. The secret fields are
    excluded from ``repr`` so a token can't leak into a log or traceback.
    """

    access_token: Optional[str] = field(default=None, repr=False)
    id_token: Optional[str] = field(default=None, repr=False)
    refresh_token: Optional[str] = field(default=None, repr=False)
    expires_at: float = 0.0  # epoch seconds; 0 == unknown
    token_type: str = 'Bearer'
    scope: Optional[str] = None
    # subject id, derived from the (unverified) JWT — PII, so keep it out of repr
    sub: Optional[str] = field(default=None, repr=False)
    issued_at: float = 0.0  # epoch seconds; 0 == unknown

    def is_valid(self, now: float, skew: float = DEFAULT_SKEW_SECONDS) -> bool:
        """True if the token is present and not within ``skew`` of expiry."""
        if self.expires_at <= 0:
            return False
        # Cap skew at half the token lifetime, so a short-lived (< 2*skew)
        # token isn't reported expired the instant it's issued. issued_at == 0
        # means the issue time is unknown; treat it as `now` so the cap still
        # applies to a short-lived token that arrives without one.
        lifetime = self.expires_at - (self.issued_at or now)
        if lifetime > 0:
            skew = min(skew, lifetime / 2)
        return now < (self.expires_at - skew)


# Module-global so a re-run notebook cell (fresh ``OidcDeviceAuth``) reuses the
# acquired token instead of re-prompting.
_MEMORY_STORE: Dict[str, TokenSet] = {}
# Per-key counter bumped on every clear(); store_if_current() uses it to drop a
# write from an acquisition that began before a concurrent clear() — even a
# clear() on a different OidcDeviceAuth sharing this store, whose per-instance
# lock doesn't serialize against this one — so clear() can't be silently undone.
_MEMORY_GENERATION: Dict[str, int] = {}
_MEMORY_LOCK = threading.Lock()


class MemoryCache:
    """
    Process-global, in-memory token cache (always on).

    Nothing ever hits disk. Tokens live for the life of the process, so
    re-running cells is silent; a kernel restart re-prompts once.
    """

    def load(self, key: str) -> Optional[TokenSet]:
        # Return a copy so callers can't mutate the cached entry in place.
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

        Capture before an IdP round-trip and pass to :meth:`store_if_current`,
        which drops the write if a ``clear()`` bumped the counter meanwhile.
        """
        with _MEMORY_LOCK:
            return _MEMORY_GENERATION.get(key, 0)

    def store_if_current(
            self, key: str, tokens: TokenSet, generation: int) -> bool:
        """
        Store ``tokens`` only if no :meth:`clear` happened since ``generation``.

        If a concurrent ``clear()`` (on any OidcDeviceAuth sharing this store)
        bumped the counter after ``generation`` was captured, the write is
        dropped (``False``) so the cleared entry isn't resurrected with a stale
        token; returns ``True`` when stored.
        """
        with _MEMORY_LOCK:
            if _MEMORY_GENERATION.get(key, 0) != generation:
                return False
            _MEMORY_STORE[key] = replace(tokens)
            return True
