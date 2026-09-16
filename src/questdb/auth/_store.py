################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2026 QuestDB
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

"""Configuration for the native plaintext OIDC file-token store."""

from __future__ import annotations

import os
from typing import Any

from ._errors import OidcConfigError


#: Environment variable overriding the default token-store directory.
#:
#: Spelled so a shell can set it: ``export QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR=/path``.
#: Java spells the same setting ``questdb.client.oidc.token.store.dir``, but that
#: is a JVM *system property* (``-D...``), which never reaches this process's
#: environment -- so borrowing that spelling shared nothing with Java, while
#: ``sh``/``bash``/``zsh`` reject a name containing ``.`` from ``export``. This
#: binding and the native client read this variable, so it still names one store
#: across both. Keep the two spellings in step.
TOKEN_STORE_DIR_ENV = 'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR'


def _default_token_store_directory() -> str:
    """Resolve the shared native/Python default token-store directory.

    Keep this as the single Python implementation of the native ``home_dir``
    contract: an absolute environment override wins; otherwise use only the
    platform's primary home variable, with no ``expanduser`` fallback.
    """
    override = os.environ.get(TOKEN_STORE_DIR_ENV)
    if override:
        if not os.path.isabs(override):
            raise OidcConfigError(
                f'{TOKEN_STORE_DIR_ENV} must be an absolute path, not '
                f'{override!r}. A relative path follows the working '
                'directory, and `~` is expanded by shells rather than by '
                'the QuestDB clients, so neither names one store shared '
                'with the native client. Use an absolute path, or pass '
                'FileTokenStore(dir) explicitly.')
        return override
    home = os.environ.get('USERPROFILE' if os.name == 'nt' else 'HOME')
    if not home or not os.path.isabs(home):
        raise OidcConfigError(
            'could not resolve the home directory for the default OIDC '
            f'token-store location; set {TOKEN_STORE_DIR_ENV} to an '
            'absolute path')
    return os.path.join(home, '.questdb', 'oidc-tokens')


class FileTokenStore:
    """Opt into the native plaintext JSON token store at ``directory``.

    The native client owns all I/O, identity checks, atomic replacement and
    cross-process locking. On POSIX it creates directories with mode ``0700``
    and token files with mode ``0600``; on other platforms protection depends
    on the directory's default ACL. This Python object carries only the selected
    directory into :class:`OidcDeviceAuth`.

    ``directory`` is expanded (``~``) and made absolute at construction, so a
    later :func:`os.chdir` cannot move the store; read back the resolved value
    from :attr:`directory`.
    """

    def __init__(self, directory: Any):
        # Reject a non-path type (0, False, an arbitrary object) as "must be
        # path-like" before the emptiness check, so only a genuinely missing or
        # empty value (None, '', b'') reports "required" -- and neither escapes
        # the package's typed-error contract with a bare TypeError.
        try:
            path = os.fspath(directory) if directory is not None else None
        except TypeError as exc:
            raise OidcConfigError(
                'the token store directory must be a path-like object '
                '(str, bytes, or os.PathLike)') from exc
        if not path:
            raise OidcConfigError('the token store directory is required')
        # Expand and absolutise, exactly as at_default_location() already does.
        # Without this the value was handed to the native side verbatim and
        # resolved against the process CWD: FileTokenStore('~/qdb-tokens') wrote
        # a long-lived plaintext refresh token into a directory literally named
        # '~' under the working directory -- often a repo checkout -- and a
        # relative path silently followed the process around, so a chdir re-ran
        # the whole device flow and left a second copy of the credential
        # somewhere else. Resolve once, at construction, so the location is
        # fixed and inspectable via `.directory`.
        expanded = os.path.expanduser(os.fsdecode(path))
        # `expanduser` returns the path UNCHANGED when it cannot resolve the
        # user -- `$HOME` unset and no `pwd` entry for the uid, which is the
        # normal state in a container run under an arbitrary uid. `abspath`
        # would then resolve the leading `~` against the working directory and
        # quietly write a long-lived plaintext refresh token into a directory
        # literally named `~`. Refuse instead, exactly as
        # `at_default_location()` already does for the same condition.
        if expanded.startswith('~'):
            raise OidcConfigError(
                f'could not resolve the home directory in {os.fsdecode(path)!r} '
                'for the OIDC token store; pass an absolute path, or set '
                f'{TOKEN_STORE_DIR_ENV}')
        self._directory = os.path.abspath(expanded)

    @classmethod
    def at(cls, directory: Any) -> 'FileTokenStore':
        """Construct a store at ``directory``.

        This is an exact constructor alias, provided to read symmetrically with
        :meth:`at_default_location`; ``FileTokenStore(directory)`` is equally
        valid.
        """
        return cls(directory)

    @classmethod
    def at_default_location(cls) -> 'FileTokenStore':
        """Use the default location shared with the native client.

        An absolute ``QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR`` override wins.
        Otherwise this
        uses ``HOME`` on POSIX or ``USERPROFILE`` on Windows and appends
        ``.questdb/oidc-tokens``. Missing, relative and ``~``-prefixed defaults
        are rejected so different clients cannot silently select different
        credential stores.
        """
        return cls(_default_token_store_directory())

    @property
    def directory(self) -> str:
        """The expanded absolute directory passed to the native token store."""
        return self._directory
