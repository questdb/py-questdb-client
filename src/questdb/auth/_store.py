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


TOKEN_STORE_DIR_ENV = 'questdb.client.oidc.token.store.dir'


class FileTokenStore:
    """Opt into the native plaintext JSON token store at ``directory``.

    The native client owns all I/O, identity checks, atomic replacement and
    cross-process locking. On POSIX it creates directories with mode ``0700``
    and token files with mode ``0600``. This Python object carries only the
    selected directory into :class:`OidcDeviceAuth`.

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
        self._directory = os.path.abspath(
            os.path.expanduser(os.fsdecode(path)))

    @classmethod
    def at(cls, directory: Any) -> 'FileTokenStore':
        return cls(directory)

    @classmethod
    def at_default_location(cls) -> 'FileTokenStore':
        override = os.environ.get(TOKEN_STORE_DIR_ENV)
        if override:
            # Reject a non-absolute override rather than normalizing it, which
            # is what the constructor does for a path the caller passes
            # directly. This setting is shared with the Java and native
            # clients, and neither expands `~`: they would create a directory
            # literally named `~` where expanding it here would land in
            # `$HOME`, so the "shared" store would silently become two. A
            # relative path is the same problem via the working directory.
            # Native rejects these too; checking here names the setting in a
            # typed error instead of surfacing an io error from build().
            # `~/x` is not absolute either, so this one test covers both.
            if not os.path.isabs(override):
                raise OidcConfigError(
                    f'{TOKEN_STORE_DIR_ENV} must be an absolute path, not '
                    f'{override!r}. A relative path follows the working '
                    'directory, and `~` is expanded by shells rather than by '
                    'the QuestDB clients, so neither names one store shared '
                    'with the Java and native clients. Use an absolute path, '
                    'or pass FileTokenStore(dir) explicitly.')
            return cls(override)
        home = os.path.expanduser('~')
        if not os.path.isabs(home):
            raise OidcConfigError(
                'could not resolve the home directory for the default OIDC '
                f'token-store location; set {TOKEN_STORE_DIR_ENV}')
        return cls(os.path.join(home, '.questdb', 'oidc-tokens'))

    @property
    def directory(self) -> str:
        return self._directory
