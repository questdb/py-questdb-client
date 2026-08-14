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


TOKEN_STORE_DIR_ENV = 'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR'


class FileTokenStore:
    """Opt into the native plaintext JSON token store at ``directory``.

    The native client owns all I/O, identity checks, atomic replacement and
    cross-process locking. On POSIX it creates directories with mode ``0700``
    and token files with mode ``0600``. This Python object carries only the
    selected directory into :class:`OidcDeviceAuth`.
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
        self._directory = os.fsdecode(path)

    @classmethod
    def at(cls, directory: Any) -> 'FileTokenStore':
        return cls(directory)

    @classmethod
    def at_default_location(cls) -> 'FileTokenStore':
        override = os.environ.get(TOKEN_STORE_DIR_ENV)
        if override:
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
