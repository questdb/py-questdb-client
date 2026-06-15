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
OIDC authentication helper for QuestDB (Jupyter-first).

Runs the OAuth 2.0 Device Authorization Grant (RFC 8628) entirely client-side,
obtains a token, and presents it to QuestDB over the auth paths it already
supports (HTTP ``Bearer`` / PG-wire ``_sso``). Designed for data scientists on
local **and remote** kernels (JupyterHub, SageMaker, Colab, VS Code-remote),
where the kernel has no browser: you authorize in any browser (laptop or
phone), the kernel only makes outbound calls to the IdP.

Two ways to use it, depending on your needs:

* **Just the token** — works with anything (PG-wire, HTTP, your own tooling)::

      from questdb.auth import OidcDeviceAuth

      auth = OidcDeviceAuth.from_questdb("https://questdb.example.com:9000")
      token = auth.token()                      # device flow on first use
      headers = auth.headers()                  # {"Authorization": "Bearer .."}

* **The integrated session** — query to a DataFrame and feed adapters::

      from questdb.auth import connect

      qdb = connect("https://questdb.example.com:9000")
      df = qdb.sql("SELECT * FROM trades LIMIT 10")
      engine = qdb.sqlalchemy_engine()          # PG-wire, token as _sso password
      with qdb.sender() as sender:              # ingestion (ILP/HTTP)
          ...

Only ``token()`` / ``headers()`` are needed for the bring-your-own-client path,
and they require no optional dependencies. ``pandas`` (for ``sql()``),
``sqlalchemy`` / ``psycopg`` (adapters), ``qrcode`` and ``IPython`` are imported
lazily, only when used.
"""

from ._device import OidcDeviceAuth
from ._discovery import OidcConfig
from ._cache import TokenCache, TokenSet, FileCache, MemoryCache, NullCache
from ._errors import (
    OidcError,
    OidcConfigError,
    OidcNetworkError,
    OidcInteractionRequired,
    OidcDeviceFlowError,
    OidcTimeoutError,
    OidcAuthError,
)
from ._questdb import QuestDB, connect

__all__ = [
    'connect',
    'QuestDB',
    'OidcDeviceAuth',
    'OidcConfig',
    'TokenCache',
    'TokenSet',
    'MemoryCache',
    'FileCache',
    'NullCache',
    'OidcError',
    'OidcConfigError',
    'OidcNetworkError',
    'OidcInteractionRequired',
    'OidcDeviceFlowError',
    'OidcTimeoutError',
    'OidcAuthError',
]
