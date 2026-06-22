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

Runs the OAuth 2.0 Device Authorization Grant (RFC 8628) client-side and
presents the token to QuestDB (HTTP ``Bearer`` / PG-wire ``_sso``). Works on
browserless local and remote kernels (JupyterHub, SageMaker, Colab,
VS Code-remote): authorize in any browser, the kernel only calls the IdP.

* **Just the token** — works with anything; no optional dependencies::

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

Optional deps (``pandas``, ``sqlalchemy``/``psycopg``, ``qrcode``, ``IPython``)
are imported lazily, only when used.
"""

from ._device import OidcDeviceAuth
from ._discovery import OidcConfig
from ._cache import TokenCache, TokenSet, MemoryCache, NullCache
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
    'MemoryCache',
    'NullCache',
    'OidcAuthError',
    'OidcConfig',
    'OidcConfigError',
    'OidcDeviceAuth',
    'OidcDeviceFlowError',
    'OidcError',
    'OidcInteractionRequired',
    'OidcNetworkError',
    'OidcTimeoutError',
    'QuestDB',
    'TokenCache',
    'TokenSet',
    'connect',
]
