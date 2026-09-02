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

"""
OIDC authentication helper for QuestDB (Jupyter-first).

Runs the OAuth 2.0 Device Authorization Grant (RFC 8628) client-side and
presents the token to QuestDB (HTTP ``Bearer`` / PG-wire ``_sso``). Works on
browserless local and remote kernels (JupyterHub, SageMaker, Colab,
VS Code-remote): authorize in any browser, the kernel only calls the IdP.

Sign in explicitly, then attach the rotating provider to a QuestDB transport::

    from questdb.auth import OidcDeviceAuth
    from questdb import connect

    auth = OidcDeviceAuth.from_questdb("https://questdb.example.com:9000")
    auth.sign_in()                            # the only interactive operation
    db = connect("wss::addr=questdb.example.com:9000;", oidc_auth=auth)

For PG-wire there are two convenience adapters that wire the token in as the
``_sso`` password — ``sqlalchemy_engine`` re-supplies a fresh, auto-refreshed
token on every new pooled connection, while ``psycopg_connect`` captures the
current token once at connect time::

    from questdb.auth import sqlalchemy_engine, psycopg_connect

    engine = sqlalchemy_engine(auth, "https://questdb.example.com:9000")
    conn = psycopg_connect(auth, "https://questdb.example.com:9000")

Optional deps (``sqlalchemy``/``psycopg``, ``qrcode``, ``IPython``) are imported
lazily, only when used.
"""

from questdb._client import OidcDeviceAuth
from ._config import OidcConfig
from ._render import Renderer, sanitize_display_text
from ._errors import (
    OidcError,
    OidcConfigError,
    OidcCancelledError,
    OidcNetworkError,
    OidcInteractionRequired,
    OidcDeviceFlowError,
    OidcTimeoutError,
)
from ._store import (
    FileTokenStore,
)
from ._adapters import sqlalchemy_engine, psycopg_connect

__all__ = [
    'FileTokenStore',
    'OidcConfig',
    'OidcConfigError',
    'OidcCancelledError',
    'OidcDeviceAuth',
    'OidcDeviceFlowError',
    'OidcError',
    'OidcInteractionRequired',
    'OidcNetworkError',
    'OidcTimeoutError',
    'Renderer',
    'psycopg_connect',
    'sanitize_display_text',
    'sqlalchemy_engine',
]
