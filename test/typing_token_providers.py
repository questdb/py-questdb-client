"""Static regression fixture; checked by TokenProviderTypingTest."""

from datetime import datetime, timedelta, timezone

import questdb
from questdb import Protocol, QuestDB, Sender
from questdb.auth import (
    ExpiringToken,
    FetchFailure,
    OidcDeviceAuth,
    RefreshingTokenProvider,
    TokenProviderStatus,
    TokenSource,
    psycopg_connect,
    sqlalchemy_engine,
)


class Source:
    """A source with fetch_token(), deliberately not callable."""

    def fetch_token(self) -> ExpiringToken:
        return ExpiringToken.expires_in('t', 3600)


def check_provider_api(
        source: TokenSource,
        provider: RefreshingTokenProvider,
        oidc: OidcDeviceAuth) -> None:
    RefreshingTokenProvider(source)
    RefreshingTokenProvider(Source())
    RefreshingTokenProvider(lambda: ExpiringToken.expires_in('t', 3600))
    failure = FetchFailure(False, 2.5, 'unavailable', datetime.now(timezone.utc))
    TokenProviderStatus(False, None, None, 1, failure)
    sqlalchemy_engine(provider, 'http://localhost:9000')
    psycopg_connect(provider, 'http://localhost:9000')
    provider.close(wait=True, timeout=5.0)
    provider.close(wait=True, timeout=timedelta(seconds=5))
    sqlalchemy_engine(oidc, 'http://localhost:9000')
    psycopg_connect(oidc, 'http://localhost:9000')
    sqlalchemy_engine(lambda: 't', 'http://localhost:9000')
    psycopg_connect(provider.token, 'http://localhost:9000')


class Provider:
    """An application provider: any object with token()."""

    def token(self) -> str:
        return 't'


def check_bearer_token_provider_arguments(
        provider: RefreshingTokenProvider) -> None:
    conf = 'wss::addr=localhost:9000;'
    questdb.connect(conf, bearer_token_provider=provider)
    QuestDB.from_conf(conf, bearer_token_provider=Provider())
    Sender(Protocol.Wss, 'localhost', 9000, bearer_token_provider=lambda: 't')
    Sender.from_conf(conf, bearer_token_provider=provider.token)
    Sender.from_env(bearer_token_provider=provider)
    # Not a provider: a static error, kept honest by --warn-unused-ignores.
    questdb.connect(conf, bearer_token_provider=42)  # type: ignore[arg-type]
    QuestDB.from_conf(conf, bearer_token_provider=42)  # type: ignore[arg-type]
    Sender(Protocol.Wss, 'localhost', 9000,
           bearer_token_provider='t')  # type: ignore[arg-type]
    Sender.from_conf(conf, bearer_token_provider=42)  # type: ignore[arg-type]
    Sender.from_env(bearer_token_provider=42)  # type: ignore[arg-type]


def token_from_inside_with(source: TokenSource) -> str:
    # `__exit__` never swallows the exception, so mypy must not report a
    # missing return after the block.
    with RefreshingTokenProvider(source) as tokens:
        return tokens.token()
