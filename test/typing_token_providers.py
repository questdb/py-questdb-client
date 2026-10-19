"""Static regression fixture; checked by TokenProviderTypingTest."""

from datetime import datetime, timedelta, timezone

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
