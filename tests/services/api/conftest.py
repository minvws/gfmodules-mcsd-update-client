from typing import Any, Dict, Final
import pytest
from yarl import URL

from app.config import Config
from app.services.api.api_service import ApiService, AuthenticationBasedApiService
from app.services.api.authenticators.aws_v4_authenticator import AwsV4Authenticator
from app.services.api.authenticators.azure_oauth2_authenticator import (
    AzureOAuth2Authenticator,
)
from tests.test_config import get_test_config

config: Final[Config] = get_test_config()


@pytest.fixture()
def mock_url() -> URL:
    return URL("http://example.com")


@pytest.fixture()
def mock_params() -> Dict[str, Any]:
    return {"param": "example"}


@pytest.fixture()
def mock_body() -> Dict[str, Any]:
    return {"example": "some data"}


@pytest.fixture()
def api_service() -> ApiService:
    return ApiService(
        timeout=config.directory_api.timeout,
        backoff=config.directory_api.backoff,
        retries=1,
        use_mtls=config.mcsd.use_mtls,
        client_cert_file=config.mcsd.client_cert_file,
        client_key_file=config.mcsd.client_key_file,
    )


@pytest.fixture()
def aws_v4_authenticator() -> AwsV4Authenticator:
    return AwsV4Authenticator(profile="example", region="example")


@pytest.fixture()
def azure_oauth_authenticator() -> AzureOAuth2Authenticator:
    return AzureOAuth2Authenticator(
        token_url="example",
        client_id="example",
        client_secret="example",
        resource="example",
    )


@pytest.fixture()
def aws_based_api_service(
    aws_v4_authenticator: AwsV4Authenticator,
) -> AuthenticationBasedApiService:
    return AuthenticationBasedApiService(
        auth=aws_v4_authenticator,
        timeout=config.directory_api.timeout,
        backoff=config.directory_api.backoff,
        retries=1,
    )


@pytest.fixture()
def azure_base_api_service(
    azure_oauth_authenticator: AzureOAuth2Authenticator,
) -> AuthenticationBasedApiService:
    return AuthenticationBasedApiService(
        auth=azure_oauth_authenticator,
        timeout=config.directory_api.timeout,
        backoff=config.directory_api.backoff,
        retries=1,
    )
