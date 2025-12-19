import os

if os.getenv("APP_ENV") is None:
    os.environ["APP_ENV"] = "test" # noqa

from typing import Any, Dict, Final
import pytest

from app.config import Config, get_config
from app.services.api.api_service import (
    HttpService,
)
from app.services.api.authenticators.authenticator import Authenticator
from app.services.api.authenticators.aws_v4_authenticator import AwsV4Authenticator
from app.services.api.authenticators.azure_oauth2_authenticator import (
    AzureOAuth2Authenticator,
)
from app.services.api.fhir_api import FhirApi


config: Final[Config] = get_config()

MOCK_AUTH_TOKEN = "some-token"
MOCK_AUTH = "some-auth"


class MockAuthenticator(Authenticator):
    """
    Dummy class for testing purposes only
    """

    def get_authentication_header(self) -> str:
        return MOCK_AUTH_TOKEN

    def get_auth(self) -> Any:
        return MOCK_AUTH


@pytest.fixture()
def base_url() -> str:
    return "http://example.com"


@pytest.fixture()
def mock_sub_route() -> str:
    return "/some-route"


@pytest.fixture()
def mock_url() -> str:
    return "http://example.com"


@pytest.fixture()
def mock_params() -> Dict[str, Any]:
    return {"param": "example"}


@pytest.fixture()
def mock_body() -> Dict[str, Any]:
    return {"example": "some data"}


@pytest.fixture()
def mock_history_params(fhir_api: FhirApi) -> Dict[str, Any]:
    # conversion to string to stay consistent with output of YARL query
    return {"_count": str(fhir_api.request_count)}


@pytest.fixture()
def http_service(base_url: str) -> HttpService:
    return HttpService(
        base_url=base_url,
        timeout=config.client_directory.timeout,
        backoff=config.client_directory.backoff,
        retries=1,
        mtls_cert=config.mcsd.mtls_client_cert_path,
        mtls_key=config.mcsd.mtls_client_key_path,
        verify_ca=config.mcsd.verify_ca,
    )


@pytest.fixture()
def mock_authenticator() -> MockAuthenticator:
    # this is used as a dummy authenticator for testing purposes
    return MockAuthenticator()


@pytest.fixture()
def mock_auth_token() -> str:
    return MOCK_AUTH_TOKEN


@pytest.fixture()
def mock_auth() -> str:
    return MOCK_AUTH


@pytest.fixture()
def mock_authentication_headers() -> Dict[str, Any]:
    return {"Content-Type": "application/json", "Authorization": MOCK_AUTH_TOKEN}


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
