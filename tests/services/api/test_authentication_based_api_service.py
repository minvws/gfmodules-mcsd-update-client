from typing import Any, Dict
from unittest.mock import patch, MagicMock

from yarl import URL

from app.services.api.api_service import AuthenticationBasedApiService

PATCHED_MODULE = "app.services.api.api_service.requests.request"
PATCHED_AWS_AUTH = "app.services.api.authenticators.aws_v4_authenticator.boto3.Session"
PATCHED_AZURE_AUTH = (
    "app.services.api.authenticators.azure_oauth2_authenticator.requests.post"
)


@patch(PATCHED_MODULE)
@patch(PATCHED_AWS_AUTH)
def test_do_request_should_succeed_with_aws_auth(
    mock_auth: MagicMock,
    mock_request: MagicMock,
    aws_based_api_service: AuthenticationBasedApiService,
    mock_url: URL,
    mock_body: Dict[str, Any],
) -> None:
    mock_auth.get_credentials.return_value = "some credentials"
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_body
    mock_response.url = mock_url.without_query_params()
    mock_request.return_value = mock_response

    actual = aws_based_api_service.do_request("GET", mock_url)

    assert actual.status_code == 200
    assert actual.json() == mock_body
    assert actual.url == mock_url.without_query_params()


@patch(PATCHED_MODULE)
@patch(PATCHED_AZURE_AUTH)
def test_do_request_should_succeed_with_azure_auth(
    mock_auth_request: MagicMock,
    mock_request: MagicMock,
    azure_base_api_service: AuthenticationBasedApiService,
    mock_url: URL,
    mock_body: Dict[str, Any],
) -> None:
    mock_auth_response = MagicMock()
    mock_auth_response.json.return_value = {
        "access_token": "some token",
        "expires_in": 10,
    }
    mock_auth_response.status_code = 200
    mock_auth_request.return_value = mock_auth_response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_body
    mock_response.url = mock_url.without_query_params()
    mock_request.return_value = mock_response

    actual = azure_base_api_service.do_request("GET", mock_url)
    assert actual.status_code == 200
    assert actual.json() == mock_body
    assert actual.url == mock_url.without_query_params()
