from typing import Dict, Any
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import Timeout, ConnectionError
from yarl import URL
from app.services.api.api_service import HttpService
from tests.services.api.conftest import MockAuthenticator

PATCHED_MODULE = "app.services.api.api_service.request"
PATCHED_AUTH = "app.services.api.api_service.Authenticator"


@patch(PATCHED_MODULE)
def test_do_request_should_succeed(
    mock_post: MagicMock,
    http_service: HttpService,
    mock_body: Dict[str, Any],
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_body
    mock_post.return_value = mock_request

    actual = http_service.do_request(method="POST", json=mock_body)

    assert actual.json() == mock_body
    assert actual.status_code == 200
    mock_post.assert_called_once()


@patch(PATCHED_MODULE)
def test_do_request_should_succeed_with_query_params(
    mock_get: MagicMock,
    http_service: HttpService,
    mock_params: Dict[str, Any],
    mock_body: Dict[str, Any],
    base_url: str,
) -> None:
    expected_url = str(URL(base_url).with_query(mock_params))
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_body
    mock_request.url = expected_url
    mock_get.return_value = mock_request
    actual = http_service.do_request(method="GET", params=mock_params)

    assert actual.json() == mock_body
    assert actual.status_code == 200
    assert actual.url == expected_url


@patch(PATCHED_MODULE)
def test_do_request_should_succeed_with_sub_routes(
    mock_get: MagicMock, http_service: HttpService, base_url: str, mock_sub_route: str
) -> None:
    expected_url = f"{base_url}/{mock_sub_route}"
    mock_request = MagicMock()
    mock_request.status_code = 201
    mock_request.url = expected_url
    mock_get.return_value = mock_request

    actual = http_service.do_request(method="GET", sub_route=mock_sub_route)

    assert actual.url == expected_url


@patch(PATCHED_MODULE)
def test_do_request_with_authenticator_should_succeed(
    mock_get: MagicMock,
    mock_auth: str,
    mock_authentication_headers: Dict[str, Any],
    http_service: HttpService,
    mock_authenticator: MockAuthenticator,
) -> None:
    http_service.authenticator = mock_authenticator
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.headers = mock_authentication_headers
    mock_request.auth = mock_auth

    mock_get.return_value = mock_request

    actual = http_service.do_request("GET")

    assert actual.status_code == 200
    assert actual.headers == mock_authentication_headers
    mock_get.assert_called_with(
        method="GET",
        url=http_service.base_url,
        headers=mock_authentication_headers,
        timeout=1,
        json=None,
        cert=None,
        verify=True,
        auth=mock_auth,
    )


@patch(PATCHED_MODULE)
def test_do_request_should_fail_on_timeout(
    mock_request: MagicMock,
    http_service: HttpService,
) -> None:
    mock_request.side_effect = Timeout("Timeout Error")

    with pytest.raises(Exception) as e:
        http_service.do_request("GET")

    assert "Failed " in str(e)


@patch(PATCHED_MODULE)
def test_do_request_should_fail_on_connection_error(
    mock_request: MagicMock, http_service: HttpService, mock_sub_route: str
) -> None:
    mock_request.side_effect = ConnectionError("Connection Error")

    with pytest.raises(Exception) as e:
        http_service.do_request(method="DELETE", sub_route=mock_sub_route)

    assert "Failed " in str(e)


@patch(PATCHED_MODULE)
def do_request_should_fail_with_unkown_request_methods(
    http_service: HttpService,
) -> None:
    with pytest.raises(Exception):
        http_service.do_request("SOME-METHOD")


def test_make_header_should_succeed(
    http_service: HttpService,
    mock_authenticator: MockAuthenticator,
    mock_authentication_headers: Dict[str, Any],
) -> None:
    http_service.authenticator = mock_authenticator

    actual = http_service.make_headers()

    assert actual == mock_authentication_headers


def test_make_header_should_succeed_without_authenticator(
    http_service: HttpService,
) -> None:
    actual = http_service.make_headers()

    assert isinstance(actual, dict)
    assert actual == {"Content-Type": "application/json"}


def test_make_target_url_should_succeed_with_sub_route(
    http_service: HttpService, base_url: str, mock_sub_route: str
) -> None:
    expected = URL(f"{base_url}/{mock_sub_route}")

    actual = http_service.make_target_url(sub_route=mock_sub_route)

    assert expected == actual


def test_make_target_url_should_succeed_with_only_base_url(
    http_service: HttpService, base_url: str
) -> None:
    expected = URL(base_url)

    actual = http_service.make_target_url()

    assert expected == actual


def test_make_target_url_should_work_with_params(
    http_service: HttpService,
    base_url: str,
    mock_url: str,
    mock_sub_route: str,
    mock_params: Dict[str, Any],
) -> None:
    expected = URL(f"{base_url}/{mock_sub_route}").with_query(mock_params)

    actual = http_service.make_target_url(sub_route=mock_sub_route, params=mock_params)

    assert expected == actual


@patch(PATCHED_MODULE)
def test_do_request_should_use_mtls_cert_when_enabled(
    mock_request: MagicMock,
    mock_url: str,
) -> None:
    api_service = HttpService(
        base_url=mock_url,
        timeout=10,
        backoff=0.1,
        retries=1,
        mtls_cert="test.crt",
        mtls_key="test.key",
        verify_ca="test.ca"
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_request.return_value = mock_response

    api_service.do_request("GET", mock_url)

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args[1]
    assert call_kwargs["cert"] == ("test.crt", "test.key")
    assert call_kwargs["verify"] == "test.ca"


@patch(PATCHED_MODULE)
def test_do_request_should_use_ca_file_for_verification_when_provided(
    mock_request: MagicMock,
    mock_url: str,
) -> None:
    api_service = HttpService(
        base_url=mock_url,
        timeout=10,
        backoff=0.1,
        retries=1,
        mtls_cert="test.crt",
        mtls_key="test.key",
        verify_ca="ca.crt"
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_request.return_value = mock_response

    api_service.do_request("GET", mock_url)

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args[1]
    assert call_kwargs["cert"] == ("test.crt", "test.key")
    assert call_kwargs["verify"] == "ca.crt"


@patch(PATCHED_MODULE)
def test_do_request_should_not_use_cert_when_mtls_disabled(
    mock_request: MagicMock,
    mock_url: str,
) -> None:
    api_service = HttpService(
        base_url=mock_url,
        timeout=10,
        backoff=0.1,
        retries=1,
        mtls_cert=None,
        mtls_key=None,
        verify_ca=True
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_request.return_value = mock_response

    api_service.do_request("GET", mock_url)

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args[1]
    assert call_kwargs["cert"] is None
    assert call_kwargs["verify"] is True

