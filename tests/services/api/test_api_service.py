from typing import Dict, Any
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import Timeout, ConnectionError
from yarl import URL
from app.services.api.api_service import ApiService

PATCHED_MODULE = "app.services.api.api_service.requests.request"


@patch(PATCHED_MODULE)
def test_do_request_should_succeed(
    mock_request: MagicMock,
    api_service: ApiService,
    mock_url: URL,
    mock_body: Dict[str, Any],
) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_body
    mock_response.url = mock_url.without_query_params()
    mock_request.return_value = mock_response

    actual = api_service.do_request("POST", mock_url, mock_body)

    assert actual.status_code == 200
    assert actual.json() == mock_body
    assert actual.url == mock_url


@patch(PATCHED_MODULE)
def test_do_request_should_succeed_with_query_params(
    mock_request: MagicMock,
    api_service: ApiService,
    mock_url: URL,
    mock_body: Dict[str, Any],
    mock_params: Dict[str, Any],
) -> None:
    mock_url_with_params = mock_url.with_query(mock_params)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_body
    mock_response.url = mock_url_with_params
    mock_request.return_value = mock_response

    actual = api_service.do_request("GET", mock_url_with_params)

    assert actual.status_code == 200
    assert actual.json() == mock_body
    assert actual.url == mock_url.with_query(mock_params)


@patch(PATCHED_MODULE)
def test_do_request_should_fail_on_timeout(
    mock_request: MagicMock,
    api_service: ApiService,
    mock_url: URL,
) -> None:
    mock_request.side_effect = Timeout("Timeout Error")

    with pytest.raises(Exception) as e:
        api_service.do_request("GET", mock_url)

    assert "Failed " in str(e)


@patch(PATCHED_MODULE)
def test_do_request_should_fail_on_connection_error(
    mock_request: MagicMock,
    api_service: ApiService,
    mock_url: URL,
) -> None:
    mock_request.side_effect = ConnectionError("Connection Error")

    with pytest.raises(Exception) as e:
        api_service.do_request("DELETE", mock_url)

    assert "Failed " in str(e)


@patch(PATCHED_MODULE)
def do_request_should_fail_with_unkown_request_methods(
    api_service: ApiService,
    mock_url: URL,
) -> None:
    with pytest.raises(Exception):
        api_service.do_request("SOME METHOD", mock_url)


@patch(PATCHED_MODULE)
def test_do_request_should_use_mtls_cert_when_enabled(
    mock_request: MagicMock,
    mock_url: URL,
) -> None:
    api_service = ApiService(
        timeout=10,
        backoff=0.1,
        retries=1,
        use_mtls=True,
        client_cert_file="test.crt",
        client_key_file="test.key"
    )
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_request.return_value = mock_response

    api_service.do_request("GET", mock_url)

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args[1]
    assert call_kwargs["cert"] == ("test.crt", "test.key")
    assert call_kwargs["verify"] is True


@patch(PATCHED_MODULE)
def test_do_request_should_use_ca_file_for_verification_when_provided(
    mock_request: MagicMock,
    mock_url: URL,
) -> None:
    api_service = ApiService(
        timeout=10,
        backoff=0.1,
        retries=1,
        use_mtls=True,
        client_cert_file="test.crt",
        client_key_file="test.key",
        client_ca_file="ca.crt"
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
    mock_url: URL,
) -> None:
    api_service = ApiService(
        timeout=10,
        backoff=0.1,
        retries=1,
        use_mtls=False,
        client_cert_file="test.crt",
        client_key_file="test.key"
    )
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_request.return_value = mock_response

    api_service.do_request("GET", mock_url)

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args[1]
    assert call_kwargs["cert"] is None
    assert call_kwargs["verify"] is False

