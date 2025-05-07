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
