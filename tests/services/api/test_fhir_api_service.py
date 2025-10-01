from datetime import datetime
from typing import Any, Dict
from urllib.parse import urlencode
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock

from fastapi.exceptions import HTTPException
from fhir.resources.R4B.bundle import Bundle
import pytest
from yarl import URL

from app.services.api.fhir_api import FhirApi
from app.services.fhir.bundle.parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService

PATCHED_MODULE = "app.services.api.api_service.HttpService.do_request"


@patch(PATCHED_MODULE)
def test_post_bundle_should_succeed(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_bundle_request: Bundle,
    mock_bundle_response: Bundle,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_bundle_response.model_dump()
    mock_response.return_value = mock_request

    actual, errs = fhir_api.post_bundle(mock_bundle_request)

    assert actual == mock_bundle_response
    assert errs == []


@patch(PATCHED_MODULE)
def test_post_bundle_should_fail_on_status_code(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_bundle_request: Bundle,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 401
    mock_response.return_value = mock_request

    with pytest.raises(HTTPException) as e:
        fhir_api.post_bundle(mock_bundle_request)

    assert e.value.status_code == 401


@patch(PATCHED_MODULE)
def test_post_bundle_with_failed_elements(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_bundle_with_errors_request: Bundle,
    mock_bundle_with_errors_response: Bundle,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_bundle_with_errors_response.model_dump()
    mock_response.return_value = mock_request

    actual, errs = fhir_api.post_bundle(mock_bundle_with_errors_request)
    assert len(errs) == 3
    assert errs[0].entry == 1
    assert errs[0].severity == "error"
    assert errs[1].entry == 1
    assert errs[1].severity == "fatal"
    assert errs[2].entry == 3
    assert errs[2].severity == "error"
    assert actual == mock_bundle_with_errors_response


@patch(PATCHED_MODULE)
def test_post_bundle_should_fail_on_connection_error(
    mock_response: MagicMock, fhir_api: FhirApi, mock_bundle_request: Bundle
) -> None:
    mock_response.side_effect = ConnectionError
    with pytest.raises(Exception):
        fhir_api.post_bundle(mock_bundle_request)


def test_build_history_params_should_succeed(
    fhir_api: FhirApi, mock_history_params: Dict[str, Any]
) -> None:
    actual = fhir_api.build_history_params()

    assert mock_history_params == actual


def test_build_history_params_should_succeed_with_since_param(
    fhir_api: FhirApi, mock_history_params: Dict[str, Any]
) -> None:
    since = datetime.fromisoformat("2025-01-01")
    expected = {**mock_history_params, "_since": datetime.isoformat(since)}
    actual = fhir_api.build_history_params(since=since)

    assert expected == actual


def test_build_next_params_should_succeed(
    fhir_api: FhirApi, base_url: str, mock_params: Dict[str, Any]
) -> None:
    url = URL(base_url).with_query(mock_params)

    actual = fhir_api.get_next_params(url)

    assert mock_params == actual


@patch(PATCHED_MODULE)
def test_get_history_batch_should_succeed(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_org_history_bundle: Dict[str, Any],
    org_history_entry_1: Dict[str, Any],
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_org_history_bundle
    mock_response.return_value = mock_request
    expected_next_url, expected_entries = None, [
        create_bundle_entry(org_history_entry_1),
    ]

    actual_next_url, actual_entries = fhir_api.get_history_batch("Organizaton")

    assert actual_next_url == expected_next_url
    assert expected_entries == actual_entries


@patch(PATCHED_MODULE)
def test_get_history_batch_should_succeed_and_return_next_params(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_org_history_bundle: Dict[str, Any],
    org_history_entry_1: Dict[str, Any],
    base_url: str,
    mock_history_params: Dict[str, Any],
) -> None:
    next_url = URL(base_url).with_query(mock_history_params)
    expected_entries = [
        create_bundle_entry(org_history_entry_1),
    ]
    mock_org_history_bundle["link"] = [{"relation": "next", "url": str(next_url)}]

    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_org_history_bundle
    mock_response.return_value = mock_request

    actual_next_params, actual_entries = fhir_api.get_history_batch(
        "Organization", mock_history_params
    )

    assert actual_next_params == mock_history_params
    assert expected_entries == actual_entries


@patch(PATCHED_MODULE)
def test_get_history_should_fail_on_error_status_code(
    mock_response: MagicMock,
    fhir_api: FhirApi,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 400
    mock_response.return_value = mock_request
    with pytest.raises(HTTPException) as e:
        fhir_api.get_history_batch("Organization")

    assert e.value.status_code == 500


@patch(PATCHED_MODULE)
def test_get_history_should_fail_on_connection_error(
    mock_response: MagicMock, fhir_api: FhirApi
) -> None:
    mock_response.side_effect = ConnectionError
    with pytest.raises(Exception):
        fhir_api.get_history_batch("Organization")


@patch(PATCHED_MODULE)
def test_get_resource_by_id_should_succeed(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_org: Dict[str, Any],
    fhir_service: FhirService,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_org
    mock_response.return_value = mock_request
    expected = fhir_service.create_resource(mock_org)

    actual = fhir_api.get_resource_by_id(
        resource_type=mock_org["resourceType"], resource_id=mock_org["id"]
    )

    assert expected == actual


@patch(PATCHED_MODULE)
def test_get_resource_by_id_should_raise_exception_when_resource_is_deleted(
    mock_response: MagicMock, fhir_api: FhirApi, mock_org: Dict[str, Any]
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 410
    mock_response.return_value = mock_request

    with pytest.raises(HTTPException) as e:
        fhir_api.get_resource_by_id(
            resource_id=mock_org["id"], resource_type=mock_org["resourceType"]
        )

    assert e.value.status_code == 410


@patch(PATCHED_MODULE)
def test_get_resource_by_id_should_raise_exception_when_error_code_from_server_occurs(
    mock_response: MagicMock, fhir_api: FhirApi, mock_org: Dict[str, Any]
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 500
    mock_response.return_value = mock_request

    with pytest.raises(HTTPException) as e:
        fhir_api.get_resource_by_id(
            resource_id=mock_org["id"], resource_type=mock_org["resourceType"]
        )

    assert e.value.status_code == 500


@patch(PATCHED_MODULE)
def test_search_resource_should_succeed(
    mock_response: MagicMock,
    mock_org_history_bundle: Dict[str, Any],
    org_history_entry_1: Dict[str, Any],
    org_history_entry_2: Dict[str, Any],
    mock_url: str,
    mock_params: Dict[str, Any],
    fhir_api: FhirApi,
) -> None:
    expected_url = f"{mock_url}?{urlencode(mock_params)}"
    mock_org_history_bundle["link"] = [
        {"relation": "next", "url": f"{mock_url}?{urlencode(mock_params)}"}
    ]
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_org_history_bundle
    mock_response.return_value = mock_request
    expected_entries = [
        create_bundle_entry(org_history_entry_1),
        create_bundle_entry(org_history_entry_2),
    ]

    actual_url, actual_entries = fhir_api.search_resource(
        resource_type="Organization", params=mock_params
    )

    assert (URL(expected_url), expected_entries) == (actual_url, actual_entries)


@patch(PATCHED_MODULE)
def test_search_resource_should_succeed_with_no_next_url(
    mock_response: MagicMock,
    mock_org_history_bundle: Dict[str, Any],
    org_history_entry_1: Dict[str, Any],
    org_history_entry_2: Dict[str, Any],
    mock_params: Dict[str, Any],
    fhir_api: FhirApi,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_org_history_bundle
    mock_response.return_value = mock_request
    expected_entries = [
        create_bundle_entry(org_history_entry_1),
        create_bundle_entry(org_history_entry_2),
    ]

    actual_url, actual_entries = fhir_api.search_resource(
        resource_type="Organization", params=mock_params
    )

    assert expected_entries == actual_entries
    assert actual_url is None


@patch(PATCHED_MODULE)
def test_search_should_raise_exception_when_error_status_code_recieved_from_server(
    mock_response: MagicMock,
    mock_params: Dict[str, Any],
    fhir_api: FhirApi,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 404
    mock_response.return_value = mock_request

    with pytest.raises(HTTPException) as e:
        fhir_api.search_resource(resource_type="Organization", params=mock_params)
    assert e.value.status_code == 500


@patch(PATCHED_MODULE)
def test_post_bundle_should_raise_exception_when_json_invalid_and_http_exception(
    mock_response: MagicMock,
    mock_bundle_request: Bundle,
    fhir_api: FhirApi,
) -> None:
    from requests import Response

    resp = Response()
    resp.status_code = 500
    resp._content = b"Invalid JSON"
    mock_response.return_value = resp

    with pytest.raises(HTTPException) as e:
        fhir_api.post_bundle(mock_bundle_request)
    assert e.value.status_code == 500


@patch(PATCHED_MODULE)
def test_post_bundle_should_raise_exception_when_json_invalid_and_http_success(
    mock_response: MagicMock,
    mock_bundle_request: Bundle,
    fhir_api: FhirApi,
) -> None:
    from requests import Response

    resp = Response()
    resp.status_code = 200
    resp._content = b"Invalid JSON"
    mock_response.return_value = resp

    with pytest.raises(HTTPException) as e:
        fhir_api.post_bundle(mock_bundle_request)
    assert e.value.status_code == 200

@patch(PATCHED_MODULE)
# @patch("app.services.fhir.capability_statement_validator.is_capability_statement_valid")
@patch("app.services.api.fhir_api.is_capability_statement_valid")
def test_update_client_service_check_capability_statement_succeeds(
    mock_is_valid: MagicMock,
    mock_response: MagicMock,
    fhir_api: FhirApi,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = {}
    mock_response.return_value = mock_request
    mock_is_valid.return_value = True

    assert fhir_api.validate_capability_statement() is True
    mock_is_valid.assert_called_once()

@patch(PATCHED_MODULE)
@patch("app.services.api.fhir_api.is_capability_statement_valid")
def test_update_client_service_check_capability_statement_fails(
    mock_is_valid: MagicMock,
    mock_response: MagicMock,
    fhir_api: FhirApi,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 500
    mock_request.json.return_value = {}
    mock_response.return_value = mock_request
    mock_is_valid.return_value = False

    with pytest.raises(HTTPException) as e:
        fhir_api.validate_capability_statement()
    mock_is_valid.assert_not_called()
    assert e.value.status_code == 500
