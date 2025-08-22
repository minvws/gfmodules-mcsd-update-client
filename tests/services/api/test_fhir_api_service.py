from datetime import datetime
from typing import Any, Dict
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock

from fastapi.exceptions import HTTPException
from fhir.resources.R4B.bundle import Bundle
import pytest
from yarl import URL

from app.services.api.fhir_api import FhirApi
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService

PATCHED_MODULE = "app.services.api.api_service.AuthenticationBasedApiService.do_request"


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

    actual = fhir_api.post_bundle(mock_bundle_request)

    assert actual == mock_bundle_response


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

    assert e.value.status_code == 500


@patch(PATCHED_MODULE)
def test_post_bundle_should_fail_on_connection_error(
    mock_response: MagicMock, fhir_api: FhirApi, mock_bundle_request: Bundle
) -> None:
    mock_response.side_effect = ConnectionError
    with pytest.raises(Exception):
        fhir_api.post_bundle(mock_bundle_request)


def test_build_base_history_url_should_succeed(fhir_api: FhirApi) -> None:
    expected = URL("http://example.com/fhir/Organization/_history?_count=10")
    actual = fhir_api.build_base_history_url("Organization")

    assert expected == actual


def test_build_base_history_url_should_succeed_with_since_param(
    fhir_api: FhirApi,
) -> None:
    expected = URL(
        "http://example.com/fhir/Organization/_history?_count=10&_since=2025-01-01T00:00:00"
    )
    actual = fhir_api.build_base_history_url(
        "Organization", datetime.fromisoformat("2025-01-01")
    )

    assert expected == actual


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

    url = fhir_api.build_base_history_url("Organization")
    actual_next_url, actual_entries = fhir_api.get_history_batch(url)

    assert actual_next_url == expected_next_url
    assert expected_entries == actual_entries


@patch(PATCHED_MODULE)
def test_get_history_batch_should_succeed_and_return_next_url(
    mock_response: MagicMock,
    fhir_api: FhirApi,
    mock_org_history_bundle: Dict[str, Any],
    org_history_entry_1: Dict[str, Any],
) -> None:
    expected_next_url = URL("some-next-url")
    expected_entries = [
        create_bundle_entry(org_history_entry_1),
    ]
    mock_org_history_bundle["link"] = [
        {"relation": "next", "url": expected_next_url.with_query(None)}
    ]

    mock_request = MagicMock()
    mock_request.status_code = 200
    mock_request.json.return_value = mock_org_history_bundle
    mock_response.return_value = mock_request

    url = fhir_api.build_base_history_url("Organization")
    actual_next_url, actual_entries = fhir_api.get_history_batch(url)

    assert actual_next_url == expected_next_url
    assert expected_entries == actual_entries


@patch(PATCHED_MODULE)
def test_get_history_should_fail_on_error_status_code(
    mock_response: MagicMock,
    fhir_api: FhirApi,
) -> None:
    mock_request = MagicMock()
    mock_request.status_code = 400
    mock_response.return_value = mock_request
    url = fhir_api.build_base_history_url("Organization")
    with pytest.raises(HTTPException) as e:
        fhir_api.get_history_batch(url)

    assert e.value.status_code == 500


@patch(PATCHED_MODULE)
def test_get_history_should_fail_on_connection_error(
    mock_response: MagicMock, fhir_api: FhirApi
) -> None:
    mock_response.side_effect = ConnectionError
    url = fhir_api.build_base_history_url("Organization")
    with pytest.raises(Exception):
        fhir_api.get_history_batch(url)


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

        assert e.value.status_code == 401


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
    mock_url: URL,
    mock_params: Dict[str, Any],
    fhir_api: FhirApi,
) -> None:
    expected_url = mock_url.with_query(mock_params)
    mock_org_history_bundle["link"] = [
        {"relation": "next", "url": mock_url.with_query(mock_params)}
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

    assert (expected_url, expected_entries) == (actual_url, actual_entries)


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
def test_search_should_raise_exepction_when_error_status_code_recieved_from_server(
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
