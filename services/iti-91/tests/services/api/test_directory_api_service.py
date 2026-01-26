from typing import List
import pytest
from unittest.mock import MagicMock
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint
from yarl import URL
from app.models.directory.dto import DirectoryDto
from app.services.api.fhir_api import FhirApi
from app.services.api.directory_api_service import DirectoryApiService
from fastapi import HTTPException


@pytest.fixture
def mock_fhir_api() -> MagicMock:
    return MagicMock(spec=FhirApi)


@pytest.fixture
def provider_url() -> str:
    return "https://base_url.example.com/fhir"


@pytest.fixture
def api_service(
    mock_fhir_api: MagicMock,
    provider_url: str
) -> DirectoryApiService:
    return DirectoryApiService(mock_fhir_api, provider_url)


def __mock_bundle_entry() -> List[BundleEntry]:
    return [
        BundleEntry(
            resource=Organization(
                id="test-org-12345",
                identifier=[
                    {
                        "system": "http://fhir.nl/fhir/NamingSystem/ura",
                        "value": "12345678",
                    }
                ],
                name="Example Organization",
                endpoint=[
                    {
                        "reference": "Endpoint/endpoint-test1",
                    }
                ],
            )
        ),
        BundleEntry(
            resource=Endpoint(
                id="endpoint-test1",
                status="active",
                connectionType={
                    "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                    "code": "hl7-fhir-rest",
                    "display": "HL7 FHIR",
                },
                name="test endpoint 1",
                payloadType=[
                    {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/endpoint-payload-type",
                                "code": "any",
                                "display": "Any",
                            }
                        ]
                    }
                ],
                address="http://example.com/fhir",
            )
        ),
    ]

def __mock_bundle_entry2() -> List[BundleEntry]:
    return [
        BundleEntry(
            resource=Organization(
                id="test-org-12346",
                identifier=[
                    {
                        "system": "http://fhir.nl/fhir/NamingSystem/ura",
                        "value": "12345678",
                    }
                ],
                name="Example Organization",
                endpoint=[
                    {
                        "reference": "https://another-origin.example.com/foo/bar",
                    }
                ],
            )
        ),
    ]

def __mock_bundle_entry3() -> List[BundleEntry]:
    return [
        BundleEntry(
            resource=Organization(
                id="test-org-12347",
                identifier=[
                    {
                        "system": "http://fhir.nl/fhir/NamingSystem/ura",
                        "value": "12345678",
                    }
                ],
                name="Example Organization",
                endpoint=[
                    {
                        "reference": "https://base_url.example.com/fhir/Endpoint/bar",
                    }
                ],
            )
        ),
        BundleEntry(
            resource=Endpoint(
                id="bar",
                status="active",
                connectionType={
                    "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                    "code": "hl7-fhir-rest",
                    "display": "HL7 FHIR",
                },
                name="test endpoint 1",
                payloadType=[
                    {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/endpoint-payload-type",
                                "code": "any",
                                "display": "Any",
                            }
                        ]
                    }
                ],
                address="http://example.com/foo/bar",
            )
        ),
    ]

def __mock_bundle_entry4() -> List[BundleEntry]:
    return [
        BundleEntry(
            resource=Organization(
                id="test-org-12347",
                identifier=[
                    {
                        "system": "http://fhir.nl/fhir/NamingSystem/ura",
                        "value": "12345678",
                    }
                ],
                name="Example Organization",
                endpoint=[
                    {
                        "reference": "https://base_url.example.com/fhir/Endpoint/bar",
                    }
                ],
            )
        ),
    ]

def test_check_if_directory_is_deleted_should_return_false_if_not_deleted(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.get_resource_by_id.return_value = Organization(id="test-org-12345")
    result = api_service.check_if_directory_is_deleted("test-org-12345")
    assert result is False

def test_check_if_directory_is_deleted_should_return_true_if_deleted(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.get_resource_by_id.side_effect = HTTPException(status_code=410)
    result = api_service.check_if_directory_is_deleted("test-org-12345")
    assert result is True

def test_check_if_directory_is_deleted_should_return_false_if_exception(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.get_resource_by_id.side_effect = HTTPException(status_code=500)
    result = api_service.check_if_directory_is_deleted("test-org-12345")
    assert result is False
    mock_fhir_api.get_resource_by_id.side_effect = Exception("Some error")
    result = api_service.check_if_directory_is_deleted("test-org-12345")
    assert result is False


def test_get_endpoint_address_should_skip_if_no_endpoint(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry4()
    )
    dirs = api_service.fetch_directories()
    assert dirs == []


def test_get_endpoint_address_should_skip_if_multiple_endpoints(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    entries = __mock_bundle_entry()
    entries[0].resource.endpoint.append( # type: ignore[attr-defined]
        {
            "reference": "Endpoint/endpoint-test2",
        }
    )
    mock_fhir_api.search_resource.return_value = (
        None,
        entries
    )
    dirs = api_service.fetch_directories()
    assert dirs == []

def test_get_endpoint_address_should_skip_if_no_matching_endpoint(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    entries = __mock_bundle_entry()
    entries[1].resource.id = "some-other-id" # type: ignore[attr-defined]
    mock_fhir_api.search_resource.return_value = (
        None,
        entries
    )
    dirs = api_service.fetch_directories()
    assert dirs == []

def test_get_endpoint_address_should_skip_if_address_missing(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    entries = __mock_bundle_entry()
    try:
        entries[1].resource.address = None # type: ignore[attr-defined]
    except Exception:
        pass
    mock_fhir_api.search_resource.return_value = (
        None,
        entries
    )
    dirs = api_service.fetch_directories()
    assert dirs == []

def test_get_all_directories_should_return_directories(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry(),
    )
    result = api_service.fetch_directories()
    assert result is not None
    assert len(result) == 1
    assert isinstance(result, list)
    assert isinstance(result[0], DirectoryDto)
    assert result[0].id == "test-org-12345"
    assert result[0].endpoint_address == "http://example.com/fhir"


def test_get_all_directories_should_handle_pagination(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    # Mock both page responses using side_effect
    mock_fhir_api.search_resource.return_value = (
        URL("http://example.com/fhir/Organization/?page=2"),
        [],
    )
    mock_fhir_api.search_resource_page.side_effect = [
        (URL("http://example.com/fhir/Organization/?page=3"), []),
        (None, []),
    ]
    api_service.fetch_directories()
    assert mock_fhir_api.search_resource.call_count == 1
    assert mock_fhir_api.search_resource_page.call_count == 2


def test_get_one_directory_should_return_directory(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry()
    )
    result = api_service.fetch_one_directory("test-org-12345")
    assert result is not None
    assert isinstance(result, DirectoryDto)
    assert result.id == "test-org-12345"
    assert result.endpoint_address == "http://example.com/fhir"
    assert result.deleted_at is None


def test_get_one_directory_should_return_none_if_not_found(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.side_effect = Exception("Not Found")
    with pytest.raises(Exception):
        api_service.fetch_one_directory("non-existing-id")

def test_absolute_endpoint_with_different_origin_skips(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock
) -> None:
    """
    mock bundle has an organization with an absolute endpoint, but a different origin than
    the provider URL. It should not be included in the results.
    """
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry2()
    )
    dirs = api_service.fetch_directories()
    assert dirs == []

def test_absolute_endpoint_with_same_origin(
    api_service: DirectoryApiService, mock_fhir_api: MagicMock, provider_url: URL
) -> None:
    """
    mock bundle has an organization with an absolute endpoint that matches the origin of the provider URL.
    """
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry3()
    )
    result = api_service.fetch_directories()
    assert result is not None
    assert len(result) == 1
    assert result[0].endpoint_address == "http://example.com/foo/bar"
