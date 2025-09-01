from typing import List
import pytest
from unittest.mock import MagicMock
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint
from yarl import URL
from app.models.directory.dto import DirectoryDto
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.api.fhir_api import FhirApi
from app.services.directory_provider.api_provider import DirectoryApiProvider


@pytest.fixture
def mock_fhir_api() -> MagicMock:
    return MagicMock(spec=FhirApi)


@pytest.fixture
def provider_url() -> str:
    return "https://base_url.example.com/fhir"


@pytest.fixture
def api_provider(
    mock_fhir_api: MagicMock,
    ignored_directory_service: IgnoredDirectoryService,
    provider_url: str
) -> DirectoryApiProvider:
    return DirectoryApiProvider(mock_fhir_api, ignored_directory_service, provider_url)


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

def test_get_all_directories_should_ignore_ignored_if_specified(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock, ignored_directory_service: IgnoredDirectoryService
) -> None:
    entries = __mock_bundle_entry()
    entries.append(
        BundleEntry(
            resource=Organization(
                id="test-org-6789",
                identifier=[
                    {
                        "system": "http://fhir.nl/fhir/NamingSystem/ura",
                        "value": "87654321",
                    }
                ],
                name="Example Organization",
                endpoint=[
                    {
                        "reference": "Endpoint/endpoint-test1",
                    }
                ],
            )
        )
    )
    mock_fhir_api.search_resource.return_value = (
        None,
        entries
    )
    ignored_directory_service.add_directory_to_ignore_list("test-org-6789")
    result = api_provider.get_all_directories()
    assert result is not None
    assert len(result) == 1
    result = api_provider.get_all_directories(include_ignored=True)
    assert result is not None
    assert len(result) == 2
    result = api_provider.get_all_directories_include_ignored(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = api_provider.get_all_directories_include_ignored(include_ignored_ids=["test-org-6789"])
    assert result is not None
    assert len(result) == 2

def test_get_all_directories_should_return_directories(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry(),
    )
    result = api_provider.get_all_directories()
    assert result is not None
    assert len(result) == 1
    assert isinstance(result, list)
    assert isinstance(result[0], DirectoryDto)
    assert result[0].id == "test-org-12345"
    assert result[0].name == "Example Organization"
    assert result[0].endpoint == "http://example.com/fhir"


def test_get_all_directories_should_handle_pagination(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock
) -> None:
    # Mock both page responses using side_effect
    mock_fhir_api.search_resource.side_effect = [
        (URL("http://example.com/fhir/Organization/?page=2"),[]),
        (URL("http://example.com/fhir/Organization/?page=3"),[]),
        (None,[]),
    ]
    api_provider.get_all_directories()
    assert mock_fhir_api.search_resource.call_count == 3


def test_get_one_directory_should_return_directory(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry()
    )
    result = api_provider.get_one_directory("test-org-12345")
    assert result is not None
    assert isinstance(result, DirectoryDto)
    assert result.id == "test-org-12345"
    assert result.name == "Example Organization"
    assert result.endpoint == "http://example.com/fhir"
    assert not result.is_deleted


def test_get_one_directory_should_return_none_if_not_found(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.side_effect = Exception("Not Found")
    with pytest.raises(Exception):
        api_provider.get_one_directory("non-existing-id")

def test_absolute_endpoint_with_different_origin(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock
) -> None:
    """
    mock bundle has an organization with an absolute endpoint, but a different origin than
    the provider URL. It should not be included in the results.
    """
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry2()
    )
    with pytest.raises(ValueError):
        api_provider.get_all_directories()

def test_absolute_endpoint_with_same_origin(
    api_provider: DirectoryApiProvider, mock_fhir_api: MagicMock, provider_url: URL
) -> None:
    """
    mock bundle has an organization with an absolute endpoint that matches the origin of the provider URL.
    """
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry3()
    )
    result = api_provider.get_all_directories()
    assert result is not None
    assert len(result) == 1
    assert result[0].endpoint == "http://example.com/foo/bar"
