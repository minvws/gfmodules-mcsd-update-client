from typing import List
import pytest
from unittest.mock import MagicMock
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint
from yarl import URL
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.api.fhir_api import FhirApi
from app.services.supplier_provider.api_provider import SupplierApiProvider


@pytest.fixture
def mock_fhir_api() -> MagicMock:
    return MagicMock(spec=FhirApi)


@pytest.fixture
def api_provider(mock_fhir_api: MagicMock, supplier_ignored_directory_service: SupplierIgnoredDirectoryService
) -> SupplierApiProvider:
    return SupplierApiProvider("http://supplier-provider.com", mock_fhir_api, supplier_ignored_directory_service)



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


def test_get_all_suppliers_should_ignore_ignored_if_specified(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock, supplier_ignored_directory_service: SupplierIgnoredDirectoryService
) -> None:
    mock_api_service.do_request.return_value.json.return_value = {
        "data": [
            {"id": "1", "name": "Supplier 1", "endpoint": "http://supplier1.com"},
            {"id": "2", "name": "Supplier 2", "endpoint": "http://supplier2.com"},
        ],
        "next_page_url": None,
    }
    supplier_ignored_directory_service.add_directory_to_ignore_list("1")
    result = api_provider.get_all_suppliers()
    assert result is not None
    assert len(result) == 1
    result = api_provider.get_all_suppliers(include_ignored=True)
    assert result is not None
    assert len(result) == 2
    result = api_provider.get_all_suppliers_include_ignored(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = api_provider.get_all_suppliers_include_ignored(include_ignored_ids=["1"])
    assert result is not None
    assert len(result) == 2

def test_get_all_suppliers_should_return_suppliers(
    api_provider: SupplierApiProvider, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry(),
    )
    result = api_provider.get_all_suppliers()
    assert result is not None
    assert len(result) == 1
    assert isinstance(result, list)
    assert isinstance(result[0], SupplierDto)
    assert result[0].id == "test-org-12345"
    assert result[0].name == "Example Organization"
    assert result[0].endpoint == "http://example.com/fhir"


def test_get_all_suppliers_should_handle_pagination(
    api_provider: SupplierApiProvider, mock_fhir_api: MagicMock
) -> None:
    # Mock both page responses using side_effect
    mock_fhir_api.search_resource.side_effect = [
        (URL("http://example.com/fhir/Organization/?page=2"),[]),
        (URL("http://example.com/fhir/Organization/?page=3"),[]),
        (None,[]),
    ]
    api_provider.get_all_suppliers()
    assert mock_fhir_api.search_resource.call_count == 3


def test_get_one_supplier_should_return_supplier(
    api_provider: SupplierApiProvider, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.return_value = (
        None,
        __mock_bundle_entry()
    )
    result = api_provider.get_one_supplier("test-org-12345")
    assert result is not None
    assert isinstance(result, SupplierDto)
    assert result.id == "test-org-12345"
    assert result.name == "Example Organization"
    assert result.endpoint == "http://example.com/fhir"
    assert not result.is_deleted


def test_get_one_supplier_should_return_none_if_not_found(
    api_provider: SupplierApiProvider, mock_fhir_api: MagicMock
) -> None:
    mock_fhir_api.search_resource.side_effect = Exception("Not Found")
    with pytest.raises(Exception):
        api_provider.get_one_supplier("non-existing-id")
