from typing import Any
import pytest
from unittest.mock import MagicMock, patch
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.api_provider import SupplierApiProvider


@pytest.fixture
def mock_api_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def api_provider(mock_api_service: MagicMock, supplier_ignored_directory_service: SupplierIgnoredDirectoryService
) -> SupplierApiProvider:
    return SupplierApiProvider("http://supplier-provider.com", mock_api_service, supplier_ignored_directory_service)


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
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    mock_api_service.do_request.return_value.json.return_value = {
        "entry": [
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "test-org-12345",
                    "identifier": [
                        {
                            "system": "http://fhir.nl/fhir/NamingSystem/ura",
                            "value": "12345678",
                        }
                    ],
                    "name": "Example Organization",
                    "endpoint": [
                        {
                            "reference": "Endpoint/endpoint-test1",
                        }
                    ],
                },
            },
            {
                "resource": {
                    "resourceType": "Endpoint",
                    "id": "endpoint-test1",
                    "status": "active",
                    "connectionType": {
                        "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                        "code": "hl7-fhir-rest",
                        "display": "HL7 FHIR",
                    },
                    "name": "test endpoint 1",
                    "payloadType": [
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
                    "address": "http://example.com/fhir"
                },
            },
        ],
    }
    result = api_provider.get_all_suppliers()
    assert result is not None
    assert len(result) == 1
    assert isinstance(result, list)
    assert isinstance(result[0], SupplierDto)
    assert result[0].id == "test-org-12345"
    assert result[0].name == "Example Organization"
    assert result[0].endpoint == "http://example.com/fhir"


def test_get_all_suppliers_should_handle_pagination(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    # Mock both page responses using side_effect
    mock_api_service.do_request.return_value.json.side_effect = [
        {
            "link": [ {
                "relation": "self",
                "url": "http://example.com/fhir/Organization/"
            },
             {"relation": "next",
                "url": "http://example.com/fhir/Organization/?page=2"
            } ],
        },
        {
            "link": [ {
                "relation": "self",
                "url": "http://example.com/fhir/Organization?_include=Organization%3Aendpoint"
            },
                {"relation": "next",
                    "url": "http://example.com/fhir/Organization/?page=3"
                } ],
            },
        {
            "link": [ {
                "relation": "self",
                "url": "http://example.com/fhir/Organization?_include=Organization%3Aendpoint"
            } ],
        },
    ]

    return_values = []

    original = api_provider._SupplierApiProvider__get_next_page_url # type: ignore

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        return_values.append(result)
        return result

    with patch.object(
        SupplierApiProvider,
        "_SupplierApiProvider__get_next_page_url",
        side_effect=wrapper
    ) as mock_get_next_page_url:

        _ = api_provider.get_all_suppliers()

        assert mock_get_next_page_url.call_count == 3
        assert str(return_values[0]) == "http://example.com/fhir/Organization/?page=2"
        assert str(return_values[1]) == "http://example.com/fhir/Organization/?page=3"
        assert return_values[-1] is None


def test_get_one_supplier_should_return_supplier(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    mock_api_service.do_request.return_value.json.return_value = {
        "entry": [
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "test-org-12345",
                    "identifier": [
                        {
                            "system": "http://fhir.nl/fhir/NamingSystem/ura",
                            "value": "12345678",
                        }
                    ],
                    "name": "Example Organization",
                    "endpoint": [
                        {
                            "reference": "Endpoint/endpoint-test1",
                        }
                    ],
                },
            },
            {
                "resource": {
                    "resourceType": "Endpoint",
                    "id": "endpoint-test1",
                    "status": "active",
                    "connectionType": {
                        "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                        "code": "hl7-fhir-rest",
                        "display": "HL7 FHIR",
                    },
                    "name": "test endpoint 1",
                    "payloadType": [
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
                    "address": "http://example.com/fhir"
                },
            },
        ],
    }
    result = api_provider.get_one_supplier("test-org-12345")
    assert result is not None
    assert isinstance(result, SupplierDto)
    assert result.id == "test-org-12345"
    assert result.name == "Example Organization"
    assert result.endpoint == "http://example.com/fhir"
    assert not result.is_deleted


def test_get_one_supplier_should_return_none_if_not_found(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    mock_api_service.do_request.side_effect = Exception("Not Found")
    with pytest.raises(Exception):
        api_provider.get_one_supplier("non-existing-id")
