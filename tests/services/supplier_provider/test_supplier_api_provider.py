import pytest
from unittest.mock import MagicMock
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
        "data": [
            {"id": "1", "name": "Supplier 1", "endpoint": "http://supplier1.com"},
            {"id": "2", "name": "Supplier 2", "endpoint": "http://supplier2.com"},
        ],
        "next_page_url": None,
    }
    result = api_provider.get_all_suppliers()
    assert result is not None
    assert len(result) == 2
    assert isinstance(result, list)
    assert isinstance(result[0], SupplierDto)
    assert isinstance(result[1], SupplierDto)
    assert result[0].id == "1"
    assert result[0].name == "Supplier 1"
    assert result[1].id == "2"
    assert result[1].name == "Supplier 2"


def test_get_all_suppliers_should_handle_pagination(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    # Mock both page responses using side_effect
    mock_api_service.do_request.return_value.json.side_effect = [
        {
            "data": [
                {"id": "1", "name": "Supplier 1", "endpoint": "http://supplier1.com"},
                {"id": "2", "name": "Supplier 2", "endpoint": "http://supplier2.com"},
            ],
            "next_page_url": "http://example.com/next_page",
        },
        {
            "data": [
                {"id": "3", "name": "Supplier 3", "endpoint": "http://supplier3.com"},
                {"id": "4", "name": "Supplier 4", "endpoint": "http://supplier4.com"},
            ],
            "next_page_url": "http://example.com/next_page_2",
        },
        {
            "data": [
                {"id": "5", "name": "Supplier 5", "endpoint": "http://supplier5.com"},
                {"id": "6", "name": "Supplier 6", "endpoint": "http://supplier6.com"},
            ],
            "next_page_url": None,
        },
    ]

    result = api_provider.get_all_suppliers()
    assert result is not None
    assert len(result) == 6
    assert all(isinstance(s, SupplierDto) for s in result)
    assert [s.id for s in result] == ["1", "2", "3", "4", "5", "6"]
    assert [s.name for s in result] == [
        "Supplier 1",
        "Supplier 2",
        "Supplier 3",
        "Supplier 4",
        "Supplier 5",
        "Supplier 6",
    ]


def test_get_one_supplier_should_return_supplier(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    mock_api_service.do_request.return_value.json.return_value = {
        "id": "1",
        "name": "Supplier 1",
        "endpoint": "http://supplier1.com",
    }
    result = api_provider.get_one_supplier("1")
    assert result is not None
    assert isinstance(result, SupplierDto)
    assert result.id == "1"
    assert result.name == "Supplier 1"


def test_get_one_supplier_should_return_none_if_not_found(
    api_provider: SupplierApiProvider, mock_api_service: MagicMock
) -> None:
    mock_api_service.do_request.side_effect = Exception("Not Found")
    with pytest.raises(Exception):
        api_provider.get_one_supplier("3")
