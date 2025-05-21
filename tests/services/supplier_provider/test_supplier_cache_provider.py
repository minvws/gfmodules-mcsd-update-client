from fastapi import HTTPException
import pytest
from unittest.mock import MagicMock
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.caching_provider import CachingSupplierProvider
from app.services.supplier_provider.api_provider import SupplierApiProvider
from app.services.entity.supplier_cache_service import SupplierCacheService


@pytest.fixture
def mock_api_provider() -> MagicMock:
    return MagicMock(spec=SupplierApiProvider)


@pytest.fixture
def mock_cache_service() -> MagicMock:
    return MagicMock(spec=SupplierCacheService)


@pytest.fixture
def caching_service(
    mock_api_provider: MagicMock, mock_cache_service: MagicMock
) -> CachingSupplierProvider:
    return CachingSupplierProvider(mock_api_provider, mock_cache_service)

def test_get_all_suppliers_should_ignore_ignored_if_specified(
    mock_api_provider: MagicMock,
    supplier_cache_service: SupplierCacheService,
    supplier_ignored_directory_service: SupplierIgnoredDirectoryService,
) -> None:
    caching_service = CachingSupplierProvider(
        mock_api_provider,
        supplier_cache_service,
    )
    mock_api_provider.get_all_suppliers.side_effect = ValueError("API error")
    supplier_cache_service.set_supplier_cache(
        supplier_id="1",
        endpoint="http://example.com/supplier1",
        is_deleted=False,
    )
    supplier_cache_service.set_supplier_cache(
        supplier_id="2",
        endpoint="http://example.com/supplier2",
        is_deleted=False,
    )
    supplier_ignored_directory_service.add_directory_to_ignore_list("1")

    # Test without ignored suppliers
    result = caching_service.get_all_suppliers(include_ignored=False)
    assert result is not None
    assert len(result) == 1
    
    # Test with ignored suppliers
    result = caching_service.get_all_suppliers(include_ignored=True)
    assert result is not None
    assert len(result) == 2

    result = caching_service.get_all_suppliers_include_ignored(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = caching_service.get_all_suppliers_include_ignored(include_ignored_ids=["1"])
    assert result is not None
    assert len(result) == 2


def test_get_all_suppliers_should_return_api_suppliers(
    caching_service: CachingSupplierProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_all_suppliers.return_value = [
        SupplierDto(
            id="1",
            name="Supplier 1",
            endpoint="http://example.com/supplier1",
            is_deleted=False,
            ura_number="12345678"
        ),
        SupplierDto(
            id="2",
            name="Supplier 2",
            endpoint="http://example.com/supplier2",
            is_deleted=False,
            ura_number="87654321"
        ),
    ]
    mock_cache_service.get_all_suppliers_caches.return_value = []
    result = caching_service.get_all_suppliers()
    assert result is not None
    assert len(result) == 2
    assert isinstance(result, list)
    assert isinstance(result[0], SupplierDto)
    assert isinstance(result[1], SupplierDto)
    assert result[0].id == "1"
    assert result[0].name == "Supplier 1"
    assert result[1].id == "2"
    assert result[1].name == "Supplier 2"


def test_get_all_suppliers_should_return_cached_suppliers_when_api_returns_none(
    caching_service: CachingSupplierProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_all_suppliers.side_effect = ValueError("No suppliers found")
    mock_cache_service.get_all_suppliers_caches.return_value = [
        SupplierDto(
            id="1",
            name="Cached Supplier 1",
            endpoint="http://example.com/supplier1",
            is_deleted=False,
            ura_number="12345678"
        ),
        SupplierDto(
            id="2",
            name="Cached Supplier 2",
            endpoint="http://example.com/supplier2",
            is_deleted=False,
            ura_number="87654321"
        ),
    ]
    result = caching_service.get_all_suppliers()
    assert result is not None
    assert len(result) == 2
    assert result[0].id == "1"
    assert result[0].name == "Cached Supplier 1"
    assert result[1].id == "2"
    assert result[1].name == "Cached Supplier 2"


def test_get_all_suppliers_should_update_cache(
    caching_service: CachingSupplierProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_all_suppliers.return_value = [
        SupplierDto(
            id="2",
            name="API Supplier",
            endpoint="http://example.com/supplier2",
            is_deleted=False,
            ura_number="87654321"
        )
    ]
    result = caching_service.get_all_suppliers()
    assert result is not None
    assert len(result) == 1
    assert result[0].id == "2"
    assert result[0].name == "API Supplier"
    mock_cache_service.set_supplier_cache.assert_called_once()


def test_get_one_supplier_should_return_cached_supplier(
    caching_service: CachingSupplierProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_one_supplier.side_effect = ValueError("Supplier not found")
    mock_cache_service.get_supplier_cache.return_value = SupplierDto(
        id="1",
        name="Cached Supplier",
        endpoint="http://example.com/supplier1",
        is_deleted=False,
        ura_number="12345678"
    )
    result = caching_service.get_one_supplier("1")
    assert result is not None
    assert result.id == "1"
    assert result.name == "Cached Supplier"


def test_get_one_supplier_should_update_cache(
    caching_service: CachingSupplierProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_one_supplier.return_value = SupplierDto(
        id="2", name="API Supplier", endpoint="http://api.com", is_deleted=False, ura_number="87654321"
    )
    result = caching_service.get_one_supplier("2")
    assert result is not None
    assert result.id == "2"
    assert result.name == "API Supplier"
    mock_cache_service.set_supplier_cache.assert_called_once()

def test_get_one_should_raise_exception_when_not_found(
    caching_service: CachingSupplierProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_one_supplier.side_effect = ValueError("Supplier not found")
    mock_cache_service.get_supplier_cache.return_value = None
    with pytest.raises(HTTPException):
        caching_service.get_one_supplier("nonexistent_id")