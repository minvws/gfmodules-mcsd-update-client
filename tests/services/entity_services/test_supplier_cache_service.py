import pytest
from unittest.mock import MagicMock
from app.services.entity.supplier_cache_service import SupplierCacheService
from app.models.supplier.dto import SupplierDto
from app.db.entities.supplier_cache import SupplierCache

@pytest.fixture
def mock_database() -> MagicMock:
    return MagicMock()

@pytest.fixture
def supplier_cache_service(mock_database: MagicMock) -> SupplierCacheService:
    return SupplierCacheService(database=mock_database)

def test_get_supplier_cache_found(supplier_cache_service: SupplierCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_cache = SupplierCache(supplier_id="123", endpoint="http://example.com", is_deleted=False)
    mock_repository.get.return_value = mock_cache
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    result = supplier_cache_service.get_supplier_cache("123")

    assert isinstance(result, SupplierDto)
    assert result.id == "123"
    assert result.endpoint == "http://example.com"
    assert result.is_deleted is False
    mock_repository.get.assert_called_once_with("123", with_deleted=False)

def test_get_supplier_cache_not_found(supplier_cache_service: SupplierCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_repository.get.return_value = None
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    result = supplier_cache_service.get_supplier_cache("123")

    assert result is None
    mock_repository.get.assert_called_once_with("123", with_deleted=False)

def test_set_supplier_cache_create_new(supplier_cache_service: SupplierCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_repository.get.return_value = None
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    supplier_cache_service.set_supplier_cache("123", "http://example.com")

    mock_session.add.assert_called_once()

def test_set_supplier_cache_update_existing(supplier_cache_service: SupplierCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_cache = SupplierCache(supplier_id="123", endpoint="http://old.com", is_deleted=False)
    mock_repository.get.return_value = mock_cache
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    supplier_cache_service.set_supplier_cache("123", "http://new.com", is_deleted=True)

    assert mock_cache.endpoint == "http://new.com"
    assert mock_cache.is_deleted is True
    mock_session.commit.assert_called_once()

def test_delete_supplier_cache_found(supplier_cache_service: SupplierCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_cache = SupplierCache(supplier_id="123", endpoint="http://example.com", is_deleted=False)
    mock_repository.get.return_value = mock_cache
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    supplier_cache_service.delete_supplier_cache("123")

    mock_session.delete.assert_called_once_with(mock_cache)

def test_delete_supplier_cache_not_found(supplier_cache_service: SupplierCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_repository.get.return_value = None
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    with pytest.raises(ValueError, match="Supplier cache 123 not found for deletion."):
        supplier_cache_service.delete_supplier_cache("123")

    mock_repository.delete.assert_not_called()