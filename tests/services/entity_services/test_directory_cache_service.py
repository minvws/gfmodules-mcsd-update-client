import pytest
from unittest.mock import MagicMock
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.models.directory.dto import DirectoryDto
from app.db.entities.directory_cache import DirectoryCache

@pytest.fixture
def mock_database() -> MagicMock:
    return MagicMock()

@pytest.fixture
def directory_cache_service(mock_database: MagicMock) -> DirectoryCacheService:
    return DirectoryCacheService(database=mock_database)

def test_get_directory_cache_found(directory_cache_service: DirectoryCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_cache = DirectoryCache(directory_id="123", endpoint="http://example.com", is_deleted=False)
    mock_repository.get.return_value = mock_cache
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    result = directory_cache_service.get_directory_cache("123")

    assert isinstance(result, DirectoryDto)
    assert result.id == "123"
    assert result.endpoint == "http://example.com"
    assert result.is_deleted is False
    mock_repository.get.assert_called_once_with("123", with_deleted=False)

def test_get_directory_cache_not_found(directory_cache_service: DirectoryCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_repository.get.return_value = None
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    result = directory_cache_service.get_directory_cache("123")

    assert result is None
    mock_repository.get.assert_called_once_with("123", with_deleted=False)

def test_set_directory_cache_create_new(directory_cache_service: DirectoryCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_repository.get.return_value = None
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    directory_cache_service.set_directory_cache(DirectoryDto(id="123", name="Directory 1", endpoint="http://example.com", is_deleted=False))

    mock_session.add.assert_called_once()

def test_set_directory_cache_update_existing(directory_cache_service: DirectoryCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_cache = DirectoryCache(directory_id="123", endpoint="http://old.com", is_deleted=False)
    mock_repository.get.return_value = mock_cache
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    directory_cache_service.set_directory_cache(DirectoryDto(id="123", name="Directory 1", endpoint="http://new.com", is_deleted=True))

    assert mock_cache.endpoint == "http://new.com"
    assert mock_cache.is_deleted is True
    mock_session.commit.assert_called_once()

def test_delete_directory_cache_found(directory_cache_service: DirectoryCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_cache = DirectoryCache(directory_id="123", endpoint="http://example.com", is_deleted=False)
    mock_repository.get.return_value = mock_cache
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    directory_cache_service.delete_directory_cache("123")

    mock_session.delete.assert_called_once_with(mock_cache)

def test_delete_directory_cache_not_found(directory_cache_service: DirectoryCacheService, mock_database: MagicMock) -> None:
    mock_session = MagicMock()
    mock_repository = MagicMock()
    mock_repository.get.return_value = None
    mock_session.get_repository.return_value = mock_repository
    mock_database.get_db_session.return_value.__enter__.return_value = mock_session

    with pytest.raises(ValueError, match="Directory cache 123 not found for deletion."):
        directory_cache_service.delete_directory_cache("123")

    mock_repository.delete.assert_not_called()