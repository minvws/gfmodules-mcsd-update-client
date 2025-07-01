from fastapi import HTTPException
import pytest
from unittest.mock import MagicMock
from app.models.directory.dto import DirectoryDto
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.directory_provider.caching_provider import CachingDirectoryProvider
from app.services.directory_provider.api_provider import DirectoryApiProvider
from app.services.entity.directory_cache_service import DirectoryCacheService


@pytest.fixture
def mock_api_provider() -> MagicMock:
    return MagicMock(spec=DirectoryApiProvider)


@pytest.fixture
def mock_cache_service() -> MagicMock:
    return MagicMock(spec=DirectoryCacheService)


@pytest.fixture
def caching_service(
    mock_api_provider: MagicMock, mock_cache_service: MagicMock
) -> CachingDirectoryProvider:
    return CachingDirectoryProvider(mock_api_provider, mock_cache_service)

def test_get_all_directories_should_ignore_ignored_if_specified(
    mock_api_provider: MagicMock,
    directory_cache_service: DirectoryCacheService,
    ignored_directory_service: IgnoredDirectoryService,
) -> None:
    caching_service = CachingDirectoryProvider(
        mock_api_provider,
        directory_cache_service,
    )
    mock_api_provider.get_all_directories.side_effect = ValueError("API error")
    directory_cache_service.set_directory_cache(
        DirectoryDto(
            id="1",
            name="Directory 1",
            endpoint="http://example.com/directory1",
            is_deleted=False,
        )
    )
    directory_cache_service.set_directory_cache(
        DirectoryDto(
        id="2",
        name="Directory 2",
        endpoint="http://example.com/directory2",
        is_deleted=False,
        )
    )
    ignored_directory_service.add_directory_to_ignore_list("1")

    # Test without ignored directories
    result = caching_service.get_all_directories(include_ignored=False)
    assert result is not None
    assert len(result) == 1
    
    # Test with ignored directories
    result = caching_service.get_all_directories(include_ignored=True)
    assert result is not None
    assert len(result) == 2

    result = caching_service.get_all_directories_include_ignored(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = caching_service.get_all_directories_include_ignored(include_ignored_ids=["1"])
    assert result is not None
    assert len(result) == 2


def test_get_all_directories_should_return_api_directories(
    caching_service: CachingDirectoryProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_all_directories.return_value = [
        DirectoryDto(
            id="1",
            name="Directory 1",
            endpoint="http://example.com/directory1",
            is_deleted=False,
        ),
        DirectoryDto(
            id="2",
            name="Directory 2",
            endpoint="http://example.com/directory2",
            is_deleted=False,
        ),
    ]
    mock_cache_service.get_all_directories_caches.return_value = []
    result = caching_service.get_all_directories()
    assert result is not None
    assert len(result) == 2
    assert isinstance(result, list)
    assert isinstance(result[0], DirectoryDto)
    assert isinstance(result[1], DirectoryDto)
    assert result[0].id == "1"
    assert result[0].name == "Directory 1"
    assert result[1].id == "2"
    assert result[1].name == "Directory 2"


def test_get_all_directories_should_return_cached_directories_when_api_returns_none(
    caching_service: CachingDirectoryProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_all_directories.side_effect = ValueError("No directories found")
    mock_cache_service.get_all_directories_caches.return_value = [
        DirectoryDto(
            id="1",
            name="Cached Directory 1",
            endpoint="http://example.com/directory1",
            is_deleted=False,
        ),
        DirectoryDto(
            id="2",
            name="Cached Directory 2",
            endpoint="http://example.com/directory2",
            is_deleted=False,
        ),
    ]
    result = caching_service.get_all_directories()
    assert result is not None
    assert len(result) == 2
    assert result[0].id == "1"
    assert result[0].name == "Cached Directory 1"
    assert result[1].id == "2"
    assert result[1].name == "Cached Directory 2"


def test_get_all_directories_should_update_cache(
    caching_service: CachingDirectoryProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_all_directories.return_value = [
        DirectoryDto(
            id="2",
            name="API Directory",
            endpoint="http://example.com/directory2",
            is_deleted=False,
        )
    ]
    result = caching_service.get_all_directories()
    assert result is not None
    assert len(result) == 1
    assert result[0].id == "2"
    assert result[0].name == "API Directory"
    mock_cache_service.set_directory_cache.assert_called_once()


def test_get_one_directory_should_return_cached_directory(
    caching_service: CachingDirectoryProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_one_directory.side_effect = ValueError("Directory not found")
    mock_cache_service.get_directory_cache.return_value = DirectoryDto(
        id="1",
        name="Cached Directory",
        endpoint="http://example.com/directory1",
        is_deleted=False,
    )
    result = caching_service.get_one_directory("1")
    assert result is not None
    assert result.id == "1"
    assert result.name == "Cached Directory"


def test_get_one_directory_should_update_cache(
    caching_service: CachingDirectoryProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_one_directory.return_value = DirectoryDto(
        id="2", name="API Directory", endpoint="http://api.com", is_deleted=False
    )
    result = caching_service.get_one_directory("2")
    assert result is not None
    assert result.id == "2"
    assert result.name == "API Directory"
    mock_cache_service.set_directory_cache.assert_called_once()

def test_get_one_should_raise_exception_when_not_found(
    caching_service: CachingDirectoryProvider,
    mock_api_provider: MagicMock,
    mock_cache_service: MagicMock,
) -> None:
    mock_api_provider.get_one_directory.side_effect = ValueError("Directory not found")
    mock_cache_service.get_directory_cache.return_value = None
    with pytest.raises(HTTPException):
        caching_service.get_one_directory("nonexistent_id")