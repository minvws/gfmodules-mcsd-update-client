from fastapi import HTTPException
import pytest
from unittest.mock import MagicMock
from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.caching_provider import CachingDirectoryProvider
from app.services.api.directory_api_service import DirectoryApiService
from app.services.entity.directory_info_service import DirectoryInfoService


@pytest.fixture
def mock_api_service() -> MagicMock:
    return MagicMock(spec=DirectoryApiService)


@pytest.fixture
def caching_provider(
    mock_api_service: MagicMock, directory_info_service: DirectoryInfoService
) -> CachingDirectoryProvider:
    return CachingDirectoryProvider(mock_api_service, directory_info_service, validate_capability_statement=False)

def test_get_all_directories_should_ignore_ignored_if_specified(
    mock_api_service: MagicMock,
    caching_provider: CachingDirectoryProvider,
    directory_info_service: DirectoryInfoService,
) -> None:
    mock_api_service.fetch_directories.side_effect = ValueError("API error")
    directory_info_service.update(
            directory_id="1",
            endpoint_address="http://example.com/directory1",
            is_ignored=True,
    )
    directory_info_service.update(
            directory_id="2",
            endpoint_address="http://example.com/directory2",
    )

    # Test without ignored directories
    result = caching_provider.get_all_directories(include_ignored=False)
    assert result is not None
    assert len(result) == 1
    
    # Test with ignored directories
    result = caching_provider.get_all_directories(include_ignored=True)
    assert result is not None
    assert len(result) == 2

    result = caching_provider.get_all_directories_include_ignored_ids(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = caching_provider.get_all_directories_include_ignored_ids(include_ignored_ids=["1"])
    assert result is not None
    assert len(result) == 2


def test_get_all_directories_should_return_api_directories(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    mock_api_service.fetch_directories.return_value = [
        DirectoryDto(
            id="1",
            endpoint_address="http://example.com/directory1",
        ),
        DirectoryDto(
            id="2",
            endpoint_address="http://example.com/directory2",
        ),
    ]
    directory_info_service.get_all()
    result = caching_provider.get_all_directories()
    assert result is not None
    assert len(result) == 2
    assert isinstance(result, list)
    assert isinstance(result[0], DirectoryDto)
    assert isinstance(result[1], DirectoryDto)
    assert result[0].id == "1"
    assert result[1].id == "2"


def test_get_all_directories_should_return_cached_directories_when_api_returns_none(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    mock_api_service.fetch_directories.side_effect = ValueError("No directories found")
    directory_info_service.update(
            directory_id="1",
            endpoint_address="http://example.com/directory1",
    )
    directory_info_service.update(
            directory_id="2",
            endpoint_address="http://example.com/directory2",
    )
    result = caching_provider.get_all_directories()
    assert result is not None
    assert len(result) == 2
    assert result[0].id == "1"
    assert result[1].id == "2"


def test_get_all_directories_should_update_cache(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
) -> None:
    mock_api_service.fetch_directories.return_value = [
        DirectoryDto(
            id="2",
            endpoint_address="http://example.com/directory2",
        )
    ]
    result = caching_provider.get_all_directories()
    assert result is not None
    assert len(result) == 1
    assert result[0].id == "2"


def test_get_one_directory_should_return_cached_directory(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    mock_api_service.fetch_one_directory.side_effect = HTTPException(status_code=404, detail="Directory not found")
    directory_info_service.update(
        directory_id="1",
        endpoint_address="http://example.com/directory1",
    )
    directory_info_service.get_one_by_id("1")
    result = caching_provider.get_one_directory("1")
    assert result is not None
    assert result.id == "1"


def test_get_one_directory_should_update_cache(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    mock_api_service.fetch_one_directory.return_value = DirectoryDto(
        id="2", endpoint_address="http://example.com"
    )
    result = caching_provider.get_one_directory("2")
    assert result is not None
    assert result.id == "2"
    assert directory_info_service.get_one_by_id("2").id == "2"

def test_get_one_should_raise_exception_when_not_found(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
) -> None:
    mock_api_service.fetch_one_directory.side_effect = HTTPException(status_code=404, detail="Directory not found")
    with pytest.raises(HTTPException):
        caching_provider.get_one_directory("nonexistent_id")

def test_get_should_fail_if_capability_statement_check_enabled_and_invalid(
    caching_provider: CachingDirectoryProvider,
    mock_api_service: MagicMock,
) -> None:
    # Simulate the capability statement check failing
    setattr(caching_provider, '_CachingDirectoryProvider__validate_capability_statement', True)
    setattr(caching_provider, 'check_capability_statement', MagicMock(return_value=False))
    mock_api_service.fetch_one_directory.return_value = DirectoryDto(
        id="2", endpoint_address="http://example.com/fhir"
    )
    with pytest.raises(HTTPException) as exc_info:
        caching_provider.get_one_directory("2")
    assert exc_info.value.status_code == 400
    assert "does not support mCSD requirements" in exc_info.value.detail
