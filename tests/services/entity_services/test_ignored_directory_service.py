import pytest
from fastapi import HTTPException
from app.db.entities.directory_info import DirectoryInfo
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.entity.directory_info_service import DirectoryInfoService


def test_add_directory_to_ignore_list(ignored_directory_service: IgnoredDirectoryService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("new_directory")
    result = ignored_directory_service.get_ignored_directory("new_directory")
    assert result is not None
    assert result.directory_id == "new_directory"

def test_filter_ignored_directory_info(ignored_directory_service: IgnoredDirectoryService, directory_info_service: DirectoryInfoService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("ignored_1")
    mock_directories = [
        DirectoryInfo(directory_id="ignored_1",last_success_sync=None, failed_attempts=0, failed_sync_count=0),
        DirectoryInfo(directory_id="not_ignored_1", last_success_sync=None, failed_attempts=0, failed_sync_count=0),
    ]
    for directory in mock_directories:
        directory_info_service.update_directory_info(directory)
    result = directory_info_service.get_all_directories_info(include_ignored=False)
    assert len(result) == 1
    assert result[0].directory_id == "not_ignored_1"
    result = directory_info_service.get_all_directories_info(include_ignored=True)
    assert len(result) == 2
    assert result[0].directory_id == "ignored_1"
    assert result[1].directory_id == "not_ignored_1"


def test_filter_ignored_directory_dto(ignored_directory_service: IgnoredDirectoryService, directory_cache_service: DirectoryCacheService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("ignored_1")
    directory_cache_service.set_directory_cache(DirectoryDto(id="ignored_1", name="ignored", endpoint="ignored"))
    directory_cache_service.set_directory_cache(DirectoryDto(id="not_ignored_1", name="not_ignored", endpoint="not_ignored"))
    result = directory_cache_service.get_all_directories_caches(with_deleted=True, include_ignored=False)
    assert len(result) == 1
    assert result[0].id == "not_ignored_1"
    result = directory_cache_service.get_all_directories_caches(with_deleted=True, include_ignored=True)
    assert len(result) == 2
    assert result[0].id == "ignored_1"
    assert result[1].id == "not_ignored_1"


def test_get_ignored_directory(ignored_directory_service: IgnoredDirectoryService) -> None:
    test_id = "ignored_1"
    ignored_directory_service.add_directory_to_ignore_list(test_id)
    result = ignored_directory_service.get_ignored_directory(test_id)
    assert result is not None
    assert result.directory_id == test_id


def test_get_all_ignored_directories(ignored_directory_service: IgnoredDirectoryService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("ignored_1")
    ignored_directory_service.add_directory_to_ignore_list("ignored_2")
    result = ignored_directory_service.get_all_ignored_directories()
    assert len(result) == 2
    assert result[0].directory_id == "ignored_1"
    assert result[1].directory_id == "ignored_2"


def test_add_directory_to_ignore_list_conflict(ignored_directory_service: IgnoredDirectoryService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    with pytest.raises(HTTPException) as exc_info:
        ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    assert exc_info.value.status_code == 409


def test_remove_directory_from_ignore_list(ignored_directory_service: IgnoredDirectoryService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    ignored_directory_service.remove_directory_from_ignore_list("existing_directory")
    result = ignored_directory_service.get_ignored_directory("existing_directory")
    assert result is None


def test_remove_directory_from_ignore_list_not_found(ignored_directory_service: IgnoredDirectoryService) -> None:
    with pytest.raises(HTTPException) as exc_info:
        ignored_directory_service.remove_directory_from_ignore_list("non_existing_directory")
    assert exc_info.value.status_code == 404

def test_remove_directory_from_ignore_list_success(ignored_directory_service: IgnoredDirectoryService) -> None:
    ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    ignored_directory_service.remove_directory_from_ignore_list("existing_directory")
    result = ignored_directory_service.get_ignored_directory("existing_directory")
    assert result is None