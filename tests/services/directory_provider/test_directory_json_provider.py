from unittest.mock import MagicMock
from fastapi import HTTPException
import pytest
from app.models.directory.dto import DirectoryDto
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.directory_provider.json_provider import DirectoryJsonProvider


@pytest.fixture
def json_data() -> list[tuple[str, str, str]]:
    return [
        ("1", "Directory 1", "http://directory1.com"),
        ("2", "Directory 2", "http://directory2.com"),
    ]


@pytest.fixture
def json_provider(json_data: list[tuple[str, str, str]], ignored_directory_service: IgnoredDirectoryService
) -> DirectoryJsonProvider:
    return DirectoryJsonProvider([
        DirectoryDto(id=id, name=name, endpoint=endpoint)
        for id, name, endpoint in json_data
    ], ignored_directory_service=ignored_directory_service)

def test_get_all_directories_should_ignore_ignored_if_specified(
    json_provider: DirectoryJsonProvider,
    ignored_directory_service: IgnoredDirectoryService,
) -> None:
    ignored_directory_service.add_directory_to_ignore_list("1")
    
    # Test without ignored directories
    result = json_provider.get_all_directories(include_ignored=False)
    assert result is not None
    assert len(result) == 1
    assert result[0].id == "2"
    
    # Test with ignored directories
    result = json_provider.get_all_directories(include_ignored=True)
    assert result is not None
    assert len(result) == 2

    result = json_provider.get_all_directories_include_ignored(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = json_provider.get_all_directories_include_ignored(include_ignored_ids=["1"])
    assert result is not None
    assert len(result) == 2

def test_get_all_directories_should_return_all_directories(
    json_provider: DirectoryJsonProvider,
) -> None:
    result = json_provider.get_all_directories()
    assert len(result) == 2
    assert isinstance(result, list)
    assert isinstance(result[0], DirectoryDto)
    assert isinstance(result[1], DirectoryDto)
    assert result[0].id == "1"
    assert result[0].name == "Directory 1"
    assert result[1].id == "2"
    assert result[1].name == "Directory 2"


def test_get_one_directory_should_return_correct_directory(
    json_provider: DirectoryJsonProvider,
) -> None:
    result = json_provider.get_one_directory("1")
    assert result is not None
    assert isinstance(result, DirectoryDto)
    assert result.id == "1"
    assert result.name == "Directory 1"


def test_get_one_directory_should_return_none_if_not_found(
    json_provider: DirectoryJsonProvider,
) -> None:
    with pytest.raises(HTTPException):
        json_provider.get_one_directory("3")

def test_get_should_fail_if_capability_statement_check_enabled_and_invalid(
    json_provider: DirectoryJsonProvider,
) -> None:
    # Simulate the capability statement check failing
    setattr(json_provider, '_DirectoryJsonProvider__validate_capability_statement', True)
    setattr(json_provider, 'check_capability_statement', MagicMock(return_value=False))
    with pytest.raises(HTTPException) as exc_info:
        json_provider.get_one_directory("2")
    assert exc_info.value.status_code == 400
    assert "does not support mCSD requirements" in exc_info.value.detail