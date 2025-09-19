from fastapi import HTTPException
import pytest
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.json_provider import DirectoryJsonProvider


@pytest.fixture
def json_data() -> list[tuple[str, str]]:
    return [
        ("1", "http://example.com/directory1"),
        ("2", "http://example.com/directory2"),
    ]


@pytest.fixture
def json_provider(json_data: list[tuple[str, str]], directory_info_service: DirectoryInfoService
) -> DirectoryJsonProvider:
    return DirectoryJsonProvider([
        DirectoryDto(id=id, endpoint_address=endpoint)
        for id, endpoint in json_data
    ], directory_info_service=directory_info_service)

def test_get_all_directories_should_ignore_ignored_if_specified(
    json_provider: DirectoryJsonProvider,
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.update(
        DirectoryDto(
            id="1",
            endpoint_address="http://example.com/directory1",
            is_ignored=True,
        )
    )
    directory_info_service.set_ignored_status("1")
    
    # Test without ignored directories
    result = json_provider.get_all_directories(include_ignored=False)
    assert result is not None
    assert len(result) == 1
    assert result[0].id == "2"
    
    # Test with ignored directories
    result = json_provider.get_all_directories(include_ignored=True)
    assert result is not None
    assert len(result) == 2

    result = json_provider.get_all_directories_include_ignored_ids(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = json_provider.get_all_directories_include_ignored_ids(include_ignored_ids=["1"])
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
    assert result[1].id == "2"


def test_get_one_directory_should_return_correct_directory(
    json_provider: DirectoryJsonProvider,
) -> None:
    result = json_provider.get_one_directory("1")
    assert result is not None
    assert isinstance(result, DirectoryDto)
    assert result.id == "1"


def test_get_one_directory_should_return_none_if_not_found(
    json_provider: DirectoryJsonProvider,
) -> None:
    with pytest.raises(HTTPException):
        json_provider.get_one_directory("3")
