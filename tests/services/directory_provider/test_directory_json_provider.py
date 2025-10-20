from unittest.mock import MagicMock
import pytest
from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.json_provider import DirectoryJsonProvider
from app.services.entity.directory_info_service import DirectoryInfoService


def dto_list() -> list[DirectoryDto]:
    return [
        DirectoryDto(id="dir_1", ura="87654321", endpoint_address="http://example.com/1"),
        DirectoryDto(id="dir_2", ura="87654322", endpoint_address="http://example.com/2"),
        DirectoryDto(id="dir_3", ura="87654323", endpoint_address="http://example.com/3", is_ignored=True),
    ]


@pytest.fixture
def dir_info_service() -> MagicMock:
    service = MagicMock()
    service.get_all_ignored = MagicMock(return_value=[])
    return service

@pytest.fixture
def json_provider(dir_info_service: DirectoryInfoService, monkeypatch: pytest.MonkeyPatch) -> DirectoryJsonProvider:
    monkeypatch.setattr(
        DirectoryJsonProvider,
        "_read_directories_file",
        staticmethod(lambda _path: dto_list()),
    )
    return DirectoryJsonProvider("/tmp/not/existing.json", dir_info_service)


def test_get_all_dirs(
    json_provider: DirectoryJsonProvider,
    dir_info_service: DirectoryInfoService,
) -> None:
    dir_info_service.get_all_ignored = MagicMock(return_value=[DirectoryDto(id="dir_3", endpoint_address="http://example.com/1")])   # type: ignore

    # Test without ignored directories
    result = json_provider.get_all_directories(include_ignored=False)
    assert result is not None
    assert len(result) == 2
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"

    # Test with ignored directories
    result = json_provider.get_all_directories(include_ignored=True)
    assert result is not None
    assert len(result) == 3
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"
    assert result[2].id == "dir_3"

    result = json_provider.get_all_directories_include_ignored_ids(include_ignored_ids=["dir_3"])
    assert result is not None
    assert len(result) == 3
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"
    assert result[2].id == "dir_3"

    result = json_provider.get_all_directories_include_ignored_ids(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 2
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"

    result = json_provider.get_all_directories_include_ignored_ids(include_ignored_ids=["dir_1"])
    assert result is not None
    assert len(result) == 2
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"

def test_get_all_directories_should_return_all_directories(
    json_provider: DirectoryJsonProvider,
) -> None:
    result = json_provider.get_all_directories()
    assert len(result) == 3
    assert isinstance(result, list)
    assert isinstance(result[0], DirectoryDto)
    assert isinstance(result[1], DirectoryDto)
    assert isinstance(result[2], DirectoryDto)
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"
    assert result[2].id == "dir_3"


def test_get_one_directory_should_return_correct_directory(
    json_provider: DirectoryJsonProvider,
) -> None:
    result = json_provider.get_one_directory("dir_1")
    assert result is not None
    assert isinstance(result, DirectoryDto)
    assert result.id == "dir_1"


def test_get_one_directory_should_return_none_if_not_found(
    json_provider: DirectoryJsonProvider,
) -> None:
    result = json_provider.get_one_directory("dir_not_existing")
    assert result is None
