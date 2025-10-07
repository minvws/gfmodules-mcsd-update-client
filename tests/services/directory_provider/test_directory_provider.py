from typing import List
import pytest
from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.directory_provider import DirectoryProvider


class MockDirectoryProvider(DirectoryProvider):
    def __init__(self, directories: list[DirectoryDto]) -> None:
        self.directories = directories

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        return self.directories

    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        return self.directories

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        directory =  next((s for s in self.directories if s.id == directory_id), None)
        if directory is None:
            raise ValueError(f"Directory with ID '{directory_id}' not found.")
        return directory

    def directory_is_deleted(self, directory_id: str) -> bool:
        directory =  next((s for s in self.directories if s.id == directory_id), None)
        if directory is None:
            raise ValueError(f"Directory with ID '{directory_id}' not found.")
        return directory.is_deleted


@pytest.fixture
def mock_directories() -> list[DirectoryDto]:
    return [
        DirectoryDto(
            id="1", name="Directory 1", endpoint="http://directory1.com", is_deleted=False
        ),
        DirectoryDto(
            id="2", name="Directory 2", endpoint="http://directory2.com", is_deleted=False
        ),
    ]


@pytest.fixture
def directory_provider(mock_directories: list[DirectoryDto]) -> MockDirectoryProvider:
    return MockDirectoryProvider(mock_directories)


def test_get_all_directories_should_return_directories(
    directory_provider: MockDirectoryProvider, mock_directories: list[DirectoryDto]
) -> None:
    result = directory_provider.get_all_directories()
    assert result == mock_directories
    assert len(result) == 2
    assert result[0].id == "1"
    assert result[1].name == "Directory 2"


def test_get_all_directories_should_return_none_when_no_directories() -> None:
    provider = MockDirectoryProvider([])
    result = provider.get_all_directories()
    assert len(result) == 0


def test_get_one_directory_should_return_directory_when_exists(
    directory_provider: MockDirectoryProvider,
) -> None:
    result = directory_provider.get_one_directory("1")
    assert result is not None
    assert result.id == "1"
    assert result.name == "Directory 1"


def test_get_one_directory_should_return_none_when_not_exists(
    directory_provider: MockDirectoryProvider,
) -> None:
    with pytest.raises(ValueError):
        directory_provider.get_one_directory("3")


def test_get_one_directory_should_return_correct_directory(
    directory_provider: MockDirectoryProvider,
) -> None:
    result = directory_provider.get_one_directory("2")
    assert result is not None
    assert result.id == "2"
    assert result.name == "Directory 2"
