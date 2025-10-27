# tests/services/directory_provider/test_fhir_directory_provider.py
import logging
from typing import Any, List
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.services.api.directory_api_service import DirectoryApiService
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.directory_provider.fhir_provider import FhirDirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService
from app.models.directory.dto import DirectoryDto


def _dto(id_: str) -> DirectoryDto:
    mock = MagicMock(spec=DirectoryDto)
    mock.id = id_
    return mock


@pytest.fixture
def api_provider() -> MagicMock:
    return MagicMock(spec=DirectoryApiService)


@pytest.fixture
def info_service() -> MagicMock:
    return MagicMock(spec=DirectoryInfoService)


@pytest.fixture
def provider(api_provider: MagicMock, info_service: MagicMock) -> FhirDirectoryProvider:
    return FhirDirectoryProvider(api_provider, info_service)


@pytest.fixture
def sample_dirs() -> list[DirectoryDto]:
    return [_dto("a"), _dto("b"), _dto("c")]


@pytest.fixture
def ignored_dirs() -> list[DirectoryDto]:
    return [_dto("b")]


def test_get_all_directories_filters_ignored(
    provider: DirectoryProvider,
    api_provider: MagicMock,
    info_service: MagicMock,
    sample_dirs: List[DirectoryDto],
    ignored_dirs: List[str],
) -> None:
    api_provider.fetch_directories.return_value = sample_dirs
    info_service.get_all_ignored.return_value = ignored_dirs

    result = provider.get_all_directories(include_ignored=False)

    assert [d.id for d in result] == ["a", "c"]
    api_provider.fetch_directories.assert_called_once()
    info_service.get_all_ignored.assert_called_once()


def test_get_all_directories_include_ignored_returns_all(
    provider: DirectoryProvider,
    api_provider: MagicMock,
    info_service: MagicMock,
    sample_dirs: List[DirectoryDto],
    ignored_dirs: List[str],
) -> None:
    api_provider.fetch_directories.return_value = sample_dirs
    info_service.get_all_ignored.return_value = (
        ignored_dirs  # should be ignored when include_ignored=True
    )

    result = provider.get_all_directories(include_ignored=True)

    assert [d.id for d in result] == ["a", "b", "c"]


def test_get_all_directories_include_ignored_ids_keeps_requested_ignored(
    provider: DirectoryProvider,
    api_provider: MagicMock,
    info_service: MagicMock,
    sample_dirs: List[DirectoryDto],
    ignored_dirs: List[str],
) -> None:
    api_provider.fetch_directories.return_value = sample_dirs
    info_service.get_all_ignored.return_value = ignored_dirs

    result = provider.get_all_directories_include_ignored_ids(include_ignored_ids=["b"])

    assert [d.id for d in result] == ["a", "b", "c"]


def test_get_all_directories_include_ignored_ids_filters_when_non_empty_list(
    provider: DirectoryProvider,
    api_provider: MagicMock,
    info_service: MagicMock,
    sample_dirs: List[DirectoryDto],
    ignored_dirs: List[str],
) -> None:
    api_provider.fetch_directories.return_value = sample_dirs
    info_service.get_all_ignored.return_value = ignored_dirs

    result = provider.get_all_directories_include_ignored_ids(include_ignored_ids=["z"])

    assert [d.id for d in result] == ["a", "c"]


def test_get_all_directories_include_ignored_ids_empty_list_returns_unfiltered(
    provider: DirectoryProvider,
    api_provider: MagicMock,
    info_service: MagicMock,
    sample_dirs: List[DirectoryDto],
    ignored_dirs: List[str],
) -> None:
    api_provider.fetch_directories.return_value = sample_dirs
    info_service.get_all_ignored.return_value = ignored_dirs

    result = provider.get_all_directories_include_ignored_ids(include_ignored_ids=[])

    assert [d.id for d in result] == ["a", "b", "c"]


def test_get_one_directory_success(
    provider: DirectoryProvider, api_provider: MagicMock
) -> None:
    d = _dto("x")
    api_provider.fetch_one_directory.return_value = d

    result = provider.get_one_directory("x")

    assert result is d


def test_get_one_directory_http_exception_returns_none_and_logs(
    provider: DirectoryProvider, api_provider: MagicMock, caplog: Any
) -> None:
    api_provider.fetch_one_directory.side_effect = HTTPException(
        status_code=404, detail="Not found"
    )

    with caplog.at_level(logging.WARNING):
        with pytest.raises(HTTPException) as exc_info:
            _result = provider.get_one_directory("missing")

    assert exc_info.value.status_code == 404


def test_is_deleted_delegates_to_api_provider(
    provider: DirectoryProvider, api_provider: MagicMock
) -> None:
    dto = _dto("gone")
    api_provider.check_if_directory_is_deleted.return_value = True

    assert provider.is_deleted(dto) is True
    api_provider.check_if_directory_is_deleted.assert_called_once_with("gone")
