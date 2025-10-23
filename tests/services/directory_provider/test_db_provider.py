from unittest.mock import MagicMock
import pytest
from fastapi import HTTPException

from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.db_provider import DbProvider
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService


def _dto(
    id_: str, ura: str = "12345678", endpoint_address: str = "http://example.com"
) -> DirectoryDto:
    return DirectoryDto(id=id_, ura=ura, endpoint_address=endpoint_address)


@pytest.fixture
def inner_provider() -> MagicMock:
    return MagicMock(spec=DirectoryProvider)


@pytest.fixture
def directory_info_service() -> MagicMock:
    return MagicMock(spec=DirectoryInfoService)


@pytest.fixture
def db_provider(
    inner_provider: MagicMock, directory_info_service: MagicMock
) -> DbProvider:
    return DbProvider(
        inner=inner_provider, directory_info_service=directory_info_service
    )


def test_get_all_directories_success(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("dir_1"), _dto("dir_2")]
    inner_provider.get_all_directories.return_value = dirs
    directory_info_service.exists.return_value = False
    directory_info_service.create_or_update.side_effect = (
        lambda directory_id, endpoint_address, ura: DirectoryDto(
            id=directory_id, ura=ura, endpoint_address=endpoint_address
        )
    )

    result = db_provider.get_all_directories(include_ignored=False)

    assert len(result) == 2
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"
    inner_provider.get_all_directories.assert_called_once_with(False)
    assert directory_info_service.create_or_update.call_count == 2


def test_get_all_directories_fallback_on_error(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    inner_provider.get_all_directories.side_effect = Exception("Provider error")
    cached_dirs = [_dto("cached_1"), _dto("cached_2")]
    directory_info_service.get_all.return_value = cached_dirs

    result = db_provider.get_all_directories(include_ignored=True)

    assert len(result) == 2
    assert result[0].id == "cached_1"
    assert result[1].id == "cached_2"
    directory_info_service.get_all.assert_called_once_with(include_ignored=True)


def test_get_all_directories_checks_deleted(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("dir_1")]
    inner_provider.get_all_directories.return_value = dirs
    directory_info_service.exists.return_value = True
    inner_provider.is_deleted.return_value = True
    directory_info_service.create_or_update.side_effect = (
        lambda directory_id, endpoint_address, ura: DirectoryDto(
            id=directory_id, ura=ura, endpoint_address=endpoint_address
        )
    )

    result = db_provider.get_all_directories()

    assert len(result) == 1
    directory_info_service.set_deleted_at.assert_called_once_with("dir_1")


def test_get_all_directories_include_ignored_ids_success(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("dir_1"), _dto("dir_2")]
    inner_provider.get_all_directories_include_ignored_ids.return_value = dirs
    directory_info_service.exists.return_value = False
    directory_info_service.create_or_update.side_effect = (
        lambda directory_id, endpoint_address, ura: DirectoryDto(
            id=directory_id, ura=ura, endpoint_address=endpoint_address
        )
    )

    result = db_provider.get_all_directories_include_ignored_ids(
        include_ignored_ids=["dir_1"]
    )

    assert len(result) == 2
    inner_provider.get_all_directories_include_ignored_ids.assert_called_once_with(
        ["dir_1"]
    )
    assert directory_info_service.create_or_update.call_count == 2


def test_get_all_directories_include_ignored_ids_fallback_on_error(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    inner_provider.get_all_directories_include_ignored_ids.side_effect = Exception(
        "Provider error"
    )
    cached_dirs = [_dto("cached_1")]
    directory_info_service.get_all_including_ignored_ids.return_value = cached_dirs

    result = db_provider.get_all_directories_include_ignored_ids(
        include_ignored_ids=["cached_1"]
    )

    assert len(result) == 1
    assert result[0].id == "cached_1"
    directory_info_service.get_all_including_ignored_ids.assert_called_once_with(
        include_ignored_ids=["cached_1"]
    )


def test_get_one_directory_success(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    dir_dto = _dto("dir_1")
    inner_provider.get_one_directory.return_value = dir_dto
    directory_info_service.get_one_by_id.return_value = dir_dto

    result = db_provider.get_one_directory("dir_1")

    assert result.id == "dir_1"
    inner_provider.get_one_directory.assert_called_once_with("dir_1")
    directory_info_service.get_one_by_id.assert_called_once_with("dir_1")


def test_get_one_directory_provider_fails_but_db_has_it(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    inner_provider.get_one_directory.side_effect = Exception("Provider error")
    db_dir = _dto("dir_1")
    directory_info_service.get_one_by_id.return_value = db_dir
    directory_info_service.exists.return_value = True

    result = db_provider.get_one_directory("dir_1")

    assert result.id == "dir_1"
    directory_info_service.get_one_by_id.assert_called_once_with("dir_1")


def test_get_one_directory_not_found_anywhere(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    inner_provider.get_one_directory.side_effect = Exception("Provider error")
    directory_info_service.get_one_by_id.side_effect = Exception("Not in DB")

    with pytest.raises(HTTPException) as exc_info:
        db_provider.get_one_directory("missing_dir")

    assert exc_info.value.status_code == 404
    assert "could not be fetched from provider or found in DB" in exc_info.value.detail


def test_get_one_directory_checks_if_deleted(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    inner_provider.get_one_directory.side_effect = Exception("Provider error")
    directory_info_service.exists.return_value = True
    inner_provider.is_deleted.return_value = True
    db_dir = _dto("dir_1")
    directory_info_service.get_one_by_id.return_value = db_dir

    result = db_provider.get_one_directory("dir_1")

    assert result.id == "dir_1"
    directory_info_service.set_deleted_at.assert_called_once_with("dir_1")


def test_update_directories_in_db(
    db_provider: DbProvider,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("dir_1"), _dto("dir_2")]
    directory_info_service.create_or_update.side_effect = (
        lambda directory_id, endpoint_address, ura: DirectoryDto(
            id=directory_id, ura=ura, endpoint_address=endpoint_address
        )
    )

    result = db_provider._update_directories_in_db(dirs)

    assert len(result) == 2
    assert result[0].id == "dir_1"
    assert result[1].id == "dir_2"
    assert directory_info_service.create_or_update.call_count == 2


def test_check_and_set_if_deleted_skips_new_directories(
    db_provider: DbProvider,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("new_dir")]
    directory_info_service.exists.return_value = False

    db_provider._check_and_set_if_deleted(dirs)

    directory_info_service.set_deleted_at.assert_not_called()


def test_check_and_set_if_deleted_marks_deleted_directories(
    db_provider: DbProvider,
    inner_provider: MagicMock,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("existing_dir")]
    directory_info_service.exists.return_value = True
    inner_provider.is_deleted.return_value = True

    db_provider._check_and_set_if_deleted(dirs)

    directory_info_service.set_deleted_at.assert_called_once_with("existing_dir")


def test_check_and_set_if_deleted_handles_errors(
    db_provider: DbProvider,
    directory_info_service: MagicMock,
) -> None:
    dirs = [_dto("dir_1")]
    directory_info_service.exists.side_effect = Exception("DB error")

    # Should not raise exception
    db_provider._check_and_set_if_deleted(dirs)

    directory_info_service.set_deleted_at.assert_not_called()
