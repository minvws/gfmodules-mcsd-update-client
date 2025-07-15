from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
import pytest
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.update.mass_update_client_service import MassUpdateClientService
from app.db.entities.directory_info import DirectoryInfo
from app.models.directory.dto import DirectoryDto
from app.stats import NoopStats


@pytest.fixture
def mock_update_client_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_directory_provider() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_directory_info_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_ignored_directory_service() -> MagicMock:
    mock = MagicMock()
    mock.get_ignored_directory.return_value = None
    return mock


@pytest.fixture
def mock_directory_cache_service() -> MagicMock:
    mock = MagicMock(spec=DirectoryCacheService)
    mock.get_deleted_directories.return_value = []
    return mock


@pytest.fixture
def mass_update_client_service(
    mock_update_client_service: MagicMock,
    mock_directory_provider: MagicMock, 
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_directory_cache_service: MagicMock,
) -> MassUpdateClientService:
    return MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=mock_directory_info_service,
        ignored_directory_service=mock_ignored_directory_service,
        directory_cache_service=mock_directory_cache_service,
        cleanup_client_directory_after_success_timeout_seconds=3600,
        stats=NoopStats(),
        cleanup_client_directory_after_directory_delete=True,
        ignore_directory_after_success_timeout_seconds=3600,
        ignore_directory_after_failed_attempts_threshold=5,
    )


@pytest.fixture
def outdated_directory() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="outdated_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(hours=2),
        failed_attempts=0,
        failed_sync_count=0,
    )


@pytest.fixture
def recent_directory() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="recent_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=30),
        failed_attempts=0,
        failed_sync_count=0,
    )


@pytest.fixture
def never_synced_directory_high_failures() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="never_synced_directory",
        last_success_sync=None,
        failed_attempts=6,
        failed_sync_count=10,
    )


@pytest.fixture
def never_synced_directory_low_failures() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="never_synced_directory",
        last_success_sync=None,
        failed_attempts=3,
        failed_sync_count=5,
    )


@pytest.fixture
def mixed_scenario_directories() -> list[DirectoryInfo]:
    return [
        DirectoryInfo(
            directory_id="very_old_directory",
            last_success_sync=datetime.now(timezone.utc) - timedelta(hours=3),
            failed_attempts=0,
            failed_sync_count=0,
        ),
        DirectoryInfo(
            directory_id="old_directory",
            last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=75),
            failed_attempts=0,
            failed_sync_count=0,
        ),
        DirectoryInfo(
            directory_id="recent_directory",
            last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=30),
            failed_attempts=0,
            failed_sync_count=0,
        ),
        DirectoryInfo(
            directory_id="never_synced",
            last_success_sync=None,
            failed_attempts=5,
            failed_sync_count=10,
        ),
    ]


@pytest.fixture
def already_ignored_failed_directory() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="already_ignored_failed_directory",
        last_success_sync=None,
        failed_attempts=10,
        failed_sync_count=15,
    )


@pytest.fixture
def single_deleted_directory() -> list[DirectoryDto]:
    return [
        DirectoryDto(id="deleted_directory", name="Deleted", endpoint="http://deleted.com", is_deleted=True),
    ]


@pytest.fixture
def never_synced_directory_threshold_failures() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="never_synced_directory",
        last_success_sync=None,
        failed_attempts=5,
        failed_sync_count=10,
    )


@pytest.fixture
def directory_to_ignore_only() -> DirectoryInfo:
    return DirectoryInfo(
        directory_id="directory_to_ignore",
        last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=75),
        failed_attempts=0,
        failed_sync_count=0,
    )


@pytest.fixture
def deleted_directories() -> list[DirectoryDto]:
    return [
        DirectoryDto(id="deleted_1", name="Deleted 1", endpoint="http://deleted1.com", is_deleted=True),
        DirectoryDto(id="deleted_2", name="Deleted 2", endpoint="http://deleted2.com", is_deleted=True),
    ]


def test_cleanup_old_directories_ignores_and_cleans_outdated_directories(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_update_client_service: MagicMock,
    mock_directory_cache_service: MagicMock,
    outdated_directory: DirectoryInfo,
    recent_directory: DirectoryInfo
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = [outdated_directory, recent_directory]

    mass_update_client_service.cleanup_old_directories()

    mock_ignored_directory_service.add_directory_to_ignore_list.assert_called_once_with("outdated_directory")
    mock_update_client_service.cleanup.assert_called_once_with("outdated_directory")
    mock_directory_info_service.delete_directory_info.assert_called_once_with("outdated_directory")
    mock_directory_cache_service.delete_directory_cache.assert_called_once_with("outdated_directory")


def test_cleanup_old_directories_does_not_ignore_already_ignored_directories(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_update_client_service: MagicMock,
    mock_directory_cache_service: MagicMock,
    outdated_directory: DirectoryInfo
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = [outdated_directory]
    mock_ignored_directory_service.get_ignored_directory.return_value = MagicMock()

    mass_update_client_service.cleanup_old_directories()

    mock_ignored_directory_service.add_directory_to_ignore_list.assert_not_called()
    mock_update_client_service.cleanup.assert_called_once_with("outdated_directory")


def test_cleanup_old_directories_only_ignores_when_exceeding_ignore_timeout(
    mock_update_client_service: MagicMock,
    mock_directory_provider: MagicMock,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_directory_cache_service: MagicMock,
    directory_to_ignore_only: DirectoryInfo
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = [directory_to_ignore_only]

    service = MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=mock_directory_info_service,
        ignored_directory_service=mock_ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=7200,
        stats=NoopStats(),
        directory_cache_service=mock_directory_cache_service,
        cleanup_client_directory_after_directory_delete=True,
        ignore_directory_after_success_timeout_seconds=3600,
        ignore_directory_after_failed_attempts_threshold=5,
    )

    service.cleanup_old_directories()

    mock_ignored_directory_service.add_directory_to_ignore_list.assert_called_once_with("directory_to_ignore")
    mock_update_client_service.cleanup.assert_not_called()


def test_cleanup_old_directories_ignores_directories_with_failed_attempts_threshold(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_update_client_service: MagicMock,
    never_synced_directory_high_failures: DirectoryInfo,
    never_synced_directory_low_failures: DirectoryInfo,
    never_synced_directory_threshold_failures: DirectoryInfo
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = [
        never_synced_directory_high_failures, 
        never_synced_directory_low_failures,
        never_synced_directory_threshold_failures
    ]

    mass_update_client_service.cleanup_old_directories()

    assert mock_ignored_directory_service.add_directory_to_ignore_list.call_count == 2
    mock_ignored_directory_service.add_directory_to_ignore_list.assert_any_call("never_synced_directory")
    mock_update_client_service.cleanup.assert_not_called()


def test_cleanup_old_directories_cleans_deleted_directories_from_cache(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_info_service: MagicMock,
    mock_directory_cache_service: MagicMock,
    mock_update_client_service: MagicMock,
    deleted_directories: list[DirectoryDto]
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = []
    mock_directory_cache_service.get_deleted_directories.return_value = deleted_directories

    mass_update_client_service.cleanup_old_directories()

    assert mock_update_client_service.cleanup.call_count == 2
    mock_update_client_service.cleanup.assert_any_call("deleted_1")
    mock_update_client_service.cleanup.assert_any_call("deleted_2")


def test_cleanup_old_directories_skips_deleted_directories_when_disabled(
    mock_update_client_service: MagicMock,
    mock_directory_provider: MagicMock,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_directory_cache_service: MagicMock
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = []

    service = MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=mock_directory_info_service,
        ignored_directory_service=mock_ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=3600,
        stats=NoopStats(),
        directory_cache_service=mock_directory_cache_service,
        cleanup_client_directory_after_directory_delete=False,
        ignore_directory_after_success_timeout_seconds=3600,
        ignore_directory_after_failed_attempts_threshold=5,
    )

    service.cleanup_old_directories()

    mock_directory_cache_service.get_deleted_directories.assert_not_called()


def test_cleanup_old_directories_handles_empty_directory_list(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_update_client_service: MagicMock
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = []

    mass_update_client_service.cleanup_old_directories()

    mock_ignored_directory_service.add_directory_to_ignore_list.assert_not_called()
    mock_update_client_service.cleanup.assert_not_called()


def test_cleanup_old_directories_mixed_scenarios(
    mock_update_client_service: MagicMock,
    mock_directory_provider: MagicMock,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_directory_cache_service: MagicMock,
    mixed_scenario_directories: list[DirectoryInfo],
    single_deleted_directory: list[DirectoryDto]
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = mixed_scenario_directories
    mock_directory_cache_service.get_deleted_directories.return_value = single_deleted_directory

    service = MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=mock_directory_info_service,
        ignored_directory_service=mock_ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=7200,
        stats=NoopStats(),
        directory_cache_service=mock_directory_cache_service,
        cleanup_client_directory_after_directory_delete=True,
        ignore_directory_after_success_timeout_seconds=3600,
        ignore_directory_after_failed_attempts_threshold=5,
    )

    service.cleanup_old_directories()

    assert mock_ignored_directory_service.add_directory_to_ignore_list.call_count == 3
    mock_ignored_directory_service.add_directory_to_ignore_list.assert_any_call("very_old_directory")
    mock_ignored_directory_service.add_directory_to_ignore_list.assert_any_call("old_directory")
    mock_ignored_directory_service.add_directory_to_ignore_list.assert_any_call("never_synced")
    
    assert mock_update_client_service.cleanup.call_count == 2
    mock_update_client_service.cleanup.assert_any_call("very_old_directory")
    mock_update_client_service.cleanup.assert_any_call("deleted_directory")


def test_cleanup_old_directories_does_not_ignore_already_ignored_failed_directories(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_info_service: MagicMock,
    mock_ignored_directory_service: MagicMock,
    mock_update_client_service: MagicMock,
    already_ignored_failed_directory: DirectoryInfo
) -> None:
    mock_directory_info_service.get_all_directories_info.return_value = [already_ignored_failed_directory]
    mock_ignored_directory_service.get_ignored_directory.return_value = MagicMock()

    mass_update_client_service.cleanup_old_directories()

    mock_ignored_directory_service.add_directory_to_ignore_list.assert_not_called()
    mock_update_client_service.cleanup.assert_not_called()
