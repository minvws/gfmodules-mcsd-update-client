from typing import Any
from unittest.mock import MagicMock
from datetime import datetime, timedelta
import pytest
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.update.mass_update_client_service import MassUpdateClientService
from app.stats import NoopStats

def create_directory(**kwargs: Any) -> DirectoryDto:
    """Factory function to create Directory with default values and overrides."""
    defaults: dict[str, Any] = {
        'id': 'basic_directory',
        'endpoint_address': 'http://example.com',
        'last_success_sync': None,
        'failed_attempts': 0,
        'failed_sync_count': 0,
        'is_ignored': False,
        'deleted_at': None,
    }
    defaults.update(kwargs)
    return DirectoryDto(**defaults)

@pytest.fixture
def mock_update_client_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_directory_provider() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mass_update_client_service(
    mock_update_client_service: MagicMock,
    mock_directory_provider: MagicMock, 
    directory_info_service: DirectoryInfoService,
) -> MassUpdateClientService:
    return MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=directory_info_service,
        cleanup_client_directory_after_success_timeout_seconds=7200, # 2 hours
        stats=NoopStats(),
        cleanup_client_directory_after_directory_delete=True,
        ignore_directory_after_success_timeout_seconds=3600, # 1 hour
        ignore_directory_after_failed_attempts_threshold=5,
    )


def test_cleanup_old_directories_ignores_outdated_directories(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService
) -> None:
    outdated_directory = create_directory(
        id="outdated_directory",
        last_success_sync=datetime.now() - timedelta(seconds=4000)
    )

    recent_directory = create_directory(
        id="recent_directory", 
        last_success_sync=datetime.now() - timedelta(minutes=30)
    )

    for item in [outdated_directory, recent_directory]:
        directory_info_service.update(item)

    mass_update_client_service.cleanup_old_directories()

    dirs = directory_info_service.get_all(include_ignored=True)
    assert len(dirs) == 2
    for d in dirs:
        if d.id == "recent_directory":
            assert not d.is_ignored
        elif d.id == "outdated_directory":
            assert d.is_ignored
        else:
            pytest.fail(f"Unexpected directory ID: {d.id}")


def test_cleanup_old_directories_only_ignores_when_exceeding_ignore_timeout(
    mass_update_client_service: MassUpdateClientService,
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService
) -> None:
    directory_to_ignore_and_cleanup = create_directory(
        id="directory_to_ignore_and_cleanup",
        last_success_sync=datetime.now() - timedelta(days=100)
    )

    directory_info_service.update(directory_to_ignore_and_cleanup)

    mass_update_client_service.cleanup_old_directories()

    with pytest.raises(Exception):
        directory_info_service.get_one_by_id("directory_to_ignore_and_cleanup")

    mock_update_client_service.cleanup.assert_called_once_with("directory_to_ignore_and_cleanup")


def test_cleanup_old_directories_ignores_directories_with_failed_attempts_threshold(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
    mock_update_client_service: MagicMock
) -> None:
    never_synced_directory_high_failures = create_directory(
        id="never_synced_directory_high_failures",
        last_success_sync=None,
        failed_attempts=6,
        failed_sync_count=10
    )

    never_synced_directory_low_failures = create_directory(
        id="never_synced_directory_low_failures",
        last_success_sync=None,
        failed_attempts=3,
        failed_sync_count=2
    )

    never_synced_directory_threshold_failures = create_directory(
        id="never_synced_directory_threshold_failures",
        last_success_sync=None,
        failed_attempts=5,
        failed_sync_count=10
    )

    for item in [
        never_synced_directory_high_failures, 
        never_synced_directory_low_failures,
        never_synced_directory_threshold_failures
    ]:
        directory_info_service.update(item)

    mass_update_client_service.cleanup_old_directories()

    dirs = directory_info_service.get_all(include_ignored=True)
    assert len(dirs) == 3
    for d in dirs:
        if "never_synced_directory_low_failures" in d.id:
            assert not d.is_ignored
        else:
            assert d.is_ignored
    
    mock_update_client_service.cleanup.assert_not_called()


def test_cleanup_old_directories_cleans_deleted_directories_from_cache(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
    mock_update_client_service: MagicMock
) -> None:
    deleted_directories = [
        create_directory(
            id="deleted_2", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        ),
        create_directory(
            id="deleted_1", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        ),
    ]

    for item in deleted_directories:
        directory_info_service.update(item)

    mass_update_client_service.cleanup_old_directories()

    assert mock_update_client_service.cleanup.call_count == 2
    
    assert len(directory_info_service.get_all(include_ignored=True, include_deleted=True)) == 0


def test_cleanup_old_directories_skips_deleted_directories_when_disabled(
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
    mass_update_client_service: MassUpdateClientService,
) -> None:
    deleted_directories = [
        create_directory(
            id="deleted_2", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        ),
        create_directory(
            id="deleted_1", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        ),
    ]

    for item in deleted_directories:
        directory_info_service.update(item)
    setattr(mass_update_client_service, "_MassUpdateClientService__cleanup_client_directory_after_directory_delete", False)
    mass_update_client_service.cleanup_old_directories()
    assert mock_update_client_service.cleanup.call_count == 0
    assert len(directory_info_service.get_all(include_ignored=True, include_deleted=True)) == 2




def test_cleanup_old_directories_handles_empty_directory_list(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
    mock_update_client_service: MagicMock
) -> None:
    mass_update_client_service.cleanup_old_directories()

    assert len(directory_info_service.get_all(include_ignored=True, include_deleted=True)) == 0
    mock_update_client_service.cleanup.assert_not_called()


def test_cleanup_old_directories_mixed_scenarios(
    mass_update_client_service: MassUpdateClientService,
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    mixed_scenario_directories = [
        create_directory( # Ignored and cleaned up
            id="very_old_directory",
            last_success_sync=datetime.now() - timedelta(hours=30),
        ),
        create_directory( # Ignored but NOT cleaned up
            id="old_directory",
            last_success_sync=datetime.now() - timedelta(seconds=4000),
        ),
        create_directory( # Not ignored, not cleaned up
            id="recent_directory",
            last_success_sync=datetime.now() - timedelta(minutes=10),
        ),
        create_directory( # Never synced so ignored
            id="never_synced",
            last_success_sync=None,
            failed_attempts=20,
            failed_sync_count=20,
        ),
    ]

    for item in mixed_scenario_directories:
        directory_info_service.update(item)

    mass_update_client_service.cleanup_old_directories()

    dirs = directory_info_service.get_all(include_ignored= True) 
    assert len(dirs) == 3
    dirs = directory_info_service.get_all(include_ignored= False)
    assert len(dirs) == 1

    mock_update_client_service.cleanup.assert_any_call("very_old_directory")