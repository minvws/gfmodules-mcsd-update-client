from unittest.mock import MagicMock
from datetime import datetime, timedelta
import pytest
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.update.mass_update_client_service import MassUpdateClientService
from app.stats import NoopStats


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
        mark_client_directory_as_deleted_after_success_timeout_seconds=7200, # 2 hours
        stats=NoopStats(),
        mark_client_directory_as_deleted_after_lrza_delete=True,
        ignore_client_directory_after_success_timeout_seconds=3600, # 1 hour
        ignore_client_directory_after_failed_attempts_threshold=5,
    )


def test_cleanup_old_directories_ignores_outdated_directories(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService
) -> None:

    directory_info_service.update(directory_id="outdated_directory", endpoint_address="https://example.com/ignored-fhir", last_success_sync=datetime.now() - timedelta(seconds=4000))
    directory_info_service.update(directory_id="recent_directory", endpoint_address="https://example.com/fhir", last_success_sync=datetime.now() - timedelta(minutes=30))

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
    directory_info_service.update(
        directory_id="directory_to_ignore_and_cleanup",
        endpoint_address="https://example.com/ignored-fhir",
        last_success_sync=datetime.now() - timedelta(days=10000))

    mass_update_client_service.cleanup_old_directories()

    assert directory_info_service.get_one_by_id("directory_to_ignore_and_cleanup").is_ignored


def test_cleanup_old_directories_ignores_directories_with_failed_attempts_threshold(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
    mock_update_client_service: MagicMock
) -> None:
    directory_info_service.update(
        directory_id="never_synced_directory_high_failures",
        endpoint_address="https://example.com/ignored-fhir",
        last_success_sync=None,
        failed_attempts=6,
        failed_sync_count=10
    )

    directory_info_service.update(
        directory_id="never_synced_directory_low_failures",
        endpoint_address="https://example.com/fhir",
        last_success_sync=None,
        failed_attempts=3,
        failed_sync_count=2
    )

    directory_info_service.update(
        directory_id="never_synced_directory_threshold_failures",
        endpoint_address="https://example.com/ignored-fhir",
        last_success_sync=None,
        failed_attempts=5,
        failed_sync_count=10
    )

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
    directory_info_service.update(
        directory_id="deleted_2", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        )
    directory_info_service.update(
        directory_id="deleted_1", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        )

    mass_update_client_service.cleanup_old_directories()

    assert mock_update_client_service.cleanup.call_count == 2
    
    assert len(directory_info_service.get_all(include_ignored=True, include_deleted=True)) == 0


def test_cleanup_old_directories_skips_deleted_directories_when_disabled(
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
    mass_update_client_service: MassUpdateClientService,
) -> None:
    directory_info_service.update(
        directory_id="deleted_2", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        )
    directory_info_service.update(
        directory_id="deleted_1", 
            endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1)
        )

    setattr(mass_update_client_service, "_MassUpdateClientService__mark_client_directory_as_deleted_after_lrza_delete", False)
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
    directory_info_service.update(
        directory_id="very_old_directory",
        endpoint_address="http://example.com", 
            last_success_sync=datetime.now() - timedelta(hours=30),
        )
    directory_info_service.update(
        directory_id="old_directory",
        endpoint_address="http://example.com", 
            last_success_sync=datetime.now() - timedelta(seconds=4000),
        )
    directory_info_service.update(
        directory_id="recent_directory",
        endpoint_address="http://example.com", 
            last_success_sync=datetime.now() - timedelta(minutes=10),
        )
    directory_info_service.update(
        directory_id="never_synced",
        endpoint_address="http://example.com", 
            last_success_sync=None,
            failed_attempts=20,
            failed_sync_count=20,
        )
    directory_info_service.update(
        directory_id="lrza_deleted_directory",
        endpoint_address="http://example.com", 
            deleted_at=datetime.now() - timedelta(days=1),
        )

    mass_update_client_service.cleanup_old_directories()

    dirs = directory_info_service.get_all(include_ignored=True)
    assert len(dirs) == 3
    # Assert the correct directories are ignored
    for d in dirs:
        if d.id in ["very_old_directory", "never_synced", "old_directory"]:
            assert d.is_ignored
        else:
            assert not d.is_ignored

    dirs = directory_info_service.get_all(include_ignored=False) # Only recent
    assert len(dirs) == 1

    deleted_at_dirs = directory_info_service.get_all_deleted()
    assert len(deleted_at_dirs) == 1
    assert deleted_at_dirs[0].id == "very_old_directory" # Added to the to be deleted list because it was very old

    mock_update_client_service.cleanup.assert_called_once_with("lrza_deleted_directory")