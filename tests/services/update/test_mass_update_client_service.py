from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.update.mass_update_client_service import MassUpdateClientService
from app.db.entities.directory_info import DirectoryInfo
from app.stats import NoopStats



def test_cleanup_old_directories(directory_info_service: DirectoryInfoService) -> None:
    mock_update_client_service = MagicMock()
    mock_directory_provider = MagicMock()
    mock_directory_info_service = MagicMock()
    mock_ignored_directory_service = MagicMock()

    mock_outdated_directory_info = DirectoryInfo(
        directory_id="outdated_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(hours=2), # more than 1 hour
        failed_attempts=0,
        failed_sync_count=0,
    )
    mock_success_directory_info = DirectoryInfo(
        directory_id="directory_1",
        last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=30), # Less than 1 hour
        failed_attempts=0,
        failed_sync_count=0,
    )
    mock_directory_info_service.get_all_directories_info.return_value = [mock_outdated_directory_info, mock_success_directory_info]

    service = MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=mock_directory_info_service,
        ignored_directory_service=mock_ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=3600, # less than 2 hours
        stats=NoopStats(),
    )

    service.cleanup_old_directories()

    mock_directory_info_service.get_all_directories_info.assert_called_once()
    mock_update_client_service.cleanup.assert_called_once_with("outdated_directory") # It should only call cleanup for the directory that is older than 1 hour
    mock_ignored_directory_service.add_directory_to_ignore_list.assert_called_once_with("outdated_directory")

    # it is now ignored, so it should not be updated on the next call
    mock_directory_info_service.get_all_directories_info.return_value = []

    service.cleanup_old_directories()
    mock_directory_info_service.get_all_directories_info.assert_called()
    assert mock_update_client_service.cleanup.call_count == 1 # it should not be called again
