from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
import pytest
from requests.exceptions import ConnectionError
from app.models.directory.dto import DirectoryDto
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
        mark_client_directory_as_deleted_after_success_timeout_seconds=7200,  # 2 hours
        stats=NoopStats(),
        mark_client_directory_as_deleted_after_lrza_delete=True,
        ignore_client_directory_after_success_timeout_seconds=3600,  # 1 hour
        ignore_client_directory_after_failed_attempts_threshold=5,
    )


def test_cleanup_old_directories_ignores_outdated_directories(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(
        directory_id="outdated_directory",
        endpoint_address="https://example.com/ignored-fhir",
        ura="12345678",
    )
    directory_info_service.update(
        directory_id="outdated_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(seconds=4000),
    )
    directory_info_service.create(
        directory_id="recent_directory",
        endpoint_address="https://example.com/fhir",
        ura="87654321",
    )
    directory_info_service.update(
        directory_id="recent_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=30),
    )

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
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(
        directory_id="directory_to_ignore_and_cleanup",
        endpoint_address="https://example.com/ignored-fhir",
        ura="12345678",
    )
    directory_info_service.update(
        directory_id="directory_to_ignore_and_cleanup",
        last_success_sync=datetime.now(timezone.utc) - timedelta(days=10000),
    )

    mass_update_client_service.cleanup_old_directories()
    assert directory_info_service.get_one_by_id(
        "directory_to_ignore_and_cleanup"
    ).is_ignored


def test_cleanup_old_directories_ignores_directories_with_failed_attempts_threshold(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
    mock_update_client_service: MagicMock,
) -> None:
    directory_info_service.create(
        directory_id="never_synced_directory_high_failures",
        endpoint_address="https://example.com/ignored-fhir",
        ura="12345678",
    )
    directory_info_service.update(
        directory_id="never_synced_directory_high_failures",
        last_success_sync=None,
        failed_attempts=6,
        failed_sync_count=10,
    )
    directory_info_service.create(
        directory_id="never_synced_directory_low_failures",
        endpoint_address="https://example.com/fhir",
        ura="87654321",
    )
    directory_info_service.update(
        directory_id="never_synced_directory_low_failures",
        last_success_sync=None,
        failed_attempts=3,
        failed_sync_count=2,
    )
    directory_info_service.create(
        directory_id="never_synced_directory_threshold_failures",
        endpoint_address="https://example.com/ignored-fhir",
        ura="11223344",
    )
    directory_info_service.update(
        directory_id="never_synced_directory_threshold_failures",
        last_success_sync=None,
        failed_attempts=5,
        failed_sync_count=10,
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
    mock_update_client_service: MagicMock,
) -> None:
    directory_info_service.create(
        directory_id="deleted_2", endpoint_address="http://example.com", ura="12345678"
    )
    directory_info_service.update(
        directory_id="deleted_2", deleted_at=datetime.now(timezone.utc) - timedelta(days=1)
    )
    directory_info_service.create(
        directory_id="deleted_1", endpoint_address="http://example.com", ura="87654321"
    )
    directory_info_service.update(
        directory_id="deleted_1", deleted_at=datetime.now(timezone.utc) - timedelta(days=1)
    )

    mass_update_client_service.cleanup_old_directories()

    assert mock_update_client_service.cleanup.call_count == 2

    assert (
        len(directory_info_service.get_all(include_ignored=True, include_deleted=True))
        == 0
    )


def test_cleanup_old_directories_skips_deleted_directories_when_disabled(
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
    mass_update_client_service: MassUpdateClientService,
) -> None:
    directory_info_service.create(
        directory_id="deleted_2", endpoint_address="http://example.com", ura="12345678"
    )
    directory_info_service.update(
        directory_id="deleted_2", deleted_at=datetime.now(timezone.utc) - timedelta(days=1)
    )
    directory_info_service.create(
        directory_id="deleted_1", endpoint_address="http://example.com", ura="87654321"
    )
    directory_info_service.update(
        directory_id="deleted_1",
        endpoint_address="http://example.com",
        deleted_at=datetime.now(timezone.utc) - timedelta(days=1),
    )

    setattr(
        mass_update_client_service,
        "_MassUpdateClientService__mark_client_directory_as_deleted_after_lrza_delete",
        False,
    )
    mass_update_client_service.cleanup_old_directories()
    assert mock_update_client_service.cleanup.call_count == 0
    assert (
        len(directory_info_service.get_all(include_ignored=True, include_deleted=True))
        == 2
    )


def test_cleanup_old_directories_handles_empty_directory_list(
    mass_update_client_service: MassUpdateClientService,
    directory_info_service: DirectoryInfoService,
    mock_update_client_service: MagicMock,
) -> None:
    mass_update_client_service.cleanup_old_directories()

    assert (
        len(directory_info_service.get_all(include_ignored=True, include_deleted=True))
        == 0
    )
    mock_update_client_service.cleanup.assert_not_called()


def test_cleanup_old_directories_mixed_scenarios(
    mass_update_client_service: MassUpdateClientService,
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(
        directory_id="very_old_directory",
        endpoint_address="http://example.com",
        ura="12345678",
    )
    directory_info_service.update(
        directory_id="very_old_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(hours=30),
    )
    directory_info_service.create(
        directory_id="old_directory",
        endpoint_address="http://example.com",
        ura="87654321",
    )
    directory_info_service.update(
        directory_id="old_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(seconds=4000),
    )
    directory_info_service.create(
        directory_id="recent_directory",
        endpoint_address="http://example.com",
        ura="11223344",
    )
    directory_info_service.update(
        directory_id="recent_directory",
        last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=10),
    )
    directory_info_service.create(
        directory_id="never_synced",
        endpoint_address="http://example.com",
        ura="44332211",
    )
    directory_info_service.update(
        directory_id="never_synced",
        last_success_sync=None,
        failed_attempts=20,
        failed_sync_count=20,
    )
    directory_info_service.create(
        directory_id="lrza_deleted_directory",
        endpoint_address="http://example.com",
        ura="55667788",
    )
    directory_info_service.update(
        directory_id="lrza_deleted_directory",
        deleted_at=datetime.now(timezone.utc) - timedelta(days=1),
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

    dirs = directory_info_service.get_all(include_ignored=False)  # Only recent
    assert len(dirs) == 1

    deleted_at_dirs = directory_info_service.get_all_deleted()
    assert len(deleted_at_dirs) == 1
    assert (
        deleted_at_dirs[0].id == "very_old_directory"
    )  # Added to the to be deleted list because it was very old

    mock_update_client_service.cleanup.assert_called_once_with("lrza_deleted_directory")


def test_update_all_should_mark_directory_offline_and_persist_reason(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_provider: MagicMock,
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(
        directory_id="offline_directory",
        endpoint_address="https://example.com/fhir",
        ura="12345678",
    )
    mock_directory_provider.get_all_directories.return_value = [
        DirectoryDto(
            id="offline_directory",
            endpoint_address="https://example.com/fhir",
            ura="12345678",
        )
    ]
    mock_update_client_service.update.side_effect = ConnectionError("DNS failure")

    results = mass_update_client_service.update_all()

    assert len(results) == 1
    assert results[0]["directory_id"] == "offline_directory"
    assert results[0]["status"] == "offline"

    info = directory_info_service.get_one_by_id("offline_directory")
    assert info.failed_attempts == 1
    assert info.failed_sync_count == 1
    assert info.reason_ignored == "Directory appears offline/unreachable."


def test_update_all_should_clear_reason_ignored_on_success(
    mass_update_client_service: MassUpdateClientService,
    mock_directory_provider: MagicMock,
    mock_update_client_service: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(
        directory_id="successful_directory",
        endpoint_address="https://example.com/fhir",
        ura="12345678",
    )
    directory_info_service.update(
        directory_id="successful_directory",
        failed_attempts=3,
        failed_sync_count=7,
        reason_ignored="some failure",
    )
    mock_directory_provider.get_all_directories.return_value = [
        DirectoryDto(
            id="successful_directory",
            endpoint_address="https://example.com/fhir",
            ura="12345678",
        )
    ]
    mock_update_client_service.update.return_value = {"status": "success"}

    results = mass_update_client_service.update_all()

    assert len(results) == 1
    assert results[0]["status"] == "success"

    info = directory_info_service.get_one_by_id("successful_directory")
    assert info.failed_attempts == 0
    assert info.reason_ignored == ""
    assert info.last_success_sync is not None


def test_update_all_parallel_should_capture_unhandled_exception(
    mock_update_client_service: MagicMock,
    mock_directory_provider: MagicMock,
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(
        directory_id="boom",
        endpoint_address="https://example.com/fhir",
        ura="12345678",
    )
    directory_info_service.create(
        directory_id="ok",
        endpoint_address="https://example.com/fhir",
        ura="87654321",
    )
    mock_directory_provider.get_all_directories.return_value = [
        DirectoryDto(id="boom", endpoint_address="https://example.com/fhir", ura="12345678"),
        DirectoryDto(id="ok", endpoint_address="https://example.com/fhir", ura="87654321"),
    ]
    mock_update_client_service.update.return_value = {"status": "success"}

    original_get_one_by_id = directory_info_service.get_one_by_id

    def get_one_by_id(directory_id: str):
        if directory_id == "boom":
            raise RuntimeError("unexpected")
        return original_get_one_by_id(directory_id)

    directory_info_service.get_one_by_id = get_one_by_id  # type: ignore[assignment]

    parallel_service = MassUpdateClientService(
        update_client_service=mock_update_client_service,
        directory_provider=mock_directory_provider,
        directory_info_service=directory_info_service,
        mark_client_directory_as_deleted_after_success_timeout_seconds=7200,
        stats=NoopStats(),
        mark_client_directory_as_deleted_after_lrza_delete=True,
        ignore_client_directory_after_success_timeout_seconds=3600,
        ignore_client_directory_after_failed_attempts_threshold=5,
        max_concurrent_directory_updates=2,
    )

    results = parallel_service.update_all()

    assert len(results) == 2
    assert any(r.get("directory_id") == "boom" and r.get("status") == "error" for r in results)
    assert any(r.get("status") == "success" for r in results)
