from fastapi import HTTPException
import pytest
from datetime import datetime, timedelta

from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService


@pytest.fixture
def sample_directory_info_ignored() -> DirectoryDto:
    return DirectoryDto(
        id="test-directory-ignored",
        ura="87654321",
        endpoint_address="https://example.com/fhir",
        failed_sync_count=2,
        failed_attempts=1,
        last_success_sync=datetime.now() - timedelta(hours=2),
        is_ignored=True,
    )

@pytest.fixture
def sample_directory_info_deleted() -> DirectoryDto:
    return DirectoryDto(
        id="test-directory-deleted",
        ura="87654321",
        endpoint_address="https://example.com/fhir",
        last_success_sync=datetime.now() - timedelta(hours=1),
        deleted_at=datetime.now(),
    )

def test_create_directory_info(
    directory_info_service: DirectoryInfoService,
) -> None:
    """Test updating a directory info entry."""
    with pytest.raises(HTTPException, match="Directory with ID test-directory-1 not found"):
        directory_info_service.get_one_by_id("test-directory-1")

    created = directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    assert created.id == "test-directory-1"
    assert created.endpoint_address == "https://example.com/fhir"
    assert not created.is_ignored

    result = directory_info_service.get_one_by_id("test-directory-1")
    assert result is not None
    assert result.id == "test-directory-1"
    assert result.endpoint_address == "https://example.com/fhir"
    assert not result.is_ignored

    with pytest.raises(HTTPException, match="Directory with ID test-directory-1 already exists"):
        directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")


def test_update_directory_info(
    directory_info_service: DirectoryInfoService,
) -> None:
    """Test updating a directory info entry."""
    with pytest.raises(HTTPException, match="Directory with ID test-directory-1 not found"):
        directory_info_service.get_one_by_id("test-directory-1")

    with pytest.raises(HTTPException, match="Directory with ID test-directory-1 not found"):
        directory_info_service.update(directory_id="test-directory-1", endpoint_address="https://example.com/fhir")

    to_be_updated = directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")

    assert to_be_updated.id == "test-directory-1"
    assert to_be_updated.endpoint_address == "https://example.com/fhir"
    assert not to_be_updated.is_ignored

    updated_again = directory_info_service.update(directory_id="test-directory-1", endpoint_address="https://example.com/updated-fhir", is_ignored=True)
    assert updated_again.id == "test-directory-1"
    assert updated_again.endpoint_address == "https://example.com/updated-fhir"
    assert updated_again.is_ignored

    retrieved = directory_info_service.get_one_by_id("test-directory-1")
    assert retrieved.id == "test-directory-1"

def test_delete_directory_info(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")

    retrieved = directory_info_service.get_one_by_id("test-directory-1")
    assert retrieved is not None

    directory_info_service.delete("test-directory-1")

    with pytest.raises(HTTPException, match="Directory with ID test-directory-1 not found"):
        directory_info_service.get_one_by_id("test-directory-1")

def test_get_one_by_id(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")

    retrieved = directory_info_service.get_one_by_id("test-directory-1")

    assert retrieved.id == "test-directory-1"
    assert retrieved.endpoint_address == "https://example.com/fhir"
    assert not retrieved.is_ignored

def test_get_one_by_id_not_found(
    directory_info_service: DirectoryInfoService,
) -> None:
    with pytest.raises(HTTPException, match="Directory with ID nonexistent not found"):
        directory_info_service.get_one_by_id("nonexistent")

def test_get_all(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    directory_info_service.create(directory_id="test-directory-ignored", endpoint_address="https://example.com/ignored-fhir", ura="12345678")
    directory_info_service.create(directory_id="test-directory-deleted", endpoint_address="https://example.com/deleted-fhir", ura="12345678")

    directory_info_service.update("test-directory-ignored", is_ignored=True)
    directory_info_service.update("test-directory-deleted", deleted_at=datetime.now()-timedelta(days=1))

    all_entries = directory_info_service.get_all()
    assert len(all_entries) == 1
    assert all_entries[0].id == "test-directory-1"

    all_with_ignored = directory_info_service.get_all(include_ignored=True)
    assert len(all_with_ignored) == 2
    ids = {entry.id for entry in all_with_ignored}
    assert ids == {"test-directory-1", "test-directory-ignored"}

    all_with_deleted = directory_info_service.get_all(include_deleted=True)
    assert len(all_with_deleted) == 2
    ids = {entry.id for entry in all_with_deleted}
    assert ids == {"test-directory-1", "test-directory-deleted"}

    all_with_both = directory_info_service.get_all(include_ignored=True, include_deleted=True)
    assert len(all_with_both) == 3
    ids = {entry.id for entry in all_with_both}
    assert ids == {"test-directory-1", "test-directory-ignored", "test-directory-deleted"}

    entries = directory_info_service.get_all_including_ignored_ids(
        include_ignored_ids=["test-directory-ignored"]
    )
    assert len(entries) == 2  # active + the specifically included ignored one
    ids = {entry.id for entry in entries}
    assert ids == {"test-directory-1", "test-directory-ignored"}

    entries_with_deleted = directory_info_service.get_all_including_ignored_ids(
        include_ignored_ids=["test-directory-ignored"],
        include_deleted=True
    )
    assert len(entries_with_deleted) == 3
    ids = {entry.id for entry in entries_with_deleted}
    assert ids == {"test-directory-1", "test-directory-ignored", "test-directory-deleted"}

    deleted_entries = directory_info_service.get_all_deleted()
    assert len(deleted_entries) == 1
    assert deleted_entries[0].id == "test-directory-deleted"

    ignored_entries = directory_info_service.get_all_ignored()
    assert len(ignored_entries) == 1
    assert ignored_entries[0].id == "test-directory-ignored"

def test_set_ignored_status(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")

    retrieved = directory_info_service.get_one_by_id("test-directory-1")
    assert retrieved.is_ignored is False

    directory_info_service.set_ignored_status("test-directory-1", ignored=True)

    retrieved = directory_info_service.get_one_by_id("test-directory-1")
    assert retrieved.is_ignored is True

    directory_info_service.set_ignored_status("test-directory-1", ignored=False)

    retrieved = directory_info_service.get_one_by_id("test-directory-1")
    assert retrieved.is_ignored is False

def test_set_ignored_status_not_found(
    directory_info_service: DirectoryInfoService,
) -> None:
    with pytest.raises(HTTPException, match="Directory with ID nonexistent not found"):
        directory_info_service.set_ignored_status("nonexistent", ignored=True)

def test_health_check_all_healthy(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    directory_info_service.update(directory_id="test-directory-1", last_success_sync = datetime.now() - timedelta(minutes=30))
    assert directory_info_service.health_check() is True

def test_health_check_unhealthy_no_sync(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    assert directory_info_service.health_check() is False

def test_health_check_unhealthy_stale(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    directory_info_service.update(directory_id="test-directory-1", last_success_sync = datetime.now() - timedelta(hours=2))
    assert directory_info_service.health_check() is False

def test_health_check_mixed(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    directory_info_service.update(directory_id="test-directory-1", last_success_sync = datetime.now() - timedelta(minutes=30))
    assert directory_info_service.health_check() is True
    directory_info_service.create(directory_id="unhealthy-dir", endpoint_address="https://example.com/fhir", ura="12345678")
    directory_info_service.update(directory_id="unhealthy-dir", last_success_sync=None)
    assert directory_info_service.health_check() is False

def test_get_prometheus_metrics(
    directory_info_service: DirectoryInfoService,
) -> None:
    directory_info_service.create(directory_id="test-directory-1", endpoint_address="https://example.com/fhir", ura="12345678")
    directory_info_service.create(directory_id="test-directory-ignored", endpoint_address="https://example.com/ignored-fhir", ura="12345678")
    directory_info_service.update(directory_id="test-directory-ignored", is_ignored=True)

    metrics = directory_info_service.get_prometheus_metrics()

    assert len(metrics) > 0
    assert any("directory_failed_sync_total" in line for line in metrics)
    assert any("directory_failed_attempts" in line for line in metrics)
    assert any("directory_last_success_sync" in line for line in metrics)
    assert any("directory_is_ignored" in line for line in metrics)

    assert not any("test-directory-ignored" in line for line in metrics)

    failed_sync_lines = [line for line in metrics if "directory_failed_sync_total{directory_id=" in line]
    assert len(failed_sync_lines) == 1
    assert "test-directory-1" in failed_sync_lines[0]
    assert "0" in failed_sync_lines[0]
