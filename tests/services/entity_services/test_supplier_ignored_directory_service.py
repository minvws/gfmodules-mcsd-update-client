import pytest
from fastapi import HTTPException
from app.db.entities.supplier_info import SupplierInfo
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_cache_service import SupplierCacheService
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.entity.supplier_info_service import SupplierInfoService


def test_add_directory_to_ignore_list(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("new_directory")
    result = supplier_ignored_directory_service.get_ignored_directory("new_directory")
    assert result is not None
    assert result.directory_id == "new_directory"

def test_filter_ignored_supplier_info(supplier_ignored_directory_service: SupplierIgnoredDirectoryService, supplier_info_service: SupplierInfoService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("ignored_1")
    mock_directories = [
        SupplierInfo(supplier_id="ignored_1",last_success_sync=None, failed_attempts=0, failed_sync_count=0),
        SupplierInfo(supplier_id="not_ignored_1", last_success_sync=None, failed_attempts=0, failed_sync_count=0),
    ]
    for directory in mock_directories:
        supplier_info_service.update_supplier_info(directory)
    result = supplier_info_service.get_all_suppliers_info(include_ignored=False)
    assert len(result) == 1
    assert result[0].supplier_id == "not_ignored_1"
    result = supplier_info_service.get_all_suppliers_info(include_ignored=True)
    assert len(result) == 2
    assert result[0].supplier_id == "ignored_1"
    assert result[1].supplier_id == "not_ignored_1"


def test_filter_ignored_supplier_dto(supplier_ignored_directory_service: SupplierIgnoredDirectoryService, supplier_cache_service: SupplierCacheService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("ignored_1")
    supplier_cache_service.set_supplier_cache(SupplierDto(id="ignored_1", name="ignored", endpoint="ignored"))
    supplier_cache_service.set_supplier_cache(SupplierDto(id="not_ignored_1", name="not_ignored", endpoint="not_ignored"))
    result = supplier_cache_service.get_all_suppliers_caches(with_deleted=True, include_ignored=False)
    assert len(result) == 1
    assert result[0].id == "not_ignored_1"
    result = supplier_cache_service.get_all_suppliers_caches(with_deleted=True, include_ignored=True)
    assert len(result) == 2
    assert result[0].id == "ignored_1"
    assert result[1].id == "not_ignored_1"


def test_get_ignored_directory(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    test_id = "ignored_1"
    supplier_ignored_directory_service.add_directory_to_ignore_list(test_id)
    result = supplier_ignored_directory_service.get_ignored_directory(test_id)
    assert result is not None
    assert result.directory_id == test_id


def test_get_all_ignored_directories(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("ignored_1")
    supplier_ignored_directory_service.add_directory_to_ignore_list("ignored_2")
    result = supplier_ignored_directory_service.get_all_ignored_directories()
    assert len(result) == 2
    assert result[0].directory_id == "ignored_1"
    assert result[1].directory_id == "ignored_2"


def test_add_directory_to_ignore_list_conflict(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    with pytest.raises(HTTPException) as exc_info:
        supplier_ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    assert exc_info.value.status_code == 409


def test_remove_directory_from_ignore_list(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    supplier_ignored_directory_service.remove_directory_from_ignore_list("existing_directory")
    result = supplier_ignored_directory_service.get_ignored_directory("existing_directory")
    assert result is None


def test_remove_directory_from_ignore_list_not_found(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    with pytest.raises(HTTPException) as exc_info:
        supplier_ignored_directory_service.remove_directory_from_ignore_list("non_existing_directory")
    assert exc_info.value.status_code == 404

def test_remove_directory_from_ignore_list_success(supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("existing_directory")
    supplier_ignored_directory_service.remove_directory_from_ignore_list("existing_directory")
    result = supplier_ignored_directory_service.get_ignored_directory("existing_directory")
    assert result is None