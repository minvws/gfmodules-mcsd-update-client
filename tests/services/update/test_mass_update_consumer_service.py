from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
from app.services.entity.supplier_info_service import SupplierInfoService
from app.services.update.mass_update_consumer_service import MassUpdateConsumerService
from app.db.entities.supplier_info import SupplierInfo
from app.stats import NoopStats



def test_cleanup_old_directories(supplier_info_service: SupplierInfoService) -> None:
    mock_update_consumer_service = MagicMock()
    mock_supplier_provider = MagicMock()
    mock_supplier_info_service = MagicMock()
    mock_supplier_ignored_directory_service = MagicMock()

    mock_outdated_supplier_info = SupplierInfo(
        supplier_id="outdated_supplier",
        last_success_sync=datetime.now(timezone.utc) - timedelta(hours=2), # more than 1 hour
        failed_attempts=0,
        failed_sync_count=0,
    )
    mock_success_supplier_info = SupplierInfo(
        supplier_id="supplier_1",
        last_success_sync=datetime.now(timezone.utc) - timedelta(minutes=30), # Less than 1 hour
        failed_attempts=0,
        failed_sync_count=0,
    )
    mock_supplier_info_service.get_all_suppliers_info.return_value = [mock_outdated_supplier_info, mock_success_supplier_info]

    service = MassUpdateConsumerService(
        update_consumer_service=mock_update_consumer_service,
        supplier_provider=mock_supplier_provider,
        supplier_info_service=mock_supplier_info_service,
        supplier_ignored_directory_service=mock_supplier_ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=3600, # less than 2 hours
        stats=NoopStats(),
    )

    service.cleanup_old_directories()

    mock_supplier_info_service.get_all_suppliers_info.assert_called_once()
    mock_update_consumer_service.cleanup.assert_called_once_with("outdated_supplier") # It should only call cleanup for the supplier that is older than 1 hour
    mock_supplier_ignored_directory_service.add_directory_to_ignore_list.assert_called_once_with("outdated_supplier")

    # it is now ignored, so it should not be updated on the next call
    mock_supplier_info_service.get_all_suppliers_info.return_value = []

    service.cleanup_old_directories()
    mock_supplier_info_service.get_all_suppliers_info.assert_called()
    assert mock_update_consumer_service.cleanup.call_count == 1 # it should not be called again
