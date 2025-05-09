import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.entity.supplier_directory_ignore_service import SupplierDirectoryIgnoreService
from app.services.entity.supplier_info_service import SupplierInfoService
from app.services.supplier_provider.supplier_provider import SupplierProvider
from app.services.update.update_consumer_service import UpdateConsumerService

logger = logging.getLogger(__name__)


class UpdateAllConsumersService:
    def __init__(
        self,
        update_consumer_service: UpdateConsumerService,
        supplier_provider: SupplierProvider,
        supplier_info_service: SupplierInfoService,
        supplier_directory_ignore_service: SupplierDirectoryIgnoreService,
        cleanup_client_directory_after_success_timeout_seconds: int,
    ) -> None:
        self.__supplier_provider = supplier_provider
        self.__update_consumer_service = update_consumer_service
        self.__supplier_info_service = supplier_info_service
        self.__supplier_directory_ignore_service = supplier_directory_ignore_service
        self.__cleanup_client_directory_after_success_timeout_seconds = cleanup_client_directory_after_success_timeout_seconds

    def update_all(self) -> list[dict[str, Any]]:
        try:
            all_suppliers = self.__supplier_provider.get_all_suppliers()
        except Exception as e:
            logging.error(f"Failed to retrieve suppliers: {e}")
            return []
        data: list[dict[str, Any]] = []
        filtered_suppliers = self.__supplier_directory_ignore_service.filter_ignored_supplier_dto(all_suppliers)
        for supplier in filtered_suppliers:

            info = self.__supplier_info_service.get_supplier_info(supplier.id)
            new_updated = datetime.now() - timedelta(seconds=60)
            try:
                data.append(
                    self.__update_consumer_service.update(
                        supplier.id, info.last_success_sync
                    )
                )

                info.last_success_sync = new_updated
                info.failed_attempts = 0
                info.last_success_sync = new_updated
                self.__supplier_info_service.update_supplier_info(info)
            except Exception as e:
                logging.error(f"Failed to update supplier {supplier.id}: {e}")
                info.failed_attempts += 1
                info.failed_sync_count += 1
                self.__supplier_info_service.update_supplier_info(info)

        return data
    
    def cleanup_old_directories(self) -> None:
        all_suppliers = self.__supplier_info_service.get_all_suppliers_info()
        suppliers = self.__supplier_directory_ignore_service.filter_ignored_supplier_info(all_suppliers)
        for supplier_info in suppliers:
            if supplier_info.last_success_sync is not None:
                elapsed_time = (datetime.now(timezone.utc) - supplier_info.last_success_sync.astimezone(timezone.utc)).total_seconds()
                if elapsed_time > self.__cleanup_client_directory_after_success_timeout_seconds:
                    logging.info(f"Cleaning up directory for outdated supplier {supplier_info.supplier_id}")
                    self.__update_consumer_service.cleanup(supplier_info.supplier_id)
                    self.__supplier_directory_ignore_service.add_directory_to_ignore_list(supplier_info.supplier_id)
       