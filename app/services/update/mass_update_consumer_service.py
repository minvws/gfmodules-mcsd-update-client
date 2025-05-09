import logging
from datetime import datetime, timedelta
from typing import Any

from app.services.entity.supplier_info_service import SupplierInfoService
from app.services.supplier_provider.supplier_provider import SupplierProvider
from app.services.update.update_consumer_service import UpdateConsumerService

logger = logging.getLogger(__name__)


class MassUpdateConsumerService:
    def __init__(
        self,
        update_consumer_service: UpdateConsumerService,
        supplier_provider: SupplierProvider,
        supplier_info_service: SupplierInfoService,
    ) -> None:
        self.__supplier_service = supplier_provider
        self.__update_consumer_service = update_consumer_service
        self.__supplier_info_service = supplier_info_service

    def update_all(self) -> list[dict[str, Any]]:
        try:
            all_suppliers = self.__supplier_service.get_all_suppliers()
        except Exception as e:
            logging.error(f"Failed to retrieve suppliers: {e}")
            return []
        data: list[dict[str, Any]] = []
        for supplier in all_suppliers:

            info = self.__supplier_info_service.get_supplier_info(supplier.id)
            new_updated = datetime.now() - timedelta(seconds=60)
            try:
                data.append(
                    self.__update_consumer_service.update(
                        supplier, info.last_success_sync
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
