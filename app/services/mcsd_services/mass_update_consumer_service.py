from datetime import datetime, timedelta
from typing import Any, Dict

from app.services.entity_services.supplier_service import SupplierService
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService


class MassUpdateConsumerService:
    def __init__(
        self,
        update_consumer_service: UpdateConsumerService,
        supplier_service: SupplierService,
    ) -> None:
        self.__supplier_service = supplier_service
        self.__update_consumer_service = update_consumer_service
        self.__last_update_dict: Dict[str, datetime] = {}

    def update_all(self) -> list[dict[str, Any]]:
        all_suppliers = self.__supplier_service.get_all()
        data: list[dict[str, Any]] = []
        for supplier in all_suppliers:
            last_updated = (
                self.__last_update_dict[supplier.id]
                if supplier.id in self.__last_update_dict
                else None
            )
            new_updated = datetime.now() - timedelta(seconds=60)
            data.append(
                self.__update_consumer_service.update_supplier(
                    supplier.id, last_updated
                )
            )
            self.__last_update_dict[supplier.id] = new_updated
        return data
