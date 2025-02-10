from datetime import datetime
from typing import Any

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
        self.__last_update: datetime | None = None

    def update_all(self) -> list[dict[str, Any]]:
        all_suppliers = self.__supplier_service.get_all()
        data: list[dict[str, Any]] = []
        for supplier in all_suppliers:
            data.append(
                self.__update_consumer_service.update_supplier(
                    supplier.id, self.__last_update
                )
            )

        self.__last_update = datetime.now()
        return data
