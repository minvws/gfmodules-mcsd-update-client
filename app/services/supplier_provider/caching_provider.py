from typing import List

from fastapi import HTTPException
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_cache_service import SupplierCacheService
from app.services.supplier_provider.api_provider import SupplierApiProvider
from app.services.supplier_provider.supplier_provider import SupplierProvider


class CachingSupplierProvider(SupplierProvider):
    def __init__(
        self,
        supplier_provider: SupplierApiProvider,
        supplier_cache_service: SupplierCacheService,
    ) -> None:
        self.__supplierProvider = supplier_provider
        self.__supplier_cache_service = supplier_cache_service

    def get_all_suppliers(self) -> List[SupplierDto]:
        original_suppliers = self.__supplier_cache_service.get_all_suppliers_caches()
        try:
            suppliers = self.__supplierProvider.get_all_suppliers()
        except Exception:  # Retrieving suppliers failed
            return original_suppliers

        for supplier in suppliers:
            self.__supplier_cache_service.set_supplier_cache(
                supplier_id=supplier.id, endpoint=supplier.endpoint
            )

        for original_supplier in original_suppliers:
            if original_supplier.id not in [supplier.id for supplier in suppliers]:
                if self.__supplierProvider.check_if_supplier_is_deleted(supplier.id):
                    self.__supplier_cache_service.set_supplier_cache(
                        supplier_id=supplier.id,
                        endpoint=supplier.endpoint,
                        is_deleted=True,
                    )
        return suppliers

    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        try:
            supplier = self.__supplierProvider.get_one_supplier(supplier_id)
        except Exception:  # Retrieving supplier failed
            original_supplier = self.__supplier_cache_service.get_supplier_cache(
                supplier_id
            )
            if original_supplier is None:
                raise HTTPException(
                status_code=404,
                detail="Failed to retrieve supplier from the supplier provider and no cached data was available.",
            )
            if self.__supplierProvider.check_if_supplier_is_deleted(supplier_id):
                self.__supplier_cache_service.set_supplier_cache(
                    supplier_id=supplier_id,
                    endpoint=original_supplier.endpoint,
                    is_deleted=True,
                )
            return original_supplier
        self.__supplier_cache_service.set_supplier_cache(
            supplier_id=supplier.id,
            endpoint=supplier.endpoint,
            is_deleted=supplier.is_deleted,
        )
        return supplier
