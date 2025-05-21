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

    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        original_suppliers = self.__supplier_cache_service.get_all_suppliers_caches(include_ignored=include_ignored)
        return self.__common_supplier_cache_logic(include_ignored=include_ignored, original_suppliers=original_suppliers)
    
    def get_all_suppliers_include_ignored(self, include_ignored_ids: List[str]) -> List[SupplierDto]:
        original_suppliers = []
        if include_ignored_ids:
            original_suppliers = self.__supplier_cache_service.get_all_suppliers_caches_include_ignored_ids(include_ignored_ids=include_ignored_ids)
        else:
            original_suppliers = self.__supplier_cache_service.get_all_suppliers_caches(include_ignored=False)

        return self.__common_supplier_cache_logic(include_ignored=True, original_suppliers=original_suppliers)

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
                original_supplier.is_deleted = True
                self.__supplier_cache_service.set_supplier_cache(
                    original_supplier
                )
            return original_supplier
        self.__supplier_cache_service.set_supplier_cache(
            supplier
        )
        return supplier

    def __common_supplier_cache_logic(self, include_ignored: bool, original_suppliers: list[SupplierDto]) -> list[SupplierDto]:
        try:
            suppliers = self.__supplierProvider.get_all_suppliers(include_ignored=include_ignored)
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
