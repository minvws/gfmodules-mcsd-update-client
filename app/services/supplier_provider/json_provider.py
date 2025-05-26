from typing import List

from fastapi import HTTPException
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.supplier_provider import SupplierProvider


class SupplierJsonProvider(SupplierProvider):
    supplier_urls: List[SupplierDto]

    def __init__(self, suppliers_json_data: List[SupplierDto], supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
        self.__suppliers = suppliers_json_data
        self.__supplier_ignored_directory_service = supplier_ignored_directory_service

    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        if not include_ignored:
            self.__supplier_ignored_directory_service.get_all_ignored_directories()
            return [
                supplier for supplier in self.__suppliers
                if not any(dir.directory_id == supplier.id for dir in self.__supplier_ignored_directory_service.get_all_ignored_directories())
            ]
        return self.__suppliers
    
    def get_all_suppliers_include_ignored(self, include_ignored_ids: List[str]) -> List[SupplierDto]:
        ignored_directories = self.__supplier_ignored_directory_service.get_all_ignored_directories()
        return [
            supplier for supplier in self.__suppliers
            if supplier.id in include_ignored_ids or not any(dir.directory_id == supplier.id for dir in ignored_directories)
        ]

    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        supplier = next((s for s in self.__suppliers if s.id == supplier_id), None)
        if supplier is None:
            raise HTTPException(
                status_code=404,
                detail=f"Supplier with ID '{supplier_id}' not found."
            )
        return supplier
