from typing import List

from fastapi import HTTPException
from app.models.supplier.dto import SupplierDto
from app.services.supplier_provider.supplier_provider import SupplierProvider


class SupplierJsonProvider(SupplierProvider):
    supplier_urls: List[SupplierDto]

    def __init__(self, suppliers_json_data: List[SupplierDto]) -> None:
        self.__suppliers = suppliers_json_data

    def get_all_suppliers(self) -> List[SupplierDto]:
        return self.__suppliers

    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        supplier = next((s for s in self.__suppliers if s.id == supplier_id), None)
        if supplier is None:
            raise HTTPException(
                status_code=404,
                detail=f"Supplier with ID '{supplier_id}' not found."
            )
        return supplier
