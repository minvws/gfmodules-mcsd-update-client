import logging
from typing import List

from app.models.supplier.dto import SupplierDto
from app.services.supplier_provider.supplier_provider import SupplierProvider

logger = logging.getLogger(__name__)


class MockApi(SupplierProvider):
    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        if supplier_id == "1":
            return SupplierDto(
                id="1",
                name="Test Supplier",
                ura_number="12345678",
                endpoint="http://test1.supplier.example.org",
            )
        elif supplier_id == "2":
            return SupplierDto(
                id="2",
                name="Test Supplier 2",
                ura_number="87654321",
                endpoint="http://test2.supplier.example.org",
            )
        else:
            raise ValueError(f"SupplierDto with ID {supplier_id} not found")

    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        return [
            SupplierDto(
                id="1",
                name="Test Supplier",
                ura_number="12345678",
                endpoint="http://test1.supplier.example.org",
            ),
            SupplierDto(
                id="2",
                name="Test Supplier 2",
                ura_number="87654321",
                endpoint="http://test2.supplier.example.org",
            ),
        ]
