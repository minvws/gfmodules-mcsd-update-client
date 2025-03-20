import logging
from collections.abc import Sequence

from app.services.entity_services.supplier_service import SupplierApi
from app.models.supplier.dto import SupplierDto

logger = logging.getLogger(__name__)


class MockApi(SupplierApi):
    def get_one(self, supplier_id: str) -> SupplierDto|None:
        if supplier_id == "1":
            return SupplierDto(
                id="1",
                name="Test Supplier",
                endpoint="http://test1.supplier.example.org",
            )
        elif supplier_id == "2":
            return SupplierDto(
                id="2",
                name="Test Supplier 2",
                endpoint="http://test2.supplier.example.org",
            )
        elif supplier_id == "3":
            return None
        else:
            raise ValueError(f"SupplierDto with ID {supplier_id} not found")

    def get_all(self) -> Sequence[SupplierDto]:
        return [
            SupplierDto(
                id="1",
                name="Test Supplier",
                endpoint="http://test1.supplier.example.org",
            ),
            SupplierDto(
                id="2",
                name="Test Supplier 2",
                endpoint="http://test2.supplier.example.org",
            ),
        ]
