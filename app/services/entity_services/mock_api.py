import logging
from collections.abc import Sequence
from datetime import datetime

from app.db.entities.supplier import Supplier
from app.services.entity_services.supplier_service import SupplierApi

logger = logging.getLogger(__name__)

class MockApi(SupplierApi):
    def get_one(self, supplier_id: str) -> Supplier:
        if supplier_id == "1":
            return Supplier(
                id="1",
                name="Test Supplier",
                endpoint="http://test1.supplier.example.org",
                created_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                modified_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
            )
        elif supplier_id == "2":
            return Supplier(
                id="2",
                name="Test Supplier 2",
                endpoint="http://test2.supplier.example.org",
                created_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                modified_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
            )
        else:
            raise ValueError(f"Supplier with ID {supplier_id} not found")


    def get_all(self) -> Sequence[Supplier]:
        return [
            Supplier(
                id="1",
                name="Test Supplier",
                endpoint="http://test1.supplier.example.org",
                created_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                modified_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
            ),
            Supplier(
                id="2",
                name="Test Supplier 2",
                endpoint="http://test2.supplier.example.org",
                created_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                modified_at=datetime.fromisoformat("2021-01-01T00:00:00Z"),
            )
        ]
