import logging
from collections.abc import Sequence

from app.db.entities.supplier import Supplier

logger = logging.getLogger(__name__)

class SupplierApi:
    def get_one(self, supplier_id: str) -> Supplier:
        raise NotImplementedError

    def get_all(self) -> Sequence[Supplier]:
        raise NotImplementedError

class SupplierService:
    def __init__(self, api: SupplierApi) -> None:
        self.api = api

    def get_one(self, supplier_id: str) -> Supplier:
        return self.api.get_one(supplier_id)

    def get_all(self) -> Sequence[Supplier]:
        return self.api.get_all()
