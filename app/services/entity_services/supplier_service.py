import logging
from collections.abc import Sequence

from app.models.supplier.dto import SupplierDto

logger = logging.getLogger(__name__)


class SupplierApi:
    def get_one(self, supplier_id: str) -> SupplierDto|None:
        raise NotImplementedError

    def get_all(self) -> Sequence[SupplierDto]:
        raise NotImplementedError


class SupplierService:
    def __init__(self, api: SupplierApi) -> None:
        self.api = api

    def get_one(self, supplier_id: str) -> SupplierDto|None:
        return self.api.get_one(supplier_id)

    def get_all(self) -> Sequence[SupplierDto]:
        return self.api.get_all()
