from collections.abc import Sequence
from fastapi import APIRouter, Depends

from app.container import (
    get_supplier_provider,
)
from app.models.supplier.dto import SupplierDto
from app.services.supplier_provider.supplier_provider import SupplierProvider

router = APIRouter(prefix="/supplier", tags=["Supplier"])


@router.get("", response_model=None)
def get_all_suppliers(
    service: SupplierProvider = Depends(get_supplier_provider),
) -> Sequence[SupplierDto]:
    return service.get_all_suppliers()

@router.get("/{_id}", response_model=None)
def get_one_supplier(
    _id: str, service: SupplierProvider = Depends(get_supplier_provider)
) -> SupplierDto:
    return service.get_one_supplier(_id)
