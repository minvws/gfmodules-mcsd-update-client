from collections.abc import Sequence
from fastapi import APIRouter, Depends, HTTPException

from app.container import (
    get_supplier_api,
)
from app.models.supplier.dto import SupplierDto
from app.services_new.api.suppliers_api import SuppliersApi

router = APIRouter(prefix="/supplier", tags=["Supplier"])


@router.get("", response_model=None)
def get_all_suppliers(
    service: SuppliersApi = Depends(get_supplier_api),
) -> Sequence[SupplierDto]:
    return service.get_all()


@router.get("/{_id}", response_model=None)
def get_one_supplier(
    _id: str, service: SuppliersApi = Depends(get_supplier_api)
) -> SupplierDto:
    supplier = service.get_one(_id)
    if supplier is None:
        raise HTTPException(status_code=404)

    return supplier
