from collections.abc import Sequence
from fastapi import APIRouter, Depends, Response

from app.container import (
    get_supplier_api,
)
from app.models.supplier.dto import SupplierDto
from app.services.entity_services.supplier_service import SupplierService
from app.services_new.api.suppliers_api import SuppliersApi

router = APIRouter(prefix="/supplier", tags=["Supplier"])


@router.get("", response_model=None)
def get_all_suppliers(
    service: SuppliersApi = Depends(get_supplier_api),
) -> Sequence[SupplierDto]:
    return service.get_all()


@router.get("/{_id}", response_model=None)
def get_one_supplier(
    _id: str, service: SupplierService = Depends(get_supplier_api)
) -> SupplierDto:
    return service.get_one(_id)
