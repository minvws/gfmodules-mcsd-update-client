from collections.abc import Sequence
from fastapi import APIRouter, Depends, Response

from app.container import (
    get_supplier_service,
)
from app.models.supplier.dto import SupplierDto
from app.services.entity_services.supplier_service import SupplierService

router = APIRouter(prefix="/supplier", tags=["Supplier"])


@router.get("", response_model=None)
def get_all_suppliers(
    service: SupplierService = Depends(get_supplier_service),
) -> Sequence[SupplierDto]:
    return service.get_all()


@router.get("/{_id}", response_model=None)
def get_one_supplier(
    _id: str, service: SupplierService = Depends(get_supplier_service)
) -> SupplierDto|Response:
    supplier = service.get_one(_id)
    if supplier is None:
        return Response(status_code=404)
    return supplier
