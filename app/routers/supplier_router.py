from collections.abc import Sequence
from fastapi import APIRouter, Depends

from app.db.entities.supplier import Supplier
from app.container import get_supplier_service
from app.services.entity_services.supplier_service import SupplierService
from app.models.supplier.dto import SupplierCreateDto, SupplierUpdateDto

router = APIRouter(prefix="/supplier", tags=["Supplier"])


@router.get("", response_model=None)
def get_all_suppliers(
    service: SupplierService = Depends(get_supplier_service),
) -> Sequence[Supplier]:
    return service.get_all()


@router.get("/{_id}", response_model=None)
def get_one_supplier(
    _id: str, service: SupplierService = Depends(get_supplier_service)
) -> Supplier:
    return service.get_one(_id)


@router.post("", response_model=None)
def add_one_supplier(
    data: SupplierCreateDto, service: SupplierService = Depends(get_supplier_service)
) -> Supplier:
    return service.add_one(data)


@router.put("/{_id}", response_model=None)
def update_one_supplier(
    _id: str,
    data: SupplierUpdateDto,
    service: SupplierService = Depends(get_supplier_service),
) -> Supplier:
    return service.update_one(supplier_id=_id, dto=data)


@router.delete("/{_id}", response_model=None)
def delete_one_supplier(
    _id: str, service: SupplierService = Depends(get_supplier_service)
) -> None:
    return service.delete_one(_id)
