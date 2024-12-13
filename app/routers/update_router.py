from datetime import datetime
from typing import Any
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.container import (
    get_update_consumer_service,
    get_supplier_service,
)
from app.services.entity_services.supplier_service import SupplierService
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None, alias="_since")


@router.post(
    "/{supplier_id}/{resource_type}",
    response_model=None,
    summary="Update by supplier ID and resource type",
)
@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier_resources(
    supplier_id: str | None = None,
    resource_type: str | None = None,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_service: SupplierService = Depends(get_supplier_service),
) -> Any:
    if supplier_id is None:
        all_suppliers = supplier_service.get_all()
        data: list[dict[str, Any]] = []
        for supplier in all_suppliers:
            data.append(
                service.update_supplier(supplier.id, resource_type, query_params.since)
            )
        return data
    else:
        return service.update_supplier(supplier_id, resource_type, query_params.since)
