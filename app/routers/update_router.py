from datetime import datetime, timezone
from typing import Any
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.container import (
    get_update_consumer_service,
    get_supplier_service,
    get_update_consumer,
)
from app.services.entity_services.supplier_service import SupplierService
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService
from app.services_new.update_service import UpdateConsumer

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None)


@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier_resources(
    supplier_id: str | None = None,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_service: SupplierService = Depends(get_supplier_service),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None
    if supplier_id is None:
        all_suppliers = supplier_service.get_all()
        data: list[dict[str, Any]] = []
        for supplier in all_suppliers:
            data.append(service.update_supplier(supplier.id, since))
        return data
    else:
        return service.update_supplier(supplier_id, since)


@router.post("/new/{supplier_id}", response_model=None)
def update_supplier_new(
    supplier_id: str,
    params: UpdateQueryParams = Depends(),
    service: UpdateConsumer = Depends(get_update_consumer),
) -> Any:
    since = params.since.astimezone() if params.since else None
    return service.update(supplier_id, since)
