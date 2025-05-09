from datetime import datetime, timezone
from typing import Any
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.container import (
    get_supplier_provider,
    get_update_consumer_service,
)
from app.services.update.update_consumer_service import UpdateConsumerService
from app.services.supplier_provider.supplier_provider import SupplierProvider

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None)


@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier(
    supplier_id: str | None = None,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_provider: SupplierProvider = Depends(get_supplier_provider),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None
    if supplier_id is None:
        all_suppliers = supplier_provider.get_all_suppliers()
        data: list[dict[str, Any]] = []
        for supplier in all_suppliers:
            data.append(service.update(supplier, since))
        return data

    supplier = supplier_provider.get_one_supplier(supplier_id)
    return service.update(supplier, since)
