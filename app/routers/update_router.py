from datetime import datetime
from typing import Any
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.container import (
    get_supplier_api,
    get_update_consumer_service,
)
from app.services.api.suppliers_api import SuppliersApi
from app.services.update.update_consumer_service import UpdateConsumerService

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None)


@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier(
    supplier_id: str | None = None,
    params: UpdateQueryParams = Depends(),
    update_service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_service: SuppliersApi = Depends(get_supplier_api),
) -> Any:
    since = params.since.astimezone() if params.since else None
    if supplier_id is None:
        suppliers = supplier_service.get_all()
        data = []
        for supplier in suppliers:
            data.append(update_service.update(supplier_id=supplier.id, since=since))
        return data

    return update_service.update(supplier_id, since)
