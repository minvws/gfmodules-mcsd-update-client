from datetime import datetime
from typing import Any, Dict, List
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.container import (
    get_update_consumer_service,
)
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])

class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(
        alias="_since",
        default=None,
    )

@router.post(
    "/{supplier_id}/{resource_type}",
    response_model=None,
    summary="Update by supplier ID and resource type",
)
@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier_resources(
    supplier_id: str|None = None,
    resource_type: str | None = None,
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    _since: UpdateQueryParams = Depends(),
) -> List[Any]:
    return service.update_resources(supplier_id, resource_type, _since.since)
