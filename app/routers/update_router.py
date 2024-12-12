from typing import Any, Dict
from fastapi import APIRouter, Depends

from app.container import (
    get_update_consumer_service,
)
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


@router.post(
    "/{supplier_id}/{resource_type}",
    response_model=None,
    summary="Update by supplier ID and resource type",
)
@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier_resources(
    supplier_id: str,
    resource_type: str,
    service: UpdateConsumerService = Depends(get_update_consumer_service),
) -> Dict[str, Any]:
    return service.update_supplier(supplier_id, resource_type)
