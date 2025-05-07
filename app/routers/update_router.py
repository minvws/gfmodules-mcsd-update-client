from datetime import datetime, timezone
from typing import Any
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.container import (
    get_supplier_ignored_directory_service,
    get_supplier_provider,
    get_update_consumer_service,
)
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.update.update_consumer_service import UpdateConsumerService
from app.services.supplier_provider.supplier_provider import SupplierProvider
from app.stats import get_stats

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None)


@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
@get_stats().timer("update_resources")
def update_supplier(
    supplier_id: str | None = None,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_provider: SupplierProvider = Depends(get_supplier_provider),
    supplier_ignored_directory_service: SupplierIgnoredDirectoryService = Depends(get_supplier_ignored_directory_service),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None
    if supplier_id is None:
        all_suppliers = supplier_provider.get_all_suppliers()
        filtered_suppliers = supplier_ignored_directory_service.filter_ignored_supplier_dto(all_suppliers)          
        data: list[dict[str, Any]] = []
        for supplier in filtered_suppliers:
            data.append(service.update(supplier, since))
        return data

    supplier = supplier_provider.get_one_supplier(supplier_id)
    return service.update(supplier, since)
