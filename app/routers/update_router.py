from datetime import datetime, timezone
from typing import Annotated, Any
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from fastapi.exceptions import HTTPException

from app.container import (
    get_supplier_ignored_directory_service,
    get_supplier_provider,
    get_update_consumer_service,
)
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.update.update_consumer_service import UpdateConsumerService
from app.services.supplier_provider.supplier_provider import SupplierProvider

router = APIRouter(prefix="/update_resources", tags=["Update consumer resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None)

@router.post("", response_model=None, summary="Update all suppliers")
def update_all_suppliers(
    override_ignore: Annotated[list[str] | None, Query()] = [],
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_provider: SupplierProvider = Depends(get_supplier_provider),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None

    suppliers = supplier_provider.get_all_suppliers_include_ignored(include_ignored_ids=override_ignore if override_ignore else [])

    return [service.update(supplier, since) for supplier in suppliers]


@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
def update_single_supplier(
    supplier_id: str,
    override_ignore: bool = False,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_provider: SupplierProvider = Depends(get_supplier_provider),
    supplier_ignored_directory_service: SupplierIgnoredDirectoryService = Depends(get_supplier_ignored_directory_service),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None
    supplier = supplier_provider.get_one_supplier(supplier_id)
    if not override_ignore:
        is_ignored = supplier_ignored_directory_service.get_ignored_directory(supplier_id) is not None

        if is_ignored:
            raise HTTPException(
                status_code=409,
                detail=f"Supplier {supplier_id} is in the ignore list. Use override_ignore to update.",
            )

    return service.update(supplier, since)

