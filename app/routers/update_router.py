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


@router.post("/{supplier_id}", response_model=None, summary="Update by supplier ID")
@router.post("", response_model=None, summary="Update all suppliers")
def update_supplier(
    supplier_id: str | None = None,
    override_ignore: Annotated[list[str] | None, Query()] = None,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateConsumerService = Depends(get_update_consumer_service),
    supplier_provider: SupplierProvider = Depends(get_supplier_provider),
    supplier_ignored_directory_service: SupplierIgnoredDirectoryService = Depends(get_supplier_ignored_directory_service),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None
    if supplier_id is None:
        all_suppliers = supplier_provider.get_all_suppliers()
        ignored_ids = {
            s.directory_id for s in supplier_ignored_directory_service.get_all_ignored_directories()
        }

        # Filter out ignored suppliers unless they are explicitly overridden
        filtered_suppliers = [
            s for s in all_suppliers if (s.id not in ignored_ids) or (override_ignore is not None and s.id in override_ignore)
        ]

        return [service.update(supplier, since) for supplier in filtered_suppliers]

    supplier = supplier_provider.get_one_supplier(supplier_id)
    is_ignored = supplier_ignored_directory_service.get_ignored_directory(supplier_id) is not None


    if is_ignored and (not override_ignore or supplier_id not in override_ignore):
            raise HTTPException(
                status_code=409,
                detail=f"Supplier {supplier_id} is in the ignore list. Use override_ignore to update.",
            )

    return service.update(supplier, since)
