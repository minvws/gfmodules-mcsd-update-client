from datetime import datetime, timezone
from typing import Annotated, Any
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from fastapi.exceptions import HTTPException

from app.container import (
    get_directory_info_service,
    get_directory_provider,
    get_update_client_service,
)
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.update.update_client_service import UpdateClientService
from app.services.directory_provider.directory_provider import DirectoryProvider

router = APIRouter(prefix="/update_resources", tags=["Update update_client resources"])


class UpdateQueryParams(BaseModel):
    since: datetime | None = Field(default=None)

@router.post("", response_model=None, summary="Update all directories")
def update_all_directories(
    override_ignore: Annotated[list[str] | None, Query()] = [],
    query_params: UpdateQueryParams = Depends(),
    service: UpdateClientService = Depends(get_update_client_service),
    directory_provider: DirectoryProvider = Depends(get_directory_provider),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None

    directories = directory_provider.get_all_directories_include_ignored_ids(include_ignored_ids=override_ignore if override_ignore else [])

    return [service.update(directory, since) for directory in directories]


@router.post("/{directory_id}", response_model=None, summary="Update by directory ID")
def update_single_directory(
    directory_id: str,
    override_ignore: bool = False,
    query_params: UpdateQueryParams = Depends(),
    service: UpdateClientService = Depends(get_update_client_service),
    directory_provider: DirectoryProvider = Depends(get_directory_provider),
    directory_service: DirectoryInfoService = Depends(get_directory_info_service),
) -> Any:
    since = query_params.since.astimezone(timezone.utc) if query_params.since else None
    directory = directory_provider.get_one_directory(directory_id)
    if not override_ignore:
        is_ignored = directory_service.get_one_by_id(directory_id).is_ignored

        if is_ignored:
            raise HTTPException(
                status_code=409,
                detail=f"Directory {directory_id} is ignored. Use override_ignore to update.",
            )

    return service.update(directory, since)

