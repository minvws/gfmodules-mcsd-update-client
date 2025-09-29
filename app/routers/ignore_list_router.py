from typing import Any, Dict, List
from fastapi import APIRouter, Depends, Response

from app.container import (
    get_directory_info_service,
)
from app.services.entity.directory_info_service import DirectoryInfoService

import logging 

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/directory_ignore_list", tags=["Directory"])

@router.get("/all", response_model=None, description="Get all ignored directories")
def find(
    service: DirectoryInfoService = Depends(get_directory_info_service),
) -> List[Dict[str, Any]]:
    ignored = service.get_all_ignored()
    return [d.model_dump() for d in ignored]

@router.get("/{directory_id}", response_model=None, description="Check if a directory is ignored by ID")
def is_directory_ignored(
    directory_id: str,
    service: DirectoryInfoService = Depends(get_directory_info_service),
) -> bool:
    directory = service.get_one_by_id(directory_id)
    return directory.is_ignored

@router.post("/{directory_id}", response_model=None, description="Ignore a directory by ID")
def ignore_directory(
    directory_id: str,
    service: DirectoryInfoService = Depends(get_directory_info_service),
) -> Response:
    service.set_ignored_status(directory_id, True)
    return Response(status_code=201, content=None)

@router.delete("/{directory_id}", response_model=None, description="Unignore a directory by ID")
def unignore_directory(
    directory_id: str,
    service: DirectoryInfoService = Depends(get_directory_info_service),
) -> Response:
    service.set_ignored_status(directory_id, False)
    return Response(status_code=201, content=None)
