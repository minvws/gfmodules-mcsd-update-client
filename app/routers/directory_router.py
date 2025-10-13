from fastapi import APIRouter, Depends, Response

from app.container import (
    get_directory_info_service,
    get_directory_provider,
)
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/directory", tags=["Directory"])

@router.get("/health", response_model=dict[str, Any], description="Check the health of the directory services")
def directory_health(
    info_service: DirectoryInfoService = Depends(get_directory_info_service),
) -> Dict[str, Any]:
    return {"status": "ok" if info_service.health_check() else "error"}


@router.get("/metrics", response_class=Response, description="Get Prometheus metrics")
def metrics(
    info_service: DirectoryInfoService = Depends(get_directory_info_service),
) -> Response:
    return Response("\n".join(info_service.get_prometheus_metrics()), media_type="text/plain")


@router.get("/all", response_model=None, description="Get all directories info")
def get_all_directories(
    provider: DirectoryProvider = Depends(get_directory_provider),
) -> List[Dict[str, Any]]:
    provider_dirs = provider.get_all_directories()
    return [directory.model_dump() for directory in provider_dirs]

@router.get("/{_id}", response_model=None, description="Get one directory info by ID")
def get_one_directory(
    _id: str, provider: DirectoryProvider = Depends(get_directory_provider),
) -> Dict[str, Any]:
    directory = provider.get_one_directory(_id)
    if directory is None:
        logger.warning(f"Directory with ID {_id} not found.")
        return {}

    return directory.model_dump()
