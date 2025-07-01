import logging
from typing import Any

from fastapi import APIRouter, Depends, Response

from app.container import get_directory_info_service
from app.services.entity.directory_info_service import DirectoryInfoService

logger = logging.getLogger(__name__)
router = APIRouter()


def ok_or_error(value: bool) -> str:
    return "ok" if value else "error"


@router.get("/directory_health")
def directory_health(
    info_service: DirectoryInfoService = Depends(get_directory_info_service),
) -> dict[str, Any]:
    logger.info("Checking directory health")
    return {"status": ok_or_error(info_service.health_check())}


@router.get("/directory_metrics")
def metrics(
    info_service: DirectoryInfoService = Depends(get_directory_info_service),
) -> Response:
    logger.info("Directory metrics in prometheus format")
    return Response("\n".join(info_service.get_prometheus_metrics()), media_type="text/plain")
