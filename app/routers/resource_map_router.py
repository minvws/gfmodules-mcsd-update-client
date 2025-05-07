from collections.abc import Sequence
from fastapi import APIRouter, Depends

from app.db.entities.resource_map import ResourceMap
from app.container import get_resource_map_service
from app.models.resource_map.query_params import ResourceMapQueryParams
from app.services.entity.resource_map_service import ResourceMapService

router = APIRouter(prefix="/resource_map", tags=["Resource Map"])


@router.get("", response_model=None)
def find(
    params: ResourceMapQueryParams = Depends(),
    service: ResourceMapService = Depends(get_resource_map_service),
) -> Sequence[ResourceMap]:
    return service.find(**params.model_dump())
