from collections.abc import Sequence
from fastapi import APIRouter, Depends

from app.container import (
    get_directory_provider,
)
from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.directory_provider import DirectoryProvider

router = APIRouter(prefix="/directory", tags=["Directory"])


@router.get("", response_model=None)
def get_all_directories(
    service: DirectoryProvider = Depends(get_directory_provider),
) -> Sequence[DirectoryDto]:
    return service.get_all_directories()

@router.get("/{_id}", response_model=None)
def get_one_directory(
    _id: str, service: DirectoryProvider = Depends(get_directory_provider)
) -> DirectoryDto:
    return service.get_one_directory(_id)
