from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse

from app.container import get_ignored_directory_service
from app.services.entity.ignored_directory_service import IgnoredDirectoryService


router = APIRouter(prefix="/ignore_list", tags=["Ignore list"])


@router.get("", response_model=None)
def find(
    service: IgnoredDirectoryService = Depends(get_ignored_directory_service),
) -> Response:
    ignored = service.get_all_ignored_directories()
    return JSONResponse(status_code=200, content={"ignored": [d.to_dict() for d in ignored]})

@router.get("/{directory_id}", response_model=None)
def find_by_id(
    directory_id: str,
    service: IgnoredDirectoryService = Depends(get_ignored_directory_service),
) -> JSONResponse|Response:
    directory = service.get_ignored_directory(directory_id)
    if directory:
        return JSONResponse(status_code=200, content=directory.to_dict())
    else:
        return Response(status_code=404)

@router.post("", response_model=None)
def add_directory_to_ignore_list(
    directory_id: str,
    service: IgnoredDirectoryService = Depends(get_ignored_directory_service),
) -> Response:
    service.add_directory_to_ignore_list(directory_id)
    return Response(status_code=201, content=None)

@router.delete("", response_model=None)
def remove_directory_from_ignore_list(
    directory_id: str,
    service: IgnoredDirectoryService = Depends(get_ignored_directory_service),
) -> Response:
    service.remove_directory_from_ignore_list(directory_id)
    return Response(status_code=204, content=None)
