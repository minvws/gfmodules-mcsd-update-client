from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.container import get_directory_registry_service
from app.services.directory_registry_service import DirectoryRegistryService

router = APIRouter(prefix="/admin/directory-registry", tags=["Directory Registry"])


class ProviderCreateRequest(BaseModel):
    url: str
    enabled: bool = True


class ManualDirectoryCreateRequest(BaseModel):
    endpoint_address: str
    id: str | None = None
    ura: str = ""


@router.get("/providers")
def list_providers(
    service: DirectoryRegistryService = Depends(get_directory_registry_service),
) -> list[dict]:
    return service.list_providers(include_disabled=True)


@router.post("/providers")
def add_provider(
    body: ProviderCreateRequest,
    service: DirectoryRegistryService = Depends(get_directory_registry_service),
) -> dict:
    return service.add_provider(url=body.url, enabled=body.enabled)


@router.post("/providers/refresh")
def refresh_providers(
    service: DirectoryRegistryService = Depends(get_directory_registry_service),
) -> dict:
    return service.refresh_all_enabled_providers()


@router.post("/directories")
def add_manual_directory(
    body: ManualDirectoryCreateRequest,
    service: DirectoryRegistryService = Depends(get_directory_registry_service),
) -> dict:
    dto = service.add_manual_directory(
        endpoint_address=body.endpoint_address,
        directory_id=body.id,
        ura=body.ura,
    )
    return dto.model_dump()
