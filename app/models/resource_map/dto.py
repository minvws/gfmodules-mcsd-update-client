from pydantic import BaseModel


class ResourceMapBase(BaseModel):
    directory_id: str
    directory_resource_id: str
    update_client_resource_id: str


class ResourceMapDto(ResourceMapBase):
    resource_type: str


class ResourceMapUpdateDto(BaseModel):
    directory_id: str
    resource_type: str
    directory_resource_id: str


class ResourceMapDeleteDto(BaseModel):
    directory_id: str
    resource_type: str
    directory_resource_id: str
