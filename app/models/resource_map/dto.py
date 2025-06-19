from pydantic import BaseModel


class ResourceMapBase(BaseModel):
    supplier_id: str
    supplier_resource_id: str
    consumer_resource_id: str


class ResourceMapDto(ResourceMapBase):
    resource_type: str


class ResourceMapUpdateDto(BaseModel):
    supplier_id: str
    resource_type: str
    supplier_resource_id: str


class ResourceMapDeleteDto(BaseModel):
    supplier_id: str
    resource_type: str
    supplier_resource_id: str
