from pydantic import BaseModel


class ResourceMapQueryParams(BaseModel):
    supplier_id: str | None = None
    resource_type: str | None = None
    supplier_resource_id: str | None = None
    consumer_resource_id: str | None = None
