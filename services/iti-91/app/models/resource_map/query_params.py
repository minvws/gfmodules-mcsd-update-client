from pydantic import BaseModel


class ResourceMapQueryParams(BaseModel):
    directory_id: str | None = None
    resource_type: str | None = None
    directory_resource_id: str | None = None
    update_client_resource_id: str | None = None
