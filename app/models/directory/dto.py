from datetime import datetime
from pydantic import BaseModel

class DirectoryDto(BaseModel):
    id: str
    endpoint_address: str
    deleted_at: datetime | None = None
    is_ignored: bool = False

