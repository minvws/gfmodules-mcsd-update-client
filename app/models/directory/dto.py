from datetime import datetime
from pydantic import BaseModel

class DirectoryDto(BaseModel):
    id: str
    endpoint_address: str
    deleted_at: datetime | None = None
    is_ignored: bool = False
    failed_sync_count: int = 0
    failed_attempts: int = 0
    last_success_sync: datetime | None = None