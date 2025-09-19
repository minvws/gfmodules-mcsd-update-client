from datetime import datetime
from typing import Any
from pydantic import BaseModel


class DirectoryDto(BaseModel):
    id: str
    endpoint_address: str
    deleted_at: datetime | None = None
    is_ignored: bool = False
    failed_sync_count: int = 0
    failed_attempts: int = 0
    last_success_sync: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "endpoint_address": self.endpoint_address,
            "deleted_at": self.deleted_at.isoformat() if self.deleted_at else None,
            "is_ignored": self.is_ignored,
            "failed_sync_count": self.failed_sync_count,
            "failed_attempts": self.failed_attempts,
            "last_success_sync": self.last_success_sync.isoformat() if self.last_success_sync else None,
        }