from typing import Dict
from app.models.fhir.r4.types import Entry
from pydantic import BaseModel

from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto


class UpdateLookupEntry(BaseModel):
    history_size: int
    entry: Entry
    resource_map: ResourceMapDto | ResourceMapUpdateDto | None = None


UpdateLookup = Dict[str, UpdateLookupEntry]
