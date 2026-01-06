from typing import List, Literal
from pydantic import BaseModel
from fhir.resources.R4B.bundle import BundleEntry

from app.models.fhir.types import HttpValidVerbs
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto, ResourceMapDeleteDto


class NodeReference(BaseModel):
    id: str
    resource_type: str

    def cache_key(self) -> str:
        """Stable key for maps/caches: <resourceType>/<id>."""
        return f"{self.resource_type}/{self.id}"


class NodeUpdateData(BaseModel):
    bundle_entry: BundleEntry | None = None
    resource_map_dto: ResourceMapDto | ResourceMapUpdateDto | ResourceMapDeleteDto | None = None


class Node(BaseModel):
    resource_id: str
    resource_type: str
    references: List[NodeReference] = []
    visited: bool = False
    updated: bool = False
    method: HttpValidVerbs
    status: Literal["ignore", "equal", "delete", "update", "new", "unknown"] = "unknown"
    directory_hash: int | None = None
    update_client_hash: int | None = None
    update_data: NodeUpdateData | None = None
    directory_entry: BundleEntry | None = None

    def cache_key(self) -> str:
        """Stable key for maps/caches: <resourceType>/<id>."""
        return f"{self.resource_type}/{self.resource_id}"

    def clear_for_cache(self) -> None:
        self.references = []
        self.visited = False
        self.update_data = None
        self.directory_entry = None
