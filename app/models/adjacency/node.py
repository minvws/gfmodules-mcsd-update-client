from typing import List, Literal
from pydantic import BaseModel
from fhir.resources.R4B.bundle import BundleEntry

from app.models.fhir.types import HttpValidVerbs
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto


class NodeReference(BaseModel):
    id: str
    resource_type: str

    def namespace_id(self, namespace: str) -> None:
        new_id = f"{namespace}-{self.id}"
        self.id = new_id


class NodeUpdateData(BaseModel):
    bundle_entry: BundleEntry | None = None
    resource_map_dto: ResourceMapDto | ResourceMapUpdateDto | None = None


class Node(BaseModel):
    resource_id: str
    resource_type: str
    references: List[NodeReference] = []
    visited: bool = False
    updated: bool = False
    method: HttpValidVerbs
    status: Literal["ignore", "equal", "delete", "update", "new", "unknown"] = "unknown"
    supplier_hash: int | None = None
    consumer_hash: int | None = None
    update_data: NodeUpdateData | None = None
    supplier_entry: BundleEntry | None = None

    def clear_for_cash(self) -> None:
        self.references = []
        self.visited = False
        self.update_data = None
        self.supplier_entry = None
