from typing import List
from pydantic import BaseModel
from fhir.resources.R4B.bundle import BundleEntry

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
    visited: bool = False
    updated: bool = False

    references: List[NodeReference]

    existing_resource_map: ResourceMapDto | ResourceMapUpdateDto | None = None
    supplier_bundle_entry: BundleEntry
