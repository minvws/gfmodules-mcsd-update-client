import copy
from typing import Dict, List, Literal
from pydantic import BaseModel, computed_field
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.domainresource import DomainResource
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)
from app.models.resource_map.dto import ResourceMapDto


class SupplierAdjacenceyReference(BaseModel):
    id: str
    resource_type: str


class SupplierAdjacencyData(BaseModel):
    supplier_id: str
    references: List[SupplierAdjacenceyReference]
    method: str  # Literal
    entry: BundleEntry

    @computed_field
    @property
    def hash_value(self) -> int | None:
        if self.entry.resource is None:
            return None

        resource = copy.deepcopy(self.entry.resource)
        namespace_resource_reference(resource, self.supplier_id)
        resource.meta = None
        resource.id = None

        return hash(resource.model_dump().__repr__())


class ConsumerAdjacencyData(BaseModel):
    resource: DomainResource | None

    @computed_field
    @property
    def hash_value(self) -> int | None:
        if self.resource is None:
            return None

        res = copy.deepcopy(self.resource)
        res.meta = None
        res.id = None
        return hash(res.model_dump().__repr__())


class AdjacencyEntry(BaseModel):
    resource_id: str
    resource_type: str
    visited: bool = False
    supplier_data: SupplierAdjacencyData
    consumer_data: ConsumerAdjacencyData
    resource_map: ResourceMapDto | None

    @computed_field
    @property
    def update_status(
        self,
    ) -> Literal["ignore", "delele", "update", "new"]:  # maybe literal
        resource_not_needed = (
            True
            if self.supplier_data.method == "DELETE"
            and self.consumer_data.hash_value is None
            else False
        )
        resources_are_equal = (
            True
            if self.supplier_data.method != "DELETE"
            and self.supplier_data.hash_value
            and self.consumer_data.hash_value
            and self.supplier_data.hash_value == self.consumer_data.hash_value
            else False
        )
        resource_needs_deletion = (
            True
            if self.supplier_data.method == "DELETE"
            and self.consumer_data.hash_value is not None
            else False
        )
        resource_is_new = (
            True
            if self.supplier_data.method != "DELETE"
            and self.supplier_data.hash_value is not None
            and self.consumer_data.hash_value is None
            else False
        )

        if resource_not_needed or resources_are_equal:
            return "ignore"

        if resource_needs_deletion:
            return "delele"

        if resource_is_new:
            return "new"

        return "update"


AdjacencyMap = Dict[str, AdjacencyEntry]
