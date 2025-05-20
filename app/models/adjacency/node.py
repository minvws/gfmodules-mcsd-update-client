import copy
from typing import List, Literal
from fhir.resources.R4B.domainresource import DomainResource
from pydantic import BaseModel, computed_field
from fhir.resources.R4B.bundle import BundleEntry, BundleEntryRequest

from app.models.fhir.types import HttpValidVerbs
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)


class NodeReference(BaseModel):
    id: str
    resource_type: str

    def namespace_id(self, namespace: str) -> None:
        new_id = f"{namespace}-{self.id}"
        self.id = new_id


class SupplierNodeData(BaseModel):
    supplier_id: str
    references: List[NodeReference]
    method: HttpValidVerbs
    entry: BundleEntry

    @computed_field  # type: ignore
    @property
    def hash_value(self) -> int | None:
        if self.entry.resource is None:
            return None

        resource = copy.deepcopy(self.entry.resource)
        namespace_resource_reference(resource, self.supplier_id)
        resource.meta = None
        resource.id = None

        return hash(resource.model_dump().__repr__())


class ConsumerNodeData(BaseModel):
    resource: DomainResource | None

    @computed_field  # type: ignore
    @property
    def hash_value(self) -> int | None:
        if self.resource is None:
            return None

        res = copy.deepcopy(self.resource)
        res.meta = None
        res.id = None
        return hash(res.model_dump().__repr__())


class NodeUpdateData(BaseModel):
    bundle_entry: BundleEntry | None = None
    resource_map_dto: ResourceMapDto | ResourceMapUpdateDto | None = None


class BaseNode(BaseModel):
    resource_id: str
    resource_type: str


class ToBeUpdatedNode(BaseNode):
    references: List[NodeReference]
    entry: BundleEntry
    status: Literal["ignore", "equal", "delete", "update", "new"]


class AlreadyPushedNode(BaseNode):
    pass


# class Node(BaseModel):
    # resource_id: str
    # resource_type: str
    # visited: bool = False
    #
    # updated: bool = False
    # references: List[NodeReference] | None
    # entry: BundleEntry | None
    #
    # status: Literal["ignore", "equal", "delete", "update", "new"]
    # resource_map: ResourceMapDto | ResourceMapUpdateDto | None = None

    # @computed_field  # type: ignore
    # @property
    # def update_status(
    #     self,
    # ) -> Literal["ignore", "equal", "delete", "update", "new"]:
    #
    #     if (
    #         self.supplier_data.method == "DELETE"
    #         and self.consumer_data.resource is None
    #     ):
    #         return "ignore"
    #
    #     if (
    #         self.supplier_data.method != "DELETE"
    #         and self.supplier_data.hash_value
    #         and self.consumer_data.hash_value
    #         and self.supplier_data.hash_value == self.consumer_data.hash_value
    #     ):
    #         return "equal"
    #
    #     if (
    #         self.supplier_data.method == "DELETE"
    #         and self.consumer_data.resource is not None
    #     ):
    #         return "delete"
    #
    #     if (
    #         self.supplier_data.method != "DELETE"
    #         and self.supplier_data.hash_value is not None
    #         and self.consumer_data.hash_value is None
    #         and self.resource_map is None
    #     ):
    #         return "new"
    #
    #     return "update"
    #
    # @computed_field  # type: ignore
    # @property
    # def update_data(self) -> NodeUpdateData | None:
    #     consumer_resource_id = f"{self.supplier_data.supplier_id}-{self.resource_id}"
    #     url = f"{self.resource_type}/{consumer_resource_id}"
    #     dto: ResourceMapDto | ResourceMapUpdateDto | None = None
    #     match self.update_status:
    #         case "ignore":
    #             return None
    #
    #         case "equal":
    #             return None
    #
    #         case "delete":
    #             entry = BundleEntry.model_construct()
    #             entry_request = BundleEntryRequest.model_construct(
    #                 method="DELETE", url=url
    #             )
    #             entry.request = entry_request
    #
    #             if self.resource_map is None:
    #                 raise Exception(
    #                     f"Resource map for {self.resource_id} {self.resource_type} cannot be None and marked as delete "
    #                 )
    #
    #             dto = ResourceMapUpdateDto(
    #                 history_size=self.resource_map.history_size + 1,
    #                 supplier_id=self.supplier_data.supplier_id,
    #                 resource_type=self.resource_type,
    #                 supplier_resource_id=self.resource_id,
    #             )
    #
    #             return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)
    #
    #         case "new":
    #             entry = BundleEntry.model_construct()
    #             entry_request = BundleEntryRequest.model_construct(
    #                 method="PUT", url=url
    #             )
    #             resource = copy.deepcopy(self.supplier_data.entry.resource)
    #             if resource is None:
    #                 raise Exception(
    #                     f"Resource {self.resource_id} {self.resource_type} cannot be None when a node is marked `new`"
    #                 )
    #             namespace_resource_reference(resource, self.supplier_data.supplier_id)
    #             resource.id = consumer_resource_id
    #             entry.resource = resource
    #             entry.request = entry_request
    #
    #             dto = ResourceMapDto(
    #                 supplier_id=self.supplier_data.supplier_id,
    #                 supplier_resource_id=self.resource_id,
    #                 consumer_resource_id=consumer_resource_id,
    #                 resource_type=self.resource_type,
    #                 history_size=1,
    #             )
    #
    #             return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)
    #
    #         case "update":
    #             entry = BundleEntry.model_construct()
    #             entry_request = BundleEntryRequest.model_construct(
    #                 method="PUT", url=url
    #             )
    #             resource = copy.deepcopy(self.supplier_data.entry.resource)
    #             if resource is None:
    #                 raise Exception(
    #                     f"Resource {self.resource_id} {self.resource_type} cannot be None and node marked as `update`"
    #                 )
    #
    #             namespace_resource_reference(resource, self.supplier_data.supplier_id)
    #             resource.id = consumer_resource_id
    #             entry.request = entry_request
    #             entry.resource = resource
    #
    #             if self.resource_map is None:
    #                 raise Exception(
    #                     f"Resource map for {self.resource_id} {self.resource_type} cannot be None and marked as `update`"
    #                 )
    #
    #             dto = ResourceMapUpdateDto(
    #                 supplier_id=self.supplier_data.supplier_id,
    #                 supplier_resource_id=self.resource_id,
    #                 resource_type=self.resource_type,
    #                 history_size=self.resource_map.history_size + 1,
    #             )
    #
    #             return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)
