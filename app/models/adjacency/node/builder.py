from abc import ABC, abstractmethod
import copy
from typing import Literal

from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.services.fhir.bundle.bundle_utils import get_request_method_from_entry
from app.services.fhir.references.reference_extractor import (
    get_references,
)
from fhir.resources.R4B.bundle import BundleEntry, BundleEntryRequest

from app.models.adjacency.node.node import Node, NodeReference, NodeUpdateData
from app.services.fhir.references.reference_misc import split_reference
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)


class Builder(ABC):
    def __init__(self) -> None:
        pass

    @property
    @abstractmethod
    def node(self) -> Node:
        pass

    @abstractmethod
    def produce_resource_id(self, id: str) -> None:
        pass

    @abstractmethod
    def produce_resource_type(self, resource_type: str) -> None:
        pass

    @abstractmethod
    def produce_supplier_id(self, supplier_id: str) -> None:
        pass

    @abstractmethod
    def produce_supplier_hash(self, entry: BundleEntry) -> None:
        pass

    @abstractmethod
    def produce_consumer_hash(self, entry: BundleEntry) -> None:
        pass

    @abstractmethod
    def produce_update_data(self, entry: BundleEntry) -> None:
        pass

    @abstractmethod
    def produce_update_status(self, entry: BundleEntry) -> None:
        pass

    @abstractmethod
    def produce_references(self, entry: BundleEntry) -> None:
        pass

    @abstractmethod
    def produce_resource_map(self, resource_map) -> None:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass


# should some properties be part of the constructor?
class NodeBuilder(Builder):
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._node = Node()

    @property
    def node(self) -> Node:
        node = self._node
        self.reset()
        return node

    def produce_resource_id(self, id: str) -> None:
        self._node.resource_id = id

    def produce_resource_type(self, resource_type: str) -> None:
        self._node.resource_type = resource_type

    def produce_supplier_id(self, supplier_id: str) -> None:
        self._node.supplier_id = supplier_id

    def produce_supplier_hash(self, entry: BundleEntry) -> None:
        res = copy.deepcopy(entry.resource)
        if res is None:
            self._node.supplier_hash = None
            return

        res.meta = None
        res.id = None
        namespace_resource_reference(res, "here should come supplier_id ")
        hash_value = hash(res.model_dump().__repr__())
        self._node.supplier_hash = hash_value

    def produce_consumer_hash(self, entry: BundleEntry) -> None:
        res = entry.resource
        if res is None:
            self._node.consumer_hash = None
            return

        res.meta = None
        res.id = None
        hash_value = hash(res.model_dump().__repr__())
        self._node.consumer_hash = hash_value

    def produce_references(self, entry: BundleEntry) -> None:
        refs = []
        if entry.resource:
            res_refs = get_references(entry.resource)
            for ref in res_refs:
                ref_type, ref_id = split_reference(ref)
                refs.append(NodeReference(id=ref_id, resource_type=ref_type))

        self._node.references = refs

    def produce_resource_map(self, resource_map: ResourceMapDto) -> None:
        self._node.resource_map = resource_map

    def produce_update_status(self, entry: BundleEntry) -> None:
        status = self.__compute_update_status(entry)
        self._node.update_status = status

    def produce_update_data(self, entry: BundleEntry) -> None:
        update_data = self.__compute_update_data(entry)
        self._node.update_data = update_data

    def __compute_update_status(
        self, entry: BundleEntry
    ) -> Literal["ignore", "equal", "delete", "update", "new", "unknown"]:
        method = get_request_method_from_entry(entry)
        if method == "DELETE" and self._node.consumer_hash is None:
            return "ignore"

        if (
            method != "DELETE"
            and self._node.supplier_hash
            and self._node.consumer_hash
            and self._node.supplier_hash == self._node.consumer_hash
        ):
            return "equal"

        if method == "DELETE" and self._node.supplier_hash is not None:
            return "delete"

        if (
            method != "DELETE"
            and self._node.supplier_hash is not None
            and self._node.consumer_hash is None
            and self._node.resource_map is None
        ):
            return "new"

        return "update"

    def __compute_update_data(self, entry: BundleEntry) -> NodeUpdateData:
        consumer_resource_id = f"{self._node.supplier_id}-{self._node.supplier_id}"
        url = f"{self._node.resource_type}/{consumer_resource_id}"
        dto: ResourceMapDto | ResourceMapUpdateDto | None = None

        match self._node.update_status:
            case "equal":
                return NodeUpdateData()

            case "ignore":
                return NodeUpdateData()

            case "delete":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="DELETE", url=url
                )
                entry.request = entry_request

                if self._node.resource_map is None:
                    raise Exception(
                        f"Resource map for {self.resource_id} {self.resource_type} cannot be None and marked as delete "
                    )

                dto = ResourceMapUpdateDto(
                    history_size=self._node.resource_map.history_size + 1,
                    supplier_id=self._node.supplier_id,
                    resource_type=self._node.resource_type,
                    supplier_resource_id=self._node.resource_id,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "new":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                resource = entry.resource
                if resource is None:
                    raise Exception(
                        f"Resource {self.resource_id} {self.resource_type} cannot be None when a node is marked `new`"
                    )
                namespace_resource_reference(resource, self._node.supplier_id)
                resource.id = consumer_resource_id
                entry.resource = resource
                entry.request = entry_request

                dto = ResourceMapDto(
                    supplier_id=self._node.supplier_id,
                    supplier_resource_id=self._node.resource_id,
                    consumer_resource_id=consumer_resource_id,
                    resource_type=self._node.resource_type,
                    history_size=1,
                )
                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "update":

                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                resource = entry.resource
                if resource is None:
                    raise Exception(
                        f"Resource {self._node.resource_id} {self._node.resource_type} cannot be None and node marked as `update`"
                    )

                namespace_resource_reference(resource, self.supplier_data.supplier_id)
                resource.id = consumer_resource_id
                entry.request = entry_request
                entry.resource = resource

                if self._node.resource_map is None:
                    raise Exception(
                        f"Resource map for {self.resource_id} {self.resource_type} cannot be None and marked as `update`"
                    )

                dto = ResourceMapUpdateDto(
                    supplier_id=self._node.supplier_id,
                    supplier_resource_id=self._node.resource_id,
                    resource_type=self._node.resource_type,
                    history_size=self._node.resource_map.history_size + 1,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "unknown":
                raise Exception("Node with status 'unknown' cannot produce update data")
