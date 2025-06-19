import copy
from typing import List
from app.db.entities.resource_map import ResourceMap
from app.models.fhir.types import BundleRequestParams
from app.services.update.cache.caching_service import CachingService
from fhir.resources.R4B.bundle import BundleEntry, BundleEntryRequest

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import (
    Node,
    NodeReference,
    NodeUpdateData,
)
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.services.entity.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
from app.services.api.fhir_api import FhirApi
from app.services.update.computation_service import ComputationService


class AdjacencyMapService:
    def __init__(
        self,
        supplier_id: str,
        fhir_service: FhirService,
        supplier_api: FhirApi,
        consumer_api: FhirApi,
        resource_map_service: ResourceMapService,
        cache_service: CachingService,
    ) -> None:
        self.supplier_id = supplier_id
        self.__fhir_service = fhir_service
        self.__supplier_api = supplier_api
        self.__consumer_api = consumer_api
        self.__resource_map_service = resource_map_service
        self.__cache_service = cache_service
        self.__computation_service = ComputationService(
            supplier_id=supplier_id, fhir_service=fhir_service
        )

    def build_adjacency_map(self, entries: List[BundleEntry]) -> AdjacencyMap:
        nodes = [self.create_node(entry) for entry in entries]
        adj_map = AdjacencyMap(nodes)
        missing_refs = adj_map.get_missing_refs()

        while missing_refs:
            # exhaust cache first
            data = [self.__cache_service.get_node(ref.id) for ref in missing_refs]
            missing_nodes = [x for x in data if x is not None]

            if len(missing_nodes) > 0:
                adj_map.add_nodes(missing_nodes)
                missing_refs = adj_map.get_missing_refs()
                continue

            # get the rest from the supplier api
            missing_entries = self.get_supplier_data(
                [BundleRequestParams(**ref.model_dump()) for ref in missing_refs]
            )
            missing_nodes = [self.create_node(entry) for entry in missing_entries]
            adj_map.add_nodes(missing_nodes)
            missing_refs = adj_map.get_missing_refs()

        consumer_targets = self.__get_consumer_missing_targets(adj_map)
        consumer_entries = self.get_consumer_data(consumer_targets)

        for entry in consumer_entries:
            _, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
            res_id = id.replace(f"{self.supplier_id}-", "")
            node = adj_map.data[res_id]
            node.consumer_hash = self.__computation_service.hash_consumer_entry(entry)

        for node in adj_map.data.values():
            if node.updated is True:
                continue

            resource_map = self.__resource_map_service.get(
                supplier_id=self.supplier_id,
                resource_type=node.resource_type,
                supplier_resource_id=node.resource_id,
            )
            node.status = self.__computation_service.get_update_status(
                method=node.method,
                supplier_hash=node.supplier_hash,
                consumer_hash=node.consumer_hash,
                resource_map=resource_map,
            )
            node.update_data = self.create_update_data(node, resource_map)

        return adj_map

    def __get_entries(
        self, refs: List[BundleRequestParams], fhir_api: FhirApi
    ) -> List[BundleEntry]:
        bundle_request = self.__fhir_service.create_bundle_request(refs)
        res = self.__fhir_service.filter_history_entries(
            self.__fhir_service.get_entries_from_bundle_of_bundles(
                fhir_api.post_bundle(bundle_request)
            )
        )
        return res

    def get_supplier_data(self, refs: List[BundleRequestParams]) -> List[BundleEntry]:
        return self.__get_entries(refs, self.__supplier_api)

    def get_consumer_data(self, refs: List[BundleRequestParams]) -> List[BundleEntry]:
        return self.__get_entries(refs, self.__consumer_api)

    def create_node(self, entry: BundleEntry) -> Node:
        res_type, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
        method = self.__fhir_service.get_request_method_from_entry(entry)
        supplier_hash = self.__computation_service.hash_supplier_entry(entry)
        references = self.create_node_references(entry)

        return Node(
            resource_id=id,
            resource_type=res_type,
            supplier_hash=supplier_hash,
            method=method,
            supplier_entry=entry,
            references=references,
        )

    def create_node_references(self, entry: BundleEntry) -> List[NodeReference]:
        if entry.resource is None:
            return []

        fhir_refs = self.__fhir_service.get_references(entry.resource)
        split_fhir_refs = [
            self.__fhir_service.split_reference(ref) for ref in fhir_refs
        ]
        return [
            NodeReference(resource_type=res_type, id=id)
            for res_type, id in split_fhir_refs
        ]

    def create_update_data(
        self, node: Node, resource_map: ResourceMap | None
    ) -> NodeUpdateData | None:
        consumer_resource_id = f"{self.supplier_id}-{node.resource_id}"
        url = f"{node.resource_type}/{consumer_resource_id}"
        dto: ResourceMapDto | ResourceMapUpdateDto | None = None

        match node.status:
            case "ignore":
                return None

            case "equal":
                return None

            case "delete":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="DELETE", url=url
                )
                entry.request = entry_request

                if resource_map is None:
                    raise Exception(
                        f"Resource map for {node.resource_id} {node.resource_type} cannot be None and marked as delete "
                    )

                dto = ResourceMapUpdateDto(
                    supplier_id=self.supplier_id,
                    resource_type=node.resource_type,
                    supplier_resource_id=node.resource_id,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "new":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                if node.supplier_entry is None:
                    raise Exception(
                        f"Supplier entry for {node.resource_id} {node.resource_type} cannot be None and node marked as `new`"
                    )
                resource = copy.deepcopy(node.supplier_entry.resource)
                if resource is None:
                    raise Exception(
                        f"Resource {node.resource_id} {node.resource_type} cannot be None when a node is marked `new`"
                    )
                self.__fhir_service.namespace_resource_references(
                    resource, self.supplier_id
                )
                resource.id = consumer_resource_id
                entry.resource = resource
                entry.request = entry_request

                dto = ResourceMapDto(
                    supplier_id=self.supplier_id,
                    supplier_resource_id=node.resource_id,
                    consumer_resource_id=consumer_resource_id,
                    resource_type=node.resource_type,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "update":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                if node.supplier_entry is None:
                    raise Exception(
                        f"Supplier entry for {node.resource_id} {node.resource_type} cannot be None and node marked as `update`"
                    )

                resource = copy.deepcopy(node.supplier_entry.resource)
                if resource is None:
                    raise Exception(
                        f"Resource {node.resource_id} {node.resource_type} cannot be None and node marked as `update`"
                    )

                self.__fhir_service.namespace_resource_references(
                    resource, self.supplier_id
                )
                resource.id = consumer_resource_id
                entry.request = entry_request
                entry.resource = resource

                if resource_map is None:
                    raise Exception(
                        f"Resource map for {node.resource_id} {node.resource_type} cannot be None and marked as `update`"
                    )

                dto = ResourceMapUpdateDto(
                    supplier_id=self.supplier_id,
                    supplier_resource_id=node.resource_id,
                    resource_type=node.resource_type,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "unknown":
                raise Exception(
                    f"node {node.resource_id} {node.resource_type} cannot be unknown at this stage"
                )

    def __get_consumer_missing_targets(
        self, adj_map: AdjacencyMap
    ) -> List[BundleRequestParams]:
        consumer_targets = []
        for id, node in adj_map.data.items():
            if not self.__cache_service.key_exists(id):
                consumer_targets.append(
                    BundleRequestParams(
                        id=f"{self.supplier_id}-{id}", resource_type=node.resource_type
                    )
                )
        return consumer_targets
