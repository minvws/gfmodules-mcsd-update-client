from collections import deque
from typing import List, Deque
from fhir.resources.R4B.bundle import BundleEntry
from app.models.adjacency.adjacency_map import (
    AdjacencyMap,
    Node,
    AdjacencyReference,
    SupplierNodeData,
    ConsumerNodeData,
)

from app.models.resource_map.dto import ResourceMapDto
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.fhir.bundle.bundle_utils import (
    filter_history_entries,
)
from app.services.fhir.bundle.bunlde_factory import (
    create_bundle_request,
    get_entries_from_bundle_of_bundles,
)
from app.services.fhir.fhir_service import FhirService
from app.services_new.api.fhir_api import FhirApi


class AdjacencyMapService:
    def __init__(
        self,
        supplier_id: str,
        fhir_service: FhirService,
        supplier_api: FhirApi,
        consumer_api: FhirApi,
        resource_map_service: ResourceMapService,
    ) -> None:
        self.supplier_id = supplier_id
        self.__fhir_service = fhir_service
        self.__supplier_api = supplier_api
        self.__consumer_api = consumer_api
        self.__resource_map_service = resource_map_service

    def create_supplier_data(
        self, supplier_id: str, entry: BundleEntry
    ) -> SupplierNodeData:
        method = self.__fhir_service.get_request_method_from_entry(entry)
        refs = []
        if entry.resource:
            res_refs = self.__fhir_service.get_references(entry.resource)
            for ref in res_refs:
                res_type, ref_id = self.__fhir_service.split_reference(ref)
                refs.append(AdjacencyReference(id=ref_id, resource_type=res_type))
        return SupplierNodeData(
            supplier_id=supplier_id,
            method=method,
            references=refs,
            entry=entry,
        )

    def create_node(self, entry: BundleEntry) -> Node:
        res_type, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
        supplier_node_data = self.create_supplier_data(self.supplier_id, entry)

        node = Node(
            resource_id=id,
            resource_type=res_type,
            supplier_data=supplier_node_data,
            # consumer_data=consumer_node_data,
            consumer_data=ConsumerNodeData(resource=None),
        )
        resource_map = self.__resource_map_service.get(
            supplier_id=self.supplier_id,
            resource_type=res_type,
            supplier_resource_id=id,
        )
        node.resource_map = (
            ResourceMapDto(**resource_map.to_dict()) if resource_map else None
        )
        return node

    def add_node(self, adj_map: AdjacencyMap, node: Node) -> None:
        if node.resource_id in adj_map.keys():
            return

        adj_map[node.resource_id] = node

    def build_adjacency_map(
        self, entries: List[BundleEntry], updated_ids: List[str] | None = None
    ) -> AdjacencyMap:
        ids = deque(
            [
                self.__fhir_service.get_resource_type_and_id_from_entry(entry)[1]
                for entry in entries
            ]
        )
        nodes = [self.create_node(entry) for entry in entries]
        adj_map: AdjacencyMap = dict(zip(ids, nodes))

        processed = []
        while ids:
            id = ids.popleft()
            node = adj_map[id]
            processed.append(id)

            # take care of missing resources from list
            missing_ids = []
            for ref in node.supplier_data.references:
                if ref.id not in ids:
                    missing_ids.append(ref)

            if len(missing_ids) > 0:
                bundle_request = create_bundle_request(missing_ids)
                res = filter_history_entries(
                    get_entries_from_bundle_of_bundles(
                        self.__supplier_api.post_bundle(bundle_request)
                    )
                )
                for e in res:
                    missing_node = self.create_node(e)
                    if missing_node.resource_id not in processed:
                        self.add_node(adj_map, missing_node)
                        ids.append(missing_node.resource_id)
                        # mark as processed
                        processed.append(missing_node.resource_id)

            # take care of existing references
            for ref in node.supplier_data.references:
                silbing = adj_map[ref.id]
                if silbing.resource_id not in processed:
                    processed.append(silbing.resource_id)

        # mark ids from previous runs as processed
        if updated_ids is not None:
            for id in updated_ids:
                if id in adj_map.keys():
                    adj_map[id].updated = True

        # get consumer data for adj list:
        self.get_consumer_data(adj_map)

        return adj_map

    def get_consumer_data(self, adj_map: AdjacencyMap) -> None:
        consumer_targets = [
            AdjacencyReference(
                id=f"{self.supplier_id}-{node.resource_id}",
                resource_type=node.resource_type,
            )
            for node in adj_map.values()
        ]
        consumer_bunlde = create_bundle_request(consumer_targets)
        res = filter_history_entries(
            get_entries_from_bundle_of_bundles(
                self.__consumer_api.post_bundle(consumer_bunlde)
            )
        )
        for entry in res:
            _, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
            if id is not None:
                sup_id = id.replace(f"{self.supplier_id}-", "")
                node = adj_map[sup_id]
                node.consumer_data = ConsumerNodeData(
                    resource=entry.resource,
                )

    def bfs(self, adj_map: AdjacencyMap, node: Node) -> List[Node]:
        queue: Deque["Node"] = deque()
        group = []

        node.visited = True
        queue.append(node)
        group.append(node)

        while queue:
            current = queue.popleft()
            for ref in current.supplier_data.references:
                sibling = adj_map[ref.id]
                if sibling.visited is False:
                    sibling.visited = True
                    queue.append(sibling)
                    group.append(sibling)

        return group
