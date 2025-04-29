from re import I
from fhir.resources.R4B.domainresource import DomainResource
from test.test_re import B
from math import exp
from collections import deque
from typing import Dict, List, Deque, Set
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
from app.services.fhir.fhir_service import FhirService
from app.services_new.api.fhir_api import FhirApi
from app.stats import get_stats


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

    def loop(self, entries: List[BundleEntry], result: AdjacencyMap) -> Set[AdjacencyReference]:
        stats = get_stats()
        with stats.timer("loop"):
            missing_refs = set()
            for entry in entries:
                if entry.resource is None:
                    continue
                _, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
                if id in result:
                    print(f"id {id} already in result")
                result[id] = self.create_node(entry)
            for entry in entries:
                if entry.resource is None:
                    continue
                refs = self.__fhir_service.get_references(entry.resource)
                for ref in refs:
                    res_type, ref_id = self.__fhir_service.split_reference(ref)
                    if ref_id in result:
                        continue
                    missing_refs.add(AdjacencyReference(id=ref_id, resource_type=res_type))
            return missing_refs

    def build_adjacency_map(
        self, entries: List[BundleEntry], updated_ids: List[str] | None = None
    ) -> AdjacencyMap:
        result :AdjacencyMap = {}
        with get_stats().timer("inside_build"):

            missing_refs = self.loop(entries, result)

            while len(missing_refs) > 0:
                with get_stats().timer("create_bundle"):
                    bundle_request = self.__fhir_service.create_bundle_request(list(missing_refs))
                with get_stats().timer("post_bundle"):
                    resp = self.__supplier_api.post_bundle(bundle_request)
                with get_stats().timer("get_entries"):
                    entries_inner = self.__fhir_service.get_entries_from_bundle_of_bundles(
                        resp
                    )
                with get_stats().timer("filter"):
                    entries = self.__fhir_service.filter_history_entries(
                        entries_inner
                    )
                missing_refs = self.loop(entries, result)


            print(f"len(result.keys()): {len(result.keys())}")
            return result

        adj_map: AdjacencyMap = {key: self.create_node(value) for key, value in result.items()}
        return adj_map
        stats = get_stats()
        with stats.timer("deque"):
            ids = deque(
                [
                    self.__fhir_service.get_resource_type_and_id_from_entry(entry)[1]
                    for entry in entries
                ]
            )
        with stats.timer("create_nodes"):
            nodes = [self.create_node(entry) for entry in entries]
        with stats.timer("zip"):
            adj_map: adjacencymap = dict(zip(ids, nodes))
        print("ids:")
        print(len(ids))
        output = []
        for x in ids:
            if x not in output:
                output.append(x)

        print(f"unique: {len(output)}")
        print(f"entries: {len(entries)}")
        processed = []
        stats = get_stats()
        with stats.timer("missing_ids_while"):
            while ids:
                id = ids.popleft()
                if id in adj_map:
                    continue
                #with stats.timer(f"per_id:{id}"):
                with stats.timer(f"per_id"):
                    node = adj_map[id]
                    processed.append(id)
                    # print(len(node.supplier_data.references))
                    # take care of missing resources from list
                    missing_ids = []
                    for ref in node.supplier_data.references:
                        if ref.id not in adj_map:
                            missing_ids.append(ref)
                        #if ref.id not in ids and ref.id not in processed:
                        #    missing_ids.append(ref)
                    print(f"missing_ids: {missing_ids}")
                    if len(missing_ids) > 0:
                        bundle_request = self.__fhir_service.create_bundle_request(missing_ids)
                        res = self.__fhir_service.filter_history_entries(
                            self.__fhir_service.get_entries_from_bundle_of_bundles(
                                self.__supplier_api.post_bundle(bundle_request)
                            )
                        )
                        with stats.timer("missing_ids_missing_nodes"):
                            for e in res:
                                missing_node = self.create_node(e)
                                if missing_node.resource_id not in processed:
                                    missing_ids.pop(missing_ids.index(missing_node.resource_id))
                                    self.add_node(adj_map, missing_node)
                                    ids.append(missing_node.resource_id)
                                    # mark as processed
                                    processed.append(missing_node.resource_id)
                        print(missing_ids)
                        if len(missing_ids) > 0:
                            print(f"missing nodes: {missing_ids}")
                            raise exception(f"missing nodes: {missing_ids}")
                    # take care of existing references
                    for ref in node.supplier_data.references:
                        silbing = adj_map[ref.id]
                        if silbing.resource_id not in processed:
                            processed.append(silbing.resource_id)

        print(len(processed))

        # mark ids from previous runs as processed
        if updated_ids is not None:
            for id in updated_ids:
                if id in adj_map.keys():
                    adj_map[id].updated = True

        # get consumer data for adj list:
        with stats.timer("get_consumer_data"):
            self.get_consumer_data(adj_map)
        print(f"adjecency map size: {len(adj_map.keys())}")
        return adj_map

    def get_consumer_data(self, adj_map: AdjacencyMap) -> None:
        consumer_targets = [
            AdjacencyReference(
                id=f"{self.supplier_id}-{node.resource_id}",
                resource_type=node.resource_type,
            )
            for node in adj_map.values()
        ]
        consumer_bunlde = self.__fhir_service.create_bundle_request(consumer_targets)
        res = self.__fhir_service.filter_history_entries(
            self.__fhir_service.get_entries_from_bundle_of_bundles(
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
