from typing import List
from fhir.resources.R4B.bundle import BundleEntry

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import (
    ConsumerNodeData,
    Node,
    NodeReference,
    SupplierNodeData,
)
from app.models.resource_map.dto import ResourceMapDto
from app.services.entity.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
from app.services.api.fhir_api import FhirApi


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

    def build_adjacency_map(
        self, entries: List[BundleEntry], updated_ids: List[str] | None = None
    ) -> AdjacencyMap:
        nodes = [self.create_node(entry) for entry in entries]
        adj_map = AdjacencyMap(nodes, updated_ids)
        missing_refs = adj_map.get_missing_refs()
        while missing_refs:
            missing_entries = self.get_supplier_data(missing_refs)
            missing_nodes = [self.create_node(entry) for entry in missing_entries]
            adj_map.add_nodes(missing_nodes)
            missing_refs = adj_map.get_missing_refs()

        consumer_targets = [
            NodeReference(
                id=f"{self.supplier_id}-{node.resource_id}",
                resource_type=node.resource_type,
            )
            for node in adj_map.data.values()
        ]
        consumer_entries = self.get_consumer_data(consumer_targets)
        for entry in consumer_entries:
            _, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
            supplier_id = id.replace(f"{self.supplier_id}-", "")
            node = adj_map.data[supplier_id]
            node.consumer_data = ConsumerNodeData(resource=entry.resource)
        return adj_map

    def get_entries(
        self, refs: List[NodeReference], fhir_api: FhirApi
    ) -> List[BundleEntry]:
        bundle_request = self.__fhir_service.create_bundle_request(refs)
        res = self.__fhir_service.filter_history_entries(
            self.__fhir_service.get_entries_from_bundle_of_bundles(
                fhir_api.post_bundle(bundle_request)
            )
        )
        return res

    def get_supplier_data(self, refs: List[NodeReference]) -> List[BundleEntry]:
        return self.get_entries(refs, self.__supplier_api)

    def get_consumer_data(self, refs: List[NodeReference]) -> List[BundleEntry]:
        return self.get_entries(refs, self.__consumer_api)

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

    def create_supplier_data(
        self, supplier_id: str, entry: BundleEntry
    ) -> SupplierNodeData:
        method = self.__fhir_service.get_request_method_from_entry(entry)
        refs = []
        if entry.resource:
            res_refs = self.__fhir_service.get_references(entry.resource)
            for ref in res_refs:
                res_type, ref_id = self.__fhir_service.split_reference(ref)
                refs.append(NodeReference(id=ref_id, resource_type=res_type))
        return SupplierNodeData(
            supplier_id=supplier_id,
            method=method,
            references=refs,
            entry=entry,
        )

    # def get_consumer_data(self, adj_map: AdjacencyData) -> None:
    #     consumer_targets = [
    #         NodeReference(
    #             id=f"{self.supplier_id}-{node.resource_id}",
    #             resource_type=node.resource_type,
    #         )
    #         for node in adj_map.values()
    #     ]
    #     res = self.get_entries(consumer_targets, self.__consumer_api)
    #     for entry in res:
    #         _, id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
    #         if id is not None:
    #             sup_id = id.replace(f"{self.supplier_id}-", "")
    #             node = adj_map[sup_id]
    #             node.consumer_data = ConsumerNodeData(
    #                 resource=entry.resource,
    #             )
