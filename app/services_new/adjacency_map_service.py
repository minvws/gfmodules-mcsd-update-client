from collections import deque
from typing import List
from fhir.resources.R4B.bundle import BundleEntry
from app.models.resource_map.dto import ResourceMapDto
from app.models.adjacency.dto import (
    AdjacencyEntry,
    AdjacencyMap,
    SupplierAdjacenceyReference,
    SupplierAdjacencyData,
    ConsumerAdjacencyData,
)

from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)
from app.services_new.api.fhir_api import FhirApi


class AdjacencyMapService:

    # TODO: FhirService should be in container
    def __init__(
        self,
        strict: bool,
        consumer_fhir_api: FhirApi,
        resource_map_service: ResourceMapService,
    ) -> None:
        self.__fhir_service = FhirService(strict)
        self.__consumer_fhir_api = consumer_fhir_api
        self.__resource_map_service = resource_map_service

    def bfs(self, adj: AdjacencyMap, resource_id: str) -> List[AdjacencyEntry]:
        queue = deque()
        visited = []
        node = adj[resource_id]
        node.visited = True

        queue.append(node)

        while queue:
            current = queue.popleft()
            visited.append(current)

            for ref in current.supplier_data.references:
                adjacent_node = adj[ref.id]
                if adjacent_node.visited is False:
                    adjacent_node.visited = True
                    queue.append(adjacent_node)

        return visited

    def create_adjacency_map(self, data: List[BundleEntry], supplier_id: str):
        adjacency_map: AdjacencyMap = {}
        for entry in data:
            res_type, id = get_resource_type_and_id_from_entry(entry)
            request_method = get_request_method_from_entry(entry)
            references = []
            if entry.resource:
                res_ref = self.__fhir_service.get_references(entry.resource)
                for ref in res_ref:
                    res_t, res_id = self.__fhir_service.split_reference(ref)
                    references.append(
                        SupplierAdjacenceyReference(resource_type=res_t, id=res_id)
                    )

            if id is not None and id not in adjacency_map:
                resource_map = self.__resource_map_service.get(
                    res_type, id, supplier_id
                )

                supplier_adj_data = SupplierAdjacencyData(
                    supplier_id=supplier_id,
                    references=references,
                    method=request_method if request_method else "",
                    entry=entry,
                )

                consumer_resource_id = f"{supplier_id}-{id}"
                consumer_resource = (
                    self.__consumer_fhir_api.get_resource(
                        res_type, consumer_resource_id
                    )
                    if res_type
                    else None
                )
                consumer_adj_data = ConsumerAdjacencyData(resource=consumer_resource)

                resource_map = self.__resource_map_service.get(
                    supplier_id=supplier_id, supplier_resource_id=id
                )

                adjacency_map[id] = AdjacencyEntry(
                    resource_id=id,
                    resource_type=res_type if res_type else "",
                    visited=False,
                    supplier_data=supplier_adj_data,
                    consumer_data=consumer_adj_data,
                    resource_map=(
                        ResourceMapDto(**resource_map.to_dict())
                        if resource_map
                        else None
                    ),
                )

        return adjacency_map


# TODO: move these functions to a different file
def get_resource_type_and_id_from_entry(
    entry: BundleEntry,
) -> tuple[str | None, str | None]:
    """
    Retrieves the resource type and id from a Bundle entry
    """
    if entry.resource is not None:
        return entry.resource.__resource_type__, entry.resource.id

    if entry.fullUrl is not None:
        return entry.fullUrl.split("/")[-2], entry.fullUrl.split("/")[-1]

    # On DELETE entries, there is no resource anymore. In that case we need to grab the resource type and id from the URL
    if entry.request is not None and entry.request.url is not None:
        return entry.request.url.split("/")[-2], entry.request.url.split("/")[-1]

    return None, None


def get_request_method_from_entry(entry: BundleEntry) -> str | None:
    """
    Retrieves the request method from a Bundle entry if a request is present
    """
    entry_request = entry.request
    if entry_request is None:
        return None

    method = entry_request.method
    if method is None:
        return None

    return method


def namespace_bundle_entry(data: BundleEntry, namespace: str) -> BundleEntry:
    if data.resource is not None:
        namespace_resource_reference(data.resource, namespace)

    return data
