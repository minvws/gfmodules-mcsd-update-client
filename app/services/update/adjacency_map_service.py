import copy
import logging
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

logger = logging.getLogger(__name__)


class AdjacencyMapException(Exception):
    """Base exception for adjacency map operations"""
    pass


class InvalidNodeStateException(AdjacencyMapException):
    pass


class AdjacencyMapService:
    def __init__(
        self,
        directory_id: str,
        fhir_service: FhirService,
        directory_api: FhirApi,
        update_client_api: FhirApi,
        resource_map_service: ResourceMapService,
        cache_service: CachingService,
    ) -> None:
        self.directory_id = directory_id
        self.__fhir_service = fhir_service
        self.__directory_api = directory_api
        self.__update_client_api = update_client_api
        self.__resource_map_service = resource_map_service
        self.__cache_service = cache_service
        self.__computation_service = ComputationService(
            directory_id=directory_id, fhir_service=fhir_service
        )

    def build_adjacency_map(self, entries: List[BundleEntry]) -> AdjacencyMap:
        nodes = [self.create_node(entry) for entry in entries]
        adj_map = AdjacencyMap(nodes)

        failed_refs: list[NodeReference] = []
        while True:
            missing_refs = adj_map.get_missing_refs()

            # Nothing left to do
            if not missing_refs:
                break

            # Remove all failed refs first
            unresolved = [r for r in missing_refs if r not in failed_refs]

            # Nothing left? Then we only have failed references left
            if not unresolved:
                logger.error("Unresolved references found: %s", failed_refs)
                raise AdjacencyMapException("Found references that could not be resolved")

            # Step 1: Check if we find references in the cache
            for ref in unresolved:
                # Find regular reference in cache
                node = self.__cache_service.get_node(ref.id)
                if node:
                    adj_map.add_node(node)

            # Step 2: Get the remaining relative references that are not yet found directly from the Directory
            # API in one single request bundle
            missing_entries = self.get_directory_data(
                [BundleRequestParams(**ref.model_dump()) for ref in unresolved]
            )
            missing_nodes = [self.create_node(entry) for entry in missing_entries]
            adj_map.add_nodes(missing_nodes)


        # All references are now resolved.

        update_client_targets = self.__get_update_client_missing_targets(adj_map)
        update_client_entries = self.get_update_client_data(update_client_targets)

        for entry in update_client_entries:
            _, _id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
            res_id = _id.replace(f"{self.directory_id}-", "")
            node = adj_map.data[res_id]
            node.update_client_hash = (
                self.__computation_service.hash_update_client_entry(entry)
            )

        for node in adj_map.data.values():
            if node.updated:
                continue

            resource_map = self.__resource_map_service.get(
                directory_id=self.directory_id,
                resource_type=node.resource_type,
                directory_resource_id=node.resource_id,
            )
            node.status = self.__computation_service.get_update_status(
                method=node.method,
                directory_hash=node.directory_hash,
                update_client_hash=node.update_client_hash,
                resource_map=resource_map,
            )
            node.update_data = self.create_update_data(node, resource_map)

        return adj_map

    def __get_entries(self, refs: List[BundleRequestParams], fhir_api: FhirApi) -> List[BundleEntry]:
        bundle_request = self.__fhir_service.create_bundle_request(refs)
        entries, errors = fhir_api.post_bundle(bundle_request)
        if errors:
            logger.error("Errors occurred when fetching entries: %s", errors)
            raise AdjacencyMapException("Errors occurred when fetching entries")

        res = self.__fhir_service.filter_history_entries(
            self.__fhir_service.get_entries_from_bundle_of_bundles(entries)
        )
        return res

    def get_directory_data(self, refs: List[BundleRequestParams]) -> List[BundleEntry]:
        return self.__get_entries(refs, self.__directory_api)

    def get_update_client_data(
        self, refs: List[BundleRequestParams]
    ) -> List[BundleEntry]:
        return self.__get_entries(refs, self.__update_client_api)

    def create_node(self, entry: BundleEntry) -> Node:
        res_type, _id = self.__fhir_service.get_resource_type_and_id_from_entry(entry)
        method = self.__fhir_service.get_request_method_from_entry(entry)
        directory_hash = self.__computation_service.hash_directory_entry(entry)
        references = self.create_node_references(entry)

        return Node(
            resource_id=_id,
            resource_type=res_type,
            directory_hash=directory_hash,
            method=method,
            directory_entry=entry,
            references=references,
        )

    def create_node_references(self, entry: BundleEntry) -> List[NodeReference]:
        """
        Convert the references found in the bundle entry to a list of NodeReferences
        This supports both absolute and relative references.
        """
        if entry.resource is None:
            return []

        ret = []

        for ref in self.__fhir_service.get_references(entry.resource):
            reference_node = self.__fhir_service.make_reference_node(ref, self.__directory_api.base_url)
            ret.append(reference_node)

        return ret

    def create_update_data(
        self, node: Node, resource_map: ResourceMap | None
    ) -> NodeUpdateData | None:
        update_client_resource_id = f"{self.directory_id}-{node.resource_id}"
        url = f"{node.resource_type}/{update_client_resource_id}"

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
                entry.request = entry_request  # type: ignore[assignment]

                if resource_map is None:
                    raise InvalidNodeStateException(
                        f"Resource map for {node.resource_id} {node.resource_type} cannot be None and node marked as delete "
                    )

                dto = ResourceMapUpdateDto(
                    directory_id=self.directory_id,
                    resource_type=node.resource_type,
                    directory_resource_id=node.resource_id,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "new":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                if node.directory_entry is None:
                    raise InvalidNodeStateException(
                        f"Directory entry for {node.resource_id} {node.resource_type} cannot be None and node marked as `new`"
                    )
                resource = copy.deepcopy(node.directory_entry.resource)
                if resource is None:
                    raise InvalidNodeStateException(
                        f"Resource {node.resource_id} {node.resource_type} cannot be None when a node is marked `new`"
                    )
                self.__fhir_service.namespace_resource_references(
                    resource, self.directory_id
                )
                resource.id = update_client_resource_id
                entry.resource = resource
                entry.request = entry_request

                dto = ResourceMapDto(
                    directory_id=self.directory_id,
                    directory_resource_id=node.resource_id,
                    update_client_resource_id=update_client_resource_id,
                    resource_type=node.resource_type,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "update":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                if node.directory_entry is None:
                    raise InvalidNodeStateException(
                        f"Directory entry for {node.resource_id} {node.resource_type} cannot be None and node marked as `update`"
                    )

                resource = copy.deepcopy(node.directory_entry.resource)
                if resource is None:
                    raise InvalidNodeStateException(
                        f"Resource {node.resource_id} {node.resource_type} cannot be None and node marked as `update`"
                    )

                self.__fhir_service.namespace_resource_references(
                    resource, self.directory_id
                )
                resource.id = update_client_resource_id
                entry.request = entry_request
                entry.resource = resource

                if resource_map is None:
                    raise InvalidNodeStateException(
                        f"Resource map for {node.resource_id} {node.resource_type} cannot be None and marked as `update`"
                    )

                dto = ResourceMapUpdateDto(
                    directory_id=self.directory_id,
                    directory_resource_id=node.resource_id,
                    resource_type=node.resource_type,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=dto)

            case "unknown":
                raise InvalidNodeStateException(
                    f"node {node.resource_id} {node.resource_type} cannot be unknown at this stage"
                )

    def __get_update_client_missing_targets(self, adj_map: AdjacencyMap) -> List[BundleRequestParams]:
        update_client_targets = []
        for node_id, node in adj_map.data.items():
            if not self.__cache_service.key_exists(node_id):
                update_client_targets.append(
                    BundleRequestParams(
                        id=f"{self.directory_id}-{node_id}", resource_type=node.resource_type
                    )
                )
        return update_client_targets
