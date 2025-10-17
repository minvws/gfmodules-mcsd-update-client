import copy
import logging
from typing import List
from app.db.entities.resource_map import ResourceMap
from app.models.fhir.types import BundleRequestParams
from app.services.update.cache.caching_service import CachingService
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest

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
from app.services.update.filter_ura import filter_ura

logger = logging.getLogger(__name__)


class AdjacencyMapException(Exception):
    """Base exception for adjacency map operations"""

    pass


class InvalidNodeStateException(AdjacencyMapException):
    pass


def ref_key(ref: NodeReference) -> tuple[str, str]:
    """
    Returns an id we can use for our attempted_keys set.
    """
    return (ref.resource_type, ref.id)


class AdjacencyMapService:
    def __init__(
        self,
        directory_id: str,
        directory_api: FhirApi,
        update_client_api: FhirApi,
        resource_map_service: ResourceMapService,
        cache_service: CachingService,
        ura_whitelist: list[str],
    ) -> None:
        self.directory_id = directory_id
        self.__directory_api = directory_api
        self.__update_client_api = update_client_api
        self.__resource_map_service = resource_map_service
        self.__cache_service = cache_service
        self.__computation_service = ComputationService(
            directory_id=directory_id,
        )
        self.__ura_whitelist = ura_whitelist

    def build_adjacency_map(self, entries: List[BundleEntry]) -> AdjacencyMap:
        nodes = [self.create_node(entry) for entry in entries]
        adj_map = AdjacencyMap(nodes)

        attempted_keys: set[tuple[str, str]] = set()
        self._resolve_all_references(adj_map, attempted_keys)
        self._update_client_hashes(adj_map)
        self._finalize_nodes(adj_map)
        return adj_map

    def _resolve_all_references(
        self,
        adj_map: AdjacencyMap,
        attempted_keys: set[tuple[str, str]]
    ) -> None:
        MAX_PASSES = 50
        passes = 0
        while True:
            passes += 1
            if passes > MAX_PASSES:
                logger.error(
                    "Aborting after %d passes to avoid infinitive loop", passes
                )
                raise AdjacencyMapException("Too many passes")

            # Check if we have resolved all references
            missing_refs = adj_map.get_missing_refs()
            if not missing_refs:
                break

            nodes_before = adj_map.node_count()
            # Step 1: Check if we find references in the cache
            self._resolve_from_cache(adj_map, missing_refs)

            # Find references that are not resolved from the cache
            still_missing = adj_map.get_missing_refs()
            if not still_missing:
                # All resolved this pass
                continue
            # Step 2: Find all the references we are still missing AND we haven't tried before yet
            self._fetch_and_add_missing(adj_map, still_missing, attempted_keys, missing_refs)

            # Check if we resolved anything this pass
            nodes_after = adj_map.node_count()
            if nodes_after == nodes_before:
                unresolved = adj_map.get_missing_refs()
                logger.error("Cannot resolve all references: %s", unresolved)
                raise AdjacencyMapException("Unresolved references found")
        # All references are now resolved.


    def _resolve_from_cache(
        self,
        adj_map: AdjacencyMap,
        missing_refs: list[NodeReference],
    ) -> None:
        for ref in missing_refs:
            node = self.__cache_service.get_node(ref.id)  # Find regular reference in cache
            if node:
                adj_map.add_node(node)

    def _fetch_and_add_missing(
        self,
        adj_map: AdjacencyMap,
        still_missing: list[NodeReference],
        attempted_keys: set[tuple[str, str]],
        missing_refs: list[NodeReference],
    ) -> None:
        to_fetch = [r for r in still_missing if ref_key(r) not in attempted_keys]
        if to_fetch:
            missing_entries = self.get_directory_data(
                [BundleRequestParams(**ref.model_dump()) for ref in missing_refs]
            )
            # Create nodes for the entries we found
            missing_nodes = [self.create_node(entry) for entry in missing_entries]
            if missing_nodes:
                adj_map.add_nodes(missing_nodes)
            attempted_keys.update(ref_key(r) for r in to_fetch)  # Update our list with elements we have fetched


    def _update_client_hashes(self, adj_map: AdjacencyMap) -> None:
        update_client_targets = self.__get_update_client_missing_targets(adj_map)
        if len(update_client_targets) > 0:
            update_client_entries = self.get_update_client_data(update_client_targets)
            for entry in update_client_entries:
                _, _id = FhirService.get_resource_type_and_id_from_entry(entry)
                res_id = _id.replace(f"{self.directory_id}-", "")
                node = adj_map.data[res_id]
                node.update_client_hash = (
                    self.__computation_service.hash_update_client_entry(entry)
                )

    def _finalize_nodes(self, adj_map: AdjacencyMap) -> None:
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



    def __filter_entries(self, entries: Bundle) -> List[BundleEntry]:
        return FhirService.filter_history_entries(
            FhirService.get_entries_from_bundle_of_bundles(entries)
        )

    def get_directory_data(self, refs: List[BundleRequestParams]) -> List[BundleEntry]:
        bundle_request = FhirService.create_bundle_request(refs)
        entries, errors = self.__directory_api.post_bundle(bundle_request)
        if errors:
            logger.error("Errors occurred when fetching entries: %s", errors)
            raise AdjacencyMapException("Errors occurred when fetching entries")
        return self.__filter_entries(entries)

    def get_update_client_data(
        self, refs: List[BundleRequestParams]
    ) -> List[BundleEntry]:
        bundle_request = FhirService.create_bundle_request(refs)
        entries, errors = self.__update_client_api.post_bundle(bundle_request)
        if errors and any(
            err.status != 404 for err in errors
        ):  # Requested resource not found is OK but other errors are not
            logger.error(
                "Non-404 error occurred: %s",
                [err for err in errors if err.status != 404],
            )
            raise AdjacencyMapException("Errors occurred when fetching entries")
        return self.__filter_entries(entries)

    def create_node(self, entry: BundleEntry) -> Node:
        res_type, _id = FhirService.get_resource_type_and_id_from_entry(entry)
        method = FhirService.get_request_method_from_entry(entry)
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

        for ref in FhirService.get_references(entry.resource):
            reference_node = FhirService.make_reference_node(
                ref, self.__directory_api.base_url
            )
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

                # Filter URAs only for Organizations
                if getattr(resource, "resource_type", None) == "Organization":
                    resource = filter_ura(resource, self.__ura_whitelist)

                FhirService.namespace_resource_references(resource, self.directory_id)
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

                FhirService.namespace_resource_references(resource, self.directory_id)
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

    def __get_update_client_missing_targets(
        self, adj_map: AdjacencyMap
    ) -> List[BundleRequestParams]:
        update_client_targets = []
        for node_id, node in adj_map.data.items():
            if not self.__cache_service.key_exists(node_id):
                update_client_targets.append(
                    BundleRequestParams(
                        id=f"{self.directory_id}-{node_id}",
                        resource_type=node.resource_type,
                    )
                )

        return update_client_targets
