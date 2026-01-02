from __future__ import annotations

import logging
import heapq
from dataclasses import replace
from datetime import datetime
import threading
import time
from typing import Dict, List
from uuid import uuid4

from fastapi import HTTPException
from fhir.resources.R4B.bundle import Bundle, BundleEntry
from fhir.resources.R4B.bundle import BundleEntryRequest
from fhir.resources.R4B.resource import Resource

from app.models.directory.dto import DirectoryDto
from app.models.fhir.types import BundleError, McsdResources

from app.models.resource_map.dto import (
    ResourceMapDeleteDto,
    ResourceMapDto,
    ResourceMapUpdateDto,
)
from app.services.api.fhir_api import FhirApi, FhirApiConfig
from app.services.entity.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
from app.services.update.adjacency_map_service import AdjacencyMapService
from app.services.update.cache.caching_service import CachingService
from app.services.update.cache.provider import CacheProvider

logger = logging.getLogger(__name__)


class UpdateClientException(Exception):
    pass


class UpdateClientService:
    def __init__(
        self,
        api_config: FhirApiConfig,
        resource_map_service: ResourceMapService,
        cache_provider: CacheProvider,
        update_client_api_config: FhirApiConfig | None = None,
        validate_capability_statement: bool = False,
        allow_missing_resources: bool = False,
    ) -> None:
        self.api_config = api_config
        if update_client_api_config is None:
            update_client_api_config = api_config
        self.__update_client_fhir_api = FhirApi(update_client_api_config)
        self.__resource_map_service = resource_map_service
        self.__cache_provider = cache_provider
        self.__validate_capability_statement = validate_capability_statement
        self.__allow_missing_resources = allow_missing_resources

        # One lock per directory id to prevent concurrent updates on the same directory.
        self.mutexes: Dict[str, threading.Lock] = {}
        self.directory_lock = threading.Lock()

    def cleanup_directory(self, directory: DirectoryDto, directory_resource_type: str) -> None:
        resource_maps = self.__get_related_resource_maps(directory.id, directory_resource_type)

        if not resource_maps:
            return

        delete_bundle = self.__create_empty_delete_bundle()
        delete_bundle_url = f"{directory_resource_type}/_history?_count=100"
        response = self.__update_client_fhir_api.do_request("GET", delete_bundle_url)
        resource_history = Bundle.model_validate_json(response.text)
        resource_history_entry = resource_history.entry if resource_history.entry else []

        # Determine which resources exist in update client but no longer in directory.
        update_client_ids = set()
        for entry in resource_history_entry:
            _, _id = FhirService.get_resource_type_and_id_from_entry(entry)
            if _id is not None:
                update_client_ids.add(_id)

        ids_to_be_deleted = list(update_client_ids - set([m.update_client_resource_id for m in resource_maps]))

        errors: list[BundleError] = []
        for i, update_client_id in enumerate(ids_to_be_deleted):
            delete_bundle.entry.append(
                BundleEntry.model_construct(
                    request=BundleEntryRequest.model_construct(
                        method="DELETE",
                        url=f"{directory_resource_type}/{update_client_id}",
                    )
                )
            )

            # Send bundle in chunks of 100 to reduce server load.
            if (i + 1) % 100 == 0:
                errors.extend(self.__update_delete_bundle(delete_bundle))
                delete_bundle = self.__create_empty_delete_bundle()

        if delete_bundle.entry:
            errors.extend(self.__update_delete_bundle(delete_bundle))

        if errors:
            raise UpdateClientException(f"Errors occurred when updating delete bundle: {errors}")

        # Remove corresponding mappings.
        for resource_map in resource_maps:
            self.__resource_map_service.delete_one(
                ResourceMapDeleteDto(
                    directory_id=resource_map.directory_id,
                    resource_type=resource_map.resource_type,
                    directory_resource_id=resource_map.directory_resource_id,
                )
            )

    def cleanup(self, directory_id: str) -> None:
        """Remove all update-client resources and DB mappings for a directory."""
        for res in McsdResources:
            self.__cleanup_resource_type(directory_id, res)
        return

        resource_maps = self.__resource_map_service.find(directory_id=directory_id)
        if not resource_maps:
            return

        maps_by_type: Dict[str, list[str]] = {}
        for m in resource_maps:
            maps_by_type.setdefault(m.resource_type, []).append(m.update_client_resource_id)

        for resource_type, update_client_ids in maps_by_type.items():
            delete_bundle = self.__create_empty_delete_bundle()
            errors: list[BundleError] = []

            for i, update_client_id in enumerate(update_client_ids):
                delete_bundle.entry.append(
                    BundleEntry.model_construct(
                        request=BundleEntryRequest.model_construct(
                            method="DELETE",
                            url=f"{resource_type}/{update_client_id}",
                        )
                    )
                )

                if (i + 1) % 100 == 0:
                    errors.extend(self.__update_delete_bundle(delete_bundle))
                    delete_bundle = self.__create_empty_delete_bundle()

            if delete_bundle.entry:
                errors.extend(self.__update_delete_bundle(delete_bundle))

            if errors:
                raise UpdateClientException(
                    f"Errors occurred when deleting resources from update client: {errors}"
                )

        # Delete all DB mappings at the end (in one go).
        for m in resource_maps:
            self.__resource_map_service.delete_one(
                ResourceMapDeleteDto(
                    directory_id=m.directory_id,
                    resource_type=m.resource_type,
                    directory_resource_id=m.directory_resource_id,
                )
            )

    def __get_related_resource_maps(
        self, directory_id: str, directory_resource_type: str
    ) -> list:
        return list(self.__resource_map_service.find(directory_id=directory_id, resource_type=directory_resource_type))

    def __cleanup_resource_type(self, directory_id: str, resource: McsdResources) -> None:
        """Compatibility helper for tests: delete all resources of a type for a directory."""
        resource_maps = self.__resource_map_service.find(
            directory_id=directory_id,
            resource_type=resource.value,
        )

        if not resource_maps:
            return

        delete_bundle = self.__create_empty_delete_bundle()
        errors: list[BundleError] = []

        for i, m in enumerate(resource_maps):
            delete_bundle.entry.append(
                BundleEntry.model_construct(
                    request=BundleEntryRequest.model_construct(
                        method="DELETE",
                        url=f"{resource.value}/{m.update_client_resource_id}?_cascade=delete",
                    )
                )
            )
            delete_bundle.total = len(delete_bundle.entry)  # type: ignore[assignment]

            if (i + 1) % 100 == 0:
                errors.extend(self.__update_delete_bundle(delete_bundle))
                delete_bundle = self.__create_empty_delete_bundle()

        if delete_bundle.entry:
            errors.extend(self.__update_delete_bundle(delete_bundle))

        if errors:
            raise UpdateClientException(
                f"Errors occurred when deleting resources from update client: {errors}"
            )

        for m in resource_maps:
            self.__resource_map_service.delete_one(
                ResourceMapDeleteDto(
                    directory_id=m.directory_id,
                    resource_type=m.resource_type,
                    directory_resource_id=m.directory_resource_id,
                )
            )

    def __flush_delete_bundle(self, bundle: Bundle, directory_id: str) -> None:
        """Compatibility helper for tests."""
        if not bundle.total:
            return
        errors = self.__update_delete_bundle(bundle)
        if errors:
            raise UpdateClientException(f"Errors occurred when flushing delete bundle for directory {directory_id}: {errors}")

    def __process_resource_for_deletion(self, resource: Resource) -> None:
        with self.directory_lock:
            self.cleanup(resource.id)  # type: ignore[arg-type]

    def __create_cache_run(self) -> CachingService:
        cache_service = self.__cache_provider.create()
        # Ensure cache is clean for this run. External cache deletes only its own prefix.
        cache_service.clear()
        return cache_service

    def update(
        self,
        directory: DirectoryDto,
        since: datetime | None = None,
        ura_whitelist: dict[str, list[str]] | None = None,
    ) -> dict:
        """Update all mCSD resource types for a single directory."""
        if ura_whitelist is None:
            ura_whitelist = {}

        lock = self.mutexes.get(directory.id)
        if lock is None:
            lock = threading.Lock()
            self.mutexes[directory.id] = lock

        if not lock.acquire(blocking=False):
            return {
                "directory_id": directory.id,
                "status": "skipped",
                "reason": "already_running",
                "log": f"cannot perform update, {directory.id} currently in the background",
            }

        start_time = time.time()
        cache_service: CachingService | None = None

        try:
            cache_service = self.__create_cache_run()
            processed = 0
            for res in McsdResources:
                processed += self.update_resource(
                    directory, res.value, cache_service, since, ura_whitelist
                )

            end_time = time.time()
            return {
                "directory_id": directory.id,
                "status": "success",
                "updated": processed,
                "log": f"updated {processed}",
                "time": end_time - start_time,
            }
        finally:
            if cache_service is not None:
                try:
                    cache_service.clear()
                except Exception:
                    logger.exception("Failed to clear cache for run. directory_id=%s", directory.id)
            lock.release()

    def update_resource(
        self,
        directory: DirectoryDto,
        resource_type: str,
        cache_service: CachingService,
        since: datetime | None,
        ura_whitelist: dict[str, list[str]],
    ) -> int:
        """Update a single resource type for one directory."""
        config = replace(self.api_config, base_url=directory.endpoint_address)
        directory_fhir_api = FhirApi(config)

        if self.__validate_capability_statement and not directory_fhir_api.validate_capability_statement():
            raise UpdateClientException(
                f"Required interactions not supported by server: {directory.endpoint_address}"
            )

        next_params = directory_fhir_api.build_history_params(since)
        processed_count = 0

        while next_params is not None:
            try:
                next_params, history = directory_fhir_api.get_history_batch(resource_type, next_params)
            except HTTPException as e:
                # Best-effort mode: some servers will 400 on unsupported resource types
                if self.__allow_missing_resources and e.status_code in (400, 404):
                    # Some FHIR servers respond 404 (not 400) for unsupported resource types.
                    # To avoid masking a misconfigured base URL, only treat 404 as "missing resource"
                    # if the endpoint still looks like a FHIR endpoint (i.e., /metadata is reachable).
                    logger.warning(
                        "Directory %s does not support resource type %s (status=%s); skipping this resource due to allow_missing_resources=True",
                        getattr(directory, "id", "<unknown>"),
                        getattr(resource_type, "value", str(resource_type)),
                        e.status_code,
                    )
                    return 0
                raise

            # Build cache ids and check existence in bulk (fast for Redis).
            entry_by_cache_id: dict[str, BundleEntry] = {}
            cache_ids: list[str] = []
            for e in history:
                res_type, _id = FhirService.get_resource_type_and_id_from_entry(e)
                if _id is None:
                    continue
                cache_id = f"{res_type}/{_id}"
                entry_by_cache_id[cache_id] = e
                cache_ids.append(cache_id)

            # De-duplicate while preserving order.
            cache_ids_unique = list(dict.fromkeys(cache_ids))
            existing = cache_service.bulk_exists(cache_ids_unique)
            if existing:
                logger.debug("Skipping %s already processed entries", len(existing))

            targets: list[BundleEntry] = [
                entry_by_cache_id[cid] for cid in cache_ids_unique if cid not in existing
            ]

            if not targets:
                continue

            updated_nodes = self.update_page(directory.id, resource_type, targets, cache_service, directory_fhir_api, ura_whitelist)
            processed_count += self.__clear_and_add_nodes(updated_nodes, cache_service)

        return processed_count

    def update_page(
        self,
        directory_id: str,
        resource_type: str,
        targets: List[BundleEntry],
        cache_service: CachingService,
        directory_fhir_api: FhirApi,
        ura_whitelist: dict[str, list[str]],
    ):
        """Process a page of history entries for one resource type."""
        uras_allowed = ura_whitelist.get(directory_id) or []
        adjacency_map_service = AdjacencyMapService(
            directory_id=directory_id,
            directory_api=directory_fhir_api,
            update_client_api=self.__update_client_fhir_api,
            resource_map_service=self.__resource_map_service,
            cache_service=cache_service,
            uras_allowed=uras_allowed,
        )

        logger.info("Building adjacency map for %s", resource_type)
        adj_map = adjacency_map_service.build_adjacency_map(targets)

        updated: List = []
        for node in adj_map.data.values():
            if node.updated:
                continue

            group = adj_map.get_group(node)
            results = self.update_with_bundle(group)
            updated.extend(results)

        return updated

    def update_with_bundle(self, nodes: List) -> List:
        """Push an adjacency group to the update client as a FHIR transaction bundle."""
        bundle = Bundle.model_construct(id=str(uuid4()), type="transaction")
        bundle.entry = []
        dtos: list[ResourceMapDto | ResourceMapUpdateDto | ResourceMapDeleteDto] = []

        nodes_ordered = self.__order_nodes_for_transaction(nodes)
        for node in nodes_ordered:
            if node.update_data is None:
                continue

            if node.update_data.bundle_entry is not None:
                bundle.entry.append(node.update_data.bundle_entry)

            if node.update_data.resource_map_dto is not None:
                dtos.append(node.update_data.resource_map_dto)

        # If there is nothing to write, skip the expensive network call.
        if not bundle.entry:
            for node in nodes:
                node.updated = True
            return nodes

        _, errors = self.__update_client_fhir_api.post_bundle(bundle)
        if errors:
            logger.error("Errors occurred when updating bundle: %s", errors)
            raise UpdateClientException(f"Errors occurred when updating bundle: {errors}")

        self.__handle_dtos(dtos)

        for node in nodes:
            node.updated = True

        return nodes

    @staticmethod
    def __order_nodes_for_transaction(nodes: List) -> List:
        """Order nodes so referenced resources are created/updated before dependents."""
        if not nodes:
            return nodes
        try:
            keys = [n.cache_key() for n in nodes]
            by_key = dict(zip(keys, nodes))
            pos = {k: i for i, k in enumerate(keys)}
            edges: dict[str, set[str]] = {k: set() for k in keys}
            indeg: dict[str, int] = {k: 0 for k in keys}

            for n in nodes:
                n_key = n.cache_key()
                for ref in getattr(n, "references", []) or []:
                    ref_key = ref.cache_key()
                    if ref_key not in by_key:
                        continue
                    if n_key in edges[ref_key]:
                        continue
                    edges[ref_key].add(n_key)
                    indeg[n_key] += 1

            heap: list[tuple[int, str]] = []
            for k in keys:
                if indeg[k] == 0:
                    heapq.heappush(heap, (pos[k], k))

            ordered: list[str] = []
            while heap:
                _, k = heapq.heappop(heap)
                ordered.append(k)
                for dep in edges[k]:
                    indeg[dep] -= 1
                    if indeg[dep] == 0:
                        heapq.heappush(heap, (pos[dep], dep))

            if len(ordered) != len(nodes):
                return nodes
            return [by_key[k] for k in ordered]
        except Exception:
            logger.exception("Failed to order nodes for transaction bundle")
            return nodes

    def __handle_dtos(
        self, dtos: List[ResourceMapDto | ResourceMapUpdateDto | ResourceMapDeleteDto]
    ) -> None:
        self.__resource_map_service.apply_dtos(dtos)

    def __clear_and_add_nodes(self, updated_nodes: List, cache_service: CachingService) -> int:
        """Clear heavy fields and store nodes in cache. Returns number of new cache entries."""
        added = 0
        for node in updated_nodes:
            node.clear_for_cache()
            if not cache_service.key_exists(node.cache_key()):
                cache_service.add_node(node)
                added += 1

        return added

    def __create_empty_delete_bundle(self) -> Bundle:
        bundle = Bundle.model_construct(id=str(uuid4()), type="transaction")
        bundle.entry = []
        return bundle

    def __update_delete_bundle(self, delete_bundle: Bundle) -> list[BundleError]:
        _, errors = self.__update_client_fhir_api.post_bundle(delete_bundle)
        return errors
