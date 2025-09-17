from datetime import datetime
import time
import logging
from typing import Dict, List, Any
from uuid import uuid4
from app.db.entities.resource_map import ResourceMap
from app.models.fhir.types import McsdResources
from app.services.update.cache.provider import CacheProvider
from app.services.update.cache.caching_service import (
    CachingService,
)
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from app.models.resource_map.dto import (
    ResourceMapDeleteDto,
    ResourceMapDto,
    ResourceMapUpdateDto,
)
from app.models.adjacency.node import (
    Node,
)
from app.models.directory.dto import DirectoryDto
from app.services.update.adjacency_map_service import (
    AdjacencyMapService,
)
from app.services.entity.resource_map_service import (
    ResourceMapService,
)
from app.services.api.authenticators.authenticator import Authenticator
from app.services.fhir.fhir_service import FhirService
from app.services.api.fhir_api import FhirApi

logger = logging.getLogger(__name__)


class UpdateClientException(Exception):
    pass


class CacheServiceUnavailableException(UpdateClientException):
    def __init__(self, *args: object) -> None:
        super().__init__(
            "Unable to continue with update, cache service is not available", *args
        )


class UpdateClientService:
    def __init__(
        self,
        update_client_url: str,
        fill_required_fields: bool,
        timeout: int,
        backoff: float,
        retries: int,
        request_count: int,
        resource_map_service: ResourceMapService,
        auth: Authenticator,
        cache_provider: CacheProvider,
        mtls_cert: str | None = None,
        mtls_key: str | None = None,
        mtls_ca: str | None = None,
    ) -> None:
        self.fill_required_fields = fill_required_fields
        self.timeout = timeout
        self.backoff = backoff
        self.auth = auth
        self.request_count = request_count
        self.__resource_map_service = resource_map_service
        self.mtls_cert = mtls_cert
        self.mtls_key = mtls_key
        self.mtls_ca = mtls_ca
        self.__update_client_fhir_api = FhirApi(
            base_url=update_client_url,
            auth=auth,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            mtls_cert=self.mtls_cert,
            mtls_key=self.mtls_key,
            mtls_ca=self.mtls_ca,
            request_count=request_count,
            fill_required_fields=fill_required_fields,
        )
        self.__cache_provider = cache_provider
        self.__cache: CachingService | None = None

    def cleanup(self, directory_id: str) -> None:
        for res_type in McsdResources:
            self.__cleanup_resource_type(directory_id, res_type)

    def __cleanup_resource_type(
        self, directory_id: str, res_type: McsdResources
    ) -> None:
        resource_map = self.__resource_map_service.find(
            directory_id=directory_id, resource_type=res_type.value
        )
        resources = list(resource_map)

        if not resources:
            return

        delete_bundle = self.__create_empty_delete_bundle()

        for res_map_item in resources:
            delete_bundle = self.__process_resource_for_deletion(
                delete_bundle, res_map_item, res_type, directory_id
            )

        self.__flush_delete_bundle(delete_bundle, directory_id)

    def __create_empty_delete_bundle(self) -> Bundle:
        return Bundle(id=str(uuid4()), type="transaction", entry=[], total=0)

    def __process_resource_for_deletion(
        self,
        delete_bundle: Bundle,
        res_map_item: ResourceMap,
        res_type: McsdResources,
        directory_id: str,
    ) -> Bundle:
        if delete_bundle.total is not None and delete_bundle.total >= 100:
            self.__flush_delete_bundle(delete_bundle, directory_id)
            delete_bundle = self.__create_empty_delete_bundle()

        self.__add_delete_entry_to_bundle(delete_bundle, res_map_item, res_type)
        self.__delete_from_resource_map(res_map_item, res_type, directory_id)

        return delete_bundle

    def __add_delete_entry_to_bundle(
        self, bundle: Bundle, res_map_item: ResourceMap, res_type: McsdResources
    ) -> None:
        if bundle.entry is None:
            bundle.entry = []

        bundle.entry.append(
            BundleEntry(
                request=BundleEntryRequest(
                    method="DELETE",
                    url=f"{res_type.value}/{res_map_item.update_client_resource_id}?_cascade=delete",
                )
            )
        )

        if bundle.total is None:
            bundle.total = 0
        bundle.total += 1

    def __delete_from_resource_map(
        self, res_map_item: ResourceMap, res_type: McsdResources, directory_id: str
    ) -> None:
        self.__resource_map_service.delete_one(
            ResourceMapDeleteDto(
                directory_id=directory_id,
                resource_type=res_type.value,
                directory_resource_id=res_map_item.directory_resource_id,
            )
        )

    def __flush_delete_bundle(self, bundle: Bundle, directory_id: str) -> None:
        if bundle.total is not None and bundle.total > 0:
            logging.info(
                f"Removing {bundle.total} items from update client originating from stale directory {directory_id}"
            )
            _entries, errors = self.__update_client_fhir_api.post_bundle(bundle)
            if len(errors) > 0:
                logging.error(
                    f"Errors occurred when flushing delete bundle for stale directory {directory_id}: {errors}"
                )
                raise UpdateClientException(
                    f"Errors occurred when flushing delete bundle for stale directory {directory_id}: {errors}"
                )

    def update(self, directory: DirectoryDto, since: datetime | None = None) -> Any:
        self.__create_cache_run()
        if self.__cache is None:
            raise CacheServiceUnavailableException()
        start_time = time.time()

        for res in McsdResources:
            self.update_resource(directory, res.value, since)
        results = self.__cache.keys()
        end_time = time.time()
        self.__end_cache_run()

        return {"log": f"updated {len(results)}", "time": end_time - start_time}

    def update_resource(
        self, directory: DirectoryDto, resource_type: str, since: datetime | None = None
    ) -> None:
        if self.__cache is None:
            raise CacheServiceUnavailableException()

        directory_fhir_api = FhirApi(
            timeout=self.timeout,
            backoff=self.backoff,
            auth=self.auth,
            request_count=self.request_count,
            fill_required_fields=self.fill_required_fields,
            base_url=directory.endpoint,
            retries=10,
            mtls_cert=self.mtls_cert,
            mtls_key=self.mtls_key,
            mtls_ca=self.mtls_ca,
        )

        adjacency_map_service = AdjacencyMapService(
            directory_id=directory.id,
            directory_api=directory_fhir_api,
            update_client_api=self.__update_client_fhir_api,
            resource_map_service=self.__resource_map_service,
            cache_service=self.__cache,
        )

        next_params: Dict[str, Any] | None = directory_fhir_api.build_history_params(
            since=since
        )
        while next_params is not None:
            next_params, history = directory_fhir_api.get_history_batch(
                resource_type, next_params
            )
            targets = []
            for e in history:
                _, _id = FhirService.get_resource_type_and_id_from_entry(e)
                if _id is not None:
                    if self.__cache.key_exists(_id):
                        logger.info(
                            f"{_id} {resource_type} already processed.. skipping.. "
                        )
                        continue
                    targets.append(e)

            if (
                not targets
            ):  # if no targets are found then there is no need to carry on with the flow
                continue

            nodes = self.update_page(targets, adjacency_map_service)
            self.__clear_and_add_nodes(nodes)

    def __clear_and_add_nodes(self, updated_nodes: List[Node]) -> None:
        if self.__cache is None:
            raise CacheServiceUnavailableException()
        for node in updated_nodes:
            if not self.__cache.key_exists(node.resource_id):
                node.clear_for_cache()
                self.__cache.add_node(node)

    def update_page(
        self, entries: List[BundleEntry], adjacency_map_service: AdjacencyMapService
    ) -> List[Node]:
        updated = []
        adj_map = adjacency_map_service.build_adjacency_map(entries)
        for node in adj_map.data.values():
            if node.updated:
                logger.info(
                    f"Item {node.resource_id} {node.resource_type} has been processed earlier, skipping..."
                )
                continue

            group = adj_map.get_group(node)
            results = self.update_with_bundle(group)
            updated.extend(results)

        return updated

    def update_with_bundle(self, nodes: List[Node]) -> List[Node]:
        bundle = Bundle.model_construct(id=uuid4(), type="transaction")
        bundle.entry = []
        dtos = []

        for node in nodes:
            if node.status == "equal":
                logger.info(
                    f"{node.resource_id} {node.resource_type} has not changed, ignoring..."
                )

            if node.status == "ignore":
                logger.info(
                    f"{node.resource_id} {node.resource_type} is not needed, ignoring..."
                )

            if node.update_data is not None:
                if node.update_data.bundle_entry is not None:
                    bundle.entry.append(node.update_data.bundle_entry)

                if node.update_data.resource_map_dto is not None:
                    dtos.append(node.update_data.resource_map_dto)

        _entries, errors = self.__update_client_fhir_api.post_bundle(bundle)
        if len(errors) > 0:
            logger.error(f"Errors occurred when updating bundle: {errors}")
            raise UpdateClientException(
                f"Errors occurred when updating bundle: {errors}"
            )

        self.__handle_dtos(dtos)

        for node in nodes:
            node.updated = True

        return nodes

    def __handle_dtos(self, dtos: List[ResourceMapDto | ResourceMapUpdateDto]) -> None:
        for dto in dtos:
            if isinstance(dto, ResourceMapDto):
                self.__resource_map_service.add_one(dto)

            if isinstance(dto, ResourceMapUpdateDto):
                self.__resource_map_service.update_one(dto)

    def __create_cache_run(self) -> None:
        cache_service = self.__cache_provider.create()
        cache_service.clear()

        self.__cache = cache_service

    def __end_cache_run(self) -> None:
        if self.__cache:
            self.__cache.clear()
        self.__cache = None
