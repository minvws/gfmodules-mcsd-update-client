from datetime import datetime
import time
from enum import Enum
import logging
from typing import List, Any
from uuid import uuid4
from app.services.update.cache.provider import CacheProvider
from app.services.update.cache.caching_service import (
    CachingService,
)
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from yarl import URL
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


class McsdResources(Enum):
    ORGANIZATION_AFFILIATION = "OrganizationAffiliation"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTH_CARE_SERVICE = "HealthcareService"
    LOCATION = "Location"
    PRACTITIONER = "Practitioner"
    ORGANIZATION = "Organization"
    ENDPOINT = "Endpoint"


class UpdateClientService:
    def __init__(
        self,
        update_client_url: str,
        strict_validation: bool,
        timeout: int,
        backoff: float,
        request_count: int,
        resource_map_service: ResourceMapService,
        auth: Authenticator,
        cache_provider: CacheProvider,
        use_mtls: bool = True,
        client_cert_file: str | None = None,
        client_key_file: str | None = None,
        client_ca_file: str | None = None,
    ) -> None:
        self.strict_validation = strict_validation
        self.timeout = timeout
        self.backoff = backoff
        self.auth = auth
        self.request_count = request_count
        self.auth = auth
        self.__resource_map_service = resource_map_service
        self.__update_client_fhir_api = FhirApi(
            timeout, backoff, auth, update_client_url, request_count, strict_validation, use_mtls, client_cert_file, client_key_file, client_ca_file
        )
        self.__fhir_service = FhirService(strict_validation)
        self.__cache_provider = cache_provider
        self.__cache: CachingService | None = None

    def cleanup(self, directory_id: str) -> None:
        for res_type in McsdResources:
            delete_bundle = Bundle(
                id=str(uuid4()), type="transaction", entry=[], total=0
            )
            resource_map = self.__resource_map_service.find(
                directory_id=directory_id, resource_type=res_type.value
            )
            resources = list(resource_map)
            while len(resources) > 0:
                if delete_bundle.total >= 100:
                    logging.info(
                        f"Removing {delete_bundle.total} items from update client originating from stale directory {directory_id}"
                    )
                    self.__update_client_fhir_api.post_bundle(delete_bundle)
                    delete_bundle = Bundle(
                        id=str(uuid4()), type="transaction", entry=[], total=0
                    )

                res_map_item = resources.pop()
                delete_bundle.entry.append(
                    BundleEntry(
                        request=BundleEntryRequest(
                            method="DELETE",
                            url=f"{res_type.value}/{res_map_item.update_client_resource_id}?_cascade=delete",
                        )
                    )
                )
                delete_bundle.total += 1
                self.__resource_map_service.delete_one(
                    ResourceMapDeleteDto(
                        directory_id=directory_id,
                        resource_type=res_type.value,
                        directory_resource_id=res_map_item.directory_resource_id,
                    )
                )
            if delete_bundle.total > 0:  # Final flush of remaining items
                logging.info(
                    f"Removing {delete_bundle.total} items from update client originating from stale directory {directory_id}"
                )
                self.__update_client_fhir_api.post_bundle(delete_bundle)
                delete_bundle = Bundle(
                    id=str(uuid4()), type="transaction", entry=[], total=0
                )

    def update(self, directory: DirectoryDto, since: datetime | None = None) -> Any:
        self.__create_cache_run()
        if self.__cache is None:
            raise Exception(
                "Unable to continue with update, cache service is not available"
            )
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
            raise Exception(
                "Unable to continue with update, cache service is not available"
            )

        directory_fhir_api = FhirApi(
            timeout=self.timeout,
            backoff=self.backoff,
            auth=self.auth,
            request_count=self.request_count,
            strict_validation=self.strict_validation,
            url=directory.endpoint,
            use_mtls=self.__update_client_fhir_api.use_mtls,
            client_cert_file=self.__update_client_fhir_api.client_cert_file,
            client_key_file=self.__update_client_fhir_api.client_key_file,
            client_ca_file=self.__update_client_fhir_api.client_ca_file,
        )

        adjacency_map_service = AdjacencyMapService(
            directory_id=directory.id,
            fhir_service=self.__fhir_service,
            directory_api=directory_fhir_api,
            update_client_api=self.__update_client_fhir_api,
            resource_map_service=self.__resource_map_service,
            cache_service=self.__cache,
        )
        next_url: URL | None = directory_fhir_api.build_base_history_url(
            resource_type, since
        )

        while next_url is not None:
            next_url, history = directory_fhir_api.get_history_batch(next_url)
            targets = []
            for e in history:
                _, _id = self.__fhir_service.get_resource_type_and_id_from_entry(e)
                if _id is not None:
                    if self.__cache.key_exists(_id):
                        logger.info(
                            f"{_id} {resource_type} already processed.. skipping.. "
                        )
                        continue
                    targets.append(e)

            updated_nodes = self.update_page(targets, adjacency_map_service)
            for node in updated_nodes:
                if not self.__cache.key_exists(node.resource_id):
                    node.clear_for_cash()
                    self.__cache.add_node(node)

    def update_page(
        self, entries: List[BundleEntry], adjacency_map_service: AdjacencyMapService
    ) -> List[Node]:
        updated = []
        adj_map = adjacency_map_service.build_adjacency_map(entries)
        for node in adj_map.data.values():
            if node.updated is True:
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

        self.__update_client_fhir_api.post_bundle(bundle)

        for dto in dtos:
            if isinstance(dto, ResourceMapDto):
                self.__resource_map_service.add_one(dto)

            if isinstance(dto, ResourceMapUpdateDto):
                self.__resource_map_service.update_one(dto)

        for node in nodes:
            node.updated = True

        return nodes

    def __create_cache_run(self) -> None:
        cache_service = self.__cache_provider.create()
        cache_service.clear()

        self.__cache = cache_service

    def __end_cache_run(self) -> None:
        if self.__cache:
            self.__cache.clear()
        self.__cache = None
