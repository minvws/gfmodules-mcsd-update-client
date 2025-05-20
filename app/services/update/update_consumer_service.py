from datetime import datetime
import time
import copy
from enum import Enum
import logging
from typing import List, Any
from uuid import uuid4
from app.services.update.cache.caching_service import InMemoryCachingService
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from yarl import URL
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.models.adjacency.node import (
    AlreadyPushedNode,
)
from app.models.supplier.dto import SupplierDto
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


class UpdateConsumerService:
    def __init__(
        self,
        consumer_url: str,
        strict_validation: bool,
        timeout: int,
        backoff: float,
        request_count: int,
        resource_map_service: ResourceMapService,
        auth: Authenticator,
    ) -> None:
        self.strict_validation = strict_validation
        self.timeout = timeout
        self.backoff = backoff
        self.auth = auth
        self.request_count = request_count
        self.auth = auth
        self.__resource_map_service = resource_map_service
        self.__consumer_fhir_api = FhirApi(
            timeout, backoff, auth, consumer_url, request_count, strict_validation
        )
        self.__fhir_service = FhirService(strict_validation)
        self.__cache = InMemoryCachingService()

    def cleanup(self, supplier_id: str) -> None:
        for res_type in McsdResources:
            delete_bundle = Bundle(
                id=str(uuid4()), type="transaction", entry=[], total=0
            )
            resource_map = self.__resource_map_service.find(
                supplier_id=supplier_id, resource_type=res_type.value
            )
            for res_map_item in resource_map:
                delete_bundle.entry.append(
                    BundleEntry(
                        request=BundleEntryRequest(
                            method="DELETE",
                            url=f"{res_type.value}/{res_map_item.consumer_resource_id}",
                        )
                    )
                )
                delete_bundle.total += 1
            logging.info(
                f"Removing {delete_bundle.total} items from consumer originating from stale supplier {supplier_id}"
            )
            self.__consumer_fhir_api.post_bundle(delete_bundle)

    def update(self, supplier: SupplierDto, since: datetime | None = None) -> Any:
        start_time = time.time()
        if not self.__cache.is_empty():
            self.__cache.clear()

        for res in McsdResources:
            self.update_resource(supplier, res.value, since)
        results = copy.deepcopy([id for id in self.__cache.data.keys()])
        self.__cache.clear()
        end_time = time.time()

        return {"log": f"updated {len(results)}", "time": end_time - start_time}

    def update_resource(
        self, supplier: SupplierDto, resource_type: str, since: datetime | None = None
    ) -> None:
        supplier_fhir_api = FhirApi(
            timeout=self.timeout,
            backoff=self.backoff,
            auth=self.auth,
            request_count=self.request_count,
            strict_validation=self.strict_validation,
            url=supplier.endpoint,
        )
        adjacency_map_service = AdjacencyMapService(
            supplier_id=supplier.id,
            fhir_service=self.__fhir_service,
            supplier_api=supplier_fhir_api,
            consumer_api=self.__consumer_fhir_api,
            resource_map_service=self.__resource_map_service,
            cache_service=self.__cache,
        )
        next_url: URL | None = supplier_fhir_api.build_base_history_url(
            resource_type, since
        )

        while next_url is not None:
            next_url, history = supplier_fhir_api.get_history_batch(next_url)
            targets = []
            for e in history:
                _, id = self.__fhir_service.get_resource_type_and_id_from_entry(e)
                if id is not None:
                    if self.__cache.key_exists(id):
                        logger.info(
                            f"{id} {resource_type} already processed.. skipping.. "
                        )
                        continue
                    targets.append(e)

            updated_nodes = self.update_page(
                targets,
                adjacency_map_service,
                supplier.id
            )
            for node in updated_nodes:
                if not self.__cache.key_exists(node.resource_id):
                    self.__cache.add_node(node)

                # if node.resource_id not in self.__cache:
                #     self.__cache.append(node.resource_id)

    def update_page(
        self, entries: List[BundleEntry], adjacency_map_service: AdjacencyMapService, supplier_id: str
    ) -> List[Node]:
        updated = []
        adj_map = adjacency_map_service.build_adjacency_map(entries, self.__cache)
        for node in adj_map.data.values():
            if node.updated is True:
                logger.info(
                    f"Item {node.resource_id} {node.resource_type} has been processed earlier, skipping..."
                )
                continue

            group = adj_map.get_group(node)
            results = self.update_with_bundle(group, supplier_id)
            updated.extend(results)

        return updated

    def update_with_bundle(self, nodes: List[Node], supplier_id: str) -> List[Node]:
        bundle = Bundle.model_construct(id=uuid4(), type="transaction")
        bundle.entry = []
        dtos = []

        for node in nodes:
            if node.update_status == "equal":
                logger.info(
                    f"{node.resource_id} {node.resource_type} has not changed, ignoring..."
                )

            if node.update_status == "ignore":
                logger.info(
                    f"{node.resource_id} {node.resource_type} is not needed, ignoring..."
                )

            ## TODO: Create updateData from Node
            if node.update_data is not None:
                if node.update_data.bundle_entry is not None:
                    bundle.entry.append(node.update_data.bundle_entry)

                if node.update_data.resource_map_dto is not None:
                    if node.status == "delete" or node.status == "update":
                        dtos.append(ResourceMapUpdateDto(
                            supplier_id=supplier_id,
                            supplier_resource_id=node.resource_id,
                            resource_type=node.resource_type,
                            history_size=0,
                        ))
                    elif node.status == "new":
                        dtos.append(ResourceMapDto(
                            supplier_id=supplier_id,
                            supplier_resource_id=node.resource_id,
                            consumer_resource_id=f"{supplier_id}-{node.resource_id}",
                            resource_type=node.resource_type,
                            history_size=0,
                        ))


        self.__consumer_fhir_api.post_bundle(bundle)

        for dto in dtos:
            if isinstance(dto, ResourceMapDto):
                self.__resource_map_service.add_one(dto)

            if isinstance(dto, ResourceMapUpdateDto):
                self.__resource_map_service.update_one(dto)

        for node in nodes:
            node.updated = True

        return nodes
