from datetime import datetime
import time
import copy
from enum import Enum
import logging
from typing import List, Any
from uuid import uuid4
from fhir.resources.R4B.bundle import Bundle, BundleEntry
from yarl import URL
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.models.adjacency.node import (
    Node,
)
from app.models.supplier.dto import SupplierDto
from app.services_new.adjacency_map_service import (
    AdjacencyMapService,
)
from app.services.entity_services.resource_map_service import (
    ResourceMapService,
)
from app.services_new.api.authenticators import Authenticator, NullAuthenticator
from app.services_new.api.suppliers_api import SuppliersApi
from app.services.fhir.fhir_service import FhirService
from app.services_new.api.fhir_api import FhirApi

logger = logging.getLogger(__name__)


class McsdResources(Enum):
    ORGANIZATION_AFFILIATION = "OrganizationAffiliation"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTH_CARE_SERVICE = "HealthcareService"
    LOCATION = "Location"
    PRACTITIONER = "Practitioner"
    ORGANIZATION = "Organization"
    ENDPOINT = "Endpoint"


class UpdateConsumer:
    def __init__(
        self,
        consumer_url: str,
        strict_validation: bool,
        timeout: int,
        backoff: float,
        request_count: int,
        resource_map_service: ResourceMapService,
        suppliers_api: SuppliersApi,
        auth: Authenticator = NullAuthenticator(),
    ) -> None:
        self.strict_validation = strict_validation
        self.timeout = timeout
        self.backoff = backoff
        self.auth = auth
        self.request_count = request_count
        self.__resource_map_service = resource_map_service
        self.__suppliers_register_api = suppliers_api
        self.__consumer_fhir_api = FhirApi(
            timeout, backoff, auth, consumer_url, request_count, strict_validation
        )
        self.__fhir_service = FhirService(strict_validation)
        self.__cache: List[str] = []

    def update(self, supplier_id: str, since: datetime | None = None) -> Any:
        start_time = time.time()
        if len(self.__cache) > 0:
            self.__cache = []

        supplier = self.__suppliers_register_api.get_one(supplier_id)
        if supplier is None:
            raise Exception(
                f"Supplier with id {supplier_id} not found in supplier provider"
            )
        for res in McsdResources:
            self.update_resource(supplier, res.value, since)
        results = copy.deepcopy(self.__cache)
        self.__cache = []
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
                    if id in self.__cache:
                        logger.info(
                            f"{id} {resource_type} already processed.. skipping.. "
                        )
                        continue
                    targets.append(e)

            updated_nodes = self.update_page(targets, adjacency_map_service)
            for node in updated_nodes:
                if node.resource_id not in self.__cache:
                    self.__cache.append(node.resource_id)

    def update_page(
        self, entries: List[BundleEntry], adjacency_map_service: AdjacencyMapService
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
            results = self.update_with_bundle(group)
            updated.extend(results)

        return updated

    def update_with_bundle(self, nodes: List[Node]) -> List[Node]:
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

            if node.update_data is not None:
                if node.update_data.bundle_entry is not None:
                    bundle.entry.append(node.update_data.bundle_entry)

                if node.update_data.resource_map_dto is not None:
                    dtos.append(node.update_data.resource_map_dto)

        self.__consumer_fhir_api.post_bundle(bundle)

        for dto in dtos:
            if isinstance(dto, ResourceMapDto):
                self.__resource_map_service.add_one(dto)

            if isinstance(dto, ResourceMapUpdateDto):
                self.__resource_map_service.update_one(dto)

        for node in nodes:
            node.updated = True

        return nodes
