import copy
import logging
from typing import List
from fhir.resources.R4B.bundle import BundleEntry, Bundle, BundleEntryRequest
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.models.adjacency.dto import AdjacencyEntry
from app.services_new.adjacency_map_service import AdjacencyMapService
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services_new.api.authenticators import Authenticator, NullAuthenticator
from app.services_new.api.suppliers_api import SuppliersApi
from app.services.fhir.fhir_service import FhirService
from app.services_new.api.fhir_api import FhirApi

logger = logging.getLogger(__name__)


class UpdateConsumer:
    def __init__(
        self,
        suppliers_register_url: str,
        consumer_url: str,
        strict_validation: bool,
        timeout: int,
        backoff: float,
        request_count: int,
        resource_map_service: ResourceMapService,
        auth: Authenticator = NullAuthenticator(),
    ) -> None:
        self.__config = {
            "timeout": timeout,
            "backoff": backoff,
            "request_count": request_count,
            "strict_validation": strict_validation,
            "auth": auth,
        }
        self.__resource_map_service = resource_map_service
        self.__suppliers_register_api = SuppliersApi(
            suppliers_register_url, timeout, backoff
        )
        self.__consumer_fhir_api = FhirApi(
            timeout, backoff, auth, consumer_url, request_count, strict_validation
        )
        self.__fhir_service = FhirService(strict_validation)
        self.__adjacency_map_service = AdjacencyMapService(
            strict=strict_validation,
            consumer_fhir_api=self.__consumer_fhir_api,
            resource_map_service=self.__resource_map_service,
        )

    def update(self, supplier_id: str) -> List[BundleEntry]:
        supplier = self.__suppliers_register_api.get_one(supplier_id)
        supplier_fhir_api = FhirApi(**self.__config, url=supplier.endpoint)

        agg = supplier_fhir_api.aggregate_all_resources_history()
        adjacency_map = self.__adjacency_map_service.create_adjacency_map(
            agg, supplier_id
        )

        for id, entry in adjacency_map.items():
            #   TODO: change the name of this
            #
            adjecency_list = self.__adjacency_map_service.bfs(adjacency_map, id)
            update_bundle, resource_map_dtos = self.prepare_data(adjecency_list)

            self.__consumer_fhir_api.post_bundle(update_bundle)

            for dto in resource_map_dtos:
                if isinstance(dto, ResourceMapDto):
                    print(dto)
                    self.__resource_map_service.add_one(dto)

                if isinstance(dto, ResourceMapUpdateDto):
                    self.__resource_map_service.update_one(dto)

        return {"message": f"{supplier_id} updated succesfully"}

    def prepare_data(
        self, adjacency_list: List[AdjacencyEntry]
    ) -> tuple[Bundle, List[ResourceMapDto | ResourceMapUpdateDto]]:
        new_bundle = Bundle.model_construct(type="transaction")
        new_bundle.entry = []
        resource_map_dtos: List[ResourceMapDto | ResourceMapUpdateDto] = []

        for item in adjacency_list:
            if item.update_status == "ignore":
                logger.info(
                    f"{item.resource_id} from supplier {item.supplier_data.supplier_id} is not needed skipping... \nsupplier_hash: {item.supplier_data.hash_value}  consumer_hash: {item.consumer_data.hash_value}"
                )
                continue

            consumer_id = f"{item.supplier_data.supplier_id}-{item.resource_id}"
            url = f"{item.resource_type}/{consumer_id}"
            resource = copy.deepcopy(item.supplier_data.entry.resource)
            if resource:
                self.__fhir_service.namespace_resource_references(
                    resource, item.supplier_data.supplier_id
                )
            new_entry = BundleEntry.model_construct()

            if item.update_status == "delele":

                entry_request = BundleEntryRequest.model_construct(
                    method="DELETE", url=url
                )
                new_entry.request = entry_request
                # soon not needed
                resource_map_dtos.append(
                    ResourceMapUpdateDto(
                        supplier_id=item.supplier_data.supplier_id,
                        resource_type=item.resource_type,
                        supplier_resource_id=item.resource_id,
                        history_size=1,
                    )
                )

            if item.update_status == "update":
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                new_entry.request = entry_request
                resource_map_dtos.append(
                    ResourceMapUpdateDto(
                        supplier_id=item.supplier_data.supplier_id,
                        resource_type=item.resource_type,
                        supplier_resource_id=item.resource_id,
                        history_size=item.resource_map.history_size + 1,
                    )
                )

            if item.update_status == "new":
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                new_entry.request = entry_request
                resource_map_dtos.append(
                    ResourceMapDto(
                        supplier_id=item.supplier_data.supplier_id,
                        supplier_resource_id=item.resource_id,
                        consumer_resource_id=consumer_id,
                        resource_type=item.resource_type,
                        history_size=1,
                    )
                )

            new_entry.resource = resource

            new_bundle.entry.append(new_entry)

        return new_bundle, resource_map_dtos
