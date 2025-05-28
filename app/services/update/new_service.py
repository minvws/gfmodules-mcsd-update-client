import copy
import logging
from typing import Literal

from fhir.resources.R4B.bundle import BundleEntry, BundleEntryRequest
from fhir.resources.R4B.domainresource import DomainResource

from app.models.adjacency.node import Node, NodeUpdateData
from app.models.fhir.types import HttpValidVerbs
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.services.fhir.references.reference_namespacer import namespace_resource_reference
from app.services.fhir.fhir_service import FhirService

logger = logging.getLogger(__name__)

class DataPreparationService():
    def __init__(self,                 fhir_service: FhirService):
        self.__fhir_service = fhir_service

    def __update_status(
        self,
        supplier_bundle_method: HttpValidVerbs,
        namespaced_supplier_data: DomainResource,
        consumer_data: BundleEntry | None,
    ) -> Literal["ignore", "equal", "delete", "update", "new"]:

        if (
            supplier_bundle_method == "DELETE"
            and consumer_data is None
        ):
            return "ignore"
        supplier_hash = self.hash_value(namespaced_supplier_data)
        consumer_hash = self.hash_value(consumer_data.resource) if consumer_data else None
        if (
            supplier_bundle_method != "DELETE"
            and supplier_hash
            and consumer_hash
            and supplier_hash == consumer_hash
        ):
            return "equal"

        if (
            supplier_bundle_method == "DELETE"
            and consumer_data is not None
        ):
            return "delete"

        if (
            supplier_bundle_method != "DELETE"
            and supplier_hash is not None
            and consumer_hash is None
        ):
            return "new"

        return "update"


    def hash_value(self, resource: DomainResource | None) -> int | None:
        if resource is None:
            return None

        res = copy.deepcopy(resource)
        res.meta = None
        res.id = None
        return hash(res.model_dump().__repr__())

    def prepare_update_data(self,
        node: Node,
        supplier_id: str,
        consumer_data: BundleEntry | None
    ) -> NodeUpdateData | None:
        resource = copy.deepcopy(node.supplier_bundle_entry.resource)
        namespaced_supplier_data = self.__fhir_service.namespace_resource_references(resource, supplier_id)
        update_status = self.__update_status(
            node.supplier_bundle_entry.request.method,
            namespaced_supplier_data,
            consumer_data
        )

        consumer_resource_id = f"{supplier_id}-{node.resource_id}"
        url = f"{node.resource_type}/{consumer_resource_id}"

        new_resource_map: ResourceMapDto | ResourceMapUpdateDto | None = None
        match update_status:
            case "ignore":
                logger.info(
                    f"{node.resource_id} {node.resource_type} is not needed, ignoring..."
                )
                return None

            case "equal":
                logger.info(
                    f"{node.resource_id} {node.resource_type} has not changed, ignoring..."
                )
                return None

            case "delete":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="DELETE", url=url
                )
                entry.request = entry_request

                if node.existing_resource_map is None:
                    raise Exception(
                        f"Resource map for {node.resource_id} {node.resource_type} cannot be None and marked as delete "
                    )

                new_resource_map = ResourceMapUpdateDto(
                    history_size=node.existing_resource_map.history_size + 1,
                    supplier_id=supplier_id,
                    resource_type=node.resource_type,
                    supplier_resource_id=node.resource_id,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=new_resource_map)

            case "new":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                resource = copy.deepcopy(node.supplier_bundle_entry.resource)
                if resource is None:
                    raise Exception(
                        f"Resource {node.resource_id} {node.resource_type} cannot be None when a node is marked `new`"
                    )
                namespace_resource_reference(resource, supplier_id)
                resource.id = consumer_resource_id
                entry.resource = resource
                entry.request = entry_request

                new_resource_map = ResourceMapDto(
                    supplier_id=supplier_id,
                    supplier_resource_id=node.resource_id,
                    consumer_resource_id=consumer_resource_id,
                    resource_type=node.resource_type,
                    history_size=1,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=new_resource_map)

            case "update":
                entry = BundleEntry.model_construct()
                entry_request = BundleEntryRequest.model_construct(
                    method="PUT", url=url
                )
                resource = copy.deepcopy(node.supplier_bundle_entry.resource)
                if resource is None:
                    raise Exception(
                        f"Resource {node.resource_id} {node.resource_type} cannot be None and node marked as `update`"
                    )

                namespace_resource_reference(resource, supplier_id)
                resource.id = consumer_resource_id
                entry.request = entry_request
                entry.resource = resource

                if node.existing_resource_map is None:
                    raise Exception(
                        f"Resource map for {node.resource_id} {node.resource_type} cannot be None and marked as `update`"
                    )

                new_resource_map = ResourceMapUpdateDto(
                    supplier_id=supplier_id,
                    supplier_resource_id=node.resource_id,
                    resource_type=node.resource_type,
                    history_size=node.existing_resource_map.history_size + 1,
                )

                return NodeUpdateData(bundle_entry=entry, resource_map_dto=new_resource_map)
            case _:
                raise Exception("Should match one of this blabla todo fixme")
