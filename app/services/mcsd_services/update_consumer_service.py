import logging
from typing import Dict, Any, Tuple
from datetime import datetime

from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.models.supplier_update.dto import UpdateLookup, UpdateLookupEntry
from app.services.bundle_tools import (
    get_resource_type_and_id_from_entry,
    get_request_method_from_entry,
)
from app.services.fhir.fhir_service import FhirService
from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)
from app.models.fhir.r4.types import Bundle, Request, Entry, Resource
from app.stats import Stats

logger = logging.getLogger(__name__)


class UpdateConsumerService:
    def __init__(
        self,
        supplier_request_service: SupplierRequestsService,
        consumer_request_service: ConsumerRequestService,
        resource_map_service: ResourceMapService,
        strict_validation: bool,
        stats: Stats,
    ):
        self.__supplier_request_service = supplier_request_service
        self.__consumer_request_service = consumer_request_service
        self.__resource_map_service = resource_map_service
        self.__fhir_service = FhirService(strict_validation=strict_validation)
        self.stats = stats

    def update_supplier(
        self,
        supplier_id: str,
        _since: datetime | None = None,
    ) -> Dict[str, Any]:
        # Fetch the history of all resources from this supplier (can take a while)
        supplier_history = self.__supplier_request_service.get_resource_history(
            supplier_id=supplier_id, _since=_since
        )
        entries = supplier_history.entry if supplier_history.entry else []

        # Map all the resources into a set of tuples (resource_type, resource_id)
        resource_ids: set[Tuple[str, str]] = set()  # Resource type & ID
        for entry in entries:
            (resource_type, resource_id) = get_resource_type_and_id_from_entry(
                entry
            )
            if resource_type is not None and resource_id is not None:
                resource_ids.add((resource_type, resource_id))

        for resource_type, resource_id in resource_ids:
            resource_history = self.__supplier_request_service.get_resource_history(
                supplier_id, resource_type, resource_id, None
            )
            if resource_history.entry is None or len(resource_history.entry) == 0:
                continue

            # Update this resource to the latest entry (if needed)
            latest_entry = resource_history.entry[0]
            self.update(supplier_id, len(resource_history.entry), latest_entry)

        return {
            "message": "organizations and endpoints are updated",
            "data": self.__resource_map_service.find(supplier_id=supplier_id),
        }

    def update(self, supplier_id: str, history_size: int, latest_entry: Entry) -> None:
        _, main_resource_id = get_resource_type_and_id_from_entry(latest_entry)
        request_method = get_request_method_from_entry(latest_entry)

        main_resource = (
            self.__fhir_service.create_resource(latest_entry.resource.model_dump())
            if latest_entry.resource
            else None
        )
        unique_refs = (
            self.__fhir_service.get_references(main_resource)
            if main_resource is not None
            else []
        )

        update_lookup: UpdateLookup = {}
        if main_resource_id:
            update_lookup.update(
                {
                    main_resource_id: UpdateLookupEntry(
                        history_size=history_size, entry=latest_entry
                    )
                }
            )

        for ref in unique_refs:
            res_type, id = self.__fhir_service.split_reference(ref)
            data = self.__supplier_request_service.get_resource_history(
                resource_type=res_type, resource_id=id, supplier_id=supplier_id
            )
            entry = data.entry
            if entry is not None and len(entry) > 0 and id is not None:
                update_lookup.update(
                    {id: UpdateLookupEntry(history_size=len(entry), entry=entry[0])}
                )

        # prepare bundle
        new_bundle = Bundle(type="transaction", entry=[])
        for id, lookup_data in update_lookup.items():
            resource_type, resource_id = get_resource_type_and_id_from_entry(
                lookup_data.entry
            )
            resource_map = self.__resource_map_service.get(
                supplier_id, resource_type, resource_id
            )
            request_method = get_request_method_from_entry(lookup_data.entry)
            original_resource = (
                self.__fhir_service.create_resource(
                    lookup_data.entry.resource.model_dump()
                )
                if lookup_data.entry.resource is not None
                else None
            )

            # resource new and method is delete
            if resource_map is None and request_method == "DELETE":
                logger.info(
                    f"resource {id} is new and already DELETED from supplier {supplier_id} ...skipping"
                )
                continue

            # resource up to date
            if (
                resource_map is not None
                and resource_map.history_size == lookup_data.history_size
            ):
                logger.info(
                    f"resource {resource_id} from {supplier_id} is up to date with consumer id: {resource_map.consumer_resource_id} ...skipping"
                )
                continue

            # resource is new
            if (
                resource_map is None
                and request_method != "DELETE"
                and original_resource is not None
            ):
                # replace references

                logger.info(
                    f"resource {resource_id} from {supplier_id} is new ...processing"
                )
                new_id = f"{supplier_id}-{resource_id}"
                new_resource = self.__fhir_service.namespace_resource_references(
                    original_resource, supplier_id
                )
                new_resource.id = new_id
                new_entry = Entry(
                    resource=Resource(**new_resource.model_dump()),
                    request=Request(method="PUT", url=f"{resource_type}/{new_id}"),
                )
                new_bundle.entry.append(new_entry)
                lookup_data.resource_map = ResourceMapDto(
                    supplier_id=supplier_id,
                    supplier_resource_id=resource_id,  # type: ignore
                    resource_type=resource_type,  # type: ignore
                    consumer_resource_id=new_id,
                    history_size=lookup_data.history_size,
                )

            # resource needs to be delete
            if resource_map is not None and request_method == "DELETE":
                logger.info(
                    f"resource {resource_id} from {supplier_id} needs to be deleted with consumer id: {resource_map.consumer_resource_id} ...processing"
                )
                new_entry = Entry(
                    request=Request(
                        method="DELETE",
                        url=f"{resource_type}/{resource_map.consumer_resource_id}",
                    )
                )
                new_bundle.entry.append(new_entry)
                lookup_data.resource_map = ResourceMapUpdateDto(
                    supplier_id=supplier_id,
                    resource_type=resource_map.resource_type,
                    supplier_resource_id=resource_map.supplier_resource_id,
                    history_size=lookup_data.history_size,
                )

            if (
                resource_map is not None
                and request_method != "DELETE"
                and original_resource is not None
            ):
                logger.info(
                    f"resource {resource_id} from {supplier_id} needs to be updated with consumer id: {resource_map.consumer_resource_id} ...processing"
                )
                # replace id with one from resource_map
                original_resource.id = resource_map.consumer_resource_id
                new_resource = self.__fhir_service.namespace_resource_references(
                    original_resource, supplier_id
                )
                new_entry = Entry(
                    resource=Resource(**new_resource.model_dump()),
                    request=Request(
                        method="PUT",
                        url=f"{resource_type}/{resource_map.consumer_resource_id}",
                    ),
                )
                new_bundle.entry.append(new_entry)
                lookup_data.resource_map = ResourceMapUpdateDto(
                    supplier_id=supplier_id,
                    resource_type=resource_map.resource_type,
                    supplier_resource_id=resource_map.supplier_resource_id,
                    history_size=lookup_data.history_size,
                )

        # only post when something has changed
        if len(new_bundle.entry) > 0:
            logger.info(f"detected changes from {supplier_id} ...updating data")
            self.__consumer_request_service.post_bundle(new_bundle)
            logger.info(f"data from {supplier_id} has been updated successfully!!")

            for v in update_lookup.values():
                if isinstance(v.resource_map, ResourceMapDto):
                    logger.info(
                        f"new resource map entry with {v.resource_map.__repr__()} ...creating"
                    )
                    self.__resource_map_service.add_one(v.resource_map)
                if isinstance(v.resource_map, ResourceMapUpdateDto):
                    self.__resource_map_service.update_one(v.resource_map)
                    logger.info(
                        f"new resource map entry with {v.resource_map.__repr__()} ...updating"
                    )

            logger.info("resource map has been updated successfully!!!")
