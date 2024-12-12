import re
from typing import Dict, Any, List, Tuple, TYPE_CHECKING

from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)
from app.models.fhir.r4.types import Resource, Entry


class UpdateConsumerService:
    def __init__(
        self,
        supplier_request_service: SupplierRequestsService,
        consumer_request_service: ConsumerRequestService,
        resource_map_service: ResourceMapService,
    ):
        self.__supplier_request_service = supplier_request_service
        self.__consumer_request_service = consumer_request_service
        self.__resource_map_service = resource_map_service

    def update_supplier(self, supplier_id: str, resource_type: str) -> Dict[str, Any]:
        supplier_history = self.__supplier_request_service.get_resource_history(
            supplier_id, resource_type
        )
        entries = supplier_history.entry if supplier_history.entry else []

        org_ids: set[str] = set()
        for entry in entries:
            if entry.resource:
                org_ids.add(entry.resource.id)  # type: ignore
        for org_id in org_ids:
            org_history = self.__supplier_request_service.get_resource_history(
                supplier_id, resource_type, org_id
            )
            latest_entry: Entry = org_history.entry[0]  # type: ignore
            self.update(supplier_id, latest_entry)

        return {
            "message": "organizations and endpoints are updated",
            "data": self.__resource_map_service.find(supplier_id=supplier_id),
        }

    def update(
        self, supplier_id: str, entry: Entry
    ) -> Resource | None:  # Returns CONSUMER resource
        resource_type, resource_id = self._get_resource_type_and_id_from_bundle_entry(
            entry
        )
        resource_map = self.__resource_map_service.get(supplier_resource_id=resource_id)
        request_type = self._get_request_method(entry)
        entry_resource = entry.resource
        resource_is_not_needed = (
            True if resource_map is None and request_type == "DELETE" else False
        )
        resource_needs_deletion = (
            True if resource_map is not None and request_type == "DELETE" else False
        )
        resource_needs_update = (
            True if resource_map is not None and request_type == "PUT" else False
        )
        resource_already_up_to_date = (
            True
            if resource_map is not None
            and resource_map.supplier_resource_version
            == self._get_latest_etag_version(entry)
            else False
        )
        resource_already_deleted = (
            True if resource_already_up_to_date and request_type == "DELETE" else False
        )

        if resource_is_not_needed or resource_already_deleted:
            return None

        if resource_already_up_to_date:
            consumer_resource = self.__consumer_request_service.get_resource(
                resource=entry_resource,
                resource_id=resource_map.consumer_resource_id,  # type: ignore
            )
            return consumer_resource

        if resource_needs_deletion:
            self.__consumer_request_service.delete_resource(
                resource_type,
                resource_map.consumer_resource_id,  # type: ignore
            )
            consumer_resource_history = (
                self.__consumer_request_service.resource_history(
                    resource_type,
                    resource_map.consumer_resource_id,  # type: ignore
                )
            )
            self.__resource_map_service.update_one(
                ResourceMapUpdateDto(
                    supplier_resource_id=resource_id,
                    supplier_resource_version=self._get_latest_etag_version(entry),
                    consumer_resource_version=self._get_latest_etag_version(
                        consumer_resource_history.entry[0]  # type: ignore
                    ),
                )
            )
            return None

        references = self._get_references(entry_resource.model_dump())  # type: ignore

        for key, ref in references:
            if isinstance(ref, List):
                refs_list = []
                for i in ref:
                    latest = (
                        self.__supplier_request_service.get_latest_entry_from_reference(
                            supplier_id, i
                        )
                    )
                    ref_resource = self.update(supplier_id, latest)
                    refs_list.append(
                        {"reference": f"{ref_resource.resource_type}/{ref_resource.id}"}  # type: ignore
                    )
                if not isinstance(key, str):
                    raise TypeError("key is not of type str")
                entry_resource.__setattr__(key, refs_list)
            else:
                latest = (
                    self.__supplier_request_service.get_latest_entry_from_reference(
                        supplier_id, ref
                    )
                )
                ref_resource = self.update(supplier_id, latest)
                entry_resource.__setattr__(
                    key,
                    {"reference": f"{ref_resource.resource_type}/{ref_resource.id}"},  # type: ignore
                )

        # put
        if resource_needs_update:
            if TYPE_CHECKING:
                if entry_resource is None or resource_map is None:
                    raise ValueError("Resource cannot be None")

            entry_resource.id = resource_map.consumer_resource_id
            updated_resource = self.__consumer_request_service.put_resource(
                entry_resource, resource_map.consumer_resource_id
            )
            self.__resource_map_service.update_one(
                ResourceMapUpdateDto(
                    supplier_resource_id=resource_id,
                    supplier_resource_version=self._get_latest_etag_version(entry),
                    consumer_resource_version=int(updated_resource.meta.versionId),
                )
            )
            return updated_resource

        # default post
        entry_resource.id = None  # type: ignore
        new_resource = self.__consumer_request_service.post_resource(entry_resource)  # type: ignore
        self.__resource_map_service.add_one(
            ResourceMapDto(
                supplier_id=supplier_id,
                supplier_resource_id=resource_id,
                supplier_resource_version=self._get_latest_etag_version(entry),
                consumer_resource_id=new_resource.id,  # type: ignore
                consumer_resource_version=int(new_resource.meta.versionId),
                resource_type=resource_type,
            )
        )
        return new_resource

    @staticmethod
    def _get_references(
        data: dict[str, Any],
    ) -> List[Tuple[str, Dict[str, str] | List[Dict[str, str]]]]:
        refs: List[Tuple[str, Dict[str, str] | List[Dict[str, str]]]] = []
        for k, v in data.items():
            try:
                if "reference" in v:
                    refs.append((k, v))  # Tuple[str, dict[str, str]]
            except TypeError:
                continue

            if isinstance(v, List):
                refs_list: List[dict[str, str]] = []
                checker = False
                for i in v:
                    try:
                        if "reference" in i:
                            checker = True
                            refs_list.append(i)  # List[dict[str, str]]
                    except TypeError:
                        continue

                if checker:
                    refs.append((k, refs_list))
        return refs

    @staticmethod
    def _get_resource_type_and_id_from_bundle_entry(entry: Entry) -> tuple[str, str]:
        request = entry.request
        if request is None:
            raise ValueError("request is None")
        url = request.url
        if url is None:
            raise ValueError("url is None")

        split = url.split("/")
        return split[0], split[1]

    @staticmethod
    def _get_request_method(entry: Entry) -> str:
        entry_request = entry.request
        if entry_request is None:
            raise ValueError("entry_request is None")
        method = entry_request.method
        if method is None:
            raise ValueError("method is None")
        return method

    @staticmethod
    def _get_latest_etag_version(first_entry: Entry) -> int:
        # response = first_entry.response
        # if response is None:
        #     raise ValueError("response is not of type BundleEntryResponse")

        # etag = first_entry.response.etag
        # if etag is None:
        #     raise ValueError("etag in response is None")

        results = re.search(r"(?<=\")\d*(?=\")", first_entry.response.etag)
        if results is None:
            raise ValueError("Did not find etag")
        return int(results.group())
