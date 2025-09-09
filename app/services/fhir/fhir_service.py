from typing import Any, Dict, List
from fhir.resources.R4B.bundle import BundleEntry, Bundle
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference

from app.models.adjacency.node import NodeReference
from app.models.fhir.types import BundleRequestParams, HttpValidVerbs
from app.services.fhir.bundle.bundle_utils import (
    filter_history_entries,
    get_request_method_from_entry,
    get_resource_type_and_id_from_entry,
)
from app.services.fhir.bundle.bundle_parser import (
    create_bundle,
    create_bundle_request,
    get_entries_from_bundle_of_bundles,
)
from app.services.fhir.resources.factory import create_resource
from app.services.fhir.references.reference_extractor import (
    get_references,
)
from app.services.fhir.references.reference_misc import build_node_reference
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)


class FhirService:
    def __init__(
        self,
        fill_required_fields: bool,
    ) -> None:
        self.fill_required_fields = fill_required_fields

    def create_resource(self, data: Dict[str, Any]) -> DomainResource:
        """
        Creates a defined FHIR mCSD resource model.
        """
        return create_resource(data, self.fill_required_fields)

    def create_bundle(self, data: Dict[str, Any]) -> Bundle:
        """
        Returns a Bundle instance from a Dict object
        """
        return create_bundle(data, self.fill_required_fields)

    @staticmethod
    def get_references(data: DomainResource) -> List[Reference]:
        """
        Retrieves a unique list of References from an FHIR mCSD Resources.
        References are returned only if they contain the property "reference" in them.
        """
        return get_references(data)

    @staticmethod
    def namespace_resource_references(
        data: DomainResource, namespace: str
    ) -> DomainResource:
        """
        Adds namespace to the References in a FHIR mCSD Resource.
        """
        return namespace_resource_reference(data, namespace)

    @staticmethod
    def make_reference_node(data: Reference, base_url: str) -> NodeReference:
        """
        Returns a reference variant from either an absolute or relative reference
        """
        return build_node_reference(data, base_url)

    @staticmethod
    def get_resource_type_and_id_from_entry(entry: BundleEntry) -> tuple[str, str]:
        """
        Retrieves the resource type and id from a Bundle entry
        """
        return get_resource_type_and_id_from_entry(entry)

    @staticmethod
    def get_request_method_from_entry(entry: BundleEntry) -> HttpValidVerbs:
        """
        Retrieves the request method from a Bundle entry if a request is present
        """
        return get_request_method_from_entry(entry)

    @staticmethod
    def filter_history_entries(entries: List[BundleEntry]) -> List[BundleEntry]:
        """
        Filters BundleEntries in a history Bundle and returns the latest for each ResourceType
        """
        return filter_history_entries(entries)

    @staticmethod
    def create_bundle_request(data: List[BundleRequestParams]) -> Bundle:
        """
        Takes a list of AdjacencyReference and creates a Bundle with GET as an HTTP request method
        """
        return create_bundle_request(data)

    @staticmethod
    def get_entries_from_bundle_of_bundles(data: Bundle) -> List[BundleEntry]:
        """
        Returns a flat list of entries from a Bundle of Bundles
        """
        return get_entries_from_bundle_of_bundles(data)
