from typing import Dict, Any

from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint

from app.services.request_services.fhir_request_service import FhirRequestService


class ConsumerRequestService:
    def __init__(self, url: str):
        self.__url = url
        self.__fhir_request_service = FhirRequestService(timeout=10, backoff=0.1)

    def post_organization(self, organization: dict[str, Any]) -> Organization:
        response = self.__fhir_request_service.post_resource(
            "Organization", self.__url, organization
        )
        return Organization(**response)

    def put_organization(
        self, organization: dict[str, Any], resource_id: str
    ) -> Organization:
        response = self.__fhir_request_service.put_resource(
            "Organization", self.__url, resource_id, organization
        )
        return Organization(**response)

    def get_organization(self, organization_id: str) -> Organization:
        response = self.__fhir_request_service.get_resource(
            "Organization", self.__url, organization_id
        )
        return Organization(**response)

    def get_endpoint(self, endpoint_id: str) -> Endpoint:
        response = self.__fhir_request_service.get_resource(
            "Endpoint", self.__url, endpoint_id
        )
        return Endpoint(**response)

    def find_organization(self, params: Dict[str, str]) -> Bundle:
        response = self.__fhir_request_service.search_for_resource(
            "Organization", self.__url, params
        )
        return Bundle(**response)

    def post_endpoint(self, endpoint: Dict[str, Any]) -> Endpoint:
        response = self.__fhir_request_service.post_resource(
            "Endpoint", self.__url, endpoint
        )
        return Endpoint(**response)

    def put_endpoint(self, endpoint: Dict[str, Any], resource_id: str) -> Endpoint:
        response = self.__fhir_request_service.put_resource(
            "Endpoint", self.__url, resource_id, endpoint
        )
        return Endpoint(**response)

    def get_endpoint_history(self, endpoint_id: str) -> Bundle:
        response = self.__fhir_request_service.get_resource_history(
            "Endpoint", self.__url, endpoint_id
        )
        return Bundle(**response)

    def delete_endpoint(self, endpoint_id: str) -> Dict[str, Any]:
        response = self.__fhir_request_service.delete_resource(
            "Endpoint", self.__url, endpoint_id
        )
        return response

    def delete_organization(self, organization_id: str) -> Dict[str, Any]:
        response = self.__fhir_request_service.delete_resource(
            "Organization", self.__url, organization_id
        )
        return response
