from typing import Any, Dict
from fastapi.encoders import jsonable_encoder
from app.models.fhir.r4.types import Bundle
from app.services.request_services.Authenticators import Authenticator
from app.services.request_services.fhir_request_service import FhirRequestService


class ConsumerRequestService:
    def __init__(self, url: str, auth: Authenticator) -> None:
        self.__url = url
        self.__fhir_request_service = FhirRequestService(
            timeout=10, backoff=0.1, auth=auth
        )

    # def post_resource(self, resource: Resource) -> Resource:
    #     response = self.__fhir_request_service.post_resource(
    #         resource_type=resource.resource_type,
    #         base_url=self.__url,
    #         resource=resource.model_dump(by_alias=True, exclude_none=True),
    #     )
    # return Resource(**response)

    # def put_resource(self, resource: Resource, resource_id: str) -> Resource:
    #     response = self.__fhir_request_service.put_resource(
    #         resource_type=resource.resource_type,
    #         base_url=self.__url,
    #         resource_id=resource_id,
    #         resource=resource.model_dump(by_alias=True),
    #     )
    #     return Resource(**response)
    #
    # def get_resource(self, resource: Resource, resource_id: str) -> Resource:
    #     response = self.__fhir_request_service.get_resource(
    #         resource_type=resource.resource_type,
    #         base_url=self.__url,
    #         resource_id=resource_id,
    #     )
    #     return Resource(**response)

    def delete_resource(self, resource_type: str, resource_id: str) -> None:
        self.__fhir_request_service.delete_resource(
            resource_type=resource_type, base_url=self.__url, resource_id=resource_id
        )

    def find_resource(self, resource_type: str, params: dict[str, str]) -> Bundle:
        response = self.__fhir_request_service.search_for_resource(
            resource_type=resource_type, base_url=self.__url, resource_params=params
        )
        return Bundle(**response)

    def resource_history(self, resource_type: str, resource_id: str) -> Bundle:
        response = self.__fhir_request_service.get_resource_history(
            base_url=self.__url, resource_type=resource_type, resource_id=resource_id
        )
        return response

    def post_bundle(self, bundle: Bundle) -> Dict[str, Any]:
        data = jsonable_encoder(bundle.model_dump(by_alias=True, exclude_none=True))
        response = self.__fhir_request_service.post_bundle(
            base_url=self.__url, bundle=data
        )
        return response
