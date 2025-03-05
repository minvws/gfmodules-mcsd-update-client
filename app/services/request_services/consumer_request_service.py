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

    def post_bundle(self, bundle: Bundle) -> Dict[str, Any]:
        data = jsonable_encoder(bundle.model_dump(by_alias=True, exclude_none=True))
        response = self.__fhir_request_service.post_bundle(
            base_url=self.__url, bundle=data
        )
        return response
