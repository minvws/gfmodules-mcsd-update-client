from typing import Sequence
from unittest.mock import ANY, MagicMock
from app.services.update.cache.provider import CacheProvider
from app.services.update.update_consumer_service import (
    UpdateConsumerService,
    McsdResources,
)
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest


def test_cleanup() -> None:
    # Mock dependencies
    mock_resource_map_service = MagicMock()
    mock_consumer_fhir_api = MagicMock()

    supplier_id = "supplier_1"

    def mock_resource_map_service_find(
        supplier_id: str | None = None,
        resource_type: str | None = None,
        supplier_resource_id: str | None = None,
        consumer_resource_id: str | None = None,
    ) -> Sequence[MagicMock]:
        assert supplier_id == "supplier_1"
        return [
            MagicMock(
                supplier_id=supplier_id,
                consumer_resource_id="resource_1",
                resource_type=resource_type,
            ),
            MagicMock(
                supplier_id=supplier_id,
                consumer_resource_id="resource_2",
                resource_type=resource_type,
            ),
        ]

    mock_resource_map_service.find.side_effect = mock_resource_map_service_find

    # Instantiate the service
    service = UpdateConsumerService(
        consumer_url="http://mock-consumer-url",
        strict_validation=True,
        timeout=30,
        backoff=0.1,
        request_count=3,
        resource_map_service=mock_resource_map_service,
        auth=MagicMock(),
        cache_provider=CacheProvider(),
    )

    # Replace the FHIR API with the mock, this is a private and protected attribute, but we can set it for testing
    service._UpdateConsumerService__consumer_fhir_api = (  # type: ignore
        mock_consumer_fhir_api
    )

    service.cleanup(supplier_id)

    mock_resource_map_service.find.assert_called_with(
        supplier_id=supplier_id, resource_type=ANY
    )
    assert mock_consumer_fhir_api.post_bundle.call_count == len(McsdResources)

    # Verify the bundle structure of the calls
    for idx, resource in enumerate(McsdResources):
        called_bundle = mock_consumer_fhir_api.post_bundle.call_args_list[idx][0][0]

        assert isinstance(called_bundle, Bundle)
        assert called_bundle.type == "transaction"
        assert called_bundle.entry is not None
        assert len(called_bundle.entry) == 2
        assert isinstance(called_bundle.entry[0], BundleEntry)
        assert isinstance(called_bundle.entry[0].request, BundleEntryRequest)
        assert called_bundle.entry[0].request.method == "DELETE"
        assert called_bundle.entry[0].request.url is not None
        split = called_bundle.entry[0].request.url.split("/")
        assert split[0] == resource.value
        assert split[1] == "resource_1" or split[1] == "resource_2"
