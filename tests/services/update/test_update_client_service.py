from typing import Sequence
from unittest.mock import ANY, MagicMock
from app.config import ConfigExternalCache
from app.services.update.cache.provider import CacheProvider
from app.services.update.update_client_service import (
    UpdateClientService,
    McsdResources,
)
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest


def test_cleanup() -> None:
    # Mock dependencies
    mock_resource_map_service = MagicMock()
    mock_update_client_fhir_api = MagicMock()

    directory_id = "directory_1"

    def mock_resource_map_service_find(
        directory_id: str | None = None,
        resource_type: str | None = None,
        directory_resource_id: str | None = None,
        update_client_resource_id: str | None = None,
    ) -> Sequence[MagicMock]:
        assert directory_id == "directory_1"
        return [
            MagicMock(
                directory_id=directory_id,
                update_client_resource_id="resource_1",
                resource_type=resource_type,
                directory_resource_id="directory_resource_1",
            ),
            MagicMock(
                directory_id=directory_id,
                update_client_resource_id="resource_2",
                resource_type=resource_type,
                directory_resource_id="directory_resource_2",
            ),
        ]

    mock_resource_map_service.find.side_effect = mock_resource_map_service_find

    # Instantiate the service
    service = UpdateClientService(
        update_client_url="http://mock-update_client-url",
        strict_validation=True,
        timeout=30,
        backoff=0.1,
        request_count=3,
        resource_map_service=mock_resource_map_service,
        auth=MagicMock(),
        cache_provider=CacheProvider(config=ConfigExternalCache()),
        retries=5,
    )

    # Replace the FHIR API with the mock, this is a private and protected attribute, but we can set it for testing
    service._UpdateClientService__update_client_fhir_api = (  # type: ignore
        mock_update_client_fhir_api
    )

    service.cleanup(directory_id)

    mock_resource_map_service.find.assert_called_with(
        directory_id=directory_id, resource_type=ANY
    )
    assert mock_update_client_fhir_api.post_bundle.call_count == len(McsdResources)

    # Verify the bundle structure of the calls
    for idx, resource in enumerate(McsdResources):
        called_bundle = mock_update_client_fhir_api.post_bundle.call_args_list[idx][0][
            0
        ]

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
        assert (
            split[1] == "resource_1?_cascade=delete"
            or split[1] == "resource_2?_cascade=delete"
        )
