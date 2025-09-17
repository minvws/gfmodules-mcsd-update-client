import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import ANY, MagicMock, patch
from uuid import uuid4

from app.config import ConfigExternalCache
from app.db.entities.resource_map import ResourceMap
from app.models.adjacency.node import Node
from app.models.directory.dto import DirectoryDto
from app.models.fhir.types import McsdResources
from app.models.resource_map.dto import ResourceMapDeleteDto, ResourceMapDto, ResourceMapUpdateDto
from app.services.api.authenticators.null_authenticator import NullAuthenticator
from app.services.entity.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
from app.services.update.cache.provider import CacheProvider
from app.services.update.update_client_service import (
    CacheServiceUnavailableException,
    UpdateClientService, UpdateClientException,
)
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
import pytest

# Some helpers
def _be(url: str) -> BundleEntry:
    return BundleEntry(request=BundleEntryRequest(method="DELETE", url=url))

def _node(  # type: ignore[no-untyped-def]
    rid: str,
    resource_type: str = "Organization",
    status: str = "changed",
    bundle_entry: BundleEntry | None = None,
    dto: object | None = None,
    updated: bool = False,
):
    class Node:
        def __init__(self) -> None:
            self.resource_id = rid
            self.resource_type = resource_type
            self.status = status
            self.updated = updated
            self.update_data = SimpleNamespace(
                bundle_entry=bundle_entry,
                resource_map_dto=dto,
            )

        def clear_for_cache(self) -> None:
            pass

    return Node()

class FakeCache:
    def __init__(self) -> None:
        self.keys_ = set[str]()
        self.added: list[str] = []

    def key_exists(self, k: str) -> bool:
        return k in self.keys_

    def add_node(self, n: Node) -> None:
        self.added.append(n.resource_id)
        self.keys_.add(n.resource_id)

    def clear(self) -> None:
        pass


@pytest.fixture
def resource_map_service() -> MagicMock:
    return MagicMock(spec=ResourceMapService)

@pytest.fixture
def directory_dto() -> DirectoryDto:
    return DirectoryDto(
                id="1",
                name="Test Directory",
                endpoint="https://example-directory.com",
            )

@pytest.fixture
def update_client_service(resource_map_service: MagicMock) -> UpdateClientService:
    return UpdateClientService(
        update_client_url="https://example.com",
        fill_required_fields=False,
        timeout=30,
        backoff=0.1,
        retries=5,
        request_count=3,
        resource_map_service=resource_map_service,
        auth=NullAuthenticator(),
        cache_provider=CacheProvider(config=ConfigExternalCache()),
    )

@patch.object(UpdateClientService, '_UpdateClientService__cleanup_resource_type', autospec=True)
def test_clean_up_calls_cleanup_resource_type(mock_cleanup_resource_type: MagicMock, update_client_service: UpdateClientService) -> None:
    directory_id = "directory_1"
    update_client_service.cleanup(directory_id)
    assert mock_cleanup_resource_type.call_count == len(McsdResources)
    mock_cleanup_resource_type.assert_any_call(update_client_service, directory_id, ANY)



@patch(
    "app.services.api.api_service.HttpService.do_request",
    autospec=True
)
@patch(
    "app.services.update.update_client_service.uuid4",
    autospec=True
)
def test_cleanup_per_resource_type_finds_and_deletes_resource_map_items(
    mock_uuid4: MagicMock,
    mock_do_request: MagicMock,
    update_client_service: UpdateClientService,
    resource_map_service: MagicMock,
) -> None:
    mock_uuid4.return_value = "638bdbfa-8658-4f29-b0e7-5abdada97067"
    resource_map_service.find.return_value = [
        ResourceMap(
            id=uuid.uuid4(),
            directory_id="directory_1",
            resource_type=McsdResources.ENDPOINT.value,
            directory_resource_id="dir_res_1",
            update_client_resource_id="update_res_1"
        ),
        ResourceMap(
            id=uuid.uuid4(),
            directory_id="directory_1",
            resource_type=McsdResources.ENDPOINT.value,
            directory_resource_id="dir_res_2",
            update_client_resource_id="update_res_2"
        )
    ]
    mock_do_request.return_value = MagicMock(status_code=200, json=MagicMock(return_value=Bundle(
            id="abc132", type="transaction", entry=[], total=0
        ).model_dump()))
    update_client_service._UpdateClientService__cleanup_resource_type("directory_1", McsdResources.ENDPOINT) # type: ignore[attr-defined]
    resource_map_service.find.assert_called_once_with(directory_id="directory_1", resource_type=McsdResources.ENDPOINT.value)
    resource_map_service.delete_one.assert_any_call(ResourceMapDeleteDto(
                directory_id="directory_1",
                resource_type=McsdResources.ENDPOINT.value,
                directory_resource_id="dir_res_1",
            ))
    resource_map_service.delete_one.assert_any_call(ResourceMapDeleteDto(
                directory_id="directory_1",
                resource_type=McsdResources.ENDPOINT.value,
                directory_resource_id="dir_res_2",
            ))
    mock_do_request.assert_called_once_with(
        ANY,
        "POST",
        json={'resourceType': 'Bundle', 'id': '638bdbfa-8658-4f29-b0e7-5abdada97067', 'type': 'transaction', 'total': 2, 'entry': [{'request': {'method': 'DELETE', 'url': 'Endpoint/update_res_1?_cascade=delete'}}, {'request': {'method': 'DELETE', 'url': 'Endpoint/update_res_2?_cascade=delete'}}]},
    )

@patch(
    "app.services.api.api_service.HttpService.do_request",
    autospec=True
)
@patch(
    "app.services.update.update_client_service.uuid4",
    autospec=True
)
def test_cleanup_with_more_than_hundred_items_creates_max_size_bundles(
    mock_uuid4: MagicMock,
    mock_do_request: MagicMock,
    update_client_service: UpdateClientService,
    resource_map_service: MagicMock,
) -> None:
    mock_uuid4.return_value = "638bdbfa-8658-4f29-b0e7-5abdada97067"
    resource_map_service.find.return_value = [
        ResourceMap(
            id=uuid.uuid4(),
            directory_id="directory_1",
            resource_type=McsdResources.ENDPOINT.value,
            directory_resource_id=f"dir_res_{x}",
            update_client_resource_id=f"update_res_{x}"
        ) for x in range(150)
    ]
    mock_do_request.return_value = MagicMock(status_code=200, json=MagicMock(return_value=Bundle(
            id="abc132", type="transaction", entry=[], total=0
        ).model_dump()))
    update_client_service._UpdateClientService__cleanup_resource_type("directory_1", McsdResources.ENDPOINT) # type: ignore[attr-defined]
    resource_map_service.find.assert_called_once_with(directory_id="directory_1", resource_type=McsdResources.ENDPOINT.value)
    for x in range(150):
        resource_map_service.delete_one.assert_any_call(ResourceMapDeleteDto(
            directory_id="directory_1",
            resource_type=McsdResources.ENDPOINT.value,
            directory_resource_id=f"dir_res_{x}",
        ))
    mock_do_request.assert_any_call(
        ANY,
        "POST",
        json={'resourceType': 'Bundle', 'id': '638bdbfa-8658-4f29-b0e7-5abdada97067', 'type': 'transaction', 'total': 100, 'entry': [{'request': {'method': 'DELETE', 'url': f'Endpoint/update_res_{x}?_cascade=delete'}} for x in range(100)]},
    )
    mock_do_request.assert_any_call(
        ANY,
        "POST",
        json={'resourceType': 'Bundle', 'id': '638bdbfa-8658-4f29-b0e7-5abdada97067', 'type': 'transaction', 'total': 50, 'entry': [{'request': {'method': 'DELETE', 'url': f'Endpoint/update_res_{x}?_cascade=delete'}} for x in range(100, 150)]},
    )

@patch(
    "app.services.update.update_client_service.UpdateClientService.update_resource"
)
def test_update_creates_and_clears_cache(
    mock_update_resource: MagicMock,
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto
) -> None:
    memory_cache_service = MagicMock()
    cache_provider = MagicMock()
    mock_update_resource.return_value = None
    update_client_service._UpdateClientService__cache_provider = cache_provider # type: ignore[attr-defined]
    cache_provider.create.return_value = memory_cache_service

    update_client_service.update(directory_dto)

    cache_provider.create.assert_called()
    assert cache_provider.create.call_count == 1
    assert memory_cache_service.clear.call_count == 2


@patch.object(
    UpdateClientService,
    "_UpdateClientService__create_cache_run",
    autospec=True
)
def test_update_raises_cache_service_unavailable_exception_when_cache_service_is_none(
    mock_create_cache_run: MagicMock,
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto
) -> None:
    mock_create_cache_run.return_value = None
    with pytest.raises(CacheServiceUnavailableException):
        update_client_service.update(directory_dto)

def test_update_resource_raises_cache_service_unavailable_exception_when_cache_service_is_none(
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto
) -> None:
    with pytest.raises(CacheServiceUnavailableException):
        update_client_service.update_resource(directory_dto, McsdResources.ENDPOINT.value, None)

@patch("app.services.update.update_client_service.FhirApi")
def test_update_resource_calls_build_history_params(
    mock_fhir_api: MagicMock,
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto
) -> None:
    mock_cache = MagicMock()
    update_client_service._UpdateClientService__cache = mock_cache # type: ignore[attr-defined]
    mock_cache.key_exists.return_value = False

    mock_directory_fhir_api = MagicMock()
    mock_fhir_api.return_value = mock_directory_fhir_api
    mock_directory_fhir_api.build_history_params.return_value = None
    since = datetime.now() - timedelta(days=30)
    update_client_service.update_resource(directory_dto, McsdResources.ENDPOINT.value, since)

    mock_directory_fhir_api.build_history_params.assert_called_once_with(since=since)

@patch(
    "app.services.api.api_service.HttpService.do_request",
    autospec=True
)
def test_update_resource_requests_history_batch(
    mock_do_request: MagicMock,
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto
) -> None:
    mock_cache = MagicMock()
    update_client_service._UpdateClientService__cache = mock_cache # type: ignore[attr-defined]
    mock_cache.key_exists.return_value = False

    since = datetime.now() - timedelta(days=30)

    mock_do_request.return_value = MagicMock(status_code=200, json=MagicMock(return_value=Bundle(
            id="abc132", type="transaction", entry=[], total=0
        ).model_dump()))
    update_client_service.update_resource(directory_dto, McsdResources.ENDPOINT.value, since)
    mock_do_request.assert_any_call(
        ANY,
        "GET",
        sub_route=f"{McsdResources.ENDPOINT.value}/_history",
        params={"_count": str(update_client_service.request_count), "_since": since.isoformat()}
    )

@patch(
    "app.services.api.api_service.request",
    autospec=True
)
def test_update_requests_resources(
    mock_request: MagicMock,
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto,
    mock_org_history_bundle: Dict[str, Any],
    mock_ep: Dict[str, Any]
) -> None:
    fhir_service = FhirService(fill_required_fields=True)
    since = datetime.now() - timedelta(days=30)

    def mock_request_side_effect(*args: Any, **kwargs: Any) -> MagicMock:
        if f"{McsdResources.ORGANIZATION.value}/_history" in kwargs.get("url", ""):
            return MagicMock(status_code=200, json=MagicMock(return_value=fhir_service.create_bundle(mock_org_history_bundle).model_dump()))
        if kwargs.get("method") == "GET":
            return MagicMock(status_code=200, json=MagicMock(return_value={'resourceType': 'Bundle', 'type': 'batch', 'entry': []}))
        if kwargs.get("method") == "POST" and kwargs.get("url") == "https://example.com":
            return MagicMock(status_code=200, json=MagicMock(return_value={'resourceType': 'Bundle', 'type': 'batch', 'entry': []}))
        if kwargs.get("method") == "POST" and kwargs.get("url") == "https://example-directory.com" and kwargs.get("json", {}).get("entry", [{}])[0].get("request", {}).get("url") == '/Endpoint/ep-id/_history':
            return MagicMock(status_code=200, json=MagicMock(return_value={'resourceType': 'Bundle', 'type': 'batch', 'entry': [{"resource": {'resourceType': 'Bundle', 'type': 'batch', 'entry': [{"resource": mock_ep, "request": {"method": "PUT", "url": "Endpoint/ep-id/_history/1"}}]}}]}))
        assert False, f"Should not reach here: {args}, {kwargs}"

    mock_request.side_effect = mock_request_side_effect
    update_client_service.update(directory_dto, since)

    for res_type in McsdResources:
        mock_request.assert_any_call(
            method='GET',
            url=f'https://example-directory.com/{res_type.value}/_history?_count={update_client_service.request_count}&_since={since.isoformat()}',
            headers=ANY,
            timeout=ANY,
            json=ANY,
            cert=ANY,
            verify=ANY,
            auth=ANY
        )

    mock_request.assert_any_call(
        method='POST',
        url='https://example.com',
        headers=ANY,
        timeout=ANY,
        json={'resourceType': 'Bundle', 'type': 'batch', 'entry': [{'request': {'method': 'GET', 'url': '/Organization/1-org-id/_history'}}, {'request': {'method': 'GET', 'url': '/Endpoint/1-ep-id/_history'}}]},
        cert=ANY,
        verify=ANY,
        auth=ANY
    )
    mock_request.assert_any_call(
        method='POST',
        url='https://example-directory.com',
        headers=ANY,
        timeout=ANY,
        json={'resourceType': 'Bundle', 'type': 'batch', 'entry': [{'request': {'method': 'GET', 'url': '/Endpoint/ep-id/_history'}}]},
        cert=ANY,
        verify=ANY,
        auth=ANY
    )



def test_flush_delete_bundle_noop_when_total_zero(update_client_service: UpdateClientService, monkeypatch: Any) -> None:
    posted = []
    def fake_post(bundle: Bundle) -> tuple[list[Bundle], list[Any]]:
        posted.append(bundle)
        return [], []
    monkeypatch.setattr(
        update_client_service,
        "_UpdateClientService__update_client_fhir_api",
        SimpleNamespace(post_bundle=fake_post)
    )

    empty = Bundle(id=str(uuid4()), type="transaction", entry=[], total=0)
    flush = getattr(update_client_service, "_UpdateClientService__flush_delete_bundle")
    flush(empty, "dir-1")
    assert posted == []


def test_flush_delete_bundle_raises_on_errors(update_client_service: UpdateClientService, monkeypatch: Any) -> None:
    def err_post(_bundle: Bundle) -> tuple[list[Bundle], list[Any]]:
        return [], ["error"]

    monkeypatch.setattr(update_client_service, "_UpdateClientService__update_client_fhir_api", SimpleNamespace(post_bundle=err_post))

    b = Bundle(id=str(uuid4()), type="transaction", entry=[_be("X")], total=1)
    flush = getattr(update_client_service, "_UpdateClientService__flush_delete_bundle")
    with pytest.raises(UpdateClientException, match="Errors occurred when flushing delete bundle"):
        flush(b, "dir-1")


def test_update_with_bundle_posts_bundle_handles_dtos_and_marks_updated(
    update_client_service: UpdateClientService,
    resource_map_service: MagicMock,
    monkeypatch: Any
) -> None:
    posted = []
    def ok_post(bundle: Bundle) -> tuple[list[Bundle], list[Any]]:
        posted.append(bundle)
        return [], []

    monkeypatch.setattr(update_client_service, "_UpdateClientService__update_client_fhir_api", SimpleNamespace(post_bundle=ok_post))

    add_dto = MagicMock(spec=ResourceMapDto)
    upd_dto = MagicMock(spec=ResourceMapUpdateDto)

    n1 = _node("A", bundle_entry=_be("Organization/A"), dto=add_dto)
    n2 = _node("B", status="equal")
    n3 = _node("C", status="ignore")
    n4 = _node("D", dto=upd_dto)

    out = update_client_service.update_with_bundle([n1, n2, n3, n4])

    assert len(posted) == 1
    assert len(posted[0].entry or []) == 1
    assert posted[0].entry[0].request.url.endswith("Organization/A") # type: ignore[index]

    resource_map_service.add_one.assert_called_once_with(add_dto)
    resource_map_service.update_one.assert_called_once_with(upd_dto)

    assert all(n.updated for n in out)


def test_update_with_bundle_raises_when_api_returns_errors(update_client_service: UpdateClientService, monkeypatch: Any) -> None:
    def bad_post(bundle: Bundle) -> tuple[list[Bundle], list[Any]]:
        return [], ["err1", "err2"]
    monkeypatch.setattr(update_client_service, "_UpdateClientService__update_client_fhir_api", SimpleNamespace(post_bundle=bad_post))

    with pytest.raises(UpdateClientException, match="Errors occurred when updating bundle"):
        update_client_service.update_with_bundle([_node("Z", bundle_entry=_be("Endpoint/Z"))])


def test_clear_and_add_nodes_skips_existing_adds_new(update_client_service: UpdateClientService, monkeypatch: Any) -> None:
    cache = FakeCache()
    monkeypatch.setattr(update_client_service, "_UpdateClientService__cache", cache)

    clear_add = getattr(update_client_service, "_UpdateClientService__clear_and_add_nodes")

    class N:
        def __init__(self, rid: str) -> None:
            self.resource_id = rid
            self.cleared = False

        def clear_for_cache(self) -> None:
            self.cleared = True

    cache.keys_.add("existing")
    n1, n2 = N("existing"), N("new_entry")

    clear_add([n1, n2])

    assert n1.cleared is False
    assert n2.cleared is True
    assert cache.added == ["new_entry"]


def test_clear_and_add_nodes_requires_cache(update_client_service: UpdateClientService) -> None:
    with pytest.raises(CacheServiceUnavailableException):
        getattr(update_client_service, "_UpdateClientService__clear_and_add_nodes")([])


def test_update_page_builds_groups_and_skips_updated(update_client_service: UpdateClientService, monkeypatch: Any) -> None:
    entries = [BundleEntry()]

    n_updated = _node("U1", updated=True)   # Already updated node
    n_fresh = _node("F1", updated=False)

    class FakeAdjMap:
        def __init__(self) -> None:
            self.data = {"u": n_updated, "f": n_fresh}
        def get_group(self, node: Node) -> list[Node]:
            return [node]

    fake_adjsvc = MagicMock()
    fake_adjsvc.build_adjacency_map.return_value = FakeAdjMap()

    called = {}
    def fake_update_with_bundle(nodes: list[Node]) -> list[Node]:
        for n in nodes:
            n.updated = True
        called["args"] = nodes
        return nodes
    monkeypatch.setattr(update_client_service, "update_with_bundle", fake_update_with_bundle)

    updated_nodes = update_client_service.update_page(entries, fake_adjsvc)

    assert called["args"] == [n_fresh]
    assert updated_nodes == [n_fresh]
    assert n_fresh.updated is True
    assert n_updated.updated is True


@patch.object(UpdateClientService, "update_resource", autospec=True)
def test_update_invokes_update_resource_for_all_resource_types(
    mock_update_resource: Any,
    update_client_service: UpdateClientService,
    directory_dto: DirectoryDto,
    monkeypatch: Any
) -> None:
    fake_cache = SimpleNamespace(keys=lambda: [], clear=lambda: None)
    monkeypatch.setattr(update_client_service, "_UpdateClientService__cache_provider", SimpleNamespace(create=lambda: fake_cache))

    result = update_client_service.update(directory_dto)
    assert "log" in result
    assert result["log"].startswith("updated ")

    assert mock_update_resource.call_count == len(McsdResources)
    for res in McsdResources:
        mock_update_resource.assert_any_call(update_client_service, directory_dto, res.value, None)
