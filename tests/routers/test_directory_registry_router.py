import os

if os.getenv("APP_ENV") is None:
    os.environ["APP_ENV"] = "test"  # noqa

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.container import get_directory_registry_service
from app.routers.directory_registry_router import router


class StubRegistryService:
    def __init__(self) -> None:
        self.providers: list[dict] = []
        self.manual_dirs: list[dict] = []

    def list_providers(self, include_disabled: bool = True) -> list[dict]:
        return self.providers

    def add_provider(self, url: str, enabled: bool = True) -> dict:
        p = {"id": len(self.providers) + 1, "url": url, "enabled": enabled, "last_refresh_at": None}
        self.providers.append(p)
        return p

    def refresh_all_enabled_providers(self) -> dict:
        return {"providers": ["ok"], "errors": []}

    def add_manual_directory(self, endpoint_address: str, directory_id: str | None = None, ura: str = ""):
        d = {"id": directory_id or "x", "endpoint_address": endpoint_address, "ura": ura, "origin": "manual"}
        self.manual_dirs.append(d)
        class _DTO:
            def __init__(self, data):
                self._data = data
            def model_dump(self):
                return self._data
        return _DTO(d)


def _make_client(service: StubRegistryService) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_directory_registry_service] = lambda: service
    return TestClient(app)


def test_list_providers_route() -> None:
    s = StubRegistryService()
    s.add_provider(url="http://p", enabled=True)
    client = _make_client(s)
    res = client.get("/admin/directory-registry/providers")
    assert res.status_code == 200
    assert res.json()[0]["url"] == "http://p"


def test_add_provider_route() -> None:
    client = _make_client(StubRegistryService())
    res = client.post("/admin/directory-registry/providers", json={"url": "http://p", "enabled": False})
    assert res.status_code == 200
    assert res.json()["url"] == "http://p"
    assert res.json()["enabled"] is False


def test_refresh_providers_route() -> None:
    client = _make_client(StubRegistryService())
    res = client.post("/admin/directory-registry/providers/refresh")
    assert res.status_code == 200
    assert res.json() == {"providers": ["ok"], "errors": []}


def test_add_manual_directory_route() -> None:
    client = _make_client(StubRegistryService())
    res = client.post("/admin/directory-registry/directories", json={"endpoint_address": "http://e"})
    assert res.status_code == 200
    body = res.json()
    assert body["endpoint_address"] == "http://e"
    assert body["origin"] == "manual"
