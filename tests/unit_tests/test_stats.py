import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.config import set_config
from app.stats import MemoryClient, Statsd, StatsdMiddleware, setup_stats, get_stats
from tests.get_test_config import get_test_config

@pytest.fixture
def memory_client() -> MemoryClient:
    return MemoryClient()

def test_memory_client_gauge(memory_client: MemoryClient) -> None:
    memory_client.gauge("test.metric", 100)
    memory = memory_client.get_memory()
    assert "test.metric" in memory
    assert len(memory["test.metric"]) == 1
    assert memory["test.metric"][0]["value"] == 100

def test_memory_client_timing(memory_client: MemoryClient) -> None:
    memory_client.timing("test.timing", 500)
    memory = memory_client.get_memory()
    assert "test.timing" in memory
    assert len(memory["test.timing"]) == 1
    assert memory["test.timing"][0] == 500

def test_memory_client_incr(memory_client: MemoryClient) -> None:
    memory_client.incr("test.counter")
    memory = memory_client.get_memory()
    # Since incr is not implemented to store data, this is a placeholder test
    assert memory == {'test.counter': 1}

def test_memory_client_decr(memory_client: MemoryClient) -> None:
    memory_client.decr("test.counter")
    memory = memory_client.get_memory()
    # Since decr is not implemented to store data, this is a placeholder test
    assert memory == {'test.counter': -1}

def test_statsd_middleware() -> None:
    test_conf = get_test_config()
    test_conf.stats.enabled = True
    test_conf.stats.host = None
    test_conf.stats.port = None
    test_conf.stats.module_name = "test_module"
    set_config(test_conf)

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    app.add_middleware(StatsdMiddleware, module_name="test_module")
    setup_stats()
    client = TestClient(app)

    response = client.get("/test")
    assert response.status_code == 200

    stats = get_stats()
    assert isinstance(stats, Statsd)
    assert isinstance(stats.client, MemoryClient)
    memory = stats.client.get_memory()
    print(memory)
    assert "test_module.http.request.get./test" in memory
    assert "test_module.http.response_time" in memory