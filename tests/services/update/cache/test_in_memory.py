from app.models.adjacency.node import Node
from app.services.update.cache.in_memory import InMemoryCachingService


def test_get_one_should_succeed_and_return_one_node(
    expected_node_org: Node, in_memory_cache_service: InMemoryCachingService
) -> None:
    in_memory_cache_service.add_node(expected_node_org)
    actual = in_memory_cache_service.get_node(expected_node_org.cache_key())

    assert expected_node_org == actual


def test_get_one_should_return_none_if_id_is_incorrect(
    expected_node_org: Node, in_memory_cache_service: InMemoryCachingService
) -> None:
    in_memory_cache_service.add_node(expected_node_org)
    actual = in_memory_cache_service.get_node("incorrect_id")

    assert actual is None


def test_add_one_should_succeed(
    expected_node_org: Node, in_memory_cache_service: InMemoryCachingService
) -> None:
    in_memory_cache_service.add_node(expected_node_org)

    assert expected_node_org.cache_key() in in_memory_cache_service.keys()


def test_clear_should_succeed(
    expected_node_org: Node, in_memory_cache_service: InMemoryCachingService
) -> None:
    in_memory_cache_service.add_node(expected_node_org)

    in_memory_cache_service.clear()

    assert len(in_memory_cache_service.keys()) == 0


def test_keys_should_succeed_and_return_keys_in_cache(
    expected_node_org: Node, in_memory_cache_service: InMemoryCachingService
) -> None:
    expected = [expected_node_org.cache_key()]

    in_memory_cache_service.add_node(expected_node_org)
    actual = in_memory_cache_service.keys()

    assert expected == actual


def test_make_target_id_should_succeed_and_return_namespaced_id(
    expected_node_org: Node, in_memory_cache_service: InMemoryCachingService
) -> None:
    expected = f"{in_memory_cache_service.namespace}:{in_memory_cache_service.run_id}:{expected_node_org.cache_key()}"

    actual = in_memory_cache_service.make_target_id(expected_node_org.cache_key())

    assert expected == actual


def test_is_healthy_should_always_be_true(
    in_memory_cache_service: InMemoryCachingService,
) -> None:
    actual = in_memory_cache_service.is_healthy()

    assert actual is True
