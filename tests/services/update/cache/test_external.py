from unittest.mock import patch, MagicMock
from redis import ConnectionError
from app.models.adjacency.node import Node
from app.services.update.cache.external import ExternalCachingService

PATCHED_MODULE = "app.services.update.cache.external.Redis"


@patch(f"{PATCHED_MODULE}.exists")
@patch(f"{PATCHED_MODULE}.get")
def test_get_one_should_succeed_and_return_node(
    mock_get: MagicMock,
    mock_exists: MagicMock,
    external_caching_service: ExternalCachingService,
    expected_node_org: Node,
) -> None:
    expected_node_org.clear_for_cash()
    target_key = external_caching_service.make_target_id(expected_node_org.resource_id)
    mock_get.return_value = expected_node_org.model_dump_json()
    mock_exists.return_value = True

    actual = external_caching_service.get_node(expected_node_org.resource_id)

    assert expected_node_org == actual
    mock_get.assert_called_once_with(target_key)
    mock_exists.assert_called_once_with(target_key)


@patch(f"{PATCHED_MODULE}.get")
@patch(f"{PATCHED_MODULE}.exists")
def test_get_one_should_return_none_if_node_does_not_exist(
    mock_exists: MagicMock,
    mock_get: MagicMock,
    external_caching_service: ExternalCachingService,
    expected_node_org: Node,
) -> None:
    target_id = external_caching_service.make_target_id(expected_node_org.resource_id)
    mock_exists.return_value = False

    actual = external_caching_service.get_node(expected_node_org.resource_id)

    assert actual is None
    mock_exists.assert_called_once_with(target_id)
    mock_get.assert_not_called()


@patch(f"{PATCHED_MODULE}.set")
def test_add_node_should_succeed(
    mock_set: MagicMock,
    external_caching_service: ExternalCachingService,
    expected_node_org: Node,
) -> None:
    target_id = external_caching_service.make_target_id(expected_node_org.resource_id)
    expected_node_org.clear_for_cash()
    data = expected_node_org.model_dump_json()
    # even though the function beign tested does not return, redis set does. So
    # will set a return value for consisntecy
    mock_set.return_value = expected_node_org.model_dump_json()

    external_caching_service.add_node(expected_node_org)

    mock_set.assert_called_once_with(target_id, data)


@patch(f"{PATCHED_MODULE}.exists")
def test_key_exists_should_return_true_if_key_is_valid(
    mock_exists: MagicMock,
    external_caching_service: ExternalCachingService,
    expected_node_org: Node,
) -> None:
    mock_exists.return_value = 1
    target_id = external_caching_service.make_target_id(expected_node_org.resource_id)

    actual = external_caching_service.key_exists(target_id)

    assert actual is True
    mock_exists.assert_called_once_with(target_id)


@patch(f"{PATCHED_MODULE}.exists")
def test_key_exists_should_return_false_if_key_is_invalid(
    mock_exists: MagicMock,
    external_caching_service: ExternalCachingService,
    expected_node_org: Node,
) -> None:
    mock_exists.return_value = 0
    target_id = external_caching_service.make_target_id(expected_node_org.resource_id)

    actual = external_caching_service.key_exists(target_id)

    assert actual is False
    mock_exists.assert_called_once_with(target_id)


@patch(f"{PATCHED_MODULE}.ping")
def test_is_healthy_should_return_true_if_connection_exits(
    mock_ping: MagicMock,
    external_caching_service: ExternalCachingService,
) -> None:
    mock_ping.return_value = "PONG"

    actual = external_caching_service.is_healthy()

    assert actual is True
    mock_ping.assert_called_once()


@patch(f"{PATCHED_MODULE}.ping")
def test_is_healthy_should_return_false_if_connection_is_down(
    mock_ping: MagicMock, external_caching_service: ExternalCachingService
) -> None:
    mock_ping.side_effect = ConnectionError("Failed to connect")

    actual = external_caching_service.is_healthy()

    assert actual is False
    mock_ping.assert_called_once()


@patch(f"{PATCHED_MODULE}.scan_iter")
def test_keys_should_return_list_of_keys_in_cache(
    mock_scan_iter: MagicMock,
    external_caching_service: ExternalCachingService,
    expected_node_org: Node,
) -> None:
    target_id = external_caching_service.make_target_id(expected_node_org.resource_id)
    mock_scan_iter.return_value = [target_id.encode()]
    expected = [expected_node_org.resource_id]

    actual = external_caching_service.keys()

    assert expected == actual
    mock_scan_iter.assert_called_once()
