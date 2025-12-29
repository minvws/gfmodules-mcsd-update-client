from unittest.mock import patch, MagicMock
from redis import ConnectionError

from app.services.update.cache.external import ExternalCachingService

PATCHED_MODULE = "app.services.update.cache.external.Redis"


@patch(f"{PATCHED_MODULE}.ping")
def test_is_healthy_should_return_false_on_exception(
    mock_ping: MagicMock, external_caching_service: ExternalCachingService
) -> None:
    mock_ping.side_effect = ConnectionError("boom")
    assert external_caching_service.is_healthy() is False


@patch(f"{PATCHED_MODULE}.scan_iter")
@patch(f"{PATCHED_MODULE}.pipeline")
@patch(f"{PATCHED_MODULE}.flushdb")
def test_clear_should_not_flushdb_and_should_delete_only_prefixed_keys(
    mock_flushdb: MagicMock,
    mock_pipeline: MagicMock,
    mock_scan_iter: MagicMock,
    external_caching_service: ExternalCachingService,
) -> None:
    # clear() must not call flushdb()
    mock_flushdb.side_effect = AssertionError("flushdb() should never be called by clear()")

    prefix = external_caching_service.make_target_id("")  # includes prefix + run id
    keys = [f"{prefix}k1".encode("utf-8"), f"{prefix}k2".encode("utf-8"), b"other:k9"]
    mock_scan_iter.return_value = keys

    pipe = MagicMock()
    mock_pipeline.return_value = pipe

    external_caching_service.clear()

    deleted = [call.args[0] for call in pipe.delete.call_args_list]
    assert f"{prefix}k1" in deleted
    assert f"{prefix}k2" in deleted
    assert "other:k9" not in deleted
    pipe.execute.assert_called_once()
