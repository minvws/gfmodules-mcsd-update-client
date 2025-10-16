import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.directory_provider.capability_provider import CapabilityProvider
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.models.directory.dto import DirectoryDto


def _dto(id_: str, url: str = "https://example.org/fhir") -> DirectoryDto:
    m = MagicMock(spec=DirectoryDto)
    m.id = id_
    m.endpoint_address = url
    return m


@pytest.fixture
def inner_provider() -> MagicMock:
    return MagicMock(spec=DirectoryProvider)


@pytest.fixture
def provider(inner_provider: MagicMock) -> CapabilityProvider:
    return CapabilityProvider(inner=inner_provider, validate_capability_statement=True)


def test_get_all_directories_filters_when_validating(provider: DirectoryProvider, inner_provider: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    dirs = [_dto("a"), _dto("b"), _dto("c")]
    inner_provider.get_all_directories.return_value = dirs

    def fake_check(dto: DirectoryDto) -> bool:
        return dto.id in {"a", "c"}

    monkeypatch.setattr(CapabilityProvider, "check_capability_statement", staticmethod(fake_check))

    out = provider.get_all_directories(include_ignored=False)
    assert [d.id for d in out] == ["a", "c"]
    inner_provider.get_all_directories.assert_called_once_with(False)


def test_get_all_directories_no_filter_when_validation_off(inner_provider: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    dirs = [_dto("a"), _dto("b")]
    inner_provider.get_all_directories.return_value = dirs

    prov = CapabilityProvider(inner=inner_provider, validate_capability_statement=False)

    called = {"n": 0}

    def fake_check(_dto: DirectoryDto) -> bool:
        called["n"] += 1
        return False

    monkeypatch.setattr(CapabilityProvider, "check_capability_statement", staticmethod(fake_check))

    out = prov.get_all_directories(include_ignored=True)
    assert [d.id for d in out] == ["a", "b"]

    assert called["n"] == 0
    inner_provider.get_all_directories.assert_called_once_with(True)


def test_get_all_directories_include_ignored_ids_filters(provider: DirectoryProvider, inner_provider: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    dirs = [_dto("x"), _dto("y")]
    inner_provider.get_all_directories_include_ignored_ids.return_value = dirs

    monkeypatch.setattr(
        CapabilityProvider, "check_capability_statement", staticmethod(lambda d: d.id == "y")
    )

    out = provider.get_all_directories_include_ignored_ids(include_ignored_ids=["y"])
    assert [d.id for d in out] == ["y"]
    inner_provider.get_all_directories_include_ignored_ids.assert_called_once_with(["y"])


def test_get_one_directory_returns_none_if_capability_fails(provider: DirectoryProvider, inner_provider: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    inner_provider.get_one_directory.return_value = _dto("a")

    monkeypatch.setattr(CapabilityProvider, "check_capability_statement", staticmethod(lambda d: False))

    out = provider.get_one_directory("a")
    assert out is None
    inner_provider.get_one_directory.assert_called_once_with("a")


def test_get_one_directory_propagates_none_from_inner(provider: DirectoryProvider, inner_provider: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    inner_provider.get_one_directory.return_value = None

    called = {"n": 0}
    def fake_check(d: DirectoryDto) -> bool:
        assert d is not None
        called["n"] += 1
        return True

    monkeypatch.setattr(CapabilityProvider, "check_capability_statement", staticmethod(fake_check))

    out = provider.get_one_directory("missing")
    assert out is None
    inner_provider.get_one_directory.assert_called_once_with("missing")
    assert called["n"] == 0


def test_check_capability_statement_success(monkeypatch: pytest.MonkeyPatch, caplog: Any) -> None:
    class FakeFhirApi:
        def __init__(self, **kwargs) -> None: # type: ignore
            assert "base_url" in kwargs
        def validate_capability_statement(self) -> bool:
            return True

    import app.services.directory_provider.capability_provider as mod
    monkeypatch.setattr(mod, "FhirApi", FakeFhirApi)

    dto = _dto("ok", "https://example.org/fhir")
    with caplog.at_level(logging.INFO):
        assert CapabilityProvider.check_capability_statement(dto) is True

    # optional: ensure we logged the INFO line
    assert any("Checking capability statement for ok" in r.message for r in caplog.records)


def test_check_capability_statement_failure_logs_warning(monkeypatch: pytest.MonkeyPatch, caplog: Any) -> None:
    class FakeFhirApi:
        def __init__(self, **kwargs): ...  # type: ignore
        def validate_capability_statement(self) -> bool:
            return False

    import app.services.directory_provider.capability_provider as mod
    monkeypatch.setattr(mod, "FhirApi", FakeFhirApi)

    dto = _dto("bad", "https://bad.example.org/fhir")
    with caplog.at_level(logging.WARNING):
        assert CapabilityProvider.check_capability_statement(dto) is False

    assert any(
        "does not support mCSD requirements" in r.message and "bad" in r.message
        for r in caplog.records
    )


def test_check_capability_statement_exception_logs_error(monkeypatch: pytest.MonkeyPatch, caplog: Any) -> None:
    class FakeFhirApi:
        def __init__(self, **kwargs): # type: ignore
            raise RuntimeError("boom")

    import app.services.directory_provider.capability_provider as mod
    monkeypatch.setattr(mod, "FhirApi", FakeFhirApi)

    dto = _dto("err", "https://err.example.org/fhir")
    with caplog.at_level(logging.ERROR):
        assert CapabilityProvider.check_capability_statement(dto) is False

    assert any("Error checking capability statement for err" in r.message for r in caplog.records)
