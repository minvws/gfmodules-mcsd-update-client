import datetime as dt
import json
import pytest


def test_run_dry_run_writes_valid_transaction_bundle_no_network(publisher_module, base_cfg, tmp_path, monkeypatch):
    out_path = tmp_path / "bundle.json"

    # Assert that no network POST happens even if something goes wrong.
    def _boom(*args, **kwargs):
        raise AssertionError("Network POST was called during dry-run, which should not happen.")

    monkeypatch.setattr(publisher_module.requests.Session, "post", _boom, raising=True)

    cfg = base_cfg.__class__(**{
        **base_cfg.__dict__,
        "dry_run": True,
        "out_file": str(out_path),
        "since_utc": None,
        "bgz_policy": "per-clinic",
    })

    publisher_module.run(cfg)

    assert out_path.exists()
    data = json.loads(out_path.read_text(encoding="utf-8"))

    # Basic Bundle shape
    assert data.get("resourceType") == "Bundle"
    assert data.get("type") == "transaction"
    assert isinstance(data.get("entry"), list)
    assert len(data["entry"]) > 0

    # Validate entries
    for e in data["entry"]:
        assert isinstance(e, dict)
        req = e.get("request")
        assert isinstance(req, dict)
        assert req.get("method") in ("PUT", "DELETE")
        assert isinstance(req.get("url"), str) and "/" in req["url"]

        # PUT entries must have a resource with matching type/id
        if req["method"] == "PUT":
            res = e.get("resource")
            assert isinstance(res, dict)
            rt = res.get("resourceType")
            rid = res.get("id")
            assert isinstance(rt, str) and rt
            assert isinstance(rid, str) and rid
            assert req["url"] == f"{rt}/{rid}"


def test_run_since_future_results_in_no_entries_and_no_file_written(publisher_module, base_cfg, tmp_path, capsys, monkeypatch):
    out_path = tmp_path / "bundle.json"

    def _boom(*args, **kwargs):
        raise AssertionError("Network POST was called; expected no publish attempt.")

    monkeypatch.setattr(publisher_module.requests.Session, "post", _boom, raising=True)

    # Since far in the future => delta filter should yield no entries
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=365)

    cfg = base_cfg.__class__(**{
        **base_cfg.__dict__,
        "dry_run": True,
        "out_file": str(out_path),
        "since_utc": future,
    })

    publisher_module.run(cfg)

    # When there are no entries, run() returns before writing a bundle
    assert not out_path.exists()

    captured = capsys.readouterr()
    assert "No entries to publish" in captured.out


def test_run_since_just_before_seed_results_in_entries_and_writes_file(publisher_module, base_cfg, tmp_path, monkeypatch):
    out_path = tmp_path / "bundle.json"

    def _boom(*args, **kwargs):
        raise AssertionError("Network POST was called during dry-run, which should not happen.")

    monkeypatch.setattr(publisher_module.requests.Session, "post", _boom, raising=True)

    # SQLite seed-data gebruikt een vaste LaatstGewijzigdOp van 2024-01-01T00:00:00.
    # Zet since_utc net daarvoor zodat delta-filtering actief is, maar niet leeg resulteert.
    since_utc = dt.datetime(2023, 12, 31, 23, 59, 0, tzinfo=dt.timezone.utc)

    cfg = base_cfg.__class__(**{
        **base_cfg.__dict__,
        "dry_run": True,
        "out_file": str(out_path),
        "since_utc": since_utc,
    })

    publisher_module.run(cfg)

    assert out_path.exists()
    data = json.loads(out_path.read_text(encoding="utf-8"))
    assert data.get("resourceType") == "Bundle"
    assert data.get("type") == "transaction"
    assert isinstance(data.get("entry"), list)
    assert len(data["entry"]) > 0


def test_bgz_policy_per_afdeling_fails_without_afdeling_endpoint(publisher_module, base_cfg):
    # Dataset:
    # - 1 active Endpoint with BGZ capability
    # - Kliniek Organization references the Endpoint
    # - Afdeling Organization references the Kliniek via partOf but has NO endpoint itself
    # Expected:
    # - per-afdeling => FAIL
    # - per-afdeling-or-clinic => PASS
    BGZ_CODE = "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities"

    ep = {
        "resourceType": "Endpoint",
        "id": "ep-1",
        "status": "active",
        "connectionType": {"system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type", "code": "hl7-fhir-rest"},
        "payloadType": [{"coding": [{"system": publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, "code": BGZ_CODE}]}],
        "address": "https://receiver.example.test/fhir",
    }

    kliniek = {
        "resourceType": "Organization",
        "id": "org-kliniek-1",
        "active": True,
        "name": "Kliniek 1",
        "identifier": [{"system": publisher_module.URA_SYSTEM, "value": "33333333"}],
        "endpoint": [{"reference": "Endpoint/ep-1"}],
    }

    afdeling = {
        "resourceType": "Organization",
        "id": "org-afdeling-10",
        "active": True,
        "name": "Afdeling 10",
        "partOf": {"reference": "Organization/org-kliniek-1"},
        # no endpoint here on purpose
    }

    all_resources = [ep, kliniek, afdeling]
    endpoint_resources_all = [ep]
    organization_resources_all = [kliniek, afdeling]

    cfg_fail = base_cfg.__class__(**{**base_cfg.__dict__, "bgz_policy": "per-afdeling", "strict": True})
    with pytest.raises(RuntimeError):
        publisher_module._sanity_check(
            cfg_fail,
            all_resources,
            since_naive=None,
            endpoint_resources_all=endpoint_resources_all,
            organization_resources_all=organization_resources_all,
        )

    cfg_pass = base_cfg.__class__(**{**base_cfg.__dict__, "bgz_policy": "per-afdeling-or-clinic", "strict": True})
    publisher_module._sanity_check(
        cfg_pass,
        all_resources,
        since_naive=None,
        endpoint_resources_all=endpoint_resources_all,
        organization_resources_all=organization_resources_all,
    )


def test_bgz_policy_per_afdeling_passes_with_afdeling_endpoint(publisher_module, base_cfg):
    BGZ_CODE = "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities"

    ep = {
        "resourceType": "Endpoint",
        "id": "ep-1",
        "status": "active",
        "connectionType": {"system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type", "code": "hl7-fhir-rest"},
        "payloadType": [{"coding": [{"system": publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, "code": BGZ_CODE}]}],
        "address": "https://receiver.example.test/fhir",
    }

    afdeling = {
        "resourceType": "Organization",
        "id": "org-afdeling-10",
        "active": True,
        "name": "Afdeling 10",
        "endpoint": [{"reference": "Endpoint/ep-1"}],
    }

    all_resources = [ep, afdeling]

    cfg = base_cfg.__class__(**{**base_cfg.__dict__, "bgz_policy": "per-afdeling", "strict": True})
    publisher_module._sanity_check(
        cfg,
        all_resources,
        since_naive=None,
        endpoint_resources_all=[ep],
        organization_resources_all=[afdeling],
    )


def test_sanity_check_allows_missing_internal_refs_when_since_is_used(publisher_module, base_cfg):
    # With --since, internal references not present in THIS run are a warning, not a hard error
    since_naive = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    org = {
        "resourceType": "Organization",
        "id": "org-kliniek-1",
        "active": True,
        "name": "Kliniek 1",
        "endpoint": [{"reference": "Endpoint/ep-999"}],  # not present in resources
        "identifier": [{"system": publisher_module.URA_SYSTEM, "value": "33333333"}],
    }

    cfg = base_cfg.__class__(**{**base_cfg.__dict__, "bgz_policy": "off", "strict": True})

    # Should not raise; should warn.
    publisher_module._sanity_check(cfg, [org], since_naive=since_naive)
