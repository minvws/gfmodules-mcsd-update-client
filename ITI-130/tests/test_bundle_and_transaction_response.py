import pytest


def test_bundle_entries_put_by_default(publisher_module, base_cfg):
    resources = [
        {"resourceType": "Organization", "id": "org-kliniek-1", "active": True},
        {"resourceType": "Endpoint", "id": "ep-1", "status": "active", "address": "https://x", "payloadType": [{"coding": [{"system": "s", "code": "c"}]}], "connectionType": {"system": "s", "code": "c"}},
    ]
    entries = publisher_module._bundle_entries(base_cfg, resources)
    assert len(entries) == 2
    assert entries[0]["request"]["method"] == "PUT"
    assert entries[0]["request"]["url"].startswith("Organization/")
    assert "resource" in entries[0]


def test_bundle_entries_delete_inactive_organization(publisher_module, base_cfg):
    cfg = base_cfg.__class__(**{**base_cfg.__dict__, "delete_inactive": True})
    resources = [{"resourceType": "Organization", "id": "org-kliniek-1", "active": False}]
    entries = publisher_module._bundle_entries(cfg, resources)
    assert entries[0]["request"]["method"] == "DELETE"
    assert "resource" not in entries[0]


def test_validate_transaction_response_entries_raises_on_failed_entry(publisher_module, base_cfg):
    req_entries = [
        {"request": {"method": "PUT", "url": "Organization/org-kliniek-1"}, "resource": {"resourceType": "Organization", "id": "org-kliniek-1"}},
        {"request": {"method": "PUT", "url": "Endpoint/ep-1"}, "resource": {"resourceType": "Endpoint", "id": "ep-1"}},
    ]
    resp_payload = {
        "resourceType": "Bundle",
        "type": "transaction-response",
        "entry": [
            {"response": {"status": "200 OK"}},
            {
                "response": {"status": "400 Bad Request"},
                "resource": {
                    "resourceType": "OperationOutcome",
                    "issue": [{"severity": "error", "code": "invalid", "details": {"text": "bad endpoint"}}],
                },
            },
        ],
    }
    with pytest.raises(RuntimeError) as exc:
        publisher_module._validate_transaction_response_entries(base_cfg, req_entries, resp_payload)
    assert "failing entry" in str(exc.value) or "failing entry(ies)" in str(exc.value)
