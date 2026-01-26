# tests/test_app.py
import os
import sys
import importlib
import pytest
from fastapi.testclient import TestClient

@pytest.fixture(scope="session")
def appmod():
    # Configure main.py to use the public HAPI FHIR R4 server
    os.environ["MCSD_BASE"] = "https://hapi.fhir.org/baseR4"
    os.environ["UPSTREAM_TIMEOUT"] = "15"
    # Ensure a fresh import so env vars are picked up
    if "main" in sys.modules:
        del sys.modules["main"]
    return importlib.import_module("main")

@pytest.fixture(scope="session")
def client(appmod):
    with TestClient(appmod.app) as c:
        yield c

def _skip_on_upstream_error(resp):
    # If the upstream is unreachable or returns a transient 400, skip gracefully.
    if resp.status_code in (400, 502, 503, 504):
        pytest.skip(f"Upstream not reachable or error {resp.status_code}: {resp.text[:200]}")

def test_healthz(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"

def test_auth_headers_accept(appmod):
    """
    Validate that upstream requests always advertise FHIR JSON via Accept header.
    This test must not trigger any network calls.
    """
    headers = appmod._auth_headers()
    assert headers.get("Accept") == "application/fhir+json"

def test_mcsd_search_practitioner_bundle(client):
    r = client.get("/mcsd/search/Practitioner?family=Smith", headers={"Accept": "application/json"})
    _skip_on_upstream_error(r)
    assert r.status_code == 200
    js = r.json()
    assert js.get("resourceType") in ("Bundle", "OperationOutcome")

def test_mcsd_search_healthcareservice_bundle(client):
    r = client.get("/mcsd/search/HealthcareService?name=card", headers={"Accept": "application/json"})
    _skip_on_upstream_error(r)
    assert r.status_code == 200
    js = r.json()
    assert js.get("resourceType") in ("Bundle", "OperationOutcome")

def test_mcsd_search_organizationaffiliation_bundle(client):
    r = client.get("/mcsd/search/OrganizationAffiliation?active=true", headers={"Accept": "application/json"})
    _skip_on_upstream_error(r)
    assert r.status_code == 200
    js = r.json()
    assert js.get("resourceType") in ("Bundle", "OperationOutcome")

def test_addressbook_search_structure(client):
    r = client.get("/addressbook/search?name=Smith&limit=5", headers={"Accept": "application/json"})
    _skip_on_upstream_error(r)
    assert r.status_code == 200
    js = r.json()
    assert "total" in js and "rows" in js
    assert isinstance(js["rows"], list)
    expected_keys = {
        "practitioner_id","practitioner_name","organization_id","organization_name",
        "role","specialty","city","postal","address","phone","email",
        "service_name","service_type","service_contact","service_org_name",
        "affiliation_primary_org","affiliation_primary_org_name",
        "affiliation_participating_org","affiliation_participating_org_name",
        "affiliation_role"
    }
    if js["rows"]:
        assert expected_keys.issubset(set(js["rows"][0].keys()))
    else:
        # Structure is correct even if no results came back
        assert js["total"] >= 0

@pytest.mark.parametrize(
    "path,expected_type,required_keys",
    [
        ("/addressbook/organization", "Organization", ["id", "name", "email"]),
        ("/addressbook/location", "Location", ["id", "name", "organizationId", "organizationName", "email", "source"]),
    ],
)
def test_addressbook_minimal_schema(client, path, expected_type, required_keys):
    r = client.get(path, params={"name": "test"})
    _skip_on_upstream_error(r)
    assert r.status_code == 200, r.text
    js = r.json()
    assert "count" in js and "items" in js
    assert isinstance(js["items"], list)

    if js["items"]:
        item = js["items"][0]
        assert item.get("resourceType") == expected_type
        for k in required_keys:
            assert k in item


def test_build_params_practitionerrole_allows_chained_keys(appmod):
    incoming = {
        "practitioner.name": "Smith",
        "organization.address-city": "Amsterdam",
        "location.address-postalcode": "1000AA",
        "location.near": "52.37|4.90|10|km",
        "location.near-distance": "10km",
        "_count": "3",
        "_include": "PractitionerRole:practitioner",
        "not_allowed_key": "x",
    }
    params = appmod.build_params("PractitionerRole", incoming)
    assert "practitioner.name" in params
    assert "organization.address-city" in params
    assert "location.address-postalcode" in params
    assert "location.near" in params
    assert "location.near-distance" in params
    assert params.get("_count") == 3
    assert "not_allowed_key" not in params

def test_mcsd_search_practitionerrole_forwards_chained_keys(appmod, client, monkeypatch):
    captured = {}

    async def _fake_get_json(client_obj, url, params):
        captured["url"] = url
        captured["params"] = dict(params or {})
        return {"resourceType": "Bundle", "type": "searchset", "entry": []}

    monkeypatch.setattr(appmod, "_get_json", _fake_get_json)

    r = client.get(
        "/mcsd/search/PractitionerRole"
        "?practitioner.name=Smith"
        "&organization.address-city=Amsterdam"
        "&location.address-postalcode=1000AA"
        "&location.near=52.37|4.90|10|km"
        "&location.near-distance=10km"
        "&_count=3"
        "&_include=PractitionerRole:practitioner"
    )
    assert r.status_code == 200, r.text
    assert captured.get("url", "").endswith("/PractitionerRole")
    p = captured.get("params", {})
    assert p.get("practitioner.name") == "Smith"
    assert p.get("organization.address-city") == "Amsterdam"
    assert p.get("location.address-postalcode") == "1000AA"
    assert p.get("location.near") == "52.37|4.90|10|km"
    assert p.get("location.near-distance") == "10km"
    assert p.get("_count") == 3
    assert p.get("_include") == "PractitionerRole:practitioner"


def test_addressbook_search_maps_chained_aliases(appmod, client, monkeypatch):
    calls = []

    async def _fake_get_json(client_obj, url, params):
        calls.append((url, dict(params or {})))
        # Minimal bundles to let addressbook_search complete
        if url.endswith("/Practitioner"):
            return {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {"resource": {"resourceType": "Practitioner", "id": "p1"}}
                ],
                "link": []
            }
        if url.endswith("/PractitionerRole"):
            return {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {"resource": {
                        "resourceType": "PractitionerRole",
                        "id": "pr1",
                        "practitioner": {"reference": "Practitioner/p1"},
                        "organization": {"reference": "Organization/o1"},
                        "location": [{"reference": "Location/l1"}],
                        "active": True
                    }},
                    {"resource": {
                        "resourceType": "Organization",
                        "id": "o1",
                        "name": "Test Org",
                        "active": True,
                        "telecom": [{"system": "email", "value": "info@test.org"}],
                        "address": [{"city": "Amsterdam"}]
                    }},
                    {"resource": {
                        "resourceType": "Location",
                        "id": "l1",
                        "name": "Test Loc",
                        "status": "active",
                        "telecom": [{"system": "email", "value": "loc@test.org"}],
                        "address": {"city": "Amsterdam", "postalCode": "1000AA"}
                    }},
                ],
                "link": []
            }
        return {"resourceType": "Bundle", "type": "searchset", "entry": [], "link": []}

    monkeypatch.setattr(appmod, "_get_json", _fake_get_json)

    r = client.get(
        "/addressbook/search"
        "?practitioner.name=Smith"
        "&location.near=52.37,4.90"
        "&location.near-distance=10km"
        "&limit=1"
    )
    assert r.status_code == 200, r.text

    # Ensure upstream calls were made and chained aliases were mapped to expected facade keys
    assert any(u.endswith("/Practitioner") for u, _ in calls), calls
    assert any(u.endswith("/PractitionerRole") for u, _ in calls), calls

    prac_call = next(p for (u, p) in calls if u.endswith("/Practitioner"))
    assert prac_call.get("name") == "Smith"
    assert "practitioner.name" not in prac_call

    pr_call = next(p for (u, p) in calls if u.endswith("/PractitionerRole"))
    assert pr_call.get("location.near") == "52.37|4.90|10|km"


def test_addressbook_search_maps_org_name_contains_and_city_filters(appmod, client, monkeypatch):
    async def _fake_get_json(client_obj, url, params):
        if url.endswith("/Practitioner"):
            return {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {"resource": {"resourceType": "Practitioner", "id": "p1", "name": [{"family": "Smith", "given": ["John"]}]}},
                    {"resource": {"resourceType": "Practitioner", "id": "p2", "name": [{"family": "Smith", "given": ["Jane"]}]}},
                ],
            }
        if url.endswith("/PractitionerRole"):
            return {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {"resource": {"resourceType": "PractitionerRole", "id": "pr1",
                                  "practitioner": {"reference": "Practitioner/p1"},
                                  "organization": {"reference": "Organization/o1"},
                                  "location": [{"reference": "Location/l1"}]}},
                    {"resource": {"resourceType": "PractitionerRole", "id": "pr2",
                                  "practitioner": {"reference": "Practitioner/p2"},
                                  "organization": {"reference": "Organization/o2"},
                                  "location": [{"reference": "Location/l2"}]}},
                    {"resource": {"resourceType": "Organization", "id": "o1", "name": "Alpha Hospital"}},
                    {"resource": {"resourceType": "Organization", "id": "o2", "name": "Beta Clinic"}},
                    {"resource": {"resourceType": "Location", "id": "l1", "address": {"city": "Amsterdam", "postalCode": "1000AA"}}},
                    {"resource": {"resourceType": "Location", "id": "l2", "address": {"city": "Utrecht", "postalCode": "3500AA"}}},
                ],
            }
        return {"resourceType": "Bundle", "type": "searchset", "entry": []}

    monkeypatch.setattr(appmod, "_get_json", _fake_get_json)

    r = client.get(
        "/addressbook/search"
        "?practitioner.name=Smith"
        "&organization.name:contains=Alpha"
        "&location.address-city=Amsterdam"
        "&limit=10"
        "&mode=fast"
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get("total") == 1
    assert isinstance(data.get("rows"), list)
    assert len(data["rows"]) == 1
    assert data["rows"][0].get("organization_name") == "Alpha Hospital"
    assert data["rows"][0].get("city") == "Amsterdam"
