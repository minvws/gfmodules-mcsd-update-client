# tests/test_bgz_endpoints.py

import importlib
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _import_app_module():
    """Importeer de FastAPI app-module.

    In verschillende PoC iteraties heet de module soms `main`, `app` of `mcsd.app`.
    """

    candidates = ("main", "app", "mcsd.app")
    last_err: Optional[Exception] = None
    for modname in candidates:
        try:
            return importlib.import_module(modname)
        except ModuleNotFoundError as exc:
            last_err = exc
            continue
    raise ModuleNotFoundError(
        "Kon de app-module niet importeren. Verwacht één van: main, app, mcsd.app"
    ) from last_err


def _data_file(appmod, filename: str) -> Path:
    """Vind een data bestand (zoals notification-task.json) op dezelfde plek als de app."""
    base = Path(appmod.__file__).resolve().parent
    return base / "data" / filename


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_dt(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise AssertionError(f"Datetime ontbreekt/ongeldig: {value!r}")
    s = value.strip()
    # Python's fromisoformat accepteert niet altijd 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _assert_urn_uuid(value: str) -> None:
    assert isinstance(value, str)
    assert value.startswith("urn:uuid:"), value
    uuid.UUID(value.split("urn:uuid:", 1)[1])


def assert_task_matches_notification_template(
    *,
    task: Dict[str, Any],
    template: Dict[str, Any],
    sender_ura: str,
    sender_name: str,
    receiver_ura: str,
    receiver_name: str,
    patient_bsn: str,
    patient_name: Optional[str],
    description: Optional[str],
    expected_owner_ref: Optional[str],
    expected_owner_display: Optional[str],
    expected_location_ref: Optional[str],
    expected_location_display: Optional[str],
    expected_workflow_task_id: Optional[str],
) -> None:
    """Asserties voor de gegenereerde BgZ Task op basis van notification-task.json.

    Doel:
    - vaste velden moeten gelijk blijven aan de template (profile, code, input, etc.)
    - dynamische velden worden op vorm/inhoud gevalideerd (UUIDs, datetimes, routing refs)
    """

    # --- Ongewijzigd t.o.v. template ---
    for key in ("resourceType", "id", "meta", "status", "intent", "code", "input"):
        assert task.get(key) == template.get(key), f"Veld '{key}' wijkt af van template"

    # --- groupIdentifier / identifier: UUID URNs maar met zelfde system ---
    assert task["groupIdentifier"]["system"] == template["groupIdentifier"]["system"]
    _assert_urn_uuid(task["groupIdentifier"]["value"])

    assert isinstance(task.get("identifier"), list) and task["identifier"], "identifier ontbreekt"
    assert task["identifier"][0]["system"] == template["identifier"][0]["system"]
    _assert_urn_uuid(task["identifier"][0]["value"])

    # --- basedOn: Workflow Task reference (PoC 9 / TA Notified Pull) ---
    assert isinstance(task.get("basedOn"), list) and task["basedOn"], "basedOn ontbreekt"
    assert isinstance(task["basedOn"][0], dict), "basedOn[0] moet een dict zijn"
    based_on_ref = task["basedOn"][0].get("reference")
    assert isinstance(based_on_ref, str) and based_on_ref.strip(), "basedOn[0].reference ontbreekt"

    if expected_workflow_task_id:
        wf = str(expected_workflow_task_id).strip()
        if "://" in wf or wf.startswith("Task/"):
            expected_ref = wf
        else:
            expected_ref = f"Task/{wf}"
        assert based_on_ref == expected_ref, f"basedOn.reference wijkt af ({based_on_ref} != {expected_ref})"
    else:
        assert based_on_ref.startswith("Task/"), based_on_ref
        uuid.UUID(based_on_ref.split("Task/", 1)[1])

    # --- authoredOn / restriction.end: iso datetimes & ~365 dagen window ---
    authored = _parse_dt(task["authoredOn"])
    end = _parse_dt(task["restriction"]["period"]["end"])
    assert end > authored
    delta_days = (end - authored).days
    assert 364 <= delta_days <= 366, f"restriction.end lijkt geen 365 dagen na authoredOn ({delta_days} dagen)"

    # --- Sender (requester.onBehalfOf) ---
    assert task["requester"]["agent"] == template["requester"]["agent"], "requester.agent moet uit template komen"
    assert task["requester"]["onBehalfOf"]["identifier"]["system"] == template["requester"]["onBehalfOf"]["identifier"]["system"]
    assert task["requester"]["onBehalfOf"]["identifier"]["value"] == sender_ura
    assert task["requester"]["onBehalfOf"]["display"] == sender_name

    # --- Receiver (owner.identifier + display) ---
    assert task["owner"]["identifier"]["system"] == template["owner"]["identifier"]["system"]
    assert task["owner"]["identifier"]["value"] == receiver_ura
    assert task["owner"]["display"] == (expected_owner_display or receiver_name)

    if expected_owner_ref is None:
        assert "reference" not in (task.get("owner") or {}), "owner.reference hoort niet aanwezig te zijn"
    else:
        assert task["owner"]["reference"] == expected_owner_ref

    # --- Location routing ---
    if expected_location_ref is None:
        assert "location" not in task, "Task.location hoort niet aanwezig te zijn"
    else:
        assert task["location"]["reference"] == expected_location_ref
        if expected_location_display is not None:
            assert task["location"].get("display") == expected_location_display

    # --- Patient (Task.for) ---
    assert task["for"]["identifier"]["system"] == template["for"]["identifier"]["system"]
    assert task["for"]["identifier"]["value"] == patient_bsn
    if patient_name:
        assert task["for"].get("display") == patient_name
    else:
        assert "display" not in task.get("for", {}), "for.display hoort niet aanwezig te zijn als patient_name leeg is"

    # --- Description ---
    if description:
        assert task.get("description") == description
    else:
        assert "description" not in task


@dataclass
class DummyResponse:
    status_code: int
    json_data: Optional[Dict[str, Any]] = None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Dict[str, Any]:
        return dict(self.json_data or {})


class FakeHttpClient:
    """Minimal async client stub.

    Belangrijk: FastAPI lifespan sluit app.state.http_client met aclose().
    """

    def __init__(self):
        self.put_calls: List[Dict[str, Any]] = []
        self.post_calls: List[Dict[str, Any]] = []
        self.get_calls: List[Dict[str, Any]] = []

        self._next_put_response: DummyResponse = DummyResponse(200)
        self._next_post_response: DummyResponse = DummyResponse(201, {"resourceType": "Task", "id": "task-1", "status": "requested"})
        self._next_get_response: DummyResponse = DummyResponse(200, {})

    def queue_put_response(self, resp: DummyResponse) -> None:
        self._next_put_response = resp

    def queue_post_response(self, resp: DummyResponse) -> None:
        self._next_post_response = resp

    def queue_get_response(self, resp: DummyResponse) -> None:
        self._next_get_response = resp

    async def put(self, url: str, *, json: Any = None, headers: Optional[Dict[str, str]] = None):
        self.put_calls.append({"url": url, "json": json, "headers": headers or {}})
        return self._next_put_response

    async def post(self, url: str, *, json: Any = None, headers: Optional[Dict[str, str]] = None):
        self.post_calls.append({"url": url, "json": json, "headers": headers or {}})
        return self._next_post_response

    async def get(self, url: str, *, headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None):
        self.get_calls.append({"url": url, "headers": headers or {}, "timeout": timeout})
        return self._next_get_response

    async def aclose(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def appmod(monkeypatch):
    mod = _import_app_module()

    # Forceer test-config zodat tests onafhankelijk zijn van de CI/env.
    monkeypatch.setattr(mod.settings, "api_key", "test-api-key", raising=False)
    monkeypatch.setattr(mod.settings, "sender_ura", "12345678", raising=False)
    monkeypatch.setattr(mod.settings, "sender_name", "Huisartsenpraktijk De Vries", raising=False)
    monkeypatch.setattr(mod.settings, "sender_bgz_base", None, raising=False)
    monkeypatch.setattr(mod.settings, "is_production", False, raising=False)
    monkeypatch.setattr(mod.settings, "allow_task_preview_in_production", True, raising=False)

    return mod


@pytest.fixture()
def client(appmod):
    with TestClient(appmod.app) as c:
        yield c


def _auth_headers(appmod) -> Dict[str, str]:
    return {"X-API-Key": str(appmod.settings.api_key)}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_bgz_load_data_puts_all_resources_and_updates_sender_ura(appmod, client, monkeypatch):
    template = _load_json(_data_file(appmod, "bgz-sample-bundle.json"))

    fake = FakeHttpClient()
    monkeypatch.setattr(appmod.app.state, "http_client", fake, raising=False)

    r = client.post(
        "/bgz/load-data",
        params={"hapi_base": "https://receiver.example/fhir/Task/", "sender_ura": "99999999"},
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is True
    assert body["target"] == "https://receiver.example/fhir"

    # Verwacht: 1 PUT per resource in de bundle.
    expected_resources = [
        (e.get("resource") or {}).get("resourceType")
        for e in (template.get("entry") or [])
        if (e.get("resource") or {}).get("resourceType") and (e.get("resource") or {}).get("id")
    ]
    assert body["resources_created"] == len(expected_resources)
    assert len(fake.put_calls) == len(expected_resources)

    # Specifiek: sender URA in Organization/organization-sender moet zijn aangepast.
    org_call = next(
        c
        for c in fake.put_calls
        if c["url"].endswith("/Organization/organization-sender")
    )
    identifiers = (org_call["json"] or {}).get("identifier") or []
    ura_ident = next(i for i in identifiers if i.get("system") == "http://fhir.nl/fhir/NamingSystem/ura")
    assert ura_ident.get("value") == "99999999"


def test_bgz_preflight_location_routing_and_metadata_probe_ok(appmod, client, monkeypatch):
    async def _fake_capability_mapping(*, target: str, organization: str | None, include_oauth: bool, limit: int):
        return {
            "supported": True,
            "notification": {"base": "https://receiver.example/fhir", "endpoint_id": "ep-1"},
            "mapping": {"twiin_ta_notification": {"chosen": {"id": "ep-1"}}},
            # Preflight gebruikt deze om owner/location routing te tonen.
            "organization": {"reference": "Organization/org-owner", "display": "Ziekenhuis Oost"},
            "target": {"reference": target},
        }

    # Capability mapping stub
    monkeypatch.setattr(appmod, "poc9_msz_capability_mapping", _fake_capability_mapping)

    fake = FakeHttpClient()
    fake.queue_get_response(
        DummyResponse(
            200,
            {
                "resourceType": "CapabilityStatement",
                "rest": [
                    {
                        "resource": [
                            {"type": "Task", "interaction": [{"code": "create"}]}  # Task-create supported
                        ]
                    }
                ],
            },
        )
    )
    monkeypatch.setattr(appmod.app.state, "http_client", fake, raising=False)

    r = client.post(
        "/bgz/preflight",
        json={
            "receiver_org_ref": "Organization/org-fallback",
            "receiver_target_ref": "Location/loc-1",
            "receiver_notification_endpoint_id": "ep-1",
            "check_receiver": True,
            "include_oauth": False,
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["supported"] is True
    assert body["resolved_receiver_base"] == "https://receiver.example/fhir"
    assert body["notification_endpoint_id"] == "ep-1"
    assert body["frontend_endpoint_id_match"] is True

    # Probe output
    probe = body["receiver_probe"]
    assert probe["attempted"] is True
    assert probe["ok"] is True
    assert probe["reachable"] is True
    assert probe["task_create_supported"] is True
    assert body["ready_to_send"] is True

    # Routing output
    routing = body["task_routing"]
    assert routing["target_type"] == "Location"
    assert routing["location_ref"] == "Location/loc-1"
    assert routing["owner_ref"] == "Organization/org-owner"  # Location.managingOrganization (via mapping.organization)


def test_bgz_preflight_not_ready_when_metadata_has_no_task_create(appmod, client, monkeypatch):
    async def _fake_capability_mapping(*, target: str, organization: str | None, include_oauth: bool, limit: int):
        return {
            "supported": True,
            "notification": {"base": "https://receiver.example/fhir", "endpoint_id": "ep-1"},
            "mapping": {"twiin_ta_notification": {"chosen": {"id": "ep-1"}}},
            "organization": {"reference": "Organization/org-owner", "display": "Ziekenhuis Oost"},
            "target": {"reference": target},
        }

    monkeypatch.setattr(appmod, "poc9_msz_capability_mapping", _fake_capability_mapping)

    fake = FakeHttpClient()
    # CapabilityStatement zonder Task.create
    fake.queue_get_response(
        DummyResponse(
            200,
            {
                "resourceType": "CapabilityStatement",
                "rest": [
                    {
                        "resource": [
                            {"type": "Task", "interaction": [{"code": "read"}]}
                        ]
                    }
                ],
            },
        )
    )
    monkeypatch.setattr(appmod.app.state, "http_client", fake, raising=False)

    r = client.post(
        "/bgz/preflight",
        json={
            "receiver_org_ref": "Organization/org-fallback",
            "receiver_target_ref": "Location/loc-1",
            "receiver_notification_endpoint_id": "ep-1",
            "check_receiver": True,
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["receiver_probe"]["attempted"] is True
    assert body["receiver_probe"]["task_create_supported"] is False

    # Verwacht: frontend krijgt duidelijk signaal dat verzenden niet kan.
    assert body["ready_to_send"] is False


def test_bgz_preflight_healthcareservice_routing_owner_ref_only(appmod, client, monkeypatch):
    async def _fake_capability_mapping(*, target: str, organization: str | None, include_oauth: bool, limit: int):
        return {
            "supported": True,
            "notification": {"base": "https://receiver.example/fhir", "endpoint_id": "ep-1"},
            "mapping": {"twiin_ta_notification": {"chosen": {"id": "ep-1"}}},
            "organization": {"reference": "Organization/org-owner", "display": "Ziekenhuis Oost"},
            "target": {"reference": target},
        }

    monkeypatch.setattr(appmod, "poc9_msz_capability_mapping", _fake_capability_mapping)

    fake = FakeHttpClient()
    fake.queue_get_response(DummyResponse(200, {"resourceType": "CapabilityStatement", "rest": []}))
    monkeypatch.setattr(appmod.app.state, "http_client", fake, raising=False)

    r = client.post(
        "/bgz/preflight",
        json={
            "receiver_target_ref": "HealthcareService/hs-1",
            "check_receiver": True,
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    routing = body["task_routing"]
    assert routing["target_type"] == "HealthcareService"
    assert routing["owner_ref"] == "HealthcareService/hs-1"
    assert routing["location_ref"] is None


def test_bgz_task_preview_location_routing_builds_task_from_template(appmod, client, monkeypatch):
    template = _load_json(_data_file(appmod, "notification-task.json"))

    async def _fake_resolve(*, receiver_target_ref: str, receiver_org_ref: str | None, receiver_notification_endpoint_id: str | None):
        return (
            {
                "organization": {"reference": "Organization/org-owner", "display": "Ziekenhuis Oost"},
            },
            "https://receiver.example/fhir",
            "ep-1",
            "Location/loc-1",
            "Organization/org-fallback",
            "Location",
        )

    monkeypatch.setattr(appmod, "_resolve_bgz_notify_destination", _fake_resolve)

    r = client.post(
        "/bgz/task-preview",
        json={
            "receiver_ura": "87654321",
            "receiver_name": "Ziekenhuis Oost - Cardiologie",
            "receiver_org_ref": "Organization/org-fallback",
            "receiver_org_name": "Ziekenhuis Oost",
            "receiver_target_ref": "Location/loc-1",
            "receiver_notification_endpoint_id": "ep-1",
            "patient_bsn": "999999990",
            "patient_name": "Test Patient",
            "description": "BgZ beschikbaar voor test (locatie)",
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is True
    assert body["resolved_receiver_base"] == "https://receiver.example/fhir"
    assert body["notification_endpoint_id"] == "ep-1"
    assert isinstance(body.get("workflow_task_id"), str)
    assert body["workflow_task_id"].strip()

    task = body["task"]
    assert_task_matches_notification_template(
        task=task,
        template=template,
        sender_ura="12345678",
        sender_name="Huisartsenpraktijk De Vries",
        receiver_ura="87654321",
        receiver_name="Ziekenhuis Oost - Cardiologie",
        patient_bsn="999999990",
        patient_name="Test Patient",
        description="BgZ beschikbaar voor test (locatie)",
        expected_owner_ref="Organization/org-owner",
        expected_owner_display="Ziekenhuis Oost",
        expected_location_ref="Location/loc-1",
        expected_location_display="Ziekenhuis Oost - Cardiologie",
        expected_workflow_task_id=body["workflow_task_id"],
    )


def test_bgz_task_preview_healthcareservice_routing_builds_task_from_template(appmod, client, monkeypatch):
    template = _load_json(_data_file(appmod, "notification-task.json"))

    async def _fake_resolve(*, receiver_target_ref: str, receiver_org_ref: str | None, receiver_notification_endpoint_id: str | None):
        return (
            {
                "organization": {"reference": "Organization/org-owner", "display": "Ziekenhuis Oost"},
            },
            "https://receiver.example/fhir",
            "ep-1",
            "HealthcareService/hs-1",
            "Organization/org-fallback",
            "HealthcareService",
        )

    monkeypatch.setattr(appmod, "_resolve_bgz_notify_destination", _fake_resolve)

    r = client.post(
        "/bgz/task-preview",
        json={
            "receiver_ura": "87654321",
            "receiver_name": "Ziekenhuis Oost - Cardiologie",
            "receiver_org_ref": "Organization/org-fallback",
            "receiver_target_ref": "HealthcareService/hs-1",
            "receiver_notification_endpoint_id": "ep-1",
            "patient_bsn": "999999990",
            "patient_name": "Test Patient",
            "description": "BgZ beschikbaar voor test (service)",
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert isinstance(body.get("workflow_task_id"), str)
    assert body["workflow_task_id"].strip()
    task = body["task"]

    assert_task_matches_notification_template(
        task=task,
        template=template,
        sender_ura="12345678",
        sender_name="Huisartsenpraktijk De Vries",
        receiver_ura="87654321",
        receiver_name="Ziekenhuis Oost - Cardiologie",
        patient_bsn="999999990",
        patient_name="Test Patient",
        description="BgZ beschikbaar voor test (service)",
        expected_owner_ref="HealthcareService/hs-1",
        expected_owner_display="Ziekenhuis Oost - Cardiologie",
        expected_location_ref=None,
        expected_location_display=None,
        expected_workflow_task_id=body["workflow_task_id"],
    )



def test_bgz_task_preview_generates_workflow_task_id_when_missing(appmod, client, monkeypatch):
    template = _load_json(_data_file(appmod, "notification-task.json"))

    async def _fake_resolve(*, receiver_target_ref: str, receiver_org_ref: str | None, receiver_notification_endpoint_id: str | None):
        return (
            {
                "organization": {"reference": "Organization/org-owner", "display": "Ziekenhuis Oost"},
            },
            "https://receiver.example/fhir",
            "ep-1",
            "Organization/org-1",
            "Organization/org-1",
            "Organization",
        )

    monkeypatch.setattr(appmod, "_resolve_bgz_notify_destination", _fake_resolve)

    r = client.post(
        "/bgz/task-preview",
        json={
            "receiver_ura": "87654321",
            "receiver_name": "Ziekenhuis Oost",
            "receiver_org_ref": "Organization/org-1",
            "receiver_target_ref": "Organization/org-1",
            "receiver_notification_endpoint_id": "ep-1",
            "patient_bsn": "999999990",
            "patient_name": "Test Patient",
            "description": "BgZ beschikbaar voor test (preview - generated workflow id)",
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert isinstance(body.get("workflow_task_id"), str)
    assert body["workflow_task_id"].strip()
    uuid.UUID(body["workflow_task_id"])

    task = body["task"]
    assert_task_matches_notification_template(
        task=task,
        template=template,
        sender_ura="12345678",
        sender_name="Huisartsenpraktijk De Vries",
        receiver_ura="87654321",
        receiver_name="Ziekenhuis Oost",
        patient_bsn="999999990",
        patient_name="Test Patient",
        description="BgZ beschikbaar voor test (preview - generated workflow id)",
        expected_owner_ref="Organization/org-1",
        expected_owner_display="Ziekenhuis Oost",
        expected_location_ref=None,
        expected_location_display=None,
        expected_workflow_task_id=body["workflow_task_id"],
    )

def test_bgz_notify_posts_task_and_returns_result(appmod, client, monkeypatch):
    template = _load_json(_data_file(appmod, "notification-task.json"))

    async def _fake_resolve(*, receiver_target_ref: str, receiver_org_ref: str | None, receiver_notification_endpoint_id: str | None):
        return (
            {
                "organization": {"reference": "Organization/org-1", "display": "Ziekenhuis Oost"},
            },
            "https://receiver.example/fhir",
            "ep-1",
            "Organization/org-1",
            "Organization/org-1",
            "Organization",
        )

    monkeypatch.setattr(appmod, "_resolve_bgz_notify_destination", _fake_resolve)

    fake = FakeHttpClient()
    fake.queue_post_response(DummyResponse(201, {"resourceType": "Task", "id": "task-123", "status": "requested"}))
    monkeypatch.setattr(appmod.app.state, "http_client", fake, raising=False)

    r = client.post(
        "/bgz/notify",
        json={
            "receiver_ura": "87654321",
            "receiver_name": "Ziekenhuis Oost",
            "receiver_org_ref": "Organization/org-1",
            "receiver_target_ref": "Organization/org-1",
            "receiver_notification_endpoint_id": "ep-1",
            "patient_bsn": "999999990",
            "patient_name": "Test Patient",
            "description": "BgZ beschikbaar voor test (notify)",
            "workflow_task_id": "wf-777",
        },
        headers=_auth_headers(appmod),
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["success"] is True
    assert body["target"] == "https://receiver.example/fhir/Task"
    assert body["resolved_receiver_base"] == "https://receiver.example/fhir"
    assert body["task_id"] == "task-123"
    assert body["task_status"] == "requested"
    assert body.get("workflow_task_id") == "wf-777"

    assert len(fake.post_calls) == 1
    post = fake.post_calls[0]
    assert post["url"] == "https://receiver.example/fhir/Task"
    assert post["headers"].get("Content-Type") == "application/fhir+json"

    sent_task = post["json"]
    assert_task_matches_notification_template(
        task=sent_task,
        template=template,
        sender_ura="12345678",
        sender_name="Huisartsenpraktijk De Vries",
        receiver_ura="87654321",
        receiver_name="Ziekenhuis Oost",
        patient_bsn="999999990",
        patient_name="Test Patient",
        description="BgZ beschikbaar voor test (notify)",
        expected_owner_ref="Organization/org-1",
        expected_owner_display="Ziekenhuis Oost",
        expected_location_ref=None,
        expected_location_display=None,
        expected_workflow_task_id="wf-777",
    )
