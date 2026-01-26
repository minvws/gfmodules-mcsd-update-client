import json
from dataclasses import replace
from typing import Any, Dict, List, Set

import pytest


def _build_all_resources(publisher_module, cfg) -> List[Dict[str, Any]]:
    """Helper: build resources similarly to run(), but returns the resources list."""
    with publisher_module._connect(cfg.sql_conn) as conn:
        if publisher_module._is_sqlite_conn_str(cfg.sql_conn):
            publisher_module._init_sqlite_schema(conn, seed=True)

        dialect = publisher_module._db_dialect(cfg.sql_conn)
        queries = publisher_module._SQLITE_QUERIES if dialect == "sqlite" else publisher_module._MSSQL_QUERIES

        kliniek_rows = publisher_module._fetch_rows(conn, queries["kliniek"])
        locatie_rows = publisher_module._fetch_rows(conn, queries["locatie"])
        kliniek_locatie_rows = publisher_module._fetch_rows(conn, queries["kliniek_locatie"])
        afdeling_rows = publisher_module._fetch_rows(conn, queries["afdeling"])
        endpoint_rows = publisher_module._fetch_rows(conn, queries["endpoint"])

        # Invert (active) KliniekLocatie mapping: kliniek -> [locatie]
        locaties_by_kliniek: Dict[int, List[int]] = {}
        for kl in kliniek_locatie_rows:
            if not bool(kl.get("Actief", True)):
                continue
            kid = int(kl["KliniekId"])
            lid = int(kl["LocatieId"])
            locaties_by_kliniek.setdefault(kid, []).append(lid)

        # Endpoints first: needed for linking to Organizations/Locations/Services
        ep_resources, ep_by_kliniek, ep_by_locatie, ep_by_afdeling = publisher_module._build_endpoints(cfg, endpoint_rows)

        # Organizations
        org_resources, ura_by_kliniek, name_by_kliniek = publisher_module._build_organizations(
            cfg,
            kliniek_rows,
            afdeling_rows,
            ep_by_kliniek,
            ep_by_afdeling,
        )

        # Locations
        loc_resources = publisher_module._build_locations(cfg, locatie_rows, ep_by_locatie, ura_by_kliniek, name_by_kliniek)

        # Services
        svc_resources = publisher_module._build_healthcare_services(cfg, afdeling_rows, locaties_by_kliniek, ep_by_afdeling, ura_by_kliniek, name_by_kliniek)

        prac_resources: List[Dict[str, Any]] = []
        pracrole_resources: List[Dict[str, Any]] = []
        if cfg.include_practitioners:
            medewerker_rows = publisher_module._fetch_rows(conn, queries["medewerker"])
            inzet_rows = publisher_module._fetch_rows(conn, queries["inzet"])

            medewerker_by_id = {int(m["MedewerkerId"]): m for m in medewerker_rows if m.get("MedewerkerId") is not None}
            afdeling_by_id = {int(a["AfdelingId"]): a for a in afdeling_rows if a.get("AfdelingId") is not None}

            # Invert (active) mapping: locatie -> kliniek
            kliniek_by_locatie: Dict[int, int] = {}
            for kl in kliniek_locatie_rows:
                if not bool(kl.get("Actief", True)):
                    continue
                kid = int(kl["KliniekId"])
                lid = int(kl["LocatieId"])
                kliniek_by_locatie.setdefault(lid, kid)

            prac_resources = publisher_module._build_practitioners(cfg, medewerker_rows, ura_by_kliniek, name_by_kliniek, kliniek_by_locatie)
            pracrole_resources = publisher_module._build_practitioner_roles(
                cfg,
                inzet_rows,
                medewerker_by_id,
                afdeling_by_id,
                locaties_by_kliniek,
                ep_by_kliniek,
                ep_by_locatie,
                ep_by_afdeling,
                ura_by_kliniek,
                name_by_kliniek,
            )

        return [*ep_resources, *org_resources, *loc_resources, *svc_resources, *prac_resources, *pracrole_resources]


def _all_reference_strings(publisher_module, resources: List[Dict[str, Any]]) -> Set[str]:
    refs: Set[str] = set()
    for r in resources:
        for s in publisher_module._iter_reference_strings(r):
            if s:
                refs.add(str(s))
    return refs


def test_iti130_transaction_bundle_entry_semantics(publisher_module, base_cfg):
    """High-level ITI-130 check: we produce a FHIR transaction bundle with valid entry.request fields."""
    resources = _build_all_resources(publisher_module, base_cfg)
    entries = publisher_module._bundle_entries(base_cfg, resources)

    assert entries, "Expected at least one entry"

    base = base_cfg.fhir_base.rstrip("/")
    full_urls = [e.get("fullUrl") for e in entries]
    assert len(full_urls) == len(set(full_urls)), "entry.fullUrl must be unique within the Bundle"
    assert all(isinstance(u, str) and u.startswith(base + "/") for u in full_urls)
    assert all("/_history/" not in u for u in full_urls), "fullUrl must not be version-specific"

    allowed_resource_types = {
        "Organization",
        "Location",
        "HealthcareService",
        "Endpoint",
        "Practitioner",
        "PractitionerRole",
    }

    for e in entries:
        assert "request" in e and isinstance(e["request"], dict)
        method = e["request"].get("method")
        url = e["request"].get("url")
        assert method in {"PUT", "DELETE"}
        assert isinstance(url, str) and "/" in url
        rt, rid = url.split("/", 1)
        assert rt in allowed_resource_types
        assert rid

        if method == "PUT":
            assert "resource" in e
            assert e["resource"].get("resourceType") == rt
            assert e["resource"].get("id") == rid
        else:
            assert "resource" not in e, "DELETE entries must not include an entry.resource"


def test_iti130_publish_posts_transaction_bundle_with_fhir_json(publisher_module, base_cfg):
    """Low-level ITI-130 check: publishing uses HTTP POST with a transaction Bundle.

    We don't perform any real network calls: we stub out the HTTP session and
    return a minimal but valid transaction-response.
    """

    resources = _build_all_resources(publisher_module, base_cfg)
    entries = publisher_module._bundle_entries(base_cfg, resources)

    # Minimal FHIR headers as required by the ITI-130 spec (JSON)
    headers = {
        "Accept": "application/fhir+json",
        "Content-Type": "application/fhir+json",
        "User-Agent": base_cfg.user_agent,
    }

    response_bundle = {
        "resourceType": "Bundle",
        "type": "transaction-response",
        "entry": [{"response": {"status": "200 OK"}} for _ in entries],
    }

    class _DummyResponse:
        def __init__(self, payload):
            self.status_code = 200
            self.headers = {"Content-Type": "application/fhir+json"}
            self._payload = payload
            self.text = json.dumps(payload)

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _DummySession:
        def __init__(self):
            self.calls = []

        def post(self, url, json=None, headers=None, timeout=None):
            self.calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
            return _DummyResponse(response_bundle)

    sess = _DummySession()
    publisher_module._publish_transaction_bundle(base_cfg, sess, headers, entries)

    assert len(sess.calls) == 1
    call = sess.calls[0]
    assert call["url"] == base_cfg.fhir_base.rstrip("/")

    sent = call["json"]
    assert sent["resourceType"] == "Bundle"
    assert sent["type"] == "transaction"
    assert isinstance(sent.get("entry"), list) and len(sent["entry"]) == len(entries)

    assert call["headers"]["Accept"] == "application/fhir+json"
    assert call["headers"]["Content-Type"] == "application/fhir+json"

    assert call["timeout"] == (base_cfg.connect_timeout_seconds, base_cfg.timeout_seconds)


def test_iti130_reference_resolvability(publisher_module, base_cfg):
    """ITI-130 requires references to be resolvable URLs.

    We interpret this as: every Reference.reference is either
    - an absolute URI (http/https/urn/...) OR
    - a relative reference in the form ResourceType/id.
    """
    resources = _build_all_resources(publisher_module, base_cfg)
    refs = _all_reference_strings(publisher_module, resources)

    assert refs, "Expected at least one reference in the generated resources"

    for ref in refs:
        # External and contained references are allowed
        if ref.startswith(("http://", "https://", "urn:", "#")):
            continue
        # Internal references must be of the form ResourceType/id
        assert "/" in ref, f"Reference is not resolvable: {ref!r}"
        rt, rid = ref.split("/", 1)
        assert rt and rid


def test_nl_gf_data_model_linking_rules(publisher_module, base_cfg):
    """NL Generic Functions: enforce the key referential integrity rules used by Update Clients."""
    cfg = replace(base_cfg, include_practitioners=True)
    resources = _build_all_resources(publisher_module, cfg)

    orgs = [r for r in resources if r.get("resourceType") == "Organization"]
    locs = [r for r in resources if r.get("resourceType") == "Location"]
    svcs = [r for r in resources if r.get("resourceType") == "HealthcareService"]
    eps = [r for r in resources if r.get("resourceType") == "Endpoint"]

    assert orgs and locs and svcs and eps

    # 1) Every Organization must be either top-level with URA or have partOf
    for o in orgs:
        idents = o.get("identifier") or []
        has_ura = any(i.get("system") == publisher_module.URA_SYSTEM for i in idents if isinstance(i, dict))
        has_parent = isinstance(o.get("partOf"), dict) and bool(o["partOf"].get("reference"))
        assert has_ura or has_parent, f"Organization/{o.get('id')} must have URA or partOf"

    # 2) Location must link to an Organization
    for l in locs:
        assert isinstance(l.get("managingOrganization"), dict)
        assert "/" in str(l["managingOrganization"].get("reference") or "")

    # 3) HealthcareService must link to an Organization
    for s in svcs:
        assert isinstance(s.get("providedBy"), dict)
        assert "/" in str(s["providedBy"].get("reference") or "")

    # 4) Endpoints must be referenced from at least one Organization or HealthcareService or Location
    endpoint_ids = {str(ep.get("id")) for ep in eps if ep.get("id")}
    referenced: Set[str] = set()
    for r in resources:
        for ref in publisher_module._iter_reference_strings(r):
            if not ref:
                continue
            s = str(ref)
            if s.startswith("Endpoint/"):
                referenced.add(s.split("/", 1)[1])
    assert endpoint_ids.issubset(referenced), "All Endpoint resources must be linked to from other resources"


def test_endpoint_delete_in_transaction_requires_explicit_allow(publisher_module, base_cfg):
    """NL GF specific safety net: endpoints are not deleted by default.

    When delete_inactive=True we delete inactive Organizations/Locations/Services,
    but endpoints should only be deleted when allow_delete_endpoint=True.
    """

    endpoint_off = {
        "resourceType": "Endpoint",
        "id": "ep-x",
        "status": "off",
        "connectionType": {
            "system": publisher_module.ENDPOINT_CONN_SYSTEM,
            "code": "hl7-fhir-rest",
        },
        "payloadType": [
            {
                "coding": [
                    {
                        "system": publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
                        "code": "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities",
                    }
                ]
            }
        ],
        "address": "https://example.invalid/fhir",
        "managingOrganization": {"reference": "Organization/org-kliniek-1"},
    }

    cfg_no_delete = replace(base_cfg, delete_inactive=True, allow_delete_endpoint=False)
    entries = publisher_module._bundle_entries(cfg_no_delete, [endpoint_off])
    assert entries[0]["request"]["method"] == "PUT"

    cfg_allow_delete = replace(base_cfg, delete_inactive=True, allow_delete_endpoint=True)
    entries2 = publisher_module._bundle_entries(cfg_allow_delete, [endpoint_off])
    assert entries2[0]["request"]["method"] == "DELETE"


def test_poc8_9_supports_department_level_routing_and_multiple_endpoint_capabilities(publisher_module, base_cfg, tmp_path):
    """PoC 8/9 addressing & routing:

    * It must be possible to register endpoints on an organisatie-eenheid (afdeling)
    * Endpoints should be able to advertise multiple 'capabilities' (payloadType)
      such as BGZ, eOverdracht, notifications and auth.
    """

    # Configure defaults to include BGZ + eOverdracht + notification + auth
    defaults = (
        (
            publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
            "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities",
            "BGZ Server",
        ),
        (
            publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
            "http://nictiz.nl/fhir/CapabilityStatement/eOverdracht-servercapabilities",
            "eOverdracht Server",
        ),
        (
            publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
            "eOverdracht-notification",
            "eOverdracht TA routering notificatie",
        ),
        (
            publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
            "Nuts-OAuth",
            "Nuts OAuth2",
        ),
    )
    cfg = replace(base_cfg, default_endpoint_payload_types=defaults)

    # Add extra endpoints that are tied to a department (AfdelingId=100).
    # This mirrors the PoC 8/9 requirement to be able to route to a specific
    # organisatie-eenheid and discover multiple technical endpoints (FHIR,
    # notificatie, authenticatie).
    with publisher_module._connect(cfg.sql_conn) as conn:
        publisher_module._init_sqlite_schema(conn, seed=True)
        conn.exec_driver_sql(
            """
            INSERT INTO tblEndpoint (
                endpointkey, kliniekkey, locatiekey, afdelingkey,
                status, connectionTypeSystemUri, connectionTypeCode, connectionTypeDisplay,
                payloadTypeSystemUri, payloadTypeCode, payloadTypeDisplay, payloadMimeType,
                adres, naam, telefoon, email,
                ingangsdatum, einddatum, actief, LaatstGewijzigdOp
            ) VALUES (
                2, 1, NULL, 100,
                'active', ?, ?, ?,
                NULL, NULL, NULL, 'application/fhir+json',
                'https://afdeling.example.test/fhir',
                'Afdeling Endpoint',
                NULL, NULL,
                '2024-01-01', NULL, 1, '2025-01-01'
            )
            """,
            (
                publisher_module.ENDPOINT_CONN_SYSTEM,
                "hl7-fhir-rest",
                "HL7 FHIR REST",
            ),
        )

        # Explicit notification endpoint
        conn.exec_driver_sql(
            """
            INSERT INTO tblEndpoint (
                endpointkey, kliniekkey, locatiekey, afdelingkey,
                status, connectionTypeSystemUri, connectionTypeCode, connectionTypeDisplay,
                payloadTypeSystemUri, payloadTypeCode, payloadTypeDisplay, payloadMimeType,
                adres, naam, telefoon, email,
                ingangsdatum, einddatum, actief, LaatstGewijzigdOp
            ) VALUES (
                3, 1, NULL, 100,
                'active', ?, ?, ?,
                ?, ?, ?, NULL,
                'https://afdeling.example.test/notify',
                'Afdeling Notificatie',
                NULL, NULL,
                '2024-01-01', NULL, 1, '2025-01-01'
            )
            """,
            (
                publisher_module.ENDPOINT_CONN_SYSTEM,
                "hl7-fhir-rest",
                "HL7 FHIR REST",
                publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
                "eOverdracht-notification",
                "eOverdracht TA routering notificatie",
            ),
        )

        # Explicit auth endpoint
        conn.exec_driver_sql(
            """
            INSERT INTO tblEndpoint (
                endpointkey, kliniekkey, locatiekey, afdelingkey,
                status, connectionTypeSystemUri, connectionTypeCode, connectionTypeDisplay,
                payloadTypeSystemUri, payloadTypeCode, payloadTypeDisplay, payloadMimeType,
                adres, naam, telefoon, email,
                ingangsdatum, einddatum, actief, LaatstGewijzigdOp
            ) VALUES (
                4, 1, NULL, 100,
                'active', ?, ?, ?,
                ?, ?, ?, NULL,
                'https://afdeling.example.test/oauth/token',
                'Afdeling Authenticatie',
                NULL, NULL,
                '2024-01-01', NULL, 1, '2025-01-01'
            )
            """,
            (
                publisher_module.ENDPOINT_CONN_SYSTEM,
                "hl7-fhir-rest",
                "HL7 FHIR REST",
                publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
                "Nuts-OAuth",
                "Nuts OAuth2",
            ),
        )
        conn.commit()

    resources = _build_all_resources(publisher_module, cfg)
    afdeling_org = next(r for r in resources if r.get("resourceType") == "Organization" and str(r.get("id", "")).startswith("org-afdeling-"))

    # Department should reference the newly inserted endpoint
    afd_eps = afdeling_org.get("endpoint") or []
    afd_ep_refs = {e.get("reference") for e in afd_eps if isinstance(e, dict)}
    assert "Endpoint/ep-2" in afd_ep_refs
    assert "Endpoint/ep-3" in afd_ep_refs
    assert "Endpoint/ep-4" in afd_ep_refs

    # The endpoint should carry ALL default payloadTypes (because the DB row had none)
    ep2 = next(r for r in resources if r.get("resourceType") == "Endpoint" and r.get("id") == "ep-2")
    payload_types = ep2.get("payloadType") or []
    codes = set()
    for cc in payload_types:
        for coding in cc.get("coding") or []:
            codes.add(coding.get("code"))

    assert "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities" in codes
    assert "http://nictiz.nl/fhir/CapabilityStatement/eOverdracht-servercapabilities" in codes
    assert "eOverdracht-notification" in codes
    assert "Nuts-OAuth" in codes

    # Endpoint/ep-3 should be a dedicated notification endpoint (explicit payloadType)
    ep3 = next(r for r in resources if r.get("resourceType") == "Endpoint" and r.get("id") == "ep-3")
    ep3_codes = {c.get("code") for cc in (ep3.get("payloadType") or []) for c in (cc.get("coding") or [])}
    assert ep3_codes == {"eOverdracht-notification"}

    # Endpoint/ep-4 should be a dedicated auth endpoint (explicit payloadType)
    ep4 = next(r for r in resources if r.get("resourceType") == "Endpoint" and r.get("id") == "ep-4")
    ep4_codes = {c.get("code") for cc in (ep4.get("payloadType") or []) for c in (cc.get("coding") or [])}
    assert ep4_codes == {"Nuts-OAuth"}


def test_meta_profile_declarations_cover_ihe_and_nl_profile_sets(publisher_module, base_cfg):
    """Both profile sets are relevant:

    * IHE mCSD profiles are the international baseline for ITI-130
    * NL GF profiles constrain mCSD for the Dutch addressing function
    """
    for profile_set, expected_map in [
        ("ihe", publisher_module.IHE_MCSD_PROFILE),
        ("nl", publisher_module.NL_GF_PROFILE),
    ]:
        cfg = replace(base_cfg, include_meta_profile=True, profile_set=profile_set)
        resources = _build_all_resources(publisher_module, cfg)
        # Check at least the common resource types
        for r in resources:
            rt = r.get("resourceType")
            if rt not in expected_map:
                continue
            meta = r.get("meta") or {}
            profs = meta.get("profile") or []
            assert expected_map[rt] in profs
