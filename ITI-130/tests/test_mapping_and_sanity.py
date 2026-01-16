import datetime as dt
import pytest


def _load_seeded_rows(publisher_module, conn):
    # Gebruik de ingebouwde queries + seed schema
    publisher_module._init_sqlite_schema(conn, seed=True)

    kliniek_rows = publisher_module._fetch_rows(conn, publisher_module._SQLITE_QUERIES["kliniek"])
    kliniek_locatie_rows = publisher_module._fetch_rows(conn, publisher_module._SQLITE_QUERIES["kliniek_locatie"])
    locatie_rows = publisher_module._fetch_rows(conn, publisher_module._SQLITE_QUERIES["locatie"])
    afdeling_rows = publisher_module._fetch_rows(conn, publisher_module._SQLITE_QUERIES["afdeling"])
    endpoint_rows = publisher_module._fetch_rows(conn, publisher_module._SQLITE_QUERIES["endpoint"])
    return kliniek_rows, kliniek_locatie_rows, locatie_rows, afdeling_rows, endpoint_rows


def test_build_resources_from_seed_sqlite_passes_sanity(publisher_module, base_cfg):
    with publisher_module._connect(base_cfg.sql_conn) as conn:
        kliniek_rows, kliniek_locatie_rows, locatie_rows, afdeling_rows, endpoint_rows = _load_seeded_rows(publisher_module, conn)

    # Indexen (zoals run() doet)
    locaties_by_kliniek = {}
    kliniek_by_locatie = {}
    for kl in kliniek_locatie_rows:
        if not bool(kl.get("Actief", True)):
            continue
        kid = int(kl["KliniekId"])
        lid = int(kl["LocatieId"])
        locaties_by_kliniek.setdefault(kid, []).append(lid)
        kliniek_by_locatie.setdefault(lid, kid)

    ep_resources_all, ep_by_kliniek, ep_by_locatie, ep_by_afdeling = publisher_module._build_endpoints(base_cfg, endpoint_rows)
    org_resources_all, ura_by_kliniek, name_by_kliniek = publisher_module._build_organizations(
        base_cfg, kliniek_rows, afdeling_rows, ep_by_kliniek, ep_by_afdeling
    )
    loc_resources_all = publisher_module._build_locations(base_cfg, locatie_rows, ep_by_locatie, ura_by_kliniek, name_by_kliniek)
    svc_resources_all = publisher_module._build_healthcare_services(base_cfg, afdeling_rows, locaties_by_kliniek, ep_by_afdeling, ura_by_kliniek, name_by_kliniek)

    all_resources = []
    all_resources.extend(ep_resources_all)
    all_resources.extend(org_resources_all)
    all_resources.extend(loc_resources_all)
    all_resources.extend(svc_resources_all)

    # Must not raise
    publisher_module._sanity_check(
        base_cfg,
        all_resources,
        since_naive=None,
        endpoint_resources_all=ep_resources_all,
        organization_resources_all=org_resources_all,
    )

    # Extra: check dat er minstens 1 Endpoint is en dat die BGZ payloadType heeft (default)
    eps = [r for r in all_resources if r.get("resourceType") == "Endpoint"]
    assert len(eps) >= 1
    assert publisher_module._endpoint_has_payload_coding(
        eps[0],
        publisher_module.NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
        "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities",
    )


def test_bgz_policy_per_clinic_fails_when_endpoint_not_active(publisher_module, base_cfg):
    # Maak resources en forceer endpoint status off, dan moet sanity check falen (strict)
    with publisher_module._connect(base_cfg.sql_conn) as conn:
        kliniek_rows, kliniek_locatie_rows, locatie_rows, afdeling_rows, endpoint_rows = _load_seeded_rows(publisher_module, conn)

    locaties_by_kliniek = {}
    for kl in kliniek_locatie_rows:
        if not bool(kl.get("Actief", True)):
            continue
        locaties_by_kliniek.setdefault(int(kl["KliniekId"]), []).append(int(kl["LocatieId"]))

    ep_resources_all, ep_by_kliniek, ep_by_locatie, ep_by_afdeling = publisher_module._build_endpoints(base_cfg, endpoint_rows)

    # Forceer: endpoint niet actief
    assert ep_resources_all
    ep_resources_all[0]["status"] = "off"

    org_resources_all, ura_by_kliniek, name_by_kliniek = publisher_module._build_organizations(
        base_cfg, kliniek_rows, afdeling_rows, ep_by_kliniek, ep_by_afdeling
    )
    loc_resources_all = publisher_module._build_locations(base_cfg, locatie_rows, ep_by_locatie, ura_by_kliniek, name_by_kliniek)
    svc_resources_all = publisher_module._build_healthcare_services(base_cfg, afdeling_rows, locaties_by_kliniek, ep_by_afdeling, ura_by_kliniek, name_by_kliniek)

    all_resources = ep_resources_all + org_resources_all + loc_resources_all + svc_resources_all

    with pytest.raises(RuntimeError) as exc:
        publisher_module._sanity_check(
            base_cfg,
            all_resources,
            since_naive=None,
            endpoint_resources_all=ep_resources_all,
            organization_resources_all=org_resources_all,
        )
    assert "No *active* Endpoint with BGZ Server payloadType" in str(exc.value)


def test_internal_reference_integrity_detects_missing_refs_on_full_publish(publisher_module, base_cfg):
    # Maak 1 Organization die naar een niet-bestaand Endpoint verwijst → moet falen (since=None)
    org = {
        "resourceType": "Organization",
        "id": "org-kliniek-1",
        "active": True,
        "name": "Test",
        "endpoint": [{"reference": "Endpoint/ep-999"}],
        "identifier": [{"system": publisher_module.URA_SYSTEM, "value": "33333333"}],
    }
    # resources bevat bewust geen Endpoint/ep-999
    with pytest.raises(RuntimeError) as exc:
        publisher_module._sanity_check(base_cfg, [org], since_naive=None)
    assert "internal references" in str(exc.value)
