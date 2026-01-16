import importlib.util
from pathlib import Path
import pytest
import sys


@pytest.fixture(scope="session")
def publisher_module():
    """Load iti130_publisher.py as a module.

    The tests were initially written assuming a layout like:

        <project-root>/iti130_publisher.py
        <project-root>/tests/conftest.py

    In this PoC repository the tests are in the same directory as
    iti130_publisher.py. To keep the test-suite portable we try multiple
    candidate locations.
    """
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "iti130_publisher.py",              # same dir as tests
        here.parents[1] / "iti130_publisher.py",          # classic tests/ layout
        here.parents[2] / "iti130_publisher.py",          # fallback
    ]
    script_path = next((p for p in candidates if p.exists()), None)
    if script_path is None:
        raise FileNotFoundError(
            "Could not find iti130_publisher.py. Tried: " + ", ".join(str(p) for p in candidates)
        )

    spec = importlib.util.spec_from_file_location("iti130_publisher", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def sqlite_conn_str(tmp_path):
    db_path = tmp_path / "test.db"
    return f"sqlite:///{db_path}"


@pytest.fixture()
def base_cfg(publisher_module, sqlite_conn_str):
    # Minimale Config om mapping/sanity te testen (geen echte publish)
    return publisher_module.Config(
        sql_conn=sqlite_conn_str,
        fhir_base="https://fhir.example.test",
        bearer_token=None,
        oauth_token_url=None,
        oauth_client_id=None,
        oauth_client_secret=None,
        oauth_scope=None,
        verify_tls=True,
        bundle_size=50,
        since_utc=None,
        include_meta_profile=False,
        profile_set="nl",
        assigned_id_system_base="https://example.org/identifiers",
        default_ura="33333333",
        default_endpoint_payload_types=publisher_module.DEFAULT_ENDPOINT_PAYLOAD_TYPES_BGZ,
        strict=True,
        bgz_policy="per-clinic",
        mtls_cert=None,
        mtls_key=None,
        include_practitioners=False,
        delete_inactive=False,
        allow_delete_endpoint=False,
        dry_run=True,
        out_file=None,
        timeout_seconds=5,
        connect_timeout_seconds=2.0,
        http_pool_connections=2,
        http_pool_maxsize=2,
        http_retries=0,
        http_backoff_factor=0.1,
        log_level="INFO",
        is_production=False,
    )
