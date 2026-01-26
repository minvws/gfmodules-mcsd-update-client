#!/usr/bin/env python3
# ITI-130 (IHE mCSD) Directory Publisher - Minimal ZBC variant
# - Reads existing EPD tables: tblKliniek, tblKliniekLocatie, tblLocatie, tblAfdeling, tblMedewerker, tblMedewerkerinzet, tblRoldefinitie
# - Derives a compact set of mCSD/FHIR resources:
#   - Organization (Kliniek + Afdeling as child-Organization)
#   - Location (Locatie)
#   - HealthcareService (derived 1:1 from Afdeling)
#   - Endpoint (derived from tblEndpoint)
#   - Practitioner + PractitionerRole (Medewerker + Medewerkerinzet) [optional via --include-practitioners]
# - Publishes via FHIR transaction Bundle (ITI-130 Feed)
#
# Notes:
# - Stable logical ids are DERIVED from table keys (no extra FhirId columns needed).
# - Optional: --delete-inactive will publish DELETE requests for inactive records.
# - Optional: --since publishes only rows changed since a timestamp (best-effort; see comments in code).

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import logging
import time
import uuid
import socket
import ssl
import sqlite3
import types
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse, unquote, quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    import pyodbc  # type: ignore
except Exception:
    pyodbc = None  # type: ignore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- begin minimal shim (before importing sqlalchemy_pytds) ---
try:
    import pytds
    from pytds import tds as _tds
    m = types.ModuleType("pytds.tds_session")
    m._token_map = getattr(_tds, "_token_map", {})
    sys.modules.setdefault("pytds.tds_session", m)
except Exception:
    pass
try:
    import sqlalchemy_pytds  # noqa: F401
except Exception:
    sqlalchemy_pytds = None  # type: ignore
# --- end minimal shim ---

# SQLAlchemy is optional.
#
# The publisher can run in a lightweight mode using the built-in sqlite3
# module when connecting to SQLite. For MS SQL Server we still prefer
# SQLAlchemy (mssql+pytds / mssql+pyodbc), but we avoid a hard dependency so
# the PoC test-suite can run in minimal environments.
try:
    from sqlalchemy import create_engine, text, bindparam
    from sqlalchemy.engine import Connection
    from sqlalchemy.exc import OperationalError, SQLAlchemyError

    _HAVE_SQLALCHEMY = True
except ModuleNotFoundError:  # pragma: no cover
    create_engine = None  # type: ignore

    def text(sql: str) -> str:  # type: ignore
        return sql

    def bindparam(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        raise NotImplementedError("bindparam requires SQLAlchemy")

    Connection = Any  # type: ignore

    class OperationalError(Exception):
        """Fallback OperationalError when SQLAlchemy isn't installed."""

    class SQLAlchemyError(Exception):
        """Fallback SQLAlchemyError when SQLAlchemy isn't installed."""

    _HAVE_SQLALCHEMY = False


RUN_ID = str(uuid.uuid4())
_LOG_CONFIGURED = False


def _configure_logging(log_level: str) -> None:
    """Configure Python logging.

    Logs go to stderr so stdout can be used for JSON output in --dry-run mode.
    A run_id is added to each record for easier correlation across messages.
    """
    global _LOG_CONFIGURED

    level_name = str(log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # Ensure every LogRecord has 'run_id' so our formatter never KeyErrors.
    if not _LOG_CONFIGURED:
        old_factory = logging.getLogRecordFactory()

        def record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:  # type: ignore[name-defined]
            record = old_factory(*args, **kwargs)
            setattr(record, "run_id", RUN_ID)
            return record

        logging.setLogRecordFactory(record_factory)
        _LOG_CONFIGURED = True

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s run_id=%(run_id)s %(message)s",
        stream=sys.stderr,
        force=True,
    )


logger = logging.getLogger("iti130.publisher.zbc")


IHE_MCSD_PROFILE = {
    "Organization": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.Organization",
    "OrganizationAffiliation": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.OrganizationAffiliation",
    "Location": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.Location",
    "HealthcareService": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.HealthcareService",
    "Endpoint": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.Endpoint",
    "Practitioner": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.Practitioner",
    "PractitionerRole": "https://profiles.ihe.net/ITI/mCSD/StructureDefinition/IHE.mCSD.PractitionerRole",
}

# Netherlands Generic Functions (GF) Adressering profiles (NL invulling op mCSD)
NL_GF_PROFILE = {
    "Organization": "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-organization",
    "Location": "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-location",
    "HealthcareService": "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-healthcareservice",
    "Endpoint": "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-endpoint",
    "Practitioner": "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-practitioner",
    "PractitionerRole": "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-practitionerrole",
    "OrganizationAffiliation": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-organizationaffiliation"},
}

ORG_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/organization-type"
ENDPOINT_CONN_SYSTEM = "http://terminology.hl7.org/CodeSystem/endpoint-connection-type"

# Nederlandse NamingSystems (FHIR NL)
AGB_ORG_SYSTEM = "http://fhir.nl/fhir/NamingSystem/agb-z"  # organisatie / zorgaanbieder
KVK_SYSTEM = "http://fhir.nl/fhir/NamingSystem/kvk"  # KvK-nummer (optioneel)
URA_SYSTEM = "http://fhir.nl/fhir/NamingSystem/ura"  # URA-nummer (leidend bij organisatie identificatie)
BIG_SYSTEM = "http://fhir.nl/fhir/NamingSystem/big"
UZI_SYSTEM = "http://fhir.nl/fhir/NamingSystem/uzi-nr-pers"
AGB_PRACT_SYSTEM = "http://fhir.nl/fhir/NamingSystem/agb-z"  # AGB-code zorgverlener

PROVENANCE_PARTICIPANT_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/provenance-participant-type"
PROVENANCE_PARTICIPANT_TYPE_AUTHOR = "author"

# Default NL GF Data Exchange Capabilities code system (for Endpoint.payloadType)
NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM = "http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities"
BGZ_SERVER_CAPABILITIES_CODE = "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities"

# NL GF profile of the IHE mCSD ITI-91 endpoint (Care Services Directory Update)
# ("Administration Directory for Update Client" in the NL Generic Functions IG)
MCSD_ITI91_CAPABILITIES_CODE = "http://nuts-foundation.github.io/nl-generic-functions-ig/CapabilityStatement/nl-gf-admin-directory-update-client"


# Default Endpoint.payloadType(s) for SYS ZBC use case.
#
# For the PoC seed data we publish:
# - BGZ server capabilities (for BgZ use-cases)
# - mCSD ITI-91 capabilities (so third-party mCSD update clients can retrieve mCSD resources)
#
# Used when a tblEndpoint row has no payloadType* columns filled.
DEFAULT_ENDPOINT_PAYLOAD_TYPES_BGZ: Tuple[Tuple[str, str, str], ...] = (
    (
        NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
        BGZ_SERVER_CAPABILITIES_CODE,
        "BGZ Server",
    ),
    (
        NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM,
        MCSD_ITI91_CAPABILITIES_CODE,
        "Care Services Directory for Update Client",
    ),
)


FHIR_RESET_DEFAULT_RESOURCE_TYPES: Tuple[str, ...] = (
    "Organization",
    "OrganizationAffiliation",
    "Location",
    "HealthcareService",
    "Endpoint",
    "Practitioner",
    "PractitionerRole",
)

FHIR_RESET_DELETE_ORDER: Tuple[str, ...] = (
    "PractitionerRole",
    "HealthcareService",
    "Location",
    "OrganizationAffiliation",
    "Practitioner",
    "Endpoint",
    "Organization",
)



class Settings(BaseSettings):
    """Environment / .env backed defaults for CLI options.

    CLI arguments always take precedence over Settings values.
    """
    sql_conn: Optional[str] = Field(default=None, validation_alias="SQL_CONN")
    fhir_base: Optional[str] = Field(default=None, validation_alias="FHIR_BASE")
    token: Optional[str] = Field(default=None, validation_alias="FHIR_TOKEN")
    oauth_token_url: Optional[str] = Field(default=None, validation_alias="OAUTH_TOKEN_URL")
    oauth_client_id: Optional[str] = Field(default=None, validation_alias="OAUTH_CLIENT_ID")
    oauth_client_secret: Optional[str] = Field(default=None, validation_alias="OAUTH_CLIENT_SECRET")
    oauth_scope: Optional[str] = Field(default=None, validation_alias="OAUTH_SCOPE")

    bundle_size: int = Field(default=50, validation_alias="BUNDLE_SIZE")
    since_utc: Optional[str] = Field(default=None, validation_alias="SINCE_UTC")
    # SQLite demo only: wipe existing demo rows and recreate seed data at startup.
    sqlite_reset_seed: bool = Field(default=False, validation_alias="SQLITE_RESET_SEED")
    fhir_reset_seed: bool = Field(default=False, validation_alias="FHIR_RESET_SEED")

    profile_set: str = Field(default="nl", validation_alias="PROFILE_SET")
    assigned_id_system_base: str = Field(default="https://sys.local/identifiers", validation_alias="ASSIGNED_ID_SYSTEM_BASE")
    default_ura: Optional[str] = Field(default=None, validation_alias="DEFAULT_URA")
    # Hard override for the URA used to identify the publishing organization (useful for PoC vs production).
    publisher_ura: Optional[str] = Field(default=None, validation_alias="PUBLISHER_URA")
    bgz_policy: str = Field(default="per-clinic", validation_alias="BGZ_POLICY")

    mtls_cert: Optional[str] = Field(default=None, validation_alias="MTLS_CERT")
    mtls_key: Optional[str] = Field(default=None, validation_alias="MTLS_KEY")

    timeout_seconds: int = Field(default=30, validation_alias="HTTP_TIMEOUT")
    connect_timeout_seconds: float = Field(default=10.0, validation_alias="HTTP_CONNECT_TIMEOUT")
    http_retries: int = Field(default=0, validation_alias="HTTP_RETRIES")
    http_backoff_factor: float = Field(default=0.5, validation_alias="HTTP_BACKOFF")
    http_pool_connections: int = Field(default=10, validation_alias="HTTP_POOL_CONNECTIONS")
    http_pool_maxsize: int = Field(default=10, validation_alias="HTTP_POOL_MAXSIZE")

    log_level: str = Field(default="INFO", validation_alias="ITI130_LOG_LEVEL")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)


@dataclass(frozen=True)
class Config:
    sql_conn: str
    fhir_base: str
    # SQLite demo only: wipe existing demo rows and recreate seed data at startup.
    sqlite_reset_seed: bool = False
    # FHIR server reset: wipe all resources before publishing (destructive).
    fhir_reset_seed: bool = False
    bearer_token: Optional[str] = None
    oauth_token_url: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_scope: Optional[str] = None
    verify_tls: bool = True
    bundle_size: int = 50
    since_utc: Optional[dt.datetime] = None
    include_meta_profile: bool = False
    # Which profile set to declare when include_meta_profile is enabled.
    # - "nl"  : NL Generic Functions (GF) Adressering profiles (recommended for PoC 8/9/10)
    # - "ihe" : IHE mCSD profiles (international base)
    profile_set: str = "nl"

    # NL GF profiles require an author-assigned identifier slice (AssignedId) on several resources.
    # This script will generate those identifiers from stable logical ids, using a configurable
    # URI namespace.
    assigned_id_system_base: str = "https://sys.local/identifiers"
    # URA of the organization that issues the AssignedId identifiers (fallback if not found in source).
    default_ura: Optional[str] = None

    # Hard override URA for the publishing organization (useful for PoC vs production; overrides brondata URANummer).
    publisher_ura: Optional[str] = None

    # Default Endpoint.payloadType codings (used when a row has no payloadType values)
    # Tuple items are (system, code, display)
    default_endpoint_payload_types: Tuple[Tuple[str, str, str], ...] = DEFAULT_ENDPOINT_PAYLOAD_TYPES_BGZ

    # Strict validation: raise on missing required NL GF elements instead of emitting placeholders.
    strict: bool = True

    # BGZ-only endpoint sanity policy.
    # Controls how strictly we enforce that active organizations have at least one *active* BGZ-capable Endpoint.
    # Choices:
    # - "off": disable BGZ-specific policy checks
    # - "any": require at least one active BGZ endpoint somewhere in the dataset
    # - "per-clinic": require each active kliniek Organization has >=1 active BGZ endpoint (default)
    # - "per-afdeling": require each active afdeling Organization has >=1 active BGZ endpoint
    # - "per-afdeling-or-clinic": require each active afdeling has >=1 active BGZ endpoint on itself OR its parent kliniek
    bgz_policy: str = "per-clinic"

    # Mutual TLS (mTLS) support.
    mtls_cert: Optional[str] = None
    mtls_key: Optional[str] = None
    include_practitioners: bool = False
    delete_inactive: bool = False
    allow_delete_endpoint: bool = False
    dry_run: bool = False
    out_file: Optional[str] = None
    timeout_seconds: int = 30
    # Separate connect timeout (seconds). Read timeout uses timeout_seconds.
    connect_timeout_seconds: float = 10.0

    # HTTP connection pooling & retry behaviour (requests.Session).
    http_pool_connections: int = 10
    http_pool_maxsize: int = 10
    http_retries: int = 0
    http_backoff_factor: float = 0.5

    # User-Agent for HTTP requests.
    user_agent: str = "ITI130-Publisher-ZBC/9"

    # Logging / safety mode.
    log_level: str = "INFO"
    is_production: bool = False


def _redact_secret(value: Optional[str], keep_last: int = 4) -> str:
    """Redact secrets for logging."""
    if value is None:
        return ""
    s = str(value)
    if not s:
        return ""
    if keep_last <= 0:
        return "***"
    if len(s) <= keep_last:
        return "*" * len(s)
    return ("*" * (len(s) - keep_last)) + s[-keep_last:]


def _describe_auth(cfg: "Config") -> str:
    if cfg.bearer_token:
        return "bearer"
    if cfg.oauth_token_url and cfg.oauth_client_id and cfg.oauth_client_secret:
        return "oauth(client_credentials)"
    if cfg.oauth_token_url:
        return "oauth(incomplete)"
    return "none"


def _validate_and_log_config(cfg: "Config") -> None:
    """Validate configuration and emit startup diagnostics.

    In production mode (cfg.is_production=True) some risky settings become hard errors.
    """
    # Basic config summary (avoid secrets).
    logger.info(
        "Start ITI-130 publisher: fhir_base=%s sql=%s auth=%s verify_tls=%s mtls=%s bundle_size=%s since=%s strict=%s dry_run=%s profile_set=%s include_meta_profile=%s bgz_policy=%s sqlite_reset_seed=%s fhir_reset_seed=%s",
        str(cfg.fhir_base),
        ("sqlite" if _is_sqlite_conn_str(cfg.sql_conn) else "mssql"),
        _describe_auth(cfg),
        bool(cfg.verify_tls),
        ("on" if (cfg.mtls_cert or cfg.mtls_key) else "off"),
        int(cfg.bundle_size),
        (cfg.since_utc.isoformat() if cfg.since_utc else "none"),
        bool(cfg.strict),
        bool(cfg.dry_run),
        str(cfg.profile_set),
        bool(cfg.include_meta_profile),
        str(cfg.bgz_policy),
        bool(getattr(cfg, 'sqlite_reset_seed', False)),
        bool(getattr(cfg, 'fhir_reset_seed', False)),
    )

    # Database URL checks (fail-fast)
    sql_conn = str(cfg.sql_conn or "").strip()
    if not sql_conn:
        raise RuntimeError("--sql-conn is required (or SQL_CONN).")
    if "://" not in sql_conn:
        # Legacy ODBC connection string. Only supported when pyodbc is installed.
        if pyodbc is None:
            raise RuntimeError(
                "--sql-conn lijkt een ODBC connection string, maar pyodbc is niet geïnstalleerd. "
                "Gebruik een SQLAlchemy URL (sqlite:///path.db of mssql+pytds://user:pass@host:1433/db) "
                "of installeer pyodbc."
            )
        logger.info("SQL_CONN detected as ODBC; using SQLAlchemy wrapper mssql+pyodbc:///?odbc_connect=... (redacted).")

    # SQLite demo seed reset option is ONLY safe/meaningful for sqlite connections.
    if bool(getattr(cfg, 'sqlite_reset_seed', False)) and not _is_sqlite_conn_str(sql_conn):
        raise RuntimeError('--sqlite-reset-seed/SQLITE_RESET_SEED is alleen ondersteund voor sqlite:/// verbindingen (SQLite demo).')

    if bool(getattr(cfg, 'fhir_reset_seed', False)):
        if cfg.is_production:
            raise RuntimeError("--fhir-reset-seed is niet toegestaan met --production (destructief).")
        logger.warning('FHIR reset ingeschakeld: alle resources op %s worden verwijderd voor publicatie.', cfg.fhir_base)

    # SQL Server transport security hints (best-effort)
    sql_conn_l = sql_conn.lower()
    if cfg.is_production:
        if "encrypt=no" in sql_conn_l or "encrypt=false" in sql_conn_l:
            raise RuntimeError("In productie moet SQL Server encryptie aan staan (Encrypt=yes).")
        if "trustservercertificate=yes" in sql_conn_l or "trustservercertificate=true" in sql_conn_l:
            raise RuntimeError("In productie is TrustServerCertificate=yes onveilig; gebruik een valide certificaat.")
    else:
        if "encrypt=no" in sql_conn_l or "encrypt=false" in sql_conn_l:
            logger.warning("SQL_CONN zet Encrypt=no (onveilig).")
        if "trustservercertificate=yes" in sql_conn_l or "trustservercertificate=true" in sql_conn_l:
            logger.warning("SQL_CONN zet TrustServerCertificate=yes (onveilig).")

    # URL checks (FHIR base)
    fhir_base = str(cfg.fhir_base or "").strip()
    parsed = urlparse(fhir_base)
    if not parsed.scheme or parsed.scheme not in ("http", "https"):
        raise RuntimeError(f"--fhir-base must be an absolute http(s) URL (got {cfg.fhir_base!r})")

    if cfg.is_production:
        if parsed.scheme != "https":
            raise RuntimeError("In productie moet --fhir-base https:// gebruiken.")
        if not cfg.verify_tls:
            raise RuntimeError("In productie mag TLS verificatie niet uit (--no-verify-tls).")
    else:
        if parsed.scheme != "https":
            logger.warning("FHIR base uses %s:// (recommended: https://).", parsed.scheme)
        if not cfg.verify_tls:
            logger.warning("TLS verificatie staat uit (--no-verify-tls). Dit is onveilig.")

    # OAuth token URL checks
    if cfg.oauth_token_url:
        tok_url = str(cfg.oauth_token_url or "").strip()
        p2 = urlparse(tok_url)
        if not p2.scheme or p2.scheme not in ("http", "https"):
            raise RuntimeError(f"--oauth-token-url must be an absolute http(s) URL (got {cfg.oauth_token_url!r})")
        if cfg.is_production and p2.scheme != "https":
            raise RuntimeError("In productie moet --oauth-token-url https:// gebruiken.")
        if not cfg.is_production and p2.scheme != "https":
            logger.warning("OAuth token URL uses %s:// (recommended: https://).", p2.scheme)

    # Auth completeness hints
    if cfg.bearer_token and cfg.oauth_token_url:
        logger.warning("Zowel --token als OAuth instellingen zijn gezet; --token heeft voorrang.")
    if cfg.oauth_token_url and (not cfg.oauth_client_id or not cfg.oauth_client_secret):
        logger.warning("OAuth token URL is gezet, maar client id/secret ontbreken. Er wordt geen token opgehaald.")
    if not cfg.bearer_token and not cfg.oauth_token_url:
        logger.warning("Geen authenticatie geconfigureerd (geen token/oauth). Server kan 401 teruggeven.")

    # mTLS file existence (fail-fast)
    for label, path in (("mtls_cert", cfg.mtls_cert), ("mtls_key", cfg.mtls_key)):
        if path:
            p = str(path)
            if not os.path.exists(p):
                raise RuntimeError(f"{label} file does not exist: {p!r}")

    # AssignedId namespace (NL GF)
    if str(cfg.assigned_id_system_base or "").strip() == "https://sys.local/identifiers":
        logger.warning(
            "assigned_id_system_base staat nog op de default %s. Gebruik in productie bij voorkeur een eigen, stabiele URI namespace.",
            cfg.assigned_id_system_base,
        )
    else:
        # Best-effort validity check: should be an absolute URI
        ps = urlparse(str(cfg.assigned_id_system_base or ""))
        if not ps.scheme:
            logger.warning("assigned_id_system_base lijkt geen absolute URI: %s", cfg.assigned_id_system_base)

    # Bundle sizing hints
    if cfg.bundle_size and int(cfg.bundle_size) < 10:
        logger.info("bundle-size=%s is klein; publiceren kan trager zijn door meer transacties.", cfg.bundle_size)
    if cfg.bundle_size and int(cfg.bundle_size) > 500:
        logger.warning(
            "bundle-size=%s is groot; sommige FHIR servers kunnen grote transacties weigeren (413/422).",
            cfg.bundle_size,
        )

    # HTTP resiliency hints
    if cfg.is_production and int(getattr(cfg, "http_retries", 0) or 0) <= 0:
        logger.warning("In productie staat http_retries=0; tijdelijke netwerkfouten kunnen een run laten falen. Overweeg --http-retries 3.")

    # Delete policy hints
    if cfg.allow_delete_endpoint and not cfg.delete_inactive:
        logger.warning("--allow-delete-endpoint heeft geen effect zonder --delete-inactive.")
    if cfg.delete_inactive and cfg.allow_delete_endpoint:
        logger.warning("Endpoint DELETE is ingeschakeld. NL GF adviseert doorgaans Endpoint.status=off i.p.v. DELETE.")

    # --since sanity
    if cfg.since_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
        if cfg.since_utc.tzinfo is None:
            logger.warning("--since heeft geen timezone; wordt als UTC geïnterpreteerd.")
        if cfg.since_utc > now_utc + dt.timedelta(seconds=5):
            logger.warning("--since ligt in de toekomst (%s). Dit levert waarschijnlijk 0 wijzigingen op.", cfg.since_utc.isoformat())

    if cfg.include_meta_profile and str(cfg.profile_set or "").strip().lower() == "none":
        logger.warning("--include-meta-profile is gezet maar --profile-set=none; er worden geen meta.profile declaraties toegevoegd.")

    if not cfg.strict:
        logger.warning("Lenient mode (--lenient): ontbrekende bronvelden kunnen worden vervangen door placeholders; controleer output extra goed.")


def _iter_exception_chain(ex: BaseException) -> Iterable[BaseException]:
    while ex is not None:
        yield ex
        ex = ex.__cause__ or ex.__context__


def _classify_requests_exception(e: BaseException) -> Dict[str, str]:
    """Classify request failures into a small set of actionable categories."""
    info: Dict[str, str] = {"reason": "network", "message": "FHIR server is niet bereikbaar."}

    for ex in _iter_exception_chain(e):
        if isinstance(ex, requests.exceptions.Timeout):
            return {"reason": "timeout", "message": "Timeout bij contact met FHIR server."}
        if isinstance(ex, (requests.exceptions.SSLError, ssl.SSLError)):
            return {"reason": "tls", "message": "TLS-fout bij contact met FHIR server."}
        if isinstance(ex, socket.gaierror):
            return {"reason": "dns", "message": "DNS-fout bij contact met FHIR server."}
    return info


def _response_excerpt(resp: requests.Response, limit: int = 2000) -> str:
    try:
        txt = resp.text or ""
    except Exception:
        return "<unable to decode response body>"
    txt = txt.strip()
    if len(txt) > limit:
        return txt[:limit] + "…<truncated>"
    return txt


def _try_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        return None


def _format_operation_outcome(oo: Dict[str, Any]) -> str:
    issues = oo.get("issue") or []
    if not isinstance(issues, list) or not issues:
        return ""
    parts: List[str] = []
    for issue in issues[:5]:
        if not isinstance(issue, dict):
            continue
        sev = str(issue.get("severity") or "").strip()
        code = str(issue.get("code") or "").strip()
        details = ""
        det = issue.get("details")
        if isinstance(det, dict):
            details = str(det.get("text") or "").strip()
        diag = str(issue.get("diagnostics") or "").strip()
        msg = details or diag
        if msg:
            parts.append(f"{sev}:{code} {msg}")
        else:
            parts.append(f"{sev}:{code}")
    return "; ".join([p for p in parts if p])


def _build_http_session(cfg: "Config") -> requests.Session:
    """Create a configured requests.Session for speed & resilience."""
    sess = requests.Session()
    sess.verify = cfg.verify_tls
    sess.cert = _http_cert(cfg)
    sess.headers.update({"User-Agent": str(getattr(cfg, "user_agent", "ITI130-Publisher-ZBC/9"))})

    # Connection pooling + optional retries
    retries = None
    try:
        total = int(getattr(cfg, "http_retries", 0) or 0)
    except Exception:
        total = 0

    if total > 0:
        backoff = float(getattr(cfg, "http_backoff_factor", 0.5) or 0.5)
        try:
            retries = Retry(
                total=total,
                connect=total,
                read=total,
                status=total,
                backoff_factor=backoff,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=None,  # retry on any method (we only use POST)
                respect_retry_after_header=True,
                raise_on_status=False,
            )
        except TypeError:
            # Compatibility with older urllib3 versions (method_whitelist)
            retries = Retry(
                total=total,
                connect=total,
                read=total,
                status=total,
                backoff_factor=backoff,
                status_forcelist=(429, 500, 502, 503, 504),
                method_whitelist=None,  # type: ignore[arg-type]
                respect_retry_after_header=True,
                raise_on_status=False,
            )

    adapter = HTTPAdapter(
        pool_connections=int(getattr(cfg, "http_pool_connections", 10) or 10),
        pool_maxsize=int(getattr(cfg, "http_pool_maxsize", 10) or 10),
        max_retries=retries,
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def _request_timeout(cfg: "Config") -> Tuple[float, float]:
    """requests timeout tuple: (connect, read)."""
    connect = float(getattr(cfg, "connect_timeout_seconds", 10.0) or 10.0)
    read = float(getattr(cfg, "timeout_seconds", 30) or 30)
    return (connect, read)



def _is_sqlite_conn_str(sql_conn: str) -> bool:
    scheme = urlparse(str(sql_conn or "")).scheme.lower()
    return scheme.startswith("sqlite")


def _sqlite_path_from_conn_str(sql_conn: str) -> str:
    p = urlparse(str(sql_conn or ""))
    path = unquote(p.path or "")
    # urlparse('sqlite:////abs/path.db').path -> '//abs/path.db'
    # urlparse('sqlite:///rel.db').path -> '/rel.db'
    # For the sqlite3 module we want:
    #   - absolute paths to remain absolute
    #   - relative paths to become relative
    if path.startswith("//"):
        # absolute path (strip one slash)
        path = path[1:]
    elif path.startswith("/"):
        # relative path (strip leading slash)
        path = path[1:]
    if path == ":memory:":
        return ":memory:"
    return path or ":memory:"


def _sqlalchemy_url_from_conn_str(sql_conn: str) -> str:
    """Return a SQLAlchemy URL for the configured database.

    - If sql_conn already looks like a SQLAlchemy URL (contains '://'), return as-is.
    - Otherwise, treat it as an ODBC connection string and wrap it for SQLAlchemy via pyodbc.
    """
    s = str(sql_conn or "").strip()
    if "://" in s:
        return s
    if pyodbc is None:
        raise RuntimeError(
            "SQL_CONN lijkt een ODBC connection string, maar pyodbc is niet geïnstalleerd. "
            "Gebruik een SQLAlchemy URL (bijv. sqlite:///path.db of mssql+pytds://user:pass@host:1433/db) "
            "of installeer pyodbc."
        )
    return "mssql+pyodbc:///?odbc_connect=" + quote_plus(s)


@contextmanager
def _connect(sql_conn: str) -> Iterable[Connection]:
    """Open a database connection.

    Prefer SQLAlchemy when available (needed for MS SQL Server connectivity).
    If SQLAlchemy is not installed we still support SQLite connections using the
    built-in sqlite3 module so the PoC can be validated with a lightweight
    dependency set.
    """

    # Lightweight sqlite3 fallback (no SQLAlchemy dependency)
    if not _HAVE_SQLALCHEMY:
        if not _is_sqlite_conn_str(sql_conn):
            raise RuntimeError(
                "SQLAlchemy is required for non-sqlite connections. "
                "Install sqlalchemy (and a suitable MSSQL driver) or use a sqlite:/// connection string."
            )

        class _SQLiteConn:
            """Small wrapper to mimic the few SQLAlchemy Connection methods we use."""

            def __init__(self, conn: sqlite3.Connection):
                self._conn = conn

            def exec_driver_sql(self, sql: str, params: Any = None):
                if params is None:
                    return self._conn.execute(sql)
                return self._conn.execute(sql, params)

            def execute(self, sql: Any, params: Any = None):
                stmt = str(sql)
                if params is None:
                    return self._conn.execute(stmt)
                return self._conn.execute(stmt, params)

            def commit(self) -> None:
                self._conn.commit()

            def close(self) -> None:
                self._conn.close()

        path = _sqlite_path_from_conn_str(sql_conn)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        logger.info("Using sqlite3 connection (SQLAlchemy not installed). path=%s", path)
        wrapped = _SQLiteConn(conn)
        try:
            yield wrapped  # type: ignore[misc]
        finally:
            try:
                conn.commit()
            except Exception:
                pass
            conn.close()
        return

    # SQLAlchemy path
    url = _sqlalchemy_url_from_conn_str(sql_conn)
    engine = create_engine(url, future=True, echo=False, pool_pre_ping=True)
    try:
        try:
            url_safe = engine.url.render_as_string(hide_password=True)
        except Exception:
            url_safe = str(engine.url)
        logger.info("Effective SQLAlchemy dialect: %s (url=%s)", getattr(engine.dialect, "name", ""), url_safe)
        with engine.connect() as conn:
            yield conn
    finally:
        engine.dispose()


def _fetch_rows(conn: Connection, sql: str, params: Any = ()) -> List[Dict[str, Any]]:
    if params is None:
        params = ()
    if isinstance(params, dict):
        stmt = text(sql)
        for k, v in params.items():
            if isinstance(v, (list, tuple, set)):
                stmt = stmt.bindparams(bindparam(k, expanding=True))
        result = conn.execute(stmt, params)
    else:
        result = conn.exec_driver_sql(sql, params)
    rows: List[Dict[str, Any]] = []

    # SQLAlchemy Result -> use mappings()
    if hasattr(result, "mappings"):
        for r in result.mappings().all():
            rows.append(dict(r))
        return rows

    # DB-API cursor fallback (sqlite3)
    try:
        fetched = result.fetchall()
    except Exception:
        fetched = []
    for r in fetched:
        if isinstance(r, sqlite3.Row):
            rows.append(dict(r))
        elif isinstance(r, dict):
            rows.append(r)
        elif isinstance(r, (tuple, list)) and getattr(result, "description", None):
            cols = [d[0] for d in result.description]
            rows.append({cols[i]: r[i] for i in range(min(len(cols), len(r)))})
        else:
            rows.append({"value": r})
    return rows



def _db_dialect(sql_conn: str) -> str:
    return "sqlite" if _is_sqlite_conn_str(sql_conn) else "mssql"


def _db_preflight(conn: Connection, dialect: str, cfg: "Config") -> None:
    """Fail-fast checks to catch misconfiguration early (connectivity + expected tables)."""
    try:
        conn.execute(text("SELECT 1"))
    except Exception as e:
        raise RuntimeError(f"Database preflight failed (SELECT 1): {e}") from e
    logger.info("Database preflight OK (dialect=%s).", dialect)

    if dialect == "mssql":
        required = ["tblKliniek", "tblKliniekLocatie", "tblLocatie", "tblAfdeling", "tblEndpoint"]
        if cfg.include_practitioners:
            required.extend(["tblMedewerker", "tblMedewerkerinzet", "tblRoldefinitie"])

        missing: List[str] = []
        for t in required:
            try:
                row = conn.execute(
                    text(
                        "SELECT 1 AS ok FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name"
                    ),
                    {"schema": "dbo", "name": t},
                ).fetchone()
            except Exception as exc:
                logger.warning("Preflight: could not verify existence of dbo.%s (%s).", t, type(exc).__name__)
                continue
            if row is None:
                missing.append(f"dbo.{t}")

        if missing:
            msg = "Database preflight: missing required tables/views: " + ", ".join(missing)
            if cfg.is_production:
                raise RuntimeError(msg)
            logger.warning("%s", msg)



def _isoformat(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    s = str(value).strip()
    return s if s else None


def _iso_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10]).isoformat()
    except Exception:
        return s


def _coerce_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return dt.date.fromisoformat(s[:10])
        except Exception:
            return None
    return None


def _coerce_datetime(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.min)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            s2 = s.replace("Z", "+00:00")
            d = dt.datetime.fromisoformat(s2)
            if d.tzinfo is not None:
                d = d.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return d
        except Exception:
            return None
    return None


def _init_sqlite_schema(conn: Connection, seed: bool = True, reset_seed: bool = False) -> None:
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblKliniek (
        kliniekkey INTEGER PRIMARY KEY,
        achternaam TEXT,
        [AGB code] TEXT,
        kvknummer TEXT,
        actief INTEGER,
        ingangsdatum TEXT,
        einddatum TEXT,
        telefoonnummer TEXT,
        [e-mail adres] TEXT,
        [web site] TEXT,
        straat TEXT,
        huisnummer TEXT,
        postcode TEXT,
        woonplaats TEXT,
        landcode TEXT,
        uranummer TEXT,
        typeSystemUri TEXT,
        typeCode TEXT,
        typeDisplay TEXT,
        LaatstGewijzigdOp TEXT
    )""")
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblKliniekLocatie (
        kliniekkey INTEGER,
        locatiekey INTEGER,
        actief INTEGER,
        LaatstGewijzigdOp TEXT
    )""")
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblLocatie (
        locatiekey INTEGER PRIMARY KEY,
        [AGB code] TEXT,
        locatieomschrijving TEXT,
        locatietype TEXT,
        actief INTEGER,
        telefoonnummer TEXT,
        [e-mail] TEXT,
        straat TEXT,
        huisnummer TEXT,
        postcode TEXT,
        woonplaats TEXT,
        landcode TEXT,
        latitude REAL,
        longitude REAL,
        typeSystemUri TEXT,
        typeCode TEXT,
        typeDisplay TEXT,
        LaatstGewijzigdOp TEXT
    )""")
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblAfdeling (
        afdelingkey INTEGER PRIMARY KEY,
        kliniekkey INTEGER,
        locatiekey INTEGER,
        afdelingomschrijving TEXT,
        actief INTEGER,
        specialismeSystemUri TEXT,
        [specialisme code] TEXT,
        specialismeDisplay TEXT,
        typeSystemUri TEXT,
        typeCode TEXT,
        typeDisplay TEXT,
        LaatstGewijzigdOp TEXT
    )""")

    # Endpoint als eigen resource (v5 schema)
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblEndpoint (
        endpointkey INTEGER PRIMARY KEY,
        kliniekkey INTEGER NOT NULL,
        locatiekey INTEGER,
        afdelingkey INTEGER,
        status TEXT,
        connectionTypeSystemUri TEXT,
        connectionTypeCode TEXT,
        connectionTypeDisplay TEXT,
        payloadTypeSystemUri TEXT,
        payloadTypeCode TEXT,
        payloadTypeDisplay TEXT,
        payloadMimeType TEXT,
        adres TEXT,
        naam TEXT,
        telefoon TEXT,
        email TEXT,
        ingangsdatum TEXT,
        einddatum TEXT,
        actief INTEGER,
        LaatstGewijzigdOp TEXT
    )""")
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblMedewerker (
        medewerkerkey INTEGER PRIMARY KEY,
        [default locatiekey] INTEGER,
        einddatum TEXT,
        actief INTEGER,
        naamWeergave TEXT,
        achternaam TEXT,
        voornaam TEXT,
        tussenvoegsel TEXT,
        [BIG-nummer] TEXT,
        [code zorgverlener] TEXT,
        [e-mail] TEXT,
        telefoonnummer_werk_mobiel TEXT,
        telefoonnummer_werk_vast TEXT,
        geslacht TEXT,
        geboortedatum TEXT,
        LaatstGewijzigdOp TEXT
    )""")
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblMedewerkerinzet (
        medewerkerinzetkey INTEGER PRIMARY KEY,
        medewerkerkey INTEGER,
        afdelingkey INTEGER,
        ingangsdatum TEXT,
        einddatum TEXT,
        actief INTEGER,
        roldefinitiekey INTEGER,
        LaatstGewijzigdOp TEXT
    )""")
    conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS tblRoldefinitie (
        roldefinitiekey INTEGER PRIMARY KEY,
        rolCodeSystemUri TEXT,
        rolCode TEXT,
        rolDisplay TEXT,
        LaatstGewijzigdOp TEXT
    )""")
    if reset_seed:
        logger.warning(
            "SQLite demo: --sqlite-reset-seed/SQLITE_RESET_SEED is ingeschakeld; bestaande rijen in demo-tabellen worden gewist en de ingebouwde seed data wordt opnieuw aangemaakt."
        )
        # Delete in dependency order (child -> parent) to be safe if foreign keys are enabled.
        for t in (
            'tblMedewerkerinzet',
            'tblMedewerker',
            'tblRoldefinitie',
            'tblEndpoint',
            'tblAfdeling',
            'tblKliniekLocatie',
            'tblLocatie',
            'tblKliniek',
        ):
            conn.exec_driver_sql(f"DELETE FROM {t}")
        conn.commit()
        if not seed:
            return

    if not seed:

        conn.commit()
        return
    res = conn.exec_driver_sql("SELECT COUNT(*) FROM tblKliniek")
    existing = int((res.fetchone() or [0])[0] or 0)
    if existing:
        conn.commit()
        return
    now = dt.datetime(2024, 1, 1, 0, 0, 0).isoformat()
    conn.exec_driver_sql("""INSERT INTO tblKliniek (
        kliniekkey, achternaam, [AGB code], kvknummer, actief, ingangsdatum, einddatum, telefoonnummer, [e-mail adres], [web site],
        straat, huisnummer, postcode, woonplaats, landcode,
        uranummer, typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        1, "ZBC Demo Kliniek", "00000000", "12345678", 1, "2000-01-01", None, "+31-20-0000000", "info@demo.invalid", "https://demo.invalid",
        "Demo Straat", "1", "1011AA", "Amsterdam", "NL",
        "00700700", ORG_TYPE_SYSTEM, "prov", "Healthcare Provider",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblLocatie (
        locatiekey, [AGB code], locatieomschrijving, locatietype, actief, telefoonnummer, [e-mail],
        straat, huisnummer, postcode, woonplaats, landcode, latitude, longitude,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        10, "00000000", "Demo Locatie", "hospital", 1, "+31-20-1111111", "locatie@demo.invalid",
        "Locatie Straat", "10", "1011AA", "Amsterdam", "NL", 52.3702, 4.8952,
        "http://terminology.hl7.org/CodeSystem/v3-RoleCode", "HOSP", "Hospital",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblKliniekLocatie (
        kliniekkey, locatiekey, actief, LaatstGewijzigdOp
    ) VALUES (?,?,?,?)""", (1, 10, 1, now))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        100, 1, 10, "Beweegpoli", 1,
        "http://snomed.info/sct", "1251536003", "Sport medicine",
        "http://terminology.hl7.org/CodeSystem/service-type", "491", "Exercise Physiology",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        101, 1, 10, "Dermatologie (huidziekten)", 1,
        "http://snomed.info/sct", "394582007", "Dermatology",
        "http://terminology.hl7.org/CodeSystem/service-type", "168", "Dermatology",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        102, 1, 10, "Interne geneeskunde", 1,
        "http://snomed.info/sct", "419192003", "Internal medicine",
        "http://terminology.hl7.org/CodeSystem/service-type", "382", "Medical Services",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        103, 1, 10, "Leefstijl coaching", 1,
        "http://snomed.info/sct", "722164000", "Dietetics and nutrition",
        "http://terminology.hl7.org/CodeSystem/service-type", "553", "1-on-1 Support /Mentoring /Coaching",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        104, 1, 10, "Orthopedie", 1,
        "http://snomed.info/sct", "394801008", "Trauma & orthopaedics",
        "http://terminology.hl7.org/CodeSystem/service-type", "218", "Orthopaedic Surgery",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        105, 1, 10, "Penispoli", 1,
        "http://snomed.info/sct", "394612005", "Urology",
        "http://terminology.hl7.org/CodeSystem/service-type", "222", "Urology",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        106, 1, 10, "Plastische chirurgie", 1,
        "http://snomed.info/sct", "394611003", "Plastic surgery",
        "http://terminology.hl7.org/CodeSystem/service-type", "220", "Plastic & Reconstructive Surgery",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        107, 1, 10, "Proctologie (anus problemen)", 1,
        "http://snomed.info/sct", "408464004", "Colorectal surgery",
        "http://terminology.hl7.org/CodeSystem/service-type", "221", "Surgery - General",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        108, 1, 10, "Reumatologie (ontstekingen gewrichten)", 1,
        "http://snomed.info/sct", "394810000", "Rheumatology",
        "http://terminology.hl7.org/CodeSystem/service-type", "182", "Rheumatology",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        109, 1, 10, "Spatader- & wondzorg (vaatchirurgie en dermatologie)", 1,
        "http://snomed.info/sct", "408463005", "Vascular surgery",
        "http://terminology.hl7.org/CodeSystem/service-type", "223", "Vascular Surgery",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        110, 1, 10, "Vasectomie/sterilisatie", 1,
        "http://snomed.info/sct", "394612005", "Urology",
        "http://terminology.hl7.org/CodeSystem/service-type", "54", "Family Planning",
        now
    ))
    conn.exec_driver_sql("""INSERT INTO tblAfdeling (
        afdelingkey, kliniekkey, locatiekey, afdelingomschrijving, actief,
        specialismeSystemUri, [specialisme code], specialismeDisplay,
        typeSystemUri, typeCode, typeDisplay,
        LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        111, 1, 10, "Vulvapoli (derma en gynaecologie)", 1,
        "http://snomed.info/sct", "394586005", "Gynaecology",
        "http://terminology.hl7.org/CodeSystem/service-type", "567", "Women's Health Clinic",
        now
    ))

    # Demo endpoint op kliniekniveau
    conn.exec_driver_sql("""INSERT INTO tblEndpoint (
        endpointkey, kliniekkey, locatiekey, afdelingkey,
        status, connectionTypeSystemUri, connectionTypeCode, connectionTypeDisplay,
        payloadTypeSystemUri, payloadTypeCode, payloadTypeDisplay, payloadMimeType,
        adres, naam, telefoon, email,
        ingangsdatum, einddatum, actief, LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        900, 1, None, None,
        "active", ENDPOINT_CONN_SYSTEM, "hl7-fhir-rest", "HL7 FHIR REST",
        # Leave payloadType* empty in the SQLite seed row so the publisher will apply
        # cfg.default_endpoint_payload_types (which includes both BGZ and ITI-91).
        None, None, None,
        "application/fhir+json",
        "https://mach2.disyepd.com/notifiedpull/fhir", "FHIR API", "+31-20-0000000", "fhir@demo.invalid",
        "2020-01-01", None, 1, now
    ))
    conn.exec_driver_sql("""INSERT INTO tblRoldefinitie (
        roldefinitiekey, rolCodeSystemUri, rolCode, rolDisplay, LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?)""", (1, "http://terminology.hl7.org/CodeSystem/practitioner-role", "doctor", "Doctor", now))
    conn.exec_driver_sql("""INSERT INTO tblMedewerker (
        medewerkerkey, [default locatiekey], einddatum, actief, naamWeergave, achternaam, voornaam, tussenvoegsel, [BIG-nummer], [code zorgverlener],
        [e-mail], telefoonnummer_werk_mobiel, telefoonnummer_werk_vast, geslacht, geboortedatum, LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        1000, 10, None, 1, "Dr. John Smith", "Smith", "John", None, "12345678901", "99999999",
        "john.smith@demo.invalid", "+31-6-12345678", None, "male", "1980-05-12", now
    ))
    conn.exec_driver_sql("""INSERT INTO tblMedewerkerinzet (
        medewerkerinzetkey, medewerkerkey, afdelingkey, ingangsdatum, einddatum, actief, roldefinitiekey, LaatstGewijzigdOp
    ) VALUES (?,?,?,?,?,?,?,?)""", (5000, 1000, 100, "2020-01-01", None, 1, 1, now))
    conn.commit()

_SQLITE_QUERIES: Dict[str, str] = {
    "kliniek": """
            SELECT
                k.kliniekkey AS KliniekId,
                k.achternaam AS Naam,
                k.[AGB code] AS AGBCode,
                k.kvknummer AS KvKNummer,
                k.actief AS Actief,
                k.ingangsdatum AS StartDatum,
                k.einddatum AS EindDatum,
                k.telefoonnummer AS Telefoon,
                k.[e-mail adres] AS Email,
                k.[web site] AS Website,
                TRIM(COALESCE(k.straat,'') || CASE WHEN k.straat IS NOT NULL AND k.huisnummer IS NOT NULL THEN ' ' ELSE '' END || COALESCE(k.huisnummer,'')) AS AdresRegel1,
                NULL AS AdresRegel2,
                k.postcode AS Postcode,
                k.woonplaats AS Plaats,
                k.landcode AS Land,
                k.uranummer AS URANummer,
                k.typeSystemUri AS TypeSystemUri,
                k.typeCode AS TypeCode,
                k.typeDisplay AS TypeDisplay,
                k.LaatstGewijzigdOp
            FROM tblKliniek k
    """,
    "kliniek_locatie": """
            SELECT
                kl.kliniekkey AS KliniekId,
                kl.locatiekey AS LocatieId,
                kl.actief AS Actief,
                kl.LaatstGewijzigdOp
            FROM tblKliniekLocatie kl
    """,
    "locatie": """
            SELECT
                l.locatiekey AS LocatieId,
                (
                    SELECT kl.kliniekkey
                    FROM tblKliniekLocatie kl
                    WHERE kl.locatiekey = l.locatiekey AND kl.actief = 1
                    ORDER BY kl.kliniekkey
                    LIMIT 1
                ) AS KliniekId,
                l.[AGB code] AS AGBCode,
                l.locatieomschrijving AS Naam,
                l.locatietype AS LocatieType,
                l.actief AS Actief,
                l.telefoonnummer AS Telefoon,
                l.[e-mail] AS Email,
                TRIM(COALESCE(l.straat,'') || CASE WHEN l.straat IS NOT NULL AND l.huisnummer IS NOT NULL THEN ' ' ELSE '' END || COALESCE(l.huisnummer,'')) AS AdresRegel1,
                NULL AS AdresRegel2,
                l.postcode AS Postcode,
                l.woonplaats AS Plaats,
                l.landcode AS Land,
                l.latitude AS Latitude,
                l.longitude AS Longitude,
                l.typeSystemUri AS TypeSystemUri,
                l.typeCode AS TypeCode,
                l.typeDisplay AS TypeDisplay,
                l.LaatstGewijzigdOp
            FROM tblLocatie l
    """,
    "afdeling": """
            SELECT
                a.afdelingkey AS AfdelingId,
                a.kliniekkey AS KliniekId,
                a.locatiekey AS LocatieId,
                a.afdelingomschrijving AS Naam,
                a.actief AS Actief,
                a.specialismeSystemUri AS SpecialismeSystemUri,
                a.[specialisme code] AS SpecialismeCode,
                a.specialismeDisplay AS SpecialismeDisplay,
                a.typeSystemUri AS TypeSystemUri,
                a.typeCode AS TypeCode,
                a.typeDisplay AS TypeDisplay,
                a.LaatstGewijzigdOp
            FROM tblAfdeling a
    """,

    "endpoint": """
            SELECT
                e.endpointkey AS EndpointId,
                e.kliniekkey AS KliniekId,
                e.locatiekey AS LocatieId,
                e.afdelingkey AS AfdelingId,
                e.status AS Status,
                e.connectionTypeSystemUri AS ConnectionTypeSystemUri,
                e.connectionTypeCode AS ConnectionTypeCode,
                e.connectionTypeDisplay AS ConnectionTypeDisplay,
                e.payloadTypeSystemUri AS PayloadTypeSystemUri,
                e.payloadTypeCode AS PayloadTypeCode,
                e.payloadTypeDisplay AS PayloadTypeDisplay,
                e.payloadMimeType AS PayloadMimeType,
                e.adres AS Address,
                e.naam AS Name,
                e.telefoon AS Telefoon,
                e.email AS Email,
                e.ingangsdatum AS StartDatum,
                e.einddatum AS EindDatum,
                e.actief AS Actief,
                e.LaatstGewijzigdOp
            FROM tblEndpoint e
    """,
    "medewerker": """
            SELECT
                m.medewerkerkey AS MedewerkerId,
                m.[default locatiekey] AS LocatieId,
                NULL AS AfdelingId,
                m.einddatum AS EindDatum,
                m.actief AS Actief,
                m.naamWeergave AS NaamWeergave,
                m.achternaam AS Achternaam,
                m.voornaam AS Voornaam,
                m.tussenvoegsel AS Tussenvoegsel,
                m.[BIG-nummer] AS BIGNummer,
                m.[code zorgverlener] AS AGBZorgverlenerCode,
                m.[e-mail] AS Email,
                COALESCE(m.telefoonnummer_werk_mobiel, m.telefoonnummer_werk_vast) AS Telefoon,
                m.geslacht AS Geslacht,
                m.geboortedatum AS Geboortedatum,
                m.LaatstGewijzigdOp
            FROM tblMedewerker m
    """,
    "inzet": """
            SELECT
                i.medewerkerinzetkey AS InzetId,
                i.medewerkerkey AS MedewerkerId,
                i.afdelingkey AS AfdelingId,
                a.kliniekkey AS KliniekId,
                a.locatiekey AS LocatieId,
                i.ingangsdatum AS StartDatum,
                i.einddatum AS EindDatum,
                i.actief AS Actief,
                r.rolCodeSystemUri AS RolCodeSystemUri,
                r.rolCode AS RolCode,
                r.rolDisplay AS RolDisplay,
                CASE WHEN r.LaatstGewijzigdOp > i.LaatstGewijzigdOp THEN r.LaatstGewijzigdOp ELSE i.LaatstGewijzigdOp END AS LaatstGewijzigdOp
            FROM tblMedewerkerinzet i
            INNER JOIN tblAfdeling a ON a.afdelingkey = i.afdelingkey
            LEFT JOIN tblRoldefinitie r ON r.roldefinitiekey = i.roldefinitiekey
    """,
}



def _prune(obj: Any) -> Any:
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            pv = _prune(v)
            if pv is None:
                continue
            if pv == []:
                continue
            if pv == {}:
                continue
            new[k] = pv
        return new
    if isinstance(obj, list):
        new_list = []
        for v in obj:
            pv = _prune(v)
            if pv is None:
                continue
            if pv == []:
                continue
            if pv == {}:
                continue
            new_list.append(pv)
        return new_list
    return obj


def _codeable_concept(system: Optional[Any], code: Optional[Any], display: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    if system is None or system == "":
        return None
    if code is None or code == "":
        return None
    c: Dict[str, Any] = {"coding": [{"system": str(system), "code": str(code)}]}
    if display is not None and str(display) != "":
        c["coding"][0]["display"] = str(display)
    return c


def _codeable_concept_text(text: Optional[Any]) -> Optional[Dict[str, Any]]:
    """Create a CodeableConcept with only 'text'.

    Useful when the binding strength is extensible and no suitable code is available.
    """
    if text is None:
        return None
    t = str(text).strip()
    if not t:
        return None
    return {"text": t}


def _coding(system: Optional[Any], code: Optional[Any], display: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """Create a FHIR Coding (used e.g. by Endpoint.connectionType)."""
    if system is None or system == "":
        return None
    if code is None or code == "":
        return None
    c: Dict[str, Any] = {"system": str(system), "code": str(code)}
    if display is not None and str(display) != "":
        c["display"] = str(display)
    return c


def _profiles_for(resource_type: str, cfg: Config) -> Optional[List[str]]:
    if not cfg.include_meta_profile:
        return None
    ps = (cfg.profile_set or "nl").lower().strip()
    if ps in ("none", "off", "false", "0"):
        return None
    if ps == "ihe":
        return [IHE_MCSD_PROFILE[resource_type]]
    # Default: NL GF
    return [NL_GF_PROFILE[resource_type]]


def _assigned_id_system(cfg: Config, bucket: str) -> str:
    base = (cfg.assigned_id_system_base or "").rstrip("/")
    if not base:
        # Fallback to a non-resolvable but valid URI.
        base = "urn:sys:assigned-ids"
    return f"{base}/{bucket.lstrip('/')}"



def _normalize_ura(value: Any) -> str:
    """Normalize URA (UZI-register abonneenummer) values.

    The NL ecosystem typically represents URA as an 8-digit numeric string (incl. leading zeros),
    while some sources/logs use shorter numeric forms or the prefix 'URA:'.

    Rules:
      - Trim whitespace
      - Strip an optional 'URA:' prefix
      - Remove internal whitespace
      - If the remaining value is numeric and shorter than 8 digits, left-pad with zeros to 8 digits
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if s.upper().startswith("URA:"):
        s = s.split(":", 1)[1].strip()
    # Remove any whitespace characters inside the string
    s = "".join(ch for ch in s if not ch.isspace())
    if s.isdigit() and len(s) < 8:
        s = s.zfill(8)
    return s


def _issuer_ura(cfg: Config, kliniek_id: Optional[int], ura_by_kliniek: Dict[int, str]) -> str:
    """Resolve the URA to use as issuer/assigner for a given resource.

    Precedence:
      1) cfg.publisher_ura (hard override; PoC/prod switch without DB changes)
      2) ura_by_kliniek[kliniek_id] (resolved from brondata)
      3) cfg.default_ura (fallback)
    """
    if cfg.publisher_ura:
        u = _normalize_ura(cfg.publisher_ura)
        if u:
            return u
    if kliniek_id is not None:
        u = _normalize_ura(ura_by_kliniek.get(int(kliniek_id)))
        if u:
            return u
    return _normalize_ura(cfg.default_ura)
def _ura_assigner_reference(ura_value: str, display: Optional[str] = None) -> Dict[str, Any]:
    """Build the Reference(Organization) used as Identifier.assigner in NL GF AssignedId slices.

    NL GF profiles constrain Reference.identifier to:
      - identifier.type.coding = provenance-participant-type#author
      - identifier.system = URA naming system
      - identifier.value = URA value
    """
    ref: Dict[str, Any] = {
        "identifier": {
            "type": {
                "coding": [
                    {
                        "system": PROVENANCE_PARTICIPANT_TYPE_SYSTEM,
                        "code": PROVENANCE_PARTICIPANT_TYPE_AUTHOR,
                    }
                ]
            },
            "system": URA_SYSTEM,
            "value": _normalize_ura(ura_value),
        }
    }
    if display:
        ref["display"] = str(display)
    return ref


def _assigned_id_identifier(system: str, value: str, assigner_ura: str, assigner_display: Optional[str] = None) -> Dict[str, Any]:
    return {
        "system": str(system),
        "value": str(value),
        "assigner": _ura_assigner_reference(assigner_ura, assigner_display),
    }


def _http_cert(cfg: Config) -> Optional[Any]:
    """requests' 'cert' parameter value."""
    if cfg.mtls_cert and cfg.mtls_key:
        return (cfg.mtls_cert, cfg.mtls_key)
    if cfg.mtls_cert:
        return cfg.mtls_cert
    return None


def _require(cfg: Config, condition: bool, message: str) -> None:
    if condition:
        return
    if cfg.strict:
        raise RuntimeError(message)
    logger.warning("%s", message)


def _ref(resource_type: str, fhir_id: str) -> Dict[str, Any]:
    return {"reference": f"{resource_type}/{fhir_id}"}


def _id(prefix: str, key: Any) -> str:
    return f"{prefix}-{key}"


def _org_kliniek_id(kliniek_id: int) -> str:
    return f"org-kliniek-{kliniek_id}"


def _org_afdeling_id(afdeling_id: int) -> str:
    return f"org-afdeling-{afdeling_id}"


def _loc_id(locatie_id: int) -> str:
    return f"loc-{locatie_id}"


def _svc_id(afdeling_id: int) -> str:
    return f"svc-afdeling-{afdeling_id}"


def _prac_id(medewerker_id: int) -> str:
    return f"prac-{medewerker_id}"


def _pracrole_id(inzet_id: int) -> str:
    return f"pracrole-{inzet_id}"


def _ep_id(endpoint_id: int) -> str:
    # Stable logical id derived from tblEndpoint.endpointkey
    return f"ep-{endpoint_id}"



def _get_bearer_token(cfg: Config, session: Optional[requests.Session] = None) -> Optional[str]:
    """Return bearer token from config or OAuth2 client_credentials.

    When a session is provided, its TLS/mTLS configuration is reused.
    """
    if cfg.bearer_token:
        return cfg.bearer_token
    if not cfg.oauth_token_url:
        return None
    if not cfg.oauth_client_id or not cfg.oauth_client_secret:
        return None

    data: Dict[str, Any] = {
        "grant_type": "client_credentials",
        "client_id": cfg.oauth_client_id,
        "client_secret": cfg.oauth_client_secret,
    }
    if cfg.oauth_scope:
        data["scope"] = cfg.oauth_scope

    req_kwargs: Dict[str, Any] = {"timeout": _request_timeout(cfg)}
    if session is None:
        req_kwargs["verify"] = cfg.verify_tls
        req_kwargs["cert"] = _http_cert(cfg)

    # Do NOT send FHIR content-type headers to the token endpoint.
    token_headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}

    try:
        resp = (session.post if session is not None else requests.post)(
            str(cfg.oauth_token_url),
            data=data,
            headers=token_headers,
            **req_kwargs,
        )
        resp.raise_for_status()
    except requests.HTTPError:
        body = _response_excerpt(resp) if "resp" in locals() else ""
        oo = _try_json(resp) if "resp" in locals() else None
        if isinstance(oo, dict) and oo.get("resourceType") == "OperationOutcome":
            msg = _format_operation_outcome(oo)
            if msg:
                logger.error("OAuth token endpoint returned HTTP %s: %s", resp.status_code, msg)
        logger.error("OAuth token endpoint returned HTTP error. Body: %s", body)
        raise
    except requests.exceptions.RequestException as e:
        info = _classify_requests_exception(e)
        logger.error("OAuth token request failed reason=%s: %s", info.get("reason"), info.get("message"))
        raise

    payload = _try_json(resp)
    if not isinstance(payload, dict):
        raise RuntimeError(f"OAuth token endpoint did not return JSON (status={resp.status_code}).")

    tok = payload.get("access_token")
    if not tok:
        raise RuntimeError("OAuth token response had no access_token")
    return str(tok)


def _bundle_entries(cfg: Config, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Create transaction Bundle entries.

    Notes:
    - For NL Generic Functions (GF) ITI-130 usage, endpoints are typically *not deleted*.
      If an endpoint is not active anymore, publish it with status=off instead.
      Set --allow-delete-endpoint to override.
    """
    entries: List[Dict[str, Any]] = []
    base = cfg.fhir_base.rstrip("/")
    for r in resources:
        rtype = r["resourceType"]
        rid = r["id"]

        method = "PUT"
        url = f"{rtype}/{rid}"

        if cfg.delete_inactive:
            if rtype in ("Organization", "HealthcareService", "Practitioner", "PractitionerRole"):
                if r.get("active") is False:
                    method = "DELETE"
            if rtype == "Location":
                if r.get("status") in ("inactive", "off", "entered-in-error"):
                    method = "DELETE"
            if rtype == "Endpoint":
                # Do not delete endpoints unless explicitly allowed.
                if cfg.allow_delete_endpoint and r.get("status") in ("off", "entered-in-error"):
                    method = "DELETE"

        entry = {
            "fullUrl": f"{base}/{rtype}/{rid}",
            "request": {"method": method, "url": url},
        }
        if method != "DELETE":
            entry["resource"] = r
        entries.append(entry)
    return entries



def _iter_reference_strings(obj: Any) -> Iterable[str]:
    """Yield all Reference.reference strings within a (nested) FHIR resource dict."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "reference" and isinstance(v, str):
                yield v
            else:
                yield from _iter_reference_strings(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_reference_strings(item)


def _endpoint_has_payload_coding(endpoint: Dict[str, Any], system: str, code: str) -> bool:
    """Check if Endpoint.payloadType contains a coding(system, code)."""
    pts = endpoint.get("payloadType") or []
    if not isinstance(pts, list):
        return False
    for cc in pts:
        if not isinstance(cc, dict):
            continue
        codings = cc.get("coding") or []
        if not isinstance(codings, list):
            continue
        for c in codings:
            if not isinstance(c, dict):
                continue
            if str(c.get("system") or "") == system and str(c.get("code") or "") == code:
                return True
    return False

def _endpoint_is_active(endpoint: Dict[str, Any]) -> bool:
    """Return True if Endpoint.status is 'active'."""
    return str(endpoint.get("status") or "").strip().lower() == "active"






def _indent_lines(text: str, prefix: str = "    ") -> str:
    return "\n".join(prefix + line for line in str(text or "").splitlines())


def _identifier_values(resource: Dict[str, Any], system: str) -> List[str]:
    """Return identifier.value entries for a given identifier.system."""
    out: List[str] = []
    ids = resource.get("identifier") or []
    if not isinstance(ids, list):
        return out
    for ident in ids:
        if not isinstance(ident, dict):
            continue
        if str(ident.get("system") or "") != str(system):
            continue
        val = ident.get("value")
        if val is None:
            continue
        s = str(val).strip()
        if s and s not in out:
            out.append(s)
    return out


def _org_debug_label(org: Dict[str, Any]) -> str:
    """Human-friendly org label incl. name + key identifiers (URA/AGB/KvK) when present."""
    oid = str(org.get("id") or "")
    name = str(org.get("name") or "").strip()

    ura = _identifier_values(org, URA_SYSTEM)
    agb = _identifier_values(org, AGB_ORG_SYSTEM)
    kvk = _identifier_values(org, KVK_SYSTEM)

    parts: List[str] = []
    if name:
        parts.append(f"name={name!r}")
    if ura:
        parts.append(f"URA={ura[0]}")
    if agb:
        parts.append(f"AGB={agb[0]}")
    if kvk:
        parts.append(f"KvK={kvk[0]}")

    active = org.get("active")
    if active is not None:
        parts.append(f"active={bool(active)}")

    return f"Organization/{oid}" + (f" ({', '.join(parts)})" if parts else "")


def _extract_endpoint_ids(endpoint_refs: Any) -> List[str]:
    ids: List[str] = []
    if not isinstance(endpoint_refs, list):
        return ids
    for ref in endpoint_refs:
        if not isinstance(ref, dict):
            continue
        rstr = str(ref.get("reference") or "")
        if not rstr.startswith("Endpoint/"):
            continue
        eid = rstr.split("/", 1)[1]
        if eid and eid not in ids:
            ids.append(eid)
    return ids


def _endpoint_payload_labels(endpoint: Dict[str, Any], bgz_code: str) -> List[str]:
    """Return a compact list of payloadType labels (prefer display; map BGZ code to 'BGZ Server')."""
    out: List[str] = []
    pts = endpoint.get("payloadType") or []
    if not isinstance(pts, list):
        return out

    for cc in pts:
        if not isinstance(cc, dict):
            continue
        codings = cc.get("coding") or []
        if isinstance(codings, list):
            for c in codings:
                if not isinstance(c, dict):
                    continue
                sys_uri = str(c.get("system") or "")
                code = str(c.get("code") or "")
                disp = str(c.get("display") or "").strip()

                if sys_uri == NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM and code == bgz_code:
                    label = disp or "BGZ Server"
                else:
                    label = disp or code

                label = str(label or "").strip()
                if label and label not in out:
                    out.append(label)

        txt = str(cc.get("text") or "").strip()
        if txt and txt not in out:
            out.append(txt)

    return out


def _endpointkey_from_fhir_id(endpoint_id: str) -> Optional[int]:
    eid = str(endpoint_id or "")
    if not eid.startswith("ep-"):
        return None
    try:
        return int(eid.split("-", 1)[1])
    except Exception:
        return None


def _endpoint_debug_line(endpoint: Dict[str, Any], bgz_code: str) -> str:
    eid = str(endpoint.get("id") or "")
    endpointkey = _endpointkey_from_fhir_id(eid)
    status = str(endpoint.get("status") or "").strip()
    address = str(endpoint.get("address") or "").strip()

    labels = _endpoint_payload_labels(endpoint, bgz_code)
    is_active = _endpoint_is_active(endpoint)
    is_bgz = _endpoint_has_payload_coding(endpoint, NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, bgz_code)

    flags = ["active" if is_active else "not-active", "BGZ" if is_bgz else "not-BGZ"]
    ek = f"endpointkey={endpointkey}" if endpointkey is not None else "endpointkey=?"

    # Keep this line readable but informative (key fields for brondata fixes).
    return (
        f"Endpoint/{eid} ({ek}, status={status!r}, address={address!r}, payloadType={labels or []}, {', '.join(flags)})"
    )


def _describe_org_endpoints(endpoint_refs: Any, ep_by_id: Dict[str, Dict[str, Any]], bgz_code: str, max_items: int = 10) -> str:
    """Describe the endpoint situation for an Organization.endpoint list.

    Includes categorization to speed up troubleshooting:
    - active BGZ-capable
    - inactive BGZ-capable
    - active non-BGZ
    - inactive non-BGZ
    """
    ids = _extract_endpoint_ids(endpoint_refs)
    if not ids:
        return (
            "Endpoints referenced: 0\n"
            "Fix: add at least one Endpoint reference on the Organization (and a matching tblEndpoint row) for BGZ."
        )

    missing = [eid for eid in ids if eid not in ep_by_id]
    found = [ep_by_id[eid] for eid in ids if eid in ep_by_id]

    active_bgz: List[Dict[str, Any]] = []
    inactive_bgz: List[Dict[str, Any]] = []
    active_not_bgz: List[Dict[str, Any]] = []
    inactive_not_bgz: List[Dict[str, Any]] = []

    for ep in found:
        is_active = _endpoint_is_active(ep)
        is_bgz = _endpoint_has_payload_coding(ep, NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, bgz_code)
        if is_active and is_bgz:
            active_bgz.append(ep)
        elif (not is_active) and is_bgz:
            inactive_bgz.append(ep)
        elif is_active and (not is_bgz):
            active_not_bgz.append(ep)
        else:
            inactive_not_bgz.append(ep)

    def _endpointkeys(eps: List[Dict[str, Any]]) -> List[str]:
        out: List[str] = []
        for ep in eps:
            eid = str(ep.get("id") or "")
            if eid.startswith("ep-"):
                out.append(eid.split("-", 1)[1])
            elif eid:
                out.append(eid)
        return out

    lines: List[str] = []
    lines.append(f"Endpoints referenced: {len(ids)} (found: {len(found)}, missing: {len(missing)})")
    lines.append(f"Active BGZ-capable endpoints: {len(active_bgz)}")
    lines.append(f"Inactive BGZ-capable endpoints: {len(inactive_bgz)}")
    lines.append(f"Active non-BGZ endpoints: {len(active_not_bgz)}")
    lines.append(f"Inactive non-BGZ endpoints: {len(inactive_not_bgz)}")

    if missing:
        ex = ", ".join(missing[:max_items]) + (" ..." if len(missing) > max_items else "")
        lines.append(f"Missing Endpoint resources for refs: {ex}")

    if found:
        lines.append(f"Referenced endpoints (up to {max_items}):")
        for ep in found[:max_items]:
            lines.append("  - " + _endpoint_debug_line(ep, bgz_code))

    # Fix hints to speed up brondata corrections
    hints: List[str] = []
    if not active_bgz:
        if inactive_bgz:
            keys = ", ".join(_endpointkeys(inactive_bgz)[:max_items])
            hints.append(
                "At least one Endpoint has the BGZ payloadType but is not active. "
                f"Consider activating it (tblEndpoint.Status='active', Actief=1, StartDatum/EindDatum). Candidate endpointkey(s): {keys}."
            )
        if active_not_bgz:
            keys = ", ".join(_endpointkeys(active_not_bgz)[:max_items])
            hints.append(
                "At least one Endpoint is active but not marked as BGZ. "
                "Fill tblEndpoint.PayloadTypeSystemUri/PayloadTypeCode for BGZ. "
                f"Candidate endpointkey(s): {keys}."
            )
        if not (inactive_bgz or active_not_bgz):
            keys = ", ".join(_endpointkeys(found)[:max_items])
            hints.append(
                "No referenced Endpoint is both active and BGZ-capable. "
                f"Check referenced endpointkey(s): {keys} (status + payloadType)."
            )

    if hints:
        lines.append("Fix hints:")
        for h in hints:
            lines.append("  - " + h)

    return "\n".join(lines)


def _is_unknown_codeable_concept(cc: Any) -> bool:
    if not isinstance(cc, dict):
        return False
    txt = str(cc.get("text") or "").strip().lower()
    if txt != "unknown":
        return False
    codings = cc.get("coding")
    return not codings  # no codings present


def _sanity_check(
    cfg: Config,
    resources: List[Dict[str, Any]],
    since_naive: Optional[dt.datetime],
    endpoint_resources_all: Optional[List[Dict[str, Any]]] = None,
    organization_resources_all: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Lightweight sanity checks before publishing.

    This is intentionally NOT a full FHIR profile validation. The goal is to catch the most
    common mapping/config mistakes early (e.g. missing ids, broken internal references,
    missing payloadTypes, invalid endpoint URLs).

    BGZ-only additional policy (cfg.bgz_policy):
      - off: no BGZ endpoint policy enforcement
      - any: require at least one *active* BGZ endpoint somewhere
      - per-clinic: require each active kliniek Organization has >=1 *active* BGZ endpoint
      - per-afdeling: require each active afdeling Organization has >=1 *active* BGZ endpoint
      - per-afdeling-or-clinic: allow afdeling to inherit BGZ endpoint from its parent kliniek
    """
    if not resources:
        return

    bgz_policy = str(getattr(cfg, "bgz_policy", "per-clinic") or "per-clinic").strip().lower()

    # 1) Basic structure + duplicate ids
    seen: Set[Tuple[str, str]] = set()
    known: Set[str] = set()

    for r in resources:
        rt = r.get("resourceType")
        rid = r.get("id")
        _require(cfg, bool(rt), "A generated resource is missing resourceType")
        _require(cfg, bool(rid), f"Generated resource {rt or '<unknown>'} is missing id")
        key = (str(rt), str(rid))
        _require(cfg, key not in seen, f"Duplicate resource id in output: {key[0]}/{key[1]}")
        seen.add(key)
        known.add(f"{key[0]}/{key[1]}")
    # 2) Internal reference integrity (within this publish run)
    missing_refs: Set[str] = set()
    for r in resources:
        for ref in _iter_reference_strings(r):
            s = str(ref or "").strip()
            if not s:
                continue
            if s.startswith(("http://", "https://", "#", "urn:")):
                # Absolute or contained or URN references are treated as external here.
                continue
            if "/" not in s:
                continue
            ref_rt, ref_id = s.split("/", 1)
            if not ref_rt or not ref_id:
                continue
            if f"{ref_rt}/{ref_id}" not in known:
                missing_refs.add(s)

    if missing_refs:
        examples = ", ".join(sorted(missing_refs)[:5])
        msg = f"Found {len(missing_refs)} internal references that are not present in this run (examples: {examples})."
        if since_naive is None:
            # Full publish: references should be self-contained.
            _require(cfg, False, msg)
        else:
            logger.warning(
                "%s Since --since was used, this can be OK if referenced resources already exist on the server.",
                msg,
            )

    # 3) Endpoint-specific checks (critical for addressing)
    BGZ_CODE = BGZ_SERVER_CAPABILITIES_CODE

    endpoints_published = [r for r in resources if str(r.get("resourceType")) == "Endpoint"]
    endpoints_all = endpoint_resources_all if endpoint_resources_all is not None else endpoints_published
    ep_by_id: Dict[str, Dict[str, Any]] = {str(ep.get("id")): ep for ep in endpoints_all if ep.get("id")}

    # Validate published endpoints (payloadType, address, URL)
    for ep in endpoints_published:
        eid = str(ep.get("id") or "")
        address = str(ep.get("address") or "").strip()
        _require(cfg, bool(address), f"Endpoint/{eid} has empty address")
        if address:
            parsed = urlparse(address)
            _require(cfg, bool(parsed.scheme), f"Endpoint/{eid} address is not a valid URL: {address!r}")
            if parsed.scheme == "http":
                logger.warning("Endpoint/%s uses http:// (recommended: https://).", eid)

        pts = ep.get("payloadType") or []
        _require(cfg, isinstance(pts, list) and len(pts) > 0, f"Endpoint/{eid} has no payloadType")

        # Flag placeholders (should not happen with BGZ defaults).
        for cc in pts if isinstance(pts, list) else []:
            if _is_unknown_codeable_concept(cc):
                _require(cfg, False, f"Endpoint/{eid} has payloadType placeholder 'unknown' (fill tblEndpoint payloadType* columns or defaults).")

    # Count *active* BGZ endpoints in the dataset (prefer full endpoint set when provided)
    active_bgz_total = 0
    for ep in endpoints_all:
        if _endpoint_is_active(ep) and _endpoint_has_payload_coding(ep, NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, BGZ_CODE):
            active_bgz_total += 1

    if bgz_policy != "off" and active_bgz_total <= 0:
        # Provide actionable diagnostics (what is present, and why it doesn't qualify as *active* BGZ).
        total_eps = len(endpoints_all)
        active_eps = sum(1 for ep in endpoints_all if _endpoint_is_active(ep))
        bgz_any = sum(
            1
            for ep in endpoints_all
            if _endpoint_has_payload_coding(ep, NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, BGZ_CODE)
        )
        bgz_inactive = sum(
            1
            for ep in endpoints_all
            if (not _endpoint_is_active(ep))
            and _endpoint_has_payload_coding(ep, NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, BGZ_CODE)
        )

        sample_lines = ["  - " + _endpoint_debug_line(ep, BGZ_CODE) for ep in endpoints_all[:10]]
        sample = "\n".join(sample_lines) if sample_lines else "  (no endpoints)"

        msg = (
            "No *active* Endpoint with BGZ Server payloadType was found (expected for BGZ-only use case).\n"
            f"Endpoint summary: total={total_eps}, active={active_eps}, with BGZ payloadType (any status)={bgz_any}, inactive BGZ={bgz_inactive}.\n"
            "Sample endpoints (up to 10):\n"
            f"{sample}"
        )
        _require(cfg, False, msg)

    # 4) BGZ endpoint policy per organization (optional but recommended for routering)
    orgs_published = [r for r in resources if str(r.get("resourceType")) == "Organization"]
    orgs_all = organization_resources_all if organization_resources_all is not None else orgs_published
    org_by_id: Dict[str, Dict[str, Any]] = {str(o.get("id")): o for o in orgs_all if o.get("id")}

    # Use the full org set when provided (prevents false negatives when --since is used).
    orgs_to_check = orgs_all

    def _warn_lines(message: str) -> None:
        for line in str(message or "").splitlines():
            logger.warning("%s", line)

    def has_active_bgz(endpoint_refs: Any) -> bool:
        if not isinstance(endpoint_refs, list):
            return False
        for ref in endpoint_refs:
            if not isinstance(ref, dict):
                continue
            rstr = str(ref.get("reference") or "")
            if not rstr.startswith("Endpoint/"):
                continue
            ref_id = rstr.split("/", 1)[1]
            ep = ep_by_id.get(ref_id)
            if not ep:
                continue
            if _endpoint_is_active(ep) and _endpoint_has_payload_coding(ep, NL_GF_DATA_EXCHANGE_CAPABILITIES_SYSTEM, BGZ_CODE):
                return True
        return False

    # Helper to build a consistent, actionable message.
    def _bgz_details_for_org(org: Dict[str, Any]) -> str:
        return _describe_org_endpoints(org.get("endpoint") or [], ep_by_id, BGZ_CODE)

    # Policy: "any" => require >=1 active BGZ endpoint somewhere (already enforced above),
    # but warn per active kliniek when it lacks an active BGZ endpoint.
    if bgz_policy == "any":
        for org in orgs_to_check:
            oid = str(org.get("id") or "")
            if not oid.startswith("org-kliniek-"):
                continue
            if org.get("active") is False:
                continue
            if not has_active_bgz(org.get("endpoint") or []):
                details = _bgz_details_for_org(org)
                _warn_lines(
                    f"BGZ policy 'any': {_org_debug_label(org)} has no *active* BGZ-capable Endpoint.\n"
                    + _indent_lines(details)
                )

    # Policy: "per-clinic" => each active kliniek must have >=1 active BGZ endpoint.
    if bgz_policy == "per-clinic":
        for org in orgs_to_check:
            oid = str(org.get("id") or "")
            if not oid.startswith("org-kliniek-"):
                continue
            if org.get("active") is False:
                continue
            if not has_active_bgz(org.get("endpoint") or []):
                details = _bgz_details_for_org(org)
                msg = (
                    f"BGZ policy '{bgz_policy}': {_org_debug_label(org)} has no *active* BGZ-capable Endpoint.\n"
                    + _indent_lines(details)
                    + "\n"
                    + "    Tip: if your BGZ endpoints are registered on afdelingen instead of the kliniek, consider --bgz-policy per-afdeling or per-afdeling-or-clinic."
                )
                _require(cfg, False, msg)

    # Policy: "per-afdeling" and "per-afdeling-or-clinic"
    if bgz_policy in ("per-afdeling", "per-afdeling-or-clinic"):
        # (Optional) warn when a kliniek has no active BGZ endpoint and thus cannot act as parent fallback.
        if bgz_policy == "per-afdeling-or-clinic":
            for org in orgs_to_check:
                oid = str(org.get("id") or "")
                if not oid.startswith("org-kliniek-"):
                    continue
                if org.get("active") is False:
                    continue
                if not has_active_bgz(org.get("endpoint") or []):
                    details = _bgz_details_for_org(org)
                    _warn_lines(
                        f"BGZ policy '{bgz_policy}': {_org_debug_label(org)} has no active BGZ endpoint; afdelingen must provide BGZ endpoints directly.\n"
                        + _indent_lines(details)
                    )

        for org in orgs_to_check:
            oid = str(org.get("id") or "")
            if not oid.startswith("org-afdeling-"):
                continue
            if org.get("active") is False:
                continue

            # Direct endpoints on afdeling
            if has_active_bgz(org.get("endpoint") or []):
                continue

            parent: Optional[Dict[str, Any]] = None
            if bgz_policy == "per-afdeling-or-clinic":
                # Allow inheriting BGZ endpoint from parent kliniek
                part_of = org.get("partOf") if isinstance(org.get("partOf"), dict) else {}
                pref = str(part_of.get("reference") or "")
                parent_id = pref.split("/", 1)[1] if pref.startswith("Organization/") else ""
                parent = org_by_id.get(parent_id) if parent_id else None
                if parent and has_active_bgz(parent.get("endpoint") or []):
                    continue

            afdeling_details = _bgz_details_for_org(org)
            msg = (
                f"BGZ policy '{bgz_policy}': {_org_debug_label(org)} has no *active* BGZ-capable Endpoint"
                + (" (neither direct nor via parent kliniek)." if bgz_policy == "per-afdeling-or-clinic" else ".")
                + "\n"
                + "    Afdeling endpoint details:\n"
                + _indent_lines(afdeling_details, prefix="        ")
            )

            if bgz_policy == "per-afdeling-or-clinic":
                if parent is None:
                    msg += "\n    Parent kliniek: (not found in this dataset)"
                else:
                    parent_details = _bgz_details_for_org(parent)
                    msg += "\n    Parent kliniek: " + _org_debug_label(parent)
                    msg += "\n    Parent endpoint details:\n" + _indent_lines(parent_details, prefix="        ")

            _require(cfg, False, msg)

    # 5) Highlight "unknown" placeholders for Location.type / HealthcareService.type (usually impacts discovery)
    for loc in [r for r in resources if str(r.get("resourceType")) == "Location"]:
        lid = str(loc.get("id") or "")
        types = loc.get("type") or []
        if isinstance(types, list) and any(_is_unknown_codeable_concept(cc) for cc in types):
            logger.warning("Location/%s has type 'unknown' placeholder.", lid)

    for svc in [r for r in resources if str(r.get("resourceType")) == "HealthcareService"]:
        sid = str(svc.get("id") or "")
        types = svc.get("type") or []
        if isinstance(types, list) and any(_is_unknown_codeable_concept(cc) for cc in types):
            logger.warning("HealthcareService/%s has type 'unknown' placeholder.", sid)

    # 6) Chunking warning (multiple transactions can make debugging harder)
    if cfg.bundle_size and len(resources) > cfg.bundle_size:
        logger.warning(
            "bundle-size=%s will split %s entries over multiple transactions. If your FHIR server enforces referential integrity, consider increasing --bundle-size for initial load.",
            cfg.bundle_size,
            len(resources),
        )
def _chunk(items: List[Any], chunk_size: int) -> Iterable[List[Any]]:
    if chunk_size <= 0:
        yield items
        return
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]



def _parse_http_status(status: Any) -> Optional[int]:
    if status is None:
        return None
    s = str(status).strip()
    if not s:
        return None
    # FHIR Bundle.entry.response.status is typically: "201 Created"
    try:
        return int(s.split()[0])
    except Exception:
        return None


def _validate_transaction_response_entries(cfg: Config, request_entries: List[Dict[str, Any]], response_payload: Any) -> None:
    """Validate that every entry in a transaction-response succeeded.

    Some servers can return HTTP 200 on the bundle while individual entries failed.
    """
    if not isinstance(response_payload, dict):
        return
    rt = str(response_payload.get("resourceType") or "")
    if rt == "OperationOutcome":
        msg = _format_operation_outcome(response_payload) or "OperationOutcome returned instead of transaction-response Bundle"
        raise RuntimeError(msg)
    if rt != "Bundle":
        logger.warning("FHIR response is not a Bundle (resourceType=%s).", rt or "<missing>")
        return

    resp_entries = response_payload.get("entry") or []
    if not isinstance(resp_entries, list):
        logger.warning("FHIR transaction-response Bundle.entry is not a list.")
        return

    errors: List[str] = []
    warnings: List[str] = []

    if len(resp_entries) != len(request_entries):
        warnings.append(f"transaction-response entry count mismatch: request={len(request_entries)} response={len(resp_entries)}")

    for i, req in enumerate(request_entries):
        req_req = req.get("request") if isinstance(req, dict) else {}
        method = str(req_req.get("method") or "").upper()
        url = str(req_req.get("url") or "")
        resp_entry = resp_entries[i] if i < len(resp_entries) else None
        if not isinstance(resp_entry, dict):
            warnings.append(f"missing/invalid response entry for request index {i} ({method} {url})")
            continue

        resp_obj = resp_entry.get("response") if isinstance(resp_entry.get("response"), dict) else {}
        status_raw = resp_obj.get("status")
        code = _parse_http_status(status_raw)

        # Outcome may be present as Bundle.entry.resource (common) or as response.outcome (less common)
        outcome: Optional[Dict[str, Any]] = None
        if isinstance(resp_entry.get("resource"), dict) and str(resp_entry["resource"].get("resourceType") or "") == "OperationOutcome":
            outcome = resp_entry["resource"]
        if isinstance(resp_obj.get("outcome"), dict) and str(resp_obj["outcome"].get("resourceType") or "") == "OperationOutcome":
            outcome = resp_obj["outcome"]

        if code is None:
            warnings.append(f"missing/invalid status for entry {i} ({method} {url}): {status_raw!r}")
            continue

        ok = 200 <= code < 300
        if method == "DELETE" and code == 404:
            ok = True  # idempotent delete

        if not ok:
            details = _format_operation_outcome(outcome) if outcome else ""
            if details:
                errors.append(f"{method} {url} -> {code} ({status_raw}); {details}")
            else:
                errors.append(f"{method} {url} -> {code} ({status_raw})")
        elif outcome:
            # 2xx but still an OperationOutcome => suspicious, log as warning
            details = _format_operation_outcome(outcome) or ""
            warnings.append(f"{method} {url} -> {code} returned OperationOutcome: {details}".strip())

    for w in warnings[:10]:
        logger.warning("Transaction-response validation: %s", w)

    if errors:
        for e in errors[:25]:
            logger.error("Transaction entry failed: %s", e)
        raise RuntimeError(f"FHIR transaction had {len(errors)} failing entry(ies). See logs for details.")


def _publish_transaction_bundle(
    cfg: Config, session: requests.Session, headers: Dict[str, str], entries: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Publish a FHIR transaction Bundle (ITI-130 feed) and return response JSON."""
    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": entries,
    }

    url = cfg.fhir_base.rstrip("/")

    t0 = time.perf_counter()
    try:
        resp = session.post(
            url,
            headers=headers,
            json=bundle,
            timeout=_request_timeout(cfg),
        )
    except requests.exceptions.RequestException as e:
        info = _classify_requests_exception(e)
        logger.error(
            "HTTP request failed reason=%s url=%s: %s (type=%s)",
            info.get("reason"),
            url,
            info.get("message"),
            type(e).__name__,
        )
        raise

    elapsed = time.perf_counter() - t0
    logger.info("FHIR transaction POST status=%s entries=%s elapsed=%.3fs", resp.status_code, len(entries), elapsed)

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        payload = _try_json(resp)
        if isinstance(payload, dict) and payload.get("resourceType") == "OperationOutcome":
            msg = _format_operation_outcome(payload)
            if msg:
                logger.error("FHIR server OperationOutcome: %s", msg)

        logger.error(
            "FHIR server returned HTTP %s (content-type=%s). Body: %s",
            resp.status_code,
            resp.headers.get("content-type"),
            _response_excerpt(resp),
        )
        raise

    payload = _try_json(resp)
    if not isinstance(payload, dict):
        # Some servers might respond with non-JSON or unexpected JSON.
        logger.error(
            "FHIR server response is not a JSON object (content-type=%s). Body: %s",
            resp.headers.get("content-type"),
            _response_excerpt(resp),
        )
        raise RuntimeError("FHIR server returned an invalid response (expected JSON object).")

    _validate_transaction_response_entries(cfg, entries, payload)
    return payload



def _resource_supports_delete(interactions: Any) -> bool:
    if not interactions:
        return True
    if not isinstance(interactions, list):
        return True
    for item in interactions:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip().lower()
        if code in ("delete", "delete-type", "delete-history"):
            return True
    return False


def _fhir_resource_types_from_capability(
    cfg: Config, session: requests.Session, headers: Dict[str, str]
) -> List[str]:
    url = cfg.fhir_base.rstrip("/") + "/metadata"
    try:
        resp = session.get(url, headers=headers, timeout=_request_timeout(cfg))
    except requests.exceptions.RequestException as e:
        info = _classify_requests_exception(e)
        logger.error(
            "FHIR metadata request failed reason=%s url=%s: %s (type=%s)",
            info.get("reason"),
            url,
            info.get("message"),
            type(e).__name__,
        )
        raise

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        payload = _try_json(resp)
        if isinstance(payload, dict) and payload.get("resourceType") == "OperationOutcome":
            msg = _format_operation_outcome(payload)
            if msg:
                logger.error("FHIR server OperationOutcome: %s", msg)
        logger.error(
            "FHIR metadata request failed HTTP %s. Body: %s",
            resp.status_code,
            _response_excerpt(resp),
        )
        raise

    payload = _try_json(resp)
    if not isinstance(payload, dict):
        raise RuntimeError("FHIR metadata response is not a JSON object.")
    if str(payload.get("resourceType") or "") != "CapabilityStatement":
        logger.warning(
            "FHIR metadata response is not a CapabilityStatement (resourceType=%s).",
            payload.get("resourceType"),
        )
        return []

    types: List[str] = []
    rest_entries = payload.get("rest") or []
    if isinstance(rest_entries, list):
        for rest in rest_entries:
            if not isinstance(rest, dict):
                continue
            resources = rest.get("resource") or []
            if not isinstance(resources, list):
                continue
            for res in resources:
                if not isinstance(res, dict):
                    continue
                rtype = str(res.get("type") or "").strip()
                if not rtype:
                    continue
                if not _resource_supports_delete(res.get("interaction")):
                    continue
                if rtype not in types:
                    types.append(rtype)
    return types


def _order_fhir_resource_types_for_delete(resource_types: Iterable[str]) -> List[str]:
    unique: List[str] = []
    seen: Set[str] = set()
    for rtype in resource_types:
        rstr = str(rtype or "").strip()
        if not rstr or rstr in seen:
            continue
        unique.append(rstr)
        seen.add(rstr)

    if not unique:
        return []

    order_set = set(FHIR_RESET_DELETE_ORDER)
    priority = [rtype for rtype in FHIR_RESET_DELETE_ORDER if rtype in seen]
    rest = [rtype for rtype in unique if rtype not in order_set]
    return priority + rest


def _fhir_unlink_org_endpoints(
    cfg: Config,
    session: requests.Session,
    headers: Dict[str, str],
    org_ids: Iterable[str],
) -> None:
    org_id_list = [str(oid) for oid in org_ids if str(oid).strip()]
    if not org_id_list:
        return

    logger.warning("FHIR reset: clearing Organization.endpoint references to unblock Endpoint deletes.")
    base = cfg.fhir_base.rstrip("/")
    updated = 0

    for oid in org_id_list:
        url = f"{base}/Organization/{oid}"
        try:
            resp = session.get(url, headers=headers, timeout=_request_timeout(cfg))
        except requests.exceptions.RequestException as e:
            info = _classify_requests_exception(e)
            logger.error(
                "FHIR read failed reason=%s url=%s: %s (type=%s)",
                info.get("reason"),
                url,
                info.get("message"),
                type(e).__name__,
            )
            raise

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            if resp.status_code == 404:
                logger.warning("FHIR reset: Organization/%s not found while unlinking endpoints.", oid)
                continue
            payload = _try_json(resp)
            if isinstance(payload, dict) and payload.get("resourceType") == "OperationOutcome":
                msg = _format_operation_outcome(payload)
                if msg:
                    logger.error("FHIR server OperationOutcome: %s", msg)
            logger.error(
                "FHIR read failed HTTP %s for Organization/%s. Body: %s",
                resp.status_code,
                oid,
                _response_excerpt(resp),
            )
            raise

        payload = _try_json(resp)
        if not isinstance(payload, dict):
            raise RuntimeError("FHIR read response is not a JSON object.")
        if str(payload.get("resourceType") or "") != "Organization":
            logger.warning("FHIR reset: unexpected resourceType for Organization/%s.", oid)
            continue
        if "endpoint" not in payload:
            continue

        payload.pop("endpoint", None)

        try:
            resp = session.put(url, headers=headers, json=payload, timeout=_request_timeout(cfg))
        except requests.exceptions.RequestException as e:
            info = _classify_requests_exception(e)
            logger.error(
                "FHIR update failed reason=%s url=%s: %s (type=%s)",
                info.get("reason"),
                url,
                info.get("message"),
                type(e).__name__,
            )
            raise

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            payload = _try_json(resp)
            if isinstance(payload, dict) and payload.get("resourceType") == "OperationOutcome":
                msg = _format_operation_outcome(payload)
                if msg:
                    logger.error("FHIR server OperationOutcome: %s", msg)
            logger.error(
                "FHIR update failed HTTP %s for Organization/%s. Body: %s",
                resp.status_code,
                oid,
                _response_excerpt(resp),
            )
            raise

        updated += 1

    logger.info("FHIR reset: cleared endpoint references on %s Organization resources.", updated)


def _fhir_search_resource_ids(
    cfg: Config,
    session: requests.Session,
    headers: Dict[str, str],
    resource_type: str,
) -> List[str]:
    base = cfg.fhir_base.rstrip("/")
    url = f"{base}/{resource_type}?_count=200"

    ids: List[str] = []
    seen_ids: Set[str] = set()
    seen_urls: Set[str] = set()

    while url:
        if url in seen_urls:
            logger.warning("FHIR reset: pagination loop detected for %s; stopping.", resource_type)
            break
        seen_urls.add(url)

        try:
            resp = session.get(url, headers=headers, timeout=_request_timeout(cfg))
        except requests.exceptions.RequestException as e:
            info = _classify_requests_exception(e)
            logger.error(
                "FHIR search failed reason=%s url=%s: %s (type=%s)",
                info.get("reason"),
                url,
                info.get("message"),
                type(e).__name__,
            )
            raise

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            payload = _try_json(resp)
            if isinstance(payload, dict) and payload.get("resourceType") == "OperationOutcome":
                msg = _format_operation_outcome(payload)
                if msg:
                    logger.error("FHIR server OperationOutcome: %s", msg)
            logger.error(
                "FHIR search failed HTTP %s for %s. Body: %s",
                resp.status_code,
                resource_type,
                _response_excerpt(resp),
            )
            raise

        payload = _try_json(resp)
        if not isinstance(payload, dict):
            raise RuntimeError("FHIR search response is not a JSON object.")
        rt = str(payload.get("resourceType") or "")
        if rt != "Bundle":
            raise RuntimeError(f"FHIR search response is not a Bundle (resourceType={rt!r}).")

        entries = payload.get("entry") or []
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                rid: Optional[str] = None
                res = entry.get("resource")
                if isinstance(res, dict):
                    rid = res.get("id")
                if not rid:
                    full = entry.get("fullUrl")
                    if isinstance(full, str) and f"/{resource_type}/" in full:
                        rid = full.rstrip("/").split("/")[-1]
                if rid:
                    rid_s = str(rid)
                    if rid_s not in seen_ids:
                        ids.append(rid_s)
                        seen_ids.add(rid_s)

        url = None
        links = payload.get("link") or []
        if isinstance(links, list):
            for link in links:
                if not isinstance(link, dict):
                    continue
                if str(link.get("relation") or "") == "next":
                    next_url = str(link.get("url") or "").strip()
                    if next_url:
                        url = next_url
                    break

    return ids


def _fhir_reset_server(cfg: Config, session: requests.Session, headers: Dict[str, str]) -> None:
    logger.warning("FHIR reset: deleting ALL resources on %s (destructief).", cfg.fhir_base)

    resource_types: List[str] = []
    try:
        resource_types = _fhir_resource_types_from_capability(cfg, session, headers)
    except Exception:
        logger.warning("FHIR reset: kan CapabilityStatement niet lezen; val terug op default resource lijst.")

    if not resource_types:
        resource_types = list(FHIR_RESET_DEFAULT_RESOURCE_TYPES)
        logger.warning("FHIR reset: gebruik default resource types: %s", ", ".join(resource_types))

    resource_types = _order_fhir_resource_types_for_delete(resource_types)
    base = cfg.fhir_base.rstrip("/")
    total_deleted = 0
    org_ids_for_delete: Optional[List[str]] = None

    if "Organization" in resource_types and "Endpoint" in resource_types:
        org_ids_for_delete = _fhir_search_resource_ids(cfg, session, headers, "Organization")
        if org_ids_for_delete:
            _fhir_unlink_org_endpoints(cfg, session, headers, org_ids_for_delete)

    for rtype in resource_types:
        if rtype == "Organization" and org_ids_for_delete is not None:
            ids = org_ids_for_delete
        else:
            ids = _fhir_search_resource_ids(cfg, session, headers, rtype)
        if not ids:
            continue
        logger.info("FHIR reset: deleting %s %s resources.", len(ids), rtype)

        entries: List[Dict[str, Any]] = []
        for rid in ids:
            entries.append({
                "fullUrl": f"{base}/{rtype}/{rid}",
                "request": {"method": "DELETE", "url": f"{rtype}/{rid}"},
            })

        for chunk in _chunk(entries, int(cfg.bundle_size)):
            _publish_transaction_bundle(cfg, session, headers, chunk)
        total_deleted += len(ids)

    logger.warning("FHIR reset complete: deleted %s resources.", total_deleted)


def _as_lines(addr1: Optional[str], addr2: Optional[str]) -> List[str]:
    return [x for x in [addr1, addr2] if x]


def _build_endpoints(
    cfg: Config,
    endpoint_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[int, List[str]], Dict[int, List[str]], Dict[int, List[str]]]:
    """Build FHIR Endpoint resources from dbo.tblEndpoint (schema v5).

    Returns:
      - endpoint resources
      - mapping kliniekkey -> [Endpoint.id]
      - mapping locatiekey -> [Endpoint.id]
      - mapping afdelingkey -> [Endpoint.id]
    """
    out: List[Dict[str, Any]] = []
    ep_by_kliniek: Dict[int, List[str]] = {}
    ep_by_locatie: Dict[int, List[str]] = {}
    ep_by_afdeling: Dict[int, List[str]] = {}
    profiles = _profiles_for("Endpoint", cfg)

    for e in endpoint_rows:
        endpoint_id = int(e["EndpointId"])
        kliniek_id = int(e["KliniekId"])
        locatie_id = int(e["LocatieId"]) if e.get("LocatieId") is not None else None
        afdeling_id = int(e["AfdelingId"]) if e.get("AfdelingId") is not None else None

        eid = _ep_id(endpoint_id)

        # Best-effort active/status handling
        active = _is_active_from_dates(bool(e.get("Actief", True)), e.get("StartDatum"), e.get("EindDatum"))
        status_raw = str(e.get("Status") or "").strip().lower()
        if status_raw in ("inactive", "inactief"):
            status_raw = "off"
        status = status_raw or ("active" if active else "off")
        if not active:
            status = "off"

        # connectionType is a FHIR Coding (not CodeableConcept!)
        conn_coding = _coding(e.get("ConnectionTypeSystemUri"), e.get("ConnectionTypeCode"), e.get("ConnectionTypeDisplay"))
        if conn_coding is None:
            conn_coding = _coding(ENDPOINT_CONN_SYSTEM, "hl7-fhir-rest", "HL7 FHIR") or {"system": ENDPOINT_CONN_SYSTEM, "code": "hl7-fhir-rest"}

        # payloadType is required (1..*) in NL GF Endpoint profile.
        payload_types: List[Dict[str, Any]] = []

        payload_cc = _codeable_concept(e.get("PayloadTypeSystemUri"), e.get("PayloadTypeCode"), e.get("PayloadTypeDisplay"))
        if payload_cc:
            payload_types.append(payload_cc)

        # If no payloadType present in the row, use configured defaults.
        if not payload_types and cfg.default_endpoint_payload_types:
            for sys_uri, code, disp in cfg.default_endpoint_payload_types:
                cc = _codeable_concept(sys_uri, code, disp)
                if cc:
                    payload_types.append(cc)

        if not payload_types:
            _require(
                cfg,
                False,
                f"Endpoint {eid} has no payloadType. Configure payloadType* columns in tblEndpoint "
                "or set defaults with --default-endpoint-payload.",
            )
            # Extensible binding fallback: provide text-only placeholder to satisfy cardinality (lenient mode).
            payload_types = [_codeable_concept_text("unknown") or {"text": "unknown"}]

        telecom: List[Dict[str, Any]] = []
        if e.get("Telefoon"):
            telecom.append({"system": "phone", "value": str(e.get("Telefoon"))})
        if e.get("Email"):
            telecom.append({"system": "email", "value": str(e.get("Email"))})

        managing_org_id = _org_afdeling_id(afdeling_id) if afdeling_id is not None else _org_kliniek_id(kliniek_id)

        _require(cfg, bool(e.get("Address")), f"Endpoint {eid} is missing required field Address")

        out.append(_prune({
            "resourceType": "Endpoint",
            "id": eid,
            "meta": {"profile": profiles} if profiles else None,
            "status": status,
            "connectionType": conn_coding,
            "payloadType": payload_types,
            "payloadMimeType": [str(e.get("PayloadMimeType"))] if e.get("PayloadMimeType") else None,
            "name": e.get("Name"),
            "contact": telecom if telecom else None,
            "managingOrganization": _ref("Organization", managing_org_id),
            "address": e.get("Address"),
            "period": _prune({
                "start": _isoformat(e.get("StartDatum")),
                "end": _isoformat(e.get("EindDatum")),
            }),
        }))

        # Build mapping indexes
        ep_by_kliniek.setdefault(kliniek_id, []).append(eid)
        if locatie_id is not None:
            ep_by_locatie.setdefault(locatie_id, []).append(eid)
        if afdeling_id is not None:
            ep_by_afdeling.setdefault(afdeling_id, []).append(eid)

    return out, ep_by_kliniek, ep_by_locatie, ep_by_afdeling


def _build_organizations(
    cfg: Config,
    kliniek_rows: List[Dict[str, Any]],
    afdeling_rows: List[Dict[str, Any]],
    ep_by_kliniek: Dict[int, List[str]],
    ep_by_afdeling: Dict[int, List[str]],
) -> Tuple[List[Dict[str, Any]], Dict[int, str], Dict[int, str]]:
    """Build Organization resources (Kliniek + Afdeling as child Organization).

    Returns:
      - resources
      - mapping kliniekkey -> URA value (used for AssignedId.assigner)
      - mapping kliniekkey -> display name
    """
    out: List[Dict[str, Any]] = []
    profiles = _profiles_for("Organization", cfg)

    ura_by_kliniek: Dict[int, str] = {}
    name_by_kliniek: Dict[int, str] = {}

    org_assigned_system = _assigned_id_system(cfg, "organizations")

    # Top-level organizations (klinieken)
    for k in kliniek_rows:
        kliniek_id = int(k["KliniekId"])
        org_id = _org_kliniek_id(kliniek_id)
        active = _is_active_from_dates(bool(k.get("Actief", True)), k.get("StartDatum"), k.get("EindDatum"))

        ura_value = _normalize_ura(cfg.publisher_ura or k.get("URANummer") or cfg.default_ura)
        _require(cfg, bool(ura_value), f"Kliniek {kliniek_id} (Organization/{org_id}) is missing URA (URANummer in brondata) and neither --publisher-ura nor --default-ura was provided")
        ura_by_kliniek[kliniek_id] = ura_value

        name = str(k.get("Naam") or "").strip() or f"Kliniek {kliniek_id}"
        name_by_kliniek[kliniek_id] = name

        identifiers: List[Dict[str, Any]] = []

        # Required by NL GF: identifier:AssignedId 1..1
        identifiers.append(
            _assigned_id_identifier(
                system=org_assigned_system,
                value=org_id,
                assigner_ura=ura_value,
                assigner_display=name,
            )
        )
        # Optional but important: URA, AGB, KvK
        identifiers.append(_prune({"system": URA_SYSTEM, "value": ura_value}))
        if k.get("AGBCode"):
            identifiers.append(_prune({"system": AGB_ORG_SYSTEM, "value": str(k.get("AGBCode"))}))
        if k.get("KvKNummer"):
            identifiers.append(_prune({"system": KVK_SYSTEM, "value": str(k.get("KvKNummer"))}))

        telecom: List[Dict[str, Any]] = []
        if k.get("Telefoon"):
            telecom.append({"system": "phone", "value": str(k.get("Telefoon"))})
        if k.get("Email"):
            telecom.append({"system": "email", "value": str(k.get("Email"))})
        if k.get("Website"):
            telecom.append({"system": "url", "value": str(k.get("Website"))})

        endpoints = [_ref("Endpoint", eid) for eid in ep_by_kliniek.get(kliniek_id, [])]

        kliniek_type_cc = _codeable_concept(k.get("TypeSystemUri"), k.get("TypeCode"), k.get("TypeDisplay"))
        types = [kliniek_type_cc] if kliniek_type_cc else [_codeable_concept(ORG_TYPE_SYSTEM, "prov", "Healthcare Provider")]

        out.append(
            _prune(
                {
                    "resourceType": "Organization",
                    "id": org_id,
                    "meta": {"profile": profiles} if profiles else None,
                    "active": active,
                    "identifier": identifiers,
                    "type": types,
                    "name": name,
                    "telecom": telecom,
                    "address": [
                        {
                            "line": _as_lines(k.get("AdresRegel1"), k.get("AdresRegel2")),
                            "postalCode": k.get("Postcode"),
                            "city": k.get("Plaats"),
                            "country": k.get("Land"),
                        }
                    ],
                    "endpoint": endpoints,
                }
            )
        )

    # Child organizations (afdelingen)
    for a in afdeling_rows:
        afdeling_id = int(a["AfdelingId"])
        kliniek_id = int(a["KliniekId"])
        afdeling_org_id = _org_afdeling_id(afdeling_id)

        ura_value = _issuer_ura(cfg, kliniek_id, ura_by_kliniek)
        _require(cfg, bool(ura_value), f"Afdeling {afdeling_id} has no kliniek URA (kliniekkey={kliniek_id}) and neither --publisher-ura nor --default-ura was provided")

        assigner_display = name_by_kliniek.get(kliniek_id)

        afdeling_type_cc = _codeable_concept(a.get("TypeSystemUri"), a.get("TypeCode"), a.get("TypeDisplay"))
        dept_types = [afdeling_type_cc] if afdeling_type_cc else [_codeable_concept(ORG_TYPE_SYSTEM, "dept", "Hospital Department")]
        dept_endpoints = [_ref("Endpoint", eid) for eid in ep_by_afdeling.get(afdeling_id, [])]
        identifiers = [
            _assigned_id_identifier(
                system=org_assigned_system,
                value=afdeling_org_id,
                assigner_ura=ura_value,
                assigner_display=assigner_display,
            ),
        ]
        # Child orgs inherit URA from parent; avoid duplicate URA when partOf is set.

        out.append(
            _prune(
                {
                    "resourceType": "Organization",
                    "id": afdeling_org_id,
                    "meta": {"profile": profiles} if profiles else None,
                    "active": bool(a.get("Actief", True)),
                    "identifier": identifiers,
                    "type": dept_types,
                    "name": a.get("Naam") or f"Afdeling {afdeling_id}",
                    "partOf": _ref("Organization", _org_kliniek_id(kliniek_id)),
                    "endpoint": dept_endpoints,
                }
            )
        )

    return out, ura_by_kliniek, name_by_kliniek


def _build_locations(
    cfg: Config,
    locatie_rows: List[Dict[str, Any]],
    ep_by_locatie: Dict[int, List[str]],
    ura_by_kliniek: Dict[int, str],
    name_by_kliniek: Dict[int, str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    profiles = _profiles_for("Location", cfg)
    loc_assigned_system = _assigned_id_system(cfg, "locations")

    for l in locatie_rows:
        locatie_id = int(l["LocatieId"])
        loc_fhir_id = _loc_id(locatie_id)

        kliniek_id = int(l["KliniekId"]) if l.get("KliniekId") is not None else None
        status = "active" if bool(l.get("Actief", True)) else "inactive"

        # NL GF Location profile requires Location.mode
        mode = "instance"

        assigner_ura = _issuer_ura(cfg, kliniek_id, ura_by_kliniek)
        _require(cfg, bool(assigner_ura), f"Location {locatie_id} has no KliniekId/URA mapping and neither --publisher-ura nor --default-ura was provided")
        assigner_display = name_by_kliniek.get(kliniek_id) if kliniek_id is not None else None

        identifiers: List[Dict[str, Any]] = [
            _assigned_id_identifier(
                system=loc_assigned_system,
                value=loc_fhir_id,
                assigner_ura=assigner_ura,
                assigner_display=assigner_display,
            ),
            _prune({"system": URA_SYSTEM, "value": assigner_ura}),
        ]

        # Optional additional identifiers
        if l.get("AGBCode"):
            identifiers.append(_prune({"system": AGB_ORG_SYSTEM, "value": str(l.get("AGBCode"))}))

        telecom: List[Dict[str, Any]] = []
        if l.get("Telefoon"):
            telecom.append({"system": "phone", "value": str(l.get("Telefoon"))})
        if l.get("Email"):
            telecom.append({"system": "email", "value": str(l.get("Email"))})

        # Location.type: prefer coded values (schema v5), fallback to free-text locatietype
        loc_type_cc = _codeable_concept(l.get("TypeSystemUri"), l.get("TypeCode"), l.get("TypeDisplay"))
        if loc_type_cc is None:
            loc_type_cc = _codeable_concept_text(l.get("LocatieType"))
        if loc_type_cc is None:
            loc_type_cc = _codeable_concept_text("unknown") or {"text": "unknown"}
        loc_types = [loc_type_cc]

        endpoints = [_ref("Endpoint", eid) for eid in ep_by_locatie.get(locatie_id, [])]

        name = str(l.get("Naam") or l.get("Omschrijving") or "").strip() or f"Locatie {locatie_id}"

        managing_org = _ref("Organization", _org_kliniek_id(kliniek_id)) if kliniek_id is not None else None
        _require(cfg, managing_org is not None, f"Location {locatie_id} is missing KliniekId for managingOrganization")

        out.append(
            _prune(
                {
                    "resourceType": "Location",
                    "id": loc_fhir_id,
                    "meta": {"profile": profiles} if profiles else None,
                    "identifier": identifiers,
                    "status": status,
                    "mode": mode,
                    "name": name,
                    "type": loc_types,
                    "telecom": telecom if telecom else None,
                    "address": _prune(
                        {
                            "line": _as_lines(l.get("AdresRegel1"), l.get("AdresRegel2")),
                            "postalCode": l.get("Postcode"),
                            "city": l.get("Plaats"),
                            "country": l.get("Land"),
                        }
                    ),
                    "managingOrganization": managing_org,
                    "position": _prune(
                        {
                            "longitude": float(l["Longitude"]) if l.get("Longitude") is not None else None,
                            "latitude": float(l["Latitude"]) if l.get("Latitude") is not None else None,
                        }
                    ),
                    "endpoint": endpoints if endpoints else None,
                }
            )
        )

    return out


def _build_healthcare_services(
    cfg: Config,
    afdeling_rows: List[Dict[str, Any]],
    locaties_by_kliniek: Dict[int, List[int]],
    ep_by_afdeling: Dict[int, List[str]],
    ura_by_kliniek: Dict[int, str],
    name_by_kliniek: Dict[int, str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    profiles = _profiles_for("HealthcareService", cfg)
    svc_assigned_system = _assigned_id_system(cfg, "healthcareservices")

    for a in afdeling_rows:
        afdeling_id = int(a["AfdelingId"])
        kliniek_id = int(a["KliniekId"])

        specialty_cc = _codeable_concept(a.get("SpecialismeSystemUri"), a.get("SpecialismeCode"), a.get("SpecialismeDisplay"))
        specialties = [specialty_cc] if specialty_cc else None

        svc_type_cc = _codeable_concept(a.get("TypeSystemUri"), a.get("TypeCode"), a.get("TypeDisplay"))
        if svc_type_cc is None:
            svc_type_cc = _codeable_concept_text("unknown") or {"text": "unknown"}
        svc_types = [svc_type_cc]

        loc_refs: List[Dict[str, Any]] = []
        if a.get("LocatieId") is not None:
            loc_refs = [_ref("Location", _loc_id(int(a["LocatieId"])))]
        else:
            # Kliniek-brede afdeling -> service geldt voor alle locaties van die kliniek
            for lid in locaties_by_kliniek.get(kliniek_id, []):
                loc_refs.append(_ref("Location", _loc_id(lid)))

        assigner_ura = _issuer_ura(cfg, kliniek_id, ura_by_kliniek)
        _require(cfg, bool(assigner_ura), f"HealthcareService {afdeling_id} has no kliniek URA (kliniekkey={kliniek_id}) and neither --publisher-ura nor --default-ura was provided")
        assigner_display = name_by_kliniek.get(kliniek_id)

        svc_id = _svc_id(afdeling_id)
        identifiers = [
            _assigned_id_identifier(
                system=svc_assigned_system,
                value=svc_id,
                assigner_ura=assigner_ura,
                assigner_display=assigner_display,
            ),
            _prune({"system": URA_SYSTEM, "value": assigner_ura}),
        ]

        out.append(
            _prune(
                {
                    "resourceType": "HealthcareService",
                    "id": svc_id,
                    "meta": {"profile": profiles} if profiles else None,
                    "identifier": identifiers,
                    "active": bool(a.get("Actief", True)),
                    "providedBy": _ref("Organization", _org_kliniek_id(kliniek_id)),
                    "name": a.get("Naam") or f"Service {afdeling_id}",
                    "type": svc_types,
                    "specialty": specialties,
                    "location": loc_refs,
                    "endpoint": [_ref("Endpoint", eid) for eid in ep_by_afdeling.get(afdeling_id, [])] or None,
                }
            )
        )

    return out


def _human_name(m: Dict[str, Any]) -> Dict[str, Any]:
    achternaam = str(m.get("Achternaam") or "").strip()
    tussen = str(m.get("Tussenvoegsel") or "").strip()
    family = achternaam
    if tussen:
        low = achternaam.lower()
        pref = (tussen + " ").lower()
        if not low.startswith(pref):
            family = f"{tussen} {achternaam}".strip()
    given = [str(m.get("Voornaam")).strip()] if m.get("Voornaam") else []
    return _prune({
        "text": m.get("NaamWeergave"),
        "family": family or None,
        "given": given if given else None,
    })


def _fhir_gender(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s in ("male", "female", "other", "unknown"):
        return s
    if s in ("man", "m"):
        return "male"
    if s in ("vrouw", "v"):
        return "female"
    if s in ("ongedifferentieerd", "onbepaald", "anders", "overig"):
        return "other"
    if s in ("onbekend", "u"):
        return "unknown"
    return None


def _build_practitioners(
    cfg: Config,
    medewerker_rows: List[Dict[str, Any]],
    ura_by_kliniek: Dict[int, str],
    name_by_kliniek: Dict[int, str],
    kliniek_by_locatie: Dict[int, int],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    profiles = _profiles_for("Practitioner", cfg)
    prac_assigned_system = _assigned_id_system(cfg, "practitioners")

    for m in medewerker_rows:
        mid = int(m["MedewerkerId"])
        prac_id = _prac_id(mid)

        # Determine the issuing organization (URA) for the author-assigned identifier.
        kliniek_id: Optional[int] = None
        if m.get("KliniekId") is not None:
            try:
                kliniek_id = int(m.get("KliniekId"))
            except Exception:
                kliniek_id = None
        if kliniek_id is None and m.get("LocatieId") is not None:
            try:
                kliniek_id = kliniek_by_locatie.get(int(m.get("LocatieId")))
            except Exception:
                kliniek_id = None

        assigner_ura = _issuer_ura(cfg, kliniek_id, ura_by_kliniek)
        _require(cfg, bool(assigner_ura), f"Practitioner {mid} has no KliniekId/URA mapping and neither --publisher-ura nor --default-ura was provided")
        assigner_display = name_by_kliniek.get(kliniek_id) if kliniek_id is not None else None

        identifiers: List[Dict[str, Any]] = []
        # Required by NL GF Practitioner profile: identifier:AssignedId 1..1
        identifiers.append(
            _assigned_id_identifier(
                system=prac_assigned_system,
                value=prac_id,
                assigner_ura=assigner_ura,
                assigner_display=assigner_display,
            )
        )
        # Optional but common Dutch identifiers
        if m.get("BIGNummer"):
            identifiers.append(_prune({"system": BIG_SYSTEM, "value": str(m.get("BIGNummer"))}))
        if m.get("AGBZorgverlenerCode"):
            identifiers.append(_prune({"system": AGB_PRACT_SYSTEM, "value": str(m.get("AGBZorgverlenerCode"))}))

        telecom: List[Dict[str, Any]] = []
        if m.get("Telefoon"):
            telecom.append({"system": "phone", "value": str(m.get("Telefoon"))})
        if m.get("Email"):
            telecom.append({"system": "email", "value": str(m.get("Email"))})

        active = _is_active_from_dates(bool(m.get("Actief", True)), None, m.get("EindDatum"))
        gender = _fhir_gender(m.get("Geslacht"))

        out.append(
            _prune(
                {
                    "resourceType": "Practitioner",
                    "id": prac_id,
                    "meta": {"profile": profiles} if profiles else None,
                    "active": active,
                    "identifier": identifiers,
                    "name": [_human_name(m)],
                    "telecom": telecom if telecom else None,
                    "gender": gender,
                    "birthDate": _iso_date(m.get("Geboortedatum")),
                }
            )
        )

    return out


def _is_active_from_dates(active_flag: bool, start_date: Any, end_date: Any) -> bool:
    if not active_flag:
        return False
    today = dt.date.today()
    sd = _coerce_date(start_date)
    if sd and today < sd:
        return False
    ed = _coerce_date(end_date)
    if ed and today > ed:
        return False
    return True


def _role_code_from(inzet: Dict[str, Any], medewerker: Dict[str, Any]) -> Dict[str, Any]:
    system = inzet.get("RolCodeSystemUri") or medewerker.get("RolCodeSystemUri") or "http://terminology.hl7.org/CodeSystem/practitioner-role"
    code = inzet.get("RolCode") or medewerker.get("RolCode") or "other"
    display = inzet.get("RolDisplay") or medewerker.get("RolDisplay") or "Other"
    cc = _codeable_concept(str(system), str(code), str(display) if display else None)
    return cc or {"text": str(display) if display else "Other"}


def _build_practitioner_roles(
    cfg: Config,
    inzet_rows: List[Dict[str, Any]],
    medewerker_by_id: Dict[int, Dict[str, Any]],
    afdeling_by_id: Dict[int, Dict[str, Any]],
    locaties_by_kliniek: Dict[int, List[int]],
    ep_by_kliniek: Dict[int, List[str]],
    ep_by_locatie: Dict[int, List[str]],
    ep_by_afdeling: Dict[int, List[str]],
    ura_by_kliniek: Dict[int, str],
    name_by_kliniek: Dict[int, str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    profiles = _profiles_for("PractitionerRole", cfg)
    role_assigned_system = _assigned_id_system(cfg, "practitionerroles")

    for inz in inzet_rows:
        inzet_id = int(inz["InzetId"])
        medewerker_id = int(inz["MedewerkerId"])
        kliniek_id = int(inz["KliniekId"])
        m = medewerker_by_id.get(medewerker_id)
        if not m:
            continue

        active_inzet = _is_active_from_dates(bool(inz.get("Actief", True)), inz.get("StartDatum"), inz.get("EindDatum"))

        # Determine afdeling + specialty
        afdeling_id = int(inz["AfdelingId"]) if inz.get("AfdelingId") is not None else (int(m["AfdelingId"]) if m.get("AfdelingId") is not None else None)
        afdeling = afdeling_by_id.get(afdeling_id) if afdeling_id is not None else None

        specialties: List[Dict[str, Any]] = []
        if afdeling:
            sp = _codeable_concept(afdeling.get("SpecialismeSystemUri"), afdeling.get("SpecialismeCode"), afdeling.get("SpecialismeDisplay"))
            if sp:
                specialties.append(sp)

        # Determine location(s) for the role (best-effort)
        loc_ids: List[int] = []
        if inz.get("LocatieId") is not None:
            loc_ids = [int(inz["LocatieId"])]
        elif afdeling and afdeling.get("LocatieId") is not None:
            loc_ids = [int(afdeling["LocatieId"])]
        elif m.get("LocatieId") is not None:
            loc_ids = [int(m["LocatieId"])]
        else:
            # If the clinic has exactly one location, use it
            clinic_locs = locaties_by_kliniek.get(kliniek_id, [])
            if len(clinic_locs) == 1:
                loc_ids = [clinic_locs[0]]

        loc_refs = [_ref("Location", _loc_id(lid)) for lid in loc_ids]

        # Link to HealthcareService (1:1 derived from Afdeling)
        svc_refs: List[Dict[str, Any]] = []
        if afdeling_id is not None:
            svc_refs = [_ref("HealthcareService", _svc_id(afdeling_id))]

        # Endpoints: best-effort union (clinic + location + department)
        ep_ids: List[str] = []
        seen_ep: Set[str] = set()

        def _add_eps(eids: List[str]) -> None:
            for _eid in eids:
                if _eid in seen_ep:
                    continue
                seen_ep.add(_eid)
                ep_ids.append(_eid)

        _add_eps(ep_by_kliniek.get(kliniek_id, []))
        if afdeling_id is not None:
            _add_eps(ep_by_afdeling.get(afdeling_id, []))
        for lid in loc_ids:
            _add_eps(ep_by_locatie.get(lid, []))

        ep_refs = [_ref("Endpoint", eid) for eid in ep_ids]

        # Telecom: you may want role-specific telecom later; for now reuse medewerker telecom if present
        telecom: List[Dict[str, Any]] = []
        if m.get("Telefoon"):
            telecom.append({"system": "phone", "value": str(m.get("Telefoon"))})
        if m.get("Email"):
            telecom.append({"system": "email", "value": str(m.get("Email"))})

        active_medewerker = _is_active_from_dates(bool(m.get("Actief", True)), None, m.get("EindDatum"))
        active = active_inzet and active_medewerker

        assigner_ura = _issuer_ura(cfg, kliniek_id, ura_by_kliniek)
        _require(cfg, bool(assigner_ura), f"PractitionerRole {inzet_id} has no kliniek URA (kliniekkey={kliniek_id}) and neither --publisher-ura nor --default-ura was provided")
        assigner_display = name_by_kliniek.get(kliniek_id)

        role_id = _pracrole_id(inzet_id)
        identifiers = [
            _assigned_id_identifier(
                system=role_assigned_system,
                value=role_id,
                assigner_ura=assigner_ura,
                assigner_display=assigner_display,
            )
        ]

        out.append(_prune({
            "resourceType": "PractitionerRole",
            "id": role_id,
            "meta": {"profile": profiles} if profiles else None,
            "identifier": identifiers,
            "active": active,
            "period": _prune({
                "start": _isoformat(inz.get("StartDatum")),
                "end": _isoformat(inz.get("EindDatum")),
            }),
            "practitioner": _ref("Practitioner", _prac_id(medewerker_id)),
            "organization": _ref("Organization", _org_afdeling_id(afdeling_id)) if afdeling_id is not None else _ref("Organization", _org_kliniek_id(kliniek_id)),
            "code": [_role_code_from(inz, m)],
            "specialty": specialties if specialties else None,
            "telecom": telecom,
            "location": loc_refs,
            "healthcareService": svc_refs,
            "endpoint": ep_refs,
        }))

    return out


def _changed_ids(rows: List[Dict[str, Any]], id_col: str, modified_col: str, since_naive: dt.datetime) -> Set[int]:
    out: Set[int] = set()
    for r in rows:
        ts = _coerce_datetime(r.get(modified_col))
        if ts is None:
            continue
        try:
            if ts >= since_naive:
                out.add(int(r[id_col]))
        except Exception:
            continue
    return out


def _validate_kliniek_ura_source(cfg: Config, kliniek_rows: List[Dict[str, Any]]) -> None:
    ura_to_kliniek_ids: Dict[str, List[int]] = {}
    missing_kliniek_ids: List[int] = []
    for k in kliniek_rows:
        try:
            kliniek_id = int(k.get("KliniekId"))
        except Exception:
            continue
        ura_value = _normalize_ura(cfg.publisher_ura or k.get("URANummer") or cfg.default_ura)
        if not ura_value:
            missing_kliniek_ids.append(kliniek_id)
            continue
        active = _is_active_from_dates(bool(k.get("Actief", True)), k.get("StartDatum"), k.get("EindDatum"))
        if active:
            ura_to_kliniek_ids.setdefault(ura_value, []).append(kliniek_id)
    if missing_kliniek_ids:
        raise RuntimeError(
            "URA ontbreekt voor kliniekkey(s): "
            + ", ".join(str(i) for i in sorted(missing_kliniek_ids))
            + ". Vul tblKliniek.uranummer (URANummer) of gebruik --publisher-ura/--default-ura."
        )
    dupes = {u: ids for u, ids in ura_to_kliniek_ids.items() if len(ids) > 1}
    if dupes:
        parts: List[str] = []
        for u in sorted(dupes.keys()):
            parts.append(f"{u} -> {','.join(str(i) for i in sorted(dupes[u]))}")
        raise RuntimeError(
            "URA is niet uniek voor actieve klinieken in tblKliniek. Dubbele URA(s): "
            + "; ".join(parts)
            + ". Geef elke kliniek een eigen URA (tblKliniek.uranummer)."
        )
def run(cfg: Config) -> None:
    _validate_and_log_config(cfg)

    with _build_http_session(cfg) as session:
        token = _get_bearer_token(cfg, session=session)

        headers = {
            "Accept": "application/fhir+json",
            "Content-Type": "application/fhir+json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        if cfg.fhir_reset_seed:
            if cfg.dry_run:
                logger.warning("--fhir-reset-seed is gezet maar --dry-run is actief; reset wordt overgeslagen.")
            else:
                if cfg.since_utc:
                    logger.warning("--fhir-reset-seed is gecombineerd met --since; alleen de delta wordt gepubliceerd.")
                _fhir_reset_server(cfg, session, headers)

        since_naive: Optional[dt.datetime] = None
        if cfg.since_utc:
            since_naive = cfg.since_utc.astimezone(dt.timezone.utc).replace(tzinfo=None)

        dialect = _db_dialect(cfg.sql_conn)
        with _connect(cfg.sql_conn) as conn:
            if dialect == "sqlite":
                _init_sqlite_schema(conn, seed=True, reset_seed=bool(getattr(cfg, "sqlite_reset_seed", False)))
            _db_preflight(conn, dialect, cfg)
            # We read full tables so we can derive references reliably.
            # Best-effort delta publishing is applied later by filtering output resources.
            if dialect == "sqlite":
                kliniek_rows = _fetch_rows(conn, _SQLITE_QUERIES["kliniek"])
            else:
                kliniek_rows = _fetch_rows(conn, """
                SELECT
                    k.kliniekkey AS KliniekId,
                    k.achternaam AS Naam,
                    k.[AGB code] AS AGBCode,
                    k.kvknummer AS KvKNummer,
                    k.actief AS Actief,
                    k.ingangsdatum AS StartDatum,
                    k.einddatum AS EindDatum,
                    k.telefoonnummer AS Telefoon,
                    k.[e-mail adres] AS Email,
                    k.[web site] AS Website,
                    LTRIM(RTRIM(CONCAT(k.straat, CASE WHEN k.straat IS NOT NULL AND k.huisnummer IS NOT NULL THEN ' ' ELSE '' END, k.huisnummer))) AS AdresRegel1,
                    NULL AS AdresRegel2,
                    k.postcode AS Postcode,
                    k.woonplaats AS Plaats,
                    k.landcode AS Land,
                    k.uranummer AS URANummer,
                    k.typeSystemUri AS TypeSystemUri,
                    k.typeCode AS TypeCode,
                    k.typeDisplay AS TypeDisplay,
                    k.LaatstGewijzigdOp
                FROM dbo.tblKliniek k
            """)
            _validate_kliniek_ura_source(cfg, kliniek_rows)
            if dialect == "sqlite":
                kliniek_locatie_rows = _fetch_rows(conn, _SQLITE_QUERIES["kliniek_locatie"])
            else:
                kliniek_locatie_rows = _fetch_rows(conn, """
                SELECT
                    kl.kliniekkey AS KliniekId,
                    kl.locatiekey AS LocatieId,
                    kl.actief AS Actief,
                    kl.LaatstGewijzigdOp
                FROM dbo.tblKliniekLocatie kl
            """)
            if dialect == "sqlite":
                locatie_rows = _fetch_rows(conn, _SQLITE_QUERIES["locatie"])
            else:
                locatie_rows = _fetch_rows(conn, """
                SELECT
                    l.locatiekey AS LocatieId,
                    map.KliniekId AS KliniekId,
                    l.[AGB code] AS AGBCode,
                    l.locatieomschrijving AS Naam,
                    l.locatietype AS LocatieType,
                    l.actief AS Actief,
                    l.telefoonnummer AS Telefoon,
                    l.[e-mail] AS Email,
                    LTRIM(RTRIM(CONCAT(l.straat, CASE WHEN l.straat IS NOT NULL AND l.huisnummer IS NOT NULL THEN ' ' ELSE '' END, l.huisnummer))) AS AdresRegel1,
                    NULL AS AdresRegel2,
                    l.postcode AS Postcode,
                    l.woonplaats AS Plaats,
                    l.landcode AS Land,
                    l.latitude AS Latitude,
                    l.longitude AS Longitude,
                    l.typeSystemUri AS TypeSystemUri,
                    l.typeCode AS TypeCode,
                    l.typeDisplay AS TypeDisplay,
                    l.LaatstGewijzigdOp
                FROM dbo.tblLocatie l
                OUTER APPLY (
                    SELECT TOP 1 kl.kliniekkey AS KliniekId
                    FROM dbo.tblKliniekLocatie kl
                    WHERE kl.locatiekey = l.locatiekey AND kl.actief = 1
                    ORDER BY kl.kliniekkey
                ) map
            """)
            if dialect == "sqlite":
                afdeling_rows = _fetch_rows(conn, _SQLITE_QUERIES["afdeling"])
            else:
                afdeling_rows = _fetch_rows(conn, """
                SELECT
                    a.afdelingkey AS AfdelingId,
                    a.kliniekkey AS KliniekId,
                    a.locatiekey AS LocatieId,
                    a.afdelingomschrijving AS Naam,
                    a.actief AS Actief,
                    a.specialismeSystemUri AS SpecialismeSystemUri,
                    a.[specialisme code] AS SpecialismeCode,
                    a.specialismeDisplay AS SpecialismeDisplay,
                    a.typeSystemUri AS TypeSystemUri,
                    a.typeCode AS TypeCode,
                    a.typeDisplay AS TypeDisplay,
                    a.LaatstGewijzigdOp
                FROM dbo.tblAfdeling a
            """)

            # Endpoint resources (schema v5)
            if dialect == "sqlite":
                endpoint_rows = _fetch_rows(conn, _SQLITE_QUERIES["endpoint"])
            else:
                endpoint_rows = _fetch_rows(conn, """
                SELECT
                    e.endpointkey AS EndpointId,
                    e.kliniekkey AS KliniekId,
                    e.locatiekey AS LocatieId,
                    e.afdelingkey AS AfdelingId,
                    e.status AS Status,
                    e.connectionTypeSystemUri AS ConnectionTypeSystemUri,
                    e.connectionTypeCode AS ConnectionTypeCode,
                    e.connectionTypeDisplay AS ConnectionTypeDisplay,
                    e.payloadTypeSystemUri AS PayloadTypeSystemUri,
                    e.payloadTypeCode AS PayloadTypeCode,
                    e.payloadTypeDisplay AS PayloadTypeDisplay,
                    e.payloadMimeType AS PayloadMimeType,
                    e.adres AS Address,
                    e.naam AS Name,
                    e.telefoon AS Telefoon,
                    e.email AS Email,
                    e.ingangsdatum AS StartDatum,
                    e.einddatum AS EindDatum,
                    e.actief AS Actief,
                    e.LaatstGewijzigdOp
                FROM dbo.tblEndpoint e
            """)

            medewerker_rows: List[Dict[str, Any]] = []
            inzet_rows: List[Dict[str, Any]] = []
            if cfg.include_practitioners:
                if dialect == "sqlite":
                    medewerker_rows = _fetch_rows(conn, _SQLITE_QUERIES["medewerker"])
                elif since_naive:
                    # --since optimisation: load only practitioners/roles relevant for this delta.
                    pre_changed_kliniek_ids = _changed_ids(kliniek_rows, "KliniekId", "LaatstGewijzigdOp", since_naive)
                    pre_changed_locatie_ids = _changed_ids(locatie_rows, "LocatieId", "LaatstGewijzigdOp", since_naive) | _changed_ids(
                        kliniek_locatie_rows, "LocatieId", "LaatstGewijzigdOp", since_naive
                    )
                    pre_changed_afdeling_ids = _changed_ids(afdeling_rows, "AfdelingId", "LaatstGewijzigdOp", since_naive)
                    pre_changed_endpoint_ids = _changed_ids(endpoint_rows, "EndpointId", "LaatstGewijzigdOp", since_naive)

                    since_date = since_naive.date()
                    today_date = dt.date.today()
                    for e in endpoint_rows:
                        eid = int(e["EndpointId"])
                        start = _coerce_date(e.get("StartDatum"))
                        end = _coerce_date(e.get("EindDatum"))
                        if start and since_date <= start <= today_date:
                            pre_changed_endpoint_ids.add(eid)
                        if end and since_date <= end <= today_date:
                            pre_changed_endpoint_ids.add(eid)
                    for e in endpoint_rows:
                        eid = int(e["EndpointId"])
                        if eid not in pre_changed_endpoint_ids:
                            continue
                        kid = e.get("KliniekId")
                        lid = e.get("LocatieId")
                        aid = e.get("AfdelingId")
                        if kid is not None:
                            pre_changed_kliniek_ids.add(int(kid))
                        if lid is not None:
                            pre_changed_locatie_ids.add(int(lid))
                        if aid is not None:
                            pre_changed_afdeling_ids.add(int(aid))

                    medewerker_change_rows = _fetch_rows(
                        conn,
                        "SELECT m.medewerkerkey AS MedewerkerId FROM dbo.tblMedewerker m WHERE m.LaatstGewijzigdOp >= :since",
                        {"since": since_naive},
                    )
                    pre_changed_medewerker_ids: Set[int] = {
                        int(r["MedewerkerId"]) for r in medewerker_change_rows if r.get("MedewerkerId") is not None
                    }

                    inzet_where: List[str] = []
                    inzet_params: Dict[str, Any] = {"since": since_naive, "since_date": since_date, "today_date": today_date}

                    inzet_where.append(
                        "(CASE WHEN r.LaatstGewijzigdOp IS NULL THEN i.LaatstGewijzigdOp WHEN r.LaatstGewijzigdOp > i.LaatstGewijzigdOp THEN r.LaatstGewijzigdOp ELSE i.LaatstGewijzigdOp END) >= :since"
                    )
                    inzet_where.append(
                        "(i.ingangsdatum IS NOT NULL AND i.ingangsdatum >= :since_date AND i.ingangsdatum <= :today_date)"
                    )
                    inzet_where.append(
                        "(i.einddatum IS NOT NULL AND i.einddatum >= :since_date AND i.einddatum <= :today_date)"
                    )
                    if pre_changed_medewerker_ids:
                        inzet_where.append("i.medewerkerkey IN :medewerker_ids")
                        inzet_params["medewerker_ids"] = sorted(pre_changed_medewerker_ids)
                    if pre_changed_afdeling_ids:
                        inzet_where.append("i.afdelingkey IN :afdeling_ids")
                        inzet_params["afdeling_ids"] = sorted(pre_changed_afdeling_ids)
                    if pre_changed_kliniek_ids:
                        inzet_where.append("a.kliniekkey IN :kliniek_ids")
                        inzet_params["kliniek_ids"] = sorted(pre_changed_kliniek_ids)
                    if pre_changed_locatie_ids:
                        inzet_where.append("a.locatiekey IN :locatie_ids")
                        inzet_params["locatie_ids"] = sorted(pre_changed_locatie_ids)

                    inzet_sql = """
                    SELECT
                        i.medewerkerinzetkey AS InzetId,
                        i.medewerkerkey AS MedewerkerId,
                        a.kliniekkey AS KliniekId,
                        i.afdelingkey AS AfdelingId,
                        a.locatiekey AS LocatieId,
                        i.ingangsdatum AS StartDatum,
                        i.einddatum AS EindDatum,
                        i.actief AS Actief,
                        r.rolCodeSystemUri AS RolCodeSystemUri,
                        r.rolCode AS RolCode,
                        r.rolDisplay AS RolDisplay,
                        CASE
                            WHEN r.LaatstGewijzigdOp IS NOT NULL AND r.LaatstGewijzigdOp > i.LaatstGewijzigdOp THEN r.LaatstGewijzigdOp
                            ELSE i.LaatstGewijzigdOp
                        END AS LaatstGewijzigdOp
                    FROM dbo.tblMedewerkerinzet i
                    INNER JOIN dbo.tblAfdeling a ON a.afdelingkey = i.afdelingkey
                    LEFT JOIN dbo.tblRoldefinitie r ON r.roldefinitiekey = i.roldefinitiekey
                """ + "\n                    WHERE " + " OR ".join(inzet_where)
                    inzet_rows = _fetch_rows(conn, inzet_sql, inzet_params)

                    medewerker_ids_needed: Set[int] = set(pre_changed_medewerker_ids)
                    for inz in inzet_rows:
                        mid = inz.get("MedewerkerId")
                        if mid is not None:
                            medewerker_ids_needed.add(int(mid))

                    if medewerker_ids_needed:
                        medewerker_sql = """
                    SELECT
                        m.medewerkerkey AS MedewerkerId,
                        m.[default locatiekey] AS LocatieId,
                        m.einddatum AS EindDatum,
                        m.actief AS Actief,
                        m.naamWeergave AS NaamWeergave,
                        m.achternaam AS Achternaam,
                        m.voornaam AS Voornaam,
                        m.tussenvoegsel AS Tussenvoegsel,
                        m.[BIG-nummer] AS BIGNummer,
                        m.[code zorgverlener] AS AGBZorgverlenerCode,
                        m.[e-mail] AS Email,
                        COALESCE(m.telefoonnummer_werk_mobiel, m.telefoonnummer_werk_vast) AS Telefoon,
                        m.geslacht AS Geslacht,
                        m.geboortedatum AS Geboortedatum,
                        m.LaatstGewijzigdOp
                    FROM dbo.tblMedewerker m
                """ + "\n                    WHERE m.medewerkerkey IN :medewerker_ids"
                        medewerker_rows = _fetch_rows(conn, medewerker_sql, {"medewerker_ids": sorted(medewerker_ids_needed)})
                else:
                    medewerker_rows = _fetch_rows(conn, """
                    SELECT
                        m.medewerkerkey AS MedewerkerId,
                        m.[default locatiekey] AS LocatieId,
                        m.einddatum AS EindDatum,
                        m.actief AS Actief,
                        m.naamWeergave AS NaamWeergave,
                        m.achternaam AS Achternaam,
                        m.voornaam AS Voornaam,
                        m.tussenvoegsel AS Tussenvoegsel,
                        m.[BIG-nummer] AS BIGNummer,
                        m.[code zorgverlener] AS AGBZorgverlenerCode,
                        m.[e-mail] AS Email,
                        COALESCE(m.telefoonnummer_werk_mobiel, m.telefoonnummer_werk_vast) AS Telefoon,
                        m.geslacht AS Geslacht,
                        m.geboortedatum AS Geboortedatum,
                        m.LaatstGewijzigdOp
                    FROM dbo.tblMedewerker m
                """)
                    inzet_rows = _fetch_rows(conn, """
                    SELECT
                        i.medewerkerinzetkey AS InzetId,
                        i.medewerkerkey AS MedewerkerId,
                        a.kliniekkey AS KliniekId,
                        i.afdelingkey AS AfdelingId,
                        a.locatiekey AS LocatieId,
                        i.ingangsdatum AS StartDatum,
                        i.einddatum AS EindDatum,
                        i.actief AS Actief,
                        r.rolCodeSystemUri AS RolCodeSystemUri,
                        r.rolCode AS RolCode,
                        r.rolDisplay AS RolDisplay,
                        CASE
                            WHEN r.LaatstGewijzigdOp IS NOT NULL AND r.LaatstGewijzigdOp > i.LaatstGewijzigdOp THEN r.LaatstGewijzigdOp
                            ELSE i.LaatstGewijzigdOp
                        END AS LaatstGewijzigdOp
                    FROM dbo.tblMedewerkerinzet i
                    INNER JOIN dbo.tblAfdeling a ON a.afdelingkey = i.afdelingkey
                    LEFT JOIN dbo.tblRoldefinitie r ON r.roldefinitiekey = i.roldefinitiekey
                """)
            # Build convenience indexes
            locaties_by_kliniek: Dict[int, List[int]] = {}
            for kl in kliniek_locatie_rows:
                if not bool(kl.get("Actief", True)):
                    continue
                kid = kl.get("KliniekId")
                lid = kl.get("LocatieId")
                if kid is None or lid is None:
                    continue
                kid_int = int(kid)
                if kid_int not in locaties_by_kliniek:
                    locaties_by_kliniek[kid_int] = []
                locaties_by_kliniek[kid_int].append(int(lid))

            afdeling_by_id: Dict[int, Dict[str, Any]] = {int(a["AfdelingId"]): a for a in afdeling_rows}
            medewerker_by_id: Dict[int, Dict[str, Any]] = {int(m["MedewerkerId"]): m for m in medewerker_rows} if cfg.include_practitioners else {}

            # Best-effort delta filters
            changed_kliniek_ids: Optional[Set[int]] = None
            changed_locatie_ids: Optional[Set[int]] = None
            changed_klinieklocatie_locatie_ids: Optional[Set[int]] = None
            changed_afdeling_ids: Optional[Set[int]] = None
            changed_endpoint_ids: Optional[Set[int]] = None
            changed_medewerker_ids: Optional[Set[int]] = None
            changed_inzet_ids: Optional[Set[int]] = None
            changed_locatie_kliniek_ids: Set[int] = set()

            if since_naive:
                changed_kliniek_ids = _changed_ids(kliniek_rows, "KliniekId", "LaatstGewijzigdOp", since_naive)
                changed_locatie_ids = _changed_ids(locatie_rows, "LocatieId", "LaatstGewijzigdOp", since_naive)
                changed_klinieklocatie_locatie_ids = _changed_ids(kliniek_locatie_rows, "LocatieId", "LaatstGewijzigdOp", since_naive)
                if changed_klinieklocatie_locatie_ids:
                    changed_locatie_ids |= changed_klinieklocatie_locatie_ids
                changed_afdeling_ids = _changed_ids(afdeling_rows, "AfdelingId", "LaatstGewijzigdOp", since_naive)

                # Endpoints (tblEndpoint) can change independently from Kliniek/Locatie/Afdeling.
                # We also include endpoints that *start* or *end* between since and today (their
                # effective status may flip without LaatstGewijzigdOp changing).
                changed_endpoint_ids = _changed_ids(endpoint_rows, "EndpointId", "LaatstGewijzigdOp", since_naive)
                since_date = since_naive.date()
                today_date = dt.date.today()
                for e in endpoint_rows:
                    try:
                        eid = int(e["EndpointId"])
                    except Exception:
                        continue
                    sd = e.get("StartDatum")
                    if sd:
                        sd_date = sd.date() if isinstance(sd, dt.datetime) else (_coerce_date(sd) or None)
                        if sd_date and since_date <= sd_date <= today_date:
                            changed_endpoint_ids.add(eid)
                    ed = e.get("EindDatum")
                    if ed:
                        ed_date = ed.date() if isinstance(ed, dt.datetime) else (_coerce_date(ed) or None)
                        if ed_date and since_date <= ed_date <= today_date:
                            changed_endpoint_ids.add(eid)

                # If an Endpoint changes, the referencing resources must be republished too (so their
                # endpoint references stay in sync).
                if changed_endpoint_ids:
                    for e in endpoint_rows:
                        try:
                            eid = int(e["EndpointId"])
                        except Exception:
                            continue
                        if eid not in changed_endpoint_ids:
                            continue
                        if e.get("KliniekId") is not None:
                            changed_kliniek_ids.add(int(e["KliniekId"]))
                        if e.get("LocatieId") is not None:
                            changed_locatie_ids.add(int(e["LocatieId"]))
                        if e.get("AfdelingId") is not None:
                            changed_afdeling_ids.add(int(e["AfdelingId"]))
                if cfg.include_practitioners:
                    changed_medewerker_ids = _changed_ids(medewerker_rows, "MedewerkerId", "LaatstGewijzigdOp", since_naive)
                    changed_inzet_ids = _changed_ids(inzet_rows, "InzetId", "LaatstGewijzigdOp", since_naive)

                if changed_locatie_ids:
                    for kl in kliniek_locatie_rows:
                        if not bool(kl.get("Actief", True)):
                            continue
                        kid = kl.get("KliniekId")
                        lid = kl.get("LocatieId")
                        if kid is None or lid is None:
                            continue
                        if int(lid) in changed_locatie_ids:
                            changed_locatie_kliniek_ids.add(int(kid))

            # Invert (active) KliniekLocatie mapping for convenience.
            kliniek_by_locatie: Dict[int, int] = {}
            for kl in kliniek_locatie_rows:
                if not bool(kl.get("Actief", True)):
                    continue
                try:
                    lid = int(kl["LocatieId"])
                    kid = int(kl["KliniekId"])
                except Exception:
                    continue
                kliniek_by_locatie.setdefault(lid, kid)

            # Derive Endpoints first so we can attach to Organizations/Locations/Services
            ep_resources_all, ep_by_kliniek, ep_by_locatie, ep_by_afdeling = _build_endpoints(cfg, endpoint_rows)

            # Organizations: Kliniek + Afdeling as child Organization
            org_resources_all, ura_by_kliniek, name_by_kliniek = _build_organizations(
                cfg,
                kliniek_rows,
                afdeling_rows,
                ep_by_kliniek,
                ep_by_afdeling,
            )

            # Locations
            loc_resources_all = _build_locations(cfg, locatie_rows, ep_by_locatie, ura_by_kliniek, name_by_kliniek)

            # HealthcareService 1:1 from Afdeling
            svc_resources_all = _build_healthcare_services(cfg, afdeling_rows, locaties_by_kliniek, ep_by_afdeling, ura_by_kliniek, name_by_kliniek)

            # Practitioners + PractitionerRoles (from inzet)
            prac_resources_all: List[Dict[str, Any]] = []
            pracrole_resources_all: List[Dict[str, Any]] = []
            if cfg.include_practitioners:
                prac_resources_all = _build_practitioners(cfg, medewerker_rows, ura_by_kliniek, name_by_kliniek, kliniek_by_locatie)
                pracrole_resources_all = _build_practitioner_roles(
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

            # Apply best-effort delta filtering to derived resources
            ep_resources = ep_resources_all
            org_resources = org_resources_all
            loc_resources = loc_resources_all
            svc_resources = svc_resources_all
            prac_resources = prac_resources_all
            pracrole_resources = pracrole_resources_all

            if since_naive:
                if changed_endpoint_ids is not None:
                    ep_resources = [e for e in ep_resources_all if int(e["id"].split("-")[1]) in changed_endpoint_ids]

                if changed_kliniek_ids is not None:
                    # Kliniek Organizations: only those klinieken
                    # Afdeling Organizations: only changed afdelingen
                    org_resources = []
                    for o in org_resources_all:
                        oid = str(o["id"])
                        if oid.startswith("org-kliniek-"):
                            kid = int(oid.split("-")[2])
                            if kid in changed_kliniek_ids:
                                org_resources.append(o)
                        elif oid.startswith("org-afdeling-"):
                            aid = int(oid.split("-")[2])
                            if changed_afdeling_ids is None or aid in changed_afdeling_ids:
                                org_resources.append(o)

                if changed_locatie_ids is not None:
                    loc_resources = [l for l in loc_resources_all if int(l["id"].split("-")[1]) in changed_locatie_ids]

                if changed_afdeling_ids is not None:
                    # HealthcareService is derived from Afdeling:
                    # - if afdeling changed -> publish
                    # - if afdeling is kliniek-breed (LocatieId NULL) and a locatie changed for that kliniek -> publish too
                    svc_resources = []
                    for s in svc_resources_all:
                        aid = int(str(s["id"]).split("-")[2])
                        a = afdeling_by_id.get(aid)
                        if not a:
                            continue
                        if aid in changed_afdeling_ids:
                            svc_resources.append(s)
                            continue
                        if a.get("LocatieId") is None and int(a["KliniekId"]) in changed_locatie_kliniek_ids:
                            svc_resources.append(s)

                if cfg.include_practitioners and changed_medewerker_ids is not None and changed_inzet_ids is not None:
                    prac_resources = [p for p in prac_resources_all if int(p["id"].split("-")[1]) in changed_medewerker_ids]

                    # PractitionerRole is derived from inzet:
                    # - inzet changed -> publish
                    # - medewerker changed -> publish roles for that medewerker (role code can fall back to medewerker default)
                    # - afdeling changed -> publish roles referencing that afdeling (specialty can change)
                    # - inzet verlopen (EindDatum tussen since en vandaag) -> publish
                    # - inzet gestart (StartDatum tussen since en vandaag) -> publish
                    changed_role_ids: Set[int] = set(changed_inzet_ids)
                    since_date = since_naive.date()
                    today_date = dt.date.today()
                    for inz in inzet_rows:
                        inz_id = int(inz["InzetId"])
                        if int(inz["MedewerkerId"]) in changed_medewerker_ids:
                            changed_role_ids.add(inz_id)
                        if changed_kliniek_ids is not None and int(inz.get("KliniekId") or 0) in changed_kliniek_ids:
                            # When the clinic changes (incl. endpoint changes), roles referencing that clinic
                            # need a refresh because we attach clinic endpoints to roles.
                            changed_role_ids.add(inz_id)
                        if inz.get("AfdelingId") is not None and changed_afdeling_ids is not None and int(inz["AfdelingId"]) in changed_afdeling_ids:
                            changed_role_ids.add(inz_id)
                        ed = inz.get("EindDatum")
                        if ed:
                            ed_date = ed.date() if isinstance(ed, dt.datetime) else _coerce_date(ed)
                            if ed_date and since_date <= ed_date <= today_date:
                                changed_role_ids.add(inz_id)
                        sd = inz.get("StartDatum")
                        if sd:
                            sd_date = sd.date() if isinstance(sd, dt.datetime) else _coerce_date(sd)
                            if sd_date and since_date <= sd_date <= today_date:
                                changed_role_ids.add(inz_id)

                    pracrole_resources = [r for r in pracrole_resources_all if int(r["id"].split("-")[1]) in changed_role_ids]

        all_resources: List[Dict[str, Any]] = []
        # Order is not strictly required in FHIR transaction, but it helps readability/debugging.
        all_resources.extend(ep_resources)
        all_resources.extend(org_resources)
        all_resources.extend(loc_resources)
        all_resources.extend(svc_resources)
        if cfg.include_practitioners:
            all_resources.extend(prac_resources)
            all_resources.extend(pracrole_resources)

        _sanity_check(cfg, all_resources, since_naive, endpoint_resources_all=ep_resources_all, organization_resources_all=org_resources_all)

        entries = _bundle_entries(cfg, all_resources)

        if not entries:
            print("[OK] No entries to publish.")
            return

        if cfg.dry_run:
            bundle = {
                "resourceType": "Bundle",
                "type": "transaction",
                "entry": entries,
            }
            if cfg.out_file:
                with open(cfg.out_file, "w", encoding="utf-8") as f:
                    json.dump(bundle, f, indent=2, ensure_ascii=False)
                print(f"[OK] Wrote bundle to {cfg.out_file}")
            else:
                print(json.dumps(bundle, indent=2, ensure_ascii=False))
            return

        total_entries = len(entries)
        total_chunks = 1
        if cfg.bundle_size and int(cfg.bundle_size) > 0:
            total_chunks = (total_entries + int(cfg.bundle_size) - 1) // int(cfg.bundle_size)
        logger.info("Publishing %s entries in %s transaction(s) (bundle_size=%s).", total_entries, total_chunks, cfg.bundle_size)

        for chunk_index, chunk in enumerate(_chunk(entries, cfg.bundle_size), start=1):
            logger.info("Publishing chunk %s/%s (entries=%s).", chunk_index, total_chunks, len(chunk))
            resp = _publish_transaction_bundle(cfg, session, headers, chunk)
            print("[OK] Published chunk:", resp.get("id") or "(no bundle id)")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Publish ZBC EPD tables as FHIR Directory resources via IHE ITI-130 (transaction Bundle). "
            "Defaults are aligned with the NL Generic Functions (GF) Adressering profiles."
        )
    )

    p.add_argument("--sql-conn", default=None, help="Database connection. Use a SQLAlchemy URL (preferred): sqlite:///path.db or mssql+pytds://user:pass@host:1433/db. Legacy ODBC strings are also supported (requires pyodbc).")
    p.add_argument(
        "--sqlite-reset-seed",
        action="store_true",
        default=None,
        help=(
            "SQLite demo: wis bestaande demo-data uit de sqlite database en maak de ingebouwde seed data opnieuw aan. "
            "DESTRUCTIEF; bedoeld voor lokaal testen. (env: SQLITE_RESET_SEED=1)"
        ),
    )
    p.add_argument(
        "--fhir-reset-seed",
        action="store_true",
        default=None,
        help=(
            "Wis alle resources op de FHIR server voordat er gepubliceerd wordt. "
            "DESTRUCTIEF; bedoeld voor lokaal testen. (env: FHIR_RESET_SEED=1)"
        ),
    )
    p.add_argument("--fhir-base", default=None, help="Base URL of FHIR server (accepts transaction Bundles).")

    p.add_argument("--token", default=None, help="Bearer token to use (overrides OAuth2 settings).")
    p.add_argument("--oauth-token-url", default=None, help="OAuth2 token URL (client_credentials).")
    p.add_argument("--oauth-client-id", default=None, help="OAuth2 client id.")
    p.add_argument("--oauth-client-secret", default=None, help="OAuth2 client secret.")
    p.add_argument("--oauth-scope", default=None, help="OAuth2 scope (optional).")

    p.add_argument("--no-verify-tls", action="store_true", help="Disable TLS verification (not recommended).")
    p.add_argument("--bundle-size", type=int, default=None, help="Max number of entries per transaction bundle.")
    p.add_argument("--since", default=None, help="Only publish changes since this UTC timestamp (ISO format).")

    p.add_argument(
        "--include-meta-profile",
        action="store_true",
        help=(
            "Add meta.profile declarations. By default, the NL GF profile set is used; "
            "switch with --profile-set."
        ),
    )
    p.add_argument(
        "--profile-set",
        choices=["nl", "ihe", "none"],
        default=None,
        help=(
            "Which profile set to declare when --include-meta-profile is used: "
            "nl=NL Generic Functions, ihe=IHE mCSD, none=no declarations."
        ),
    )
    p.add_argument(
        "--assigned-id-system-base",
        default=None,
        help=(
            "Base URI for author-assigned identifier systems (Required by NL GF AssignedId slices). "
            "Example: https://example.org/identifiers"
        ),
    )
    p.add_argument(
        "--default-ura",
        default=None,
        help=(
            "Fallback URA number to use as identifier.assigner when the source data cannot be mapped to a kliniek URA. "
            "Prefer to provide URANummer per kliniek in the brondata."
        ),
    )
    p.add_argument(
        "--publisher-ura",
        "--ura",
        default=None,
        help=(
            "Hard override for the URA used to identify the publishing organization (PoC/prod switch without DB changes). "
            "When set, this value is used for Organization.identifier(URA) and as the assigner URA for AssignedId identifiers on Organization/Location/HealthcareService/etc. "
            "If not set, URANummer from brondata is used; if that is missing, --default-ura is the fallback."
        ),
    )
    p.add_argument(
        "--default-endpoint-payload",
        action="append",
        default=None,
        help=(
            "Default Endpoint.payloadType to use when the source row has none. "
            "Format: system|code|display (repeatable). Example: "
            "http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities|http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities|BGZ" 
        ),
    )
    p.add_argument(
        "--bgz-policy",
        choices=["off", "any", "per-clinic", "per-afdeling", "per-afdeling-or-clinic"],
        default=None,
        help=(
            "BGZ-only endpoint sanity policy. "
            "off=disable BGZ policy checks; any=require >=1 active BGZ endpoint somewhere; "
            "per-clinic=require >=1 active BGZ endpoint per active kliniek (default); "
            "per-afdeling=require >=1 active BGZ endpoint per active afdeling; "
            "per-afdeling-or-clinic=afdeling may inherit from parent kliniek."
        ),
    )

    p.add_argument("--include-practitioners", action="store_true", help="Also publish Practitioner and PractitionerRole (via tblMedewerker + tblMedewerkerinzet).")
    p.add_argument(
        "--delete-inactive",
        action="store_true",
        help=(
            "Use DELETE for inactive records (instead of PUT with inactive status). "
            "NOTE: Endpoint is NOT deleted by default (NL GF recommendation). Use --allow-delete-endpoint to override."
        ),
    )
    p.add_argument(
        "--allow-delete-endpoint",
        action="store_true",
        help="Allow DELETE for Endpoint when --delete-inactive is set (not recommended).",
    )

    p.add_argument(
        "--mtls-cert",
        default=None,
        help="Client certificate (PEM). If MTLS_KEY is not provided, this must include the private key.",
    )
    p.add_argument(
        "--mtls-key",
        default=None,
        help="Client certificate private key (PEM) for mutual TLS (optional if included in --mtls-cert).",
    )
    p.add_argument(
        "--lenient",
        action="store_true",
        help="Do not hard-fail on missing optional NL-GF-required mapping inputs; emit warnings and use safe fallbacks where possible.",
    )

    p.add_argument("--dry-run", action="store_true", help="Don't POST; print or write the Bundle JSON.")
    p.add_argument("--out", default=None, help="Write Bundle JSON to file (only used with --dry-run).")

    # HTTP behaviour
    p.add_argument("--timeout", type=int, default=None, help="HTTP read timeout in seconds.")
    p.add_argument("--connect-timeout", type=float, default=None, help="HTTP connect timeout in seconds.")
    p.add_argument("--http-retries", type=int, default=None, help="HTTP retries on network/status errors (0=disabled).")
    p.add_argument("--http-backoff", type=float, default=None, help="Retry backoff factor (e.g., 0.5).")
    p.add_argument("--http-pool-connections", type=int, default=None, help="HTTP connection pool: number of pools.")
    p.add_argument("--http-pool-maxsize", type=int, default=None, help="HTTP connection pool: max connections per pool.")

    # Observability / safety
    p.add_argument("--log-level", default=None, help="Log level (DEBUG, INFO, WARNING, ERROR).")
    p.add_argument("--production", action="store_true", help="Treat risky settings as errors (e.g. http:// fhir-base, --no-verify-tls).")

    return p


def main() -> int:
    p = _build_arg_parser()
    args = p.parse_args()

    # Load defaults from environment/.env (CLI has precedence).
    try:
        settings = Settings()
    except Exception as e:
        print(f"Invalid environment/.env settings: {e}", file=sys.stderr)
        return 2

    if not args.sql_conn:
        args.sql_conn = settings.sql_conn
    if not args.fhir_base:
        args.fhir_base = settings.fhir_base
    if getattr(args, 'sqlite_reset_seed', None) is None:
        args.sqlite_reset_seed = bool(getattr(settings, 'sqlite_reset_seed', False))
    if getattr(args, 'fhir_reset_seed', None) is None:
        args.fhir_reset_seed = bool(getattr(settings, 'fhir_reset_seed', False))
    if not args.token:
        args.token = settings.token
    if not args.oauth_token_url:
        args.oauth_token_url = settings.oauth_token_url
    if not args.oauth_client_id:
        args.oauth_client_id = settings.oauth_client_id
    if not args.oauth_client_secret:
        args.oauth_client_secret = settings.oauth_client_secret
    if not args.oauth_scope:
        args.oauth_scope = settings.oauth_scope
    if args.bundle_size is None:
        args.bundle_size = settings.bundle_size
    if args.since is None:
        args.since = settings.since_utc
    if args.profile_set is None:
        args.profile_set = settings.profile_set
    if args.assigned_id_system_base is None:
        args.assigned_id_system_base = settings.assigned_id_system_base
    if args.default_ura is None:
        args.default_ura = settings.default_ura
    if getattr(args, "publisher_ura", None) is None:
        args.publisher_ura = settings.publisher_ura
    if args.bgz_policy is None:
        args.bgz_policy = settings.bgz_policy
    if args.mtls_cert is None:
        args.mtls_cert = settings.mtls_cert
    if args.mtls_key is None:
        args.mtls_key = settings.mtls_key
    if args.timeout is None:
        args.timeout = settings.timeout_seconds
    if args.connect_timeout is None:
        args.connect_timeout = settings.connect_timeout_seconds
    if args.http_retries is None:
        args.http_retries = settings.http_retries
    if args.http_backoff is None:
        args.http_backoff = settings.http_backoff_factor
    if args.http_pool_connections is None:
        args.http_pool_connections = settings.http_pool_connections
    if args.http_pool_maxsize is None:
        args.http_pool_maxsize = settings.http_pool_maxsize
    if args.log_level is None:
        args.log_level = settings.log_level

    # Validate settings that bypass argparse choices
    if args.profile_set is not None and str(args.profile_set).strip().lower() not in ("nl", "ihe", "none"):
        print("Invalid --profile-set (expected 'nl'|'ihe'|'none').", file=sys.stderr)
        return 2
    if args.bgz_policy is not None and str(args.bgz_policy).strip().lower() not in ("off", "any", "per-clinic", "per-afdeling", "per-afdeling-or-clinic"):
        print("Invalid --bgz-policy (expected off|any|per-clinic|per-afdeling|per-afdeling-or-clinic).", file=sys.stderr)
        return 2

    if not args.sql_conn:
        print("Missing --sql-conn (or SQL_CONN).", file=sys.stderr)
        return 2
    if not args.fhir_base:
        print("Missing --fhir-base (or FHIR_BASE).", file=sys.stderr)
        return 2

    since_dt: Optional[dt.datetime] = None
    if args.since:
        try:
            s = str(args.since).replace("Z", "+00:00")
            since_dt = dt.datetime.fromisoformat(s)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=dt.timezone.utc)
        except Exception as e:
            print(f"Invalid --since value: {args.since!r} ({e})", file=sys.stderr)
            return 2

    # Parse default Endpoint.payloadType(s).
    # If the user does not provide --default-endpoint-payload, we default to BGZ-only (SYS ZBC use case).
    default_payload_types: List[Tuple[str, str, str]] = []
    if args.default_endpoint_payload is None:
        default_payload_types = list(DEFAULT_ENDPOINT_PAYLOAD_TYPES_BGZ)
    elif args.default_endpoint_payload:
        for raw in args.default_endpoint_payload:
            sraw = str(raw).strip()
            if not sraw:
                continue
            parts = [p.strip() for p in sraw.split("|")]
            if len(parts) < 2:
                print(
                    f"Invalid --default-endpoint-payload value: {raw!r}. Expected system|code|display (display optional).",
                    file=sys.stderr,
                )
                return 2
            system = parts[0]
            code = parts[1]
            display = "|".join(parts[2:]).strip() if len(parts) > 2 else ""
            default_payload_types.append((system, code, display))

    cfg = Config(
        sql_conn=args.sql_conn,
        fhir_base=args.fhir_base,
        sqlite_reset_seed=bool(getattr(args, 'sqlite_reset_seed', False)),
        fhir_reset_seed=bool(getattr(args, 'fhir_reset_seed', False)),
        bearer_token=args.token or None,
        oauth_token_url=args.oauth_token_url or None,
        oauth_client_id=args.oauth_client_id or None,
        oauth_client_secret=args.oauth_client_secret or None,
        oauth_scope=args.oauth_scope or None,
        verify_tls=not bool(args.no_verify_tls),
        bundle_size=max(1, int(args.bundle_size)),
        since_utc=since_dt,
        include_meta_profile=bool(args.include_meta_profile),
        profile_set=str(args.profile_set or "nl"),
        assigned_id_system_base=str(args.assigned_id_system_base or "").strip() or "https://sys.local/identifiers",
        default_ura=_normalize_ura(args.default_ura) or None,
        publisher_ura=_normalize_ura(getattr(args, "publisher_ura", None)) or None,
        default_endpoint_payload_types=tuple(default_payload_types),
        bgz_policy=str(args.bgz_policy or "per-clinic"),
        mtls_cert=str(args.mtls_cert).strip() if args.mtls_cert else None,
        mtls_key=str(args.mtls_key).strip() if args.mtls_key else None,
        include_practitioners=bool(args.include_practitioners),
        delete_inactive=bool(args.delete_inactive),
        allow_delete_endpoint=bool(args.allow_delete_endpoint),
        strict=not bool(args.lenient),
        dry_run=bool(args.dry_run),
        out_file=args.out,
        timeout_seconds=max(1, int(args.timeout)),
        connect_timeout_seconds=float(args.connect_timeout),
        http_pool_connections=max(1, int(args.http_pool_connections)),
        http_pool_maxsize=max(1, int(args.http_pool_maxsize)),
        http_retries=max(0, int(args.http_retries)),
        http_backoff_factor=float(args.http_backoff),
        log_level=str(args.log_level or "INFO"),
        is_production=bool(args.production),
    )

    _configure_logging(cfg.log_level)

    try:
        run(cfg)
        return 0
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        status = resp.status_code if resp is not None else None

        if resp is not None:
            oo = _try_json(resp)
            if isinstance(oo, dict) and oo.get("resourceType") == "OperationOutcome":
                msg = _format_operation_outcome(oo)
                if msg:
                    logger.error("FHIR server OperationOutcome (HTTP %s): %s", status, msg)

            logger.error("HTTP error during publish (status=%s): %s", status, str(e))
            logger.debug("HTTP response headers: %s", dict(resp.headers))
            logger.debug("HTTP response body excerpt: %s", _response_excerpt(resp))
        else:
            logger.error("HTTP error during publish: %s", str(e))

        return 1
    except requests.exceptions.RequestException as e:
        info = _classify_requests_exception(e)
        logger.error("HTTP request failed reason=%s: %s", info.get("reason"), info.get("message"))
        logger.debug("RequestException type=%s", type(e).__name__, exc_info=True)
        return 1
    except OSError as e:
        logger.error("OS fout: %s", str(e))
        logger.debug("OSError type=%s", type(e).__name__, exc_info=True)
        return 1
    except OperationalError as e:
        detail = str(getattr(e, "orig", e))
        logger.error("Database-operatie mislukt: %s", detail)
        logger.debug("OperationalError type=%s", type(e).__name__, exc_info=True)
        return 1
    except SQLAlchemyError as e:
        logger.error("Databasefout: %s", str(e))
        logger.debug("SQLAlchemyError type=%s", type(e).__name__, exc_info=True)
        return 1
    except Exception:
        logger.exception("Onverwachte fout tijdens uitvoeren van ITI-130 publisher.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
