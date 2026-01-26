# main.py
import base64
import copy
import hashlib
import hmac
import httpx
import json
import re
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import time
import socket
import ssl
import sys
import uuid
from pathlib import Path
from functools import lru_cache
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from contextvars import ContextVar
from fastapi import FastAPI, Query, HTTPException, Request, Depends, Header, Body
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional, Tuple, Literal
from urllib.parse import urlparse
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stdout)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("mcsd.app")
audit_logger = logging.getLogger("mcsd.audit")
REQUEST_ID_CTX: ContextVar[str] = ContextVar("request_id", default="")
UP_SEQ_CTX: ContextVar[int] = ContextVar("up_seq", default=0)
def _fmt_rid(rid: str) -> str:
    if not rid:
        return "-"
    if len(rid) <= 12:
        return rid
    return f"{rid[:8]}..{rid[-4:]}"
def _fmt_up_url(url: httpx.URL) -> str:
    try:
        return str(url)
    except Exception:
        return str(url)

class Settings(BaseSettings):
    base_url: str = Field("https://example-fhir/mcsd", validation_alias="MCSD_BASE")
    upstream_timeout: float = Field(30.0, validation_alias="MCSD_UPSTREAM_TIMEOUT")
    bearer_token: Optional[str] = Field(None, validation_alias="MCSD_BEARER_TOKEN")
    verify_tls: bool = Field(True, validation_alias="MCSD_VERIFY_TLS")
    ca_certs_file: Optional[str] = Field(None, validation_alias="MCSD_CA_CERTS_FILE")
    allow_origins: List[str] = Field(["*"], validation_alias="MCSD_ALLOW_ORIGINS")
    allowed_hosts: List[str] = Field(["*"], validation_alias="MCSD_ALLOWED_HOSTS")
    api_key: Optional[str] = Field(None, validation_alias="MCSD_API_KEY")
    sender_ura: Optional[str] = Field(None, validation_alias="MCSD_SENDER_URA")
    sender_name: Optional[str] = Field(None, validation_alias="MCSD_SENDER_NAME")
    sender_uzi_sys: Optional[str] = Field(None, validation_alias="MCSD_SENDER_UZI_SYS")
    sender_system_name: Optional[str] = Field(None, validation_alias="MCSD_SENDER_SYSTEM_NAME")
    sender_bgz_base: Optional[str] = Field(None, validation_alias="MCSD_SENDER_BGZ_BASE")
    audit_hmac_key: Optional[str] = Field(None, validation_alias="MCSD_AUDIT_HMAC_KEY")
    allow_task_preview_in_production: bool = Field(False, validation_alias="MCSD_ALLOW_TASK_PREVIEW_IN_PRODUCTION")
    log_level: str = Field("INFO", validation_alias="MCSD_LOG_LEVEL")
    is_production: bool = Field(False, validation_alias="MCSD_IS_PRODUCTION")
    httpx_max_connections: int = Field(50, validation_alias="MCSD_HTTPX_MAX_CONNECTIONS")
    httpx_max_keepalive_connections: int = Field(20, validation_alias="MCSD_HTTPX_MAX_KEEPALIVE_CONNECTIONS")
    max_query_params: int = Field(50, validation_alias="MCSD_MAX_QUERY_PARAMS")
    max_query_value_length: int = Field(256, validation_alias="MCSD_MAX_QUERY_VALUE_LENGTH")
    max_query_param_values: int = Field(20, validation_alias="MCSD_MAX_QUERY_PARAM_VALUES")
    capability_cache_ttl_seconds: int = Field(600, validation_alias="MCSD_CAPABILITY_CACHE_TTL_SECONDS")
    debug_dump_json: bool = Field(False, validation_alias="MCSD_DEBUG_DUMP_JSON")
    debug_dump_dir: str = Field("/tmp/mcsd-debug", validation_alias="MCSD_DEBUG_DUMP_DIR")
    debug_dump_redact: bool = Field(True, validation_alias="MCSD_DEBUG_DUMP_REDACT")
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent / ".env"),
        case_sensitive=False,
    )

# Load .env early so its values (like MCSD_DEBUG_DUMP_DIR) are applied consistently.
try:
    from dotenv import dotenv_values, load_dotenv
    _dotenv_path = (Path(__file__).parent / ".env").resolve()
    _is_prod_raw = os.getenv("MCSD_IS_PRODUCTION")
    if _is_prod_raw is None and _dotenv_path.exists():
        try:
            _is_prod_raw = (dotenv_values(str(_dotenv_path)).get("MCSD_IS_PRODUCTION") or "")
        except Exception:
            _is_prod_raw = ""
    _is_prod = str(_is_prod_raw or "").strip().lower() in ("1", "true", "yes", "on")
    if _dotenv_path.exists():
        load_dotenv(dotenv_path=str(_dotenv_path), override=not _is_prod)
        logger.info("[mCSD] .env loaded file=%s override=%s", str(_dotenv_path), "on" if (not _is_prod) else "off")
    else:
        logger.info("[mCSD] .env not found file=%s", str(_dotenv_path))
except Exception:
    logger.exception("[mCSD] .env load failed")
settings = Settings()
def _audit_hash(value: str | None) -> str | None:
    secret = (settings.audit_hmac_key or "").encode("utf-8")
    raw = (value or "").strip()
    if not secret or not raw:
        return None
    digest = hmac.new(secret, raw.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest[:16]
def audit_event(event_type: str, **fields: Any) -> None:
    rid = REQUEST_ID_CTX.get()
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": str(event_type),
        "request_id": str(rid or ""),
    }
    for k, v in fields.items():
        if v is None:
            continue
        payload[str(k)] = v
    try:
        audit_logger.info("%s", json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        #Never break the request flow due to audit logging.
        logger.exception("audit_event logging failed")

def _redact_debug_payload(payload: Any) -> Any:
    if not settings.debug_dump_redact:
        return payload
    try:
        out = copy.deepcopy(payload)
    except Exception:
        return payload
    bsn_systems = {"http://fhir.nl/fhir/NamingSystem/bsn", "urn:oid:2.16.840.1.113883.2.4.6.3"}
    def _walk(x: Any) -> None:
        if isinstance(x, dict):
            sys_val = x.get("system")
            if isinstance(sys_val, str) and sys_val in bsn_systems:
                val = x.get("value")
                if isinstance(val, str) and val:
                    x["value"] = "***REDACTED***"
            for k, v in list(x.items()):
                if k in {"patient_bsn", "bsn"} and isinstance(v, str) and v:
                    x[k] = "***REDACTED***"
                else:
                    _walk(v)
        elif isinstance(x, list):
            for it in x:
                _walk(it)
    _walk(out)
    return out
def _resolve_debug_dump_dir() -> Path:
    raw = str(settings.debug_dump_dir or "").strip()
    if not raw:
        raw = "/tmp/mcsd-debug"
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (Path(__file__).parent / p).resolve()
    return p

def _setup_file_logging() -> None:
    # Write logs to a rotating file in the debug dump directory (useful when running under systemd/journal).
    try:
        log_dir = _resolve_debug_dump_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        log_file = log_dir / f"mcsd_{ts}.log"
        # Ensure the file exists immediately at startup
        try:
            with open(log_file, "a", encoding="utf-8"):
                pass
        except Exception:
            pass
        root = logging.getLogger()
        desired_name = (settings.log_level or "INFO").upper()
        desired_level = logging._nameToLevel.get(desired_name, logging.INFO)
        if root.level > desired_level:
            root.setLevel(desired_level)
        for h in list(getattr(root, "handlers", []) or []):
            try:
                if isinstance(h, TimedRotatingFileHandler) and getattr(h, "baseFilename", "") == str(log_file):
                    return
                if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(log_file):
                    return
            except Exception:
                continue
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        fh.setLevel(desired_level)
        root.addHandler(fh)
        logger.info("[mCSD] file logging enabled file=%s", str(log_file))
    except Exception:
        # Never fail app startup due to file logging configuration.
        logger.exception("File logging setup failed")

def _dump_debug_json(label: str, payload: Any) -> None:
    if not settings.debug_dump_json:
        return
    try:
        out_dir = _resolve_debug_dump_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        rid_part = (_fmt_rid(REQUEST_ID_CTX.get() or "") or "-").replace("..", "_").replace(".", "_")
        safe_label = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in (label or "payload"))
        filename = f"{safe_label}_{rid_part}_{uuid.uuid4().hex}_{ts}.json"
        data = _redact_debug_payload(payload)
        with open(out_dir / filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("Debug JSON dump failed")

logger.setLevel(settings.log_level.upper())

async def on_startup():
    _setup_file_logging()
    logger.info("[mCSD] pydantic env_file=%s", str((Path(__file__).parent / ".env").resolve()))
    logger.info(
        "[mCSD] base=%s timeout=%ss auth=%s",
        settings.base_url,
        settings.upstream_timeout,
        "on" if settings.bearer_token else "off",
    )
    if settings.debug_dump_json:
        out_dir = _resolve_debug_dump_dir()
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            logger.info("[mCSD] debug dump JSON=on dir=%s", str(out_dir))
        except Exception:
            logger.exception("[mCSD] debug dump JSON=on but cannot create dir=%s", str(out_dir))
    if settings.is_production:
        if settings.allow_origins == ["*"]:
            raise RuntimeError("MCSD_ALLOW_ORIGINS staat op '*' in productie; stel expliciete origins in.")
        if settings.allowed_hosts == ["*"]:
            raise RuntimeError("MCSD_ALLOWED_HOSTS staat op '*' in productie; stel expliciete hosts in.")
        if not settings.verify_tls:
            raise RuntimeError("MCSD_VERIFY_TLS staat uit in productie; dit is onveilig.")

    # PoC 9: valideer BgZ templates (fail fast) om runtime KeyErrors te voorkomen.
    try:
        _validate_notification_task_template()
        _validate_workflow_task_template()
    except Exception as exc:
        raise RuntimeError(f"BgZ template validatie faalde: {exc}")





async def _httpx_log_request(request: httpx.Request):
    rid_full = REQUEST_ID_CTX.get()
    rid = _fmt_rid(rid_full)
    if rid_full:
        request.headers.setdefault("X-Request-ID", rid_full)
    seq = UP_SEQ_CTX.get() + 1
    UP_SEQ_CTX.set(seq)
    request.extensions["up_seq"] = seq
    url = _fmt_up_url(request.url)
    logger.info("UPSTREAM REQ  %-7s %s", request.method, url)

async def _httpx_log_response(response: httpx.Response):
    rid_full = REQUEST_ID_CTX.get()
    rid = _fmt_rid(rid_full)
    seq = response.request.extensions.get("up_seq")
    url = _fmt_up_url(response.request.url)
    status = response.status_code
    if status >= 400:
        try:
            dbg = _extract_receiver_error_debug(response, max_body_chars=1200)
            msg = (dbg.get("receiver_message") or "").strip() if isinstance(dbg.get("receiver_message"), str) else ""
            if msg:
                logger.warning("UPSTREAM RESP status=%s %s msg=%s", status, url, msg)
            else:
                logger.warning("UPSTREAM RESP status=%s %s", status, url)
        except Exception:
            logger.warning("UPSTREAM RESP status=%s %s", status, url)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await on_startup()
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise
    app.state.http_client = httpx.AsyncClient(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS, verify=HTTPX_VERIFY, event_hooks={"request": [_httpx_log_request], "response": [_httpx_log_response]})
    app.state.capability_cache = {}
    try:
        yield
    finally:
        await app.state.http_client.aclose()

class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = (request.headers.get("X-Request-ID") or "").strip()
        if not request_id:
            request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        token = REQUEST_ID_CTX.set(request_id)
        seq_token = UP_SEQ_CTX.set(0)
        client_host = request.client.host if request.client else ""
        rid = _fmt_rid(request_id)
        logger.info(
            "CLIENT   REQ  %-7s %s client=%s",
            request.method,
            request.url.path,
            client_host or "-",
        )
        response = None
        try:
            response = await call_next(request)
            return response
        finally:
            status_code = response.status_code if response is not None else 500
            if status_code >= 400:
                logger.warning("CLIENT RESP  status=%s", status_code)
            if response is not None:
                response.headers["X-Request-ID"] = request_id
            UP_SEQ_CTX.reset(seq_token)
            REQUEST_ID_CTX.reset(token)


# --- Uniform error responses ---------------------------------------------

class ErrorResponse(BaseModel):
    """Uniform error response.

    PoC 9 afspraak: één consistent error-object met `reason`, `message`, `request_id`.

    Voor debug/PoC kan optioneel `details` worden meegegeven. In productie wordt dit
    standaard niet teruggegeven om onbedoelde lekken van interne informatie te voorkomen.
    """

    reason: str
    message: str
    request_id: str
    details: Optional[Dict[str, Any]] = None


def _default_reason_for_status(status_code: int) -> str:
    if status_code == 400:
        return "bad_request"
    if status_code == 401:
        return "unauthorized"
    if status_code == 403:
        return "forbidden"
    if status_code == 404:
        return "not_found"
    if status_code == 409:
        return "conflict"
    if status_code == 422:
        return "validation_error"
    if status_code == 502:
        return "bad_gateway"
    if status_code >= 500:
        return "internal_error"
    return "http_error"


def _make_error_payload(
    *,
    status_code: int,
    request_id: str,
    detail: Any,
) -> Dict[str, Any]:
    """Normalize various FastAPI/HTTPException detail shapes to the PoC9 error contract."""
    reason = _default_reason_for_status(status_code)
    message = "Er ging iets mis."
    details: Optional[Dict[str, Any]] = None
    # Keep error payload minimal & consistent for the frontend contract.
    # Extra debug-informatie kan in `details` worden meegegeven (maar niet in productie).

    # If the detail is already an object with reason/message, prefer those.
    if isinstance(detail, dict):
        if isinstance(detail.get("reason"), str):
            reason = detail.get("reason")
        if isinstance(detail.get("message"), str):
            message = detail.get("message")
        elif isinstance(detail.get("detail"), str):
            message = detail.get("detail")
        else:
            # Don't spam a huge upstream payload into the message.
            message = message if message else "HTTP-fout."

        raw_details = detail.get("details")
        if isinstance(raw_details, dict):
            details = raw_details
    elif isinstance(detail, str):
        message = detail
    else:
        message = str(detail) if detail is not None else message

    # Never expose debug details in production.
    if settings.is_production:
        details = None

    return ErrorResponse(
        reason=str(reason or "http_error"),
        message=str(message or "Er ging iets mis."),
        request_id=str(request_id or ""),
        details=details,
    ).model_dump(exclude_none=True)

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.allowed_hosts,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestIdMiddleware)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "") or str(uuid.uuid4())
    payload = _make_error_payload(
        status_code=int(exc.status_code),
        request_id=request_id,
        detail=exc.detail,
    )
    return JSONResponse(
        status_code=int(exc.status_code),
        content=payload,
        headers={"X-Request-ID": request_id},
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    request_id = getattr(request.state, "request_id", "") or str(uuid.uuid4())
    payload = ErrorResponse(
        reason="validation_error",
        message="Request validatie mislukt.",
        request_id=request_id,
    ).model_dump(exclude_none=True)
    return JSONResponse(
        status_code=422,
        content=payload,
        headers={"X-Request-ID": request_id},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "") or str(uuid.uuid4())
    logger.exception("Onverwachte fout bij %s %s req_id=%s", request.method, request.url.path, request_id)
    payload = ErrorResponse(
        reason="internal_error",
        message="Interne serverfout. Neem contact op met de beheerder.",
        request_id=request_id,
    ).model_dump(exclude_none=True)
    return JSONResponse(
        status_code=500,
        content=payload,
        headers={"X-Request-ID": request_id},
    )

MCSDBASE = settings.base_url
TIMEOUT = settings.upstream_timeout
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=TIMEOUT, write=TIMEOUT, pool=TIMEOUT)
HTTPX_LIMITS = httpx.Limits(max_connections=settings.httpx_max_connections, max_keepalive_connections=settings.httpx_max_keepalive_connections)
HTTPX_VERIFY = settings.ca_certs_file if settings.verify_tls and settings.ca_certs_file else settings.verify_tls

def verify_api_key(x_api_key: str | None = Header(default=None)):
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Ongeldige API key.")

# Whitelist aligned with ITI-90 (commonly used parameters per resource)
ALLOWED_PARAMS = {
    "Practitioner": {"active", "identifier", "name", "given", "family"},
    "PractitionerRole": {
        "active", "location", "organization", "practitioner", "role", "service", "specialty", "_include",
        "practitioner.name", "practitioner.family", "practitioner.given", "practitioner.identifier", "practitioner.active",
        "organization.name", "organization.name:contains", "organization.identifier", "organization.type", "organization.address-city", "organization.active",
        "location.name:contains", "location.address-line", "location.address-city", "location.address-postalcode", "location.address-country", "location.near", "location.near-distance",
    },
    "HealthcareService": {"active", "identifier", "location", "name", "organization", "service-type"},
    "Location": {"identifier", "name", "address", "near", "organization", "status", "type", "_include"},
    "Organization": {"active", "identifier", "name", "address", "type", "partof", "_include"},
    "Endpoint": {"identifier", "organization", "status", "address", "connection-type", "payload-type", "payload-mime-type"},
    "OrganizationAffiliation": {"active", "date", "identifier", "participating-organization", "primary-organization", "role", "_include"},
}

def build_params(resource: str, incoming: Dict[str, Any]) -> Dict[str, Any]:
    allowed = ALLOWED_PARAMS.get(resource)
    if not allowed:
        raise HTTPException(status_code=400, detail=f"Resource '{resource}' not allowed for ITI-90.")
    params = {k: v for k, v in incoming.items() if v is not None and (k in allowed or k in {"_count","_include","_revinclude"})}
    if "_count" in params:
        raw_count = params.get("_count")
        if isinstance(raw_count, list):
            raw_count = raw_count[-1] if raw_count else None
        try:
            count_int = int(raw_count)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Ongeldige _count waarde.")
        if count_int < 1:
            count_int = 1
        if count_int > 200:
            count_int = 200
        params["_count"] = count_int
    return params

def _auth_headers() -> Dict[str, str]:
    headers = {"Accept": "application/fhir+json"}
    headers["User-Agent"] = "mCSD-Client/1.0"
    token = settings.bearer_token
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

def _classify_upstream_exception(e: Exception) -> dict:
    # Bepaal hoofdreden van failure op de upstream-verbinding
    # dns: socket.gaierror in de cause chain
    # timeout: httpx.ReadTimeout / httpx.ConnectTimeout
    # tls: ssl.SSLError
    # network: overige connect/transport errors
    def _chain(ex: BaseException):
        while ex is not None:
            yield ex
            ex = ex.__cause__ or ex.__context__
    info = {"reason": "network", "message": "Upstream mCSD server is niet bereikbaar."}
    for ex in _chain(e):
        if isinstance(ex, (httpx.ReadTimeout, httpx.ConnectTimeout)):
            info["reason"] = "timeout"
            info["message"] = "Timeout bij contact met upstream mCSD server."
            return info
        if isinstance(ex, ssl.SSLError):
            info["reason"] = "tls"
            info["message"] = "TLS-fout bij contact met upstream mCSD server."
            return info
        if isinstance(ex, socket.gaierror):
            info["reason"] = "dns"
            info["message"] = "DNS-fout bij contact met upstream mCSD server."
            return info
    return info

def _raise_502_with_reason(e: Exception):
    info = _classify_upstream_exception(e)
    logger.error("UP  ERR req_id=%s reason=%s type=%s", REQUEST_ID_CTX.get() or "-", info.get("reason"), type(e).__name__)
    raise HTTPException(status_code=502, detail=info)


def _truncate_text(text: str, limit: int = 2000) -> str:
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + f"...(truncated, len={len(text)})"


def _redact_text_minimal(text: str) -> str:
    """Conservatieve redactie voor tekst (alleen BSN OID waarden)."""
    try:
        return re.sub(r"(urn:oid:2\.16\.840\.1\.113883\.2\.4\.6\.3\.)\d+", r"\1***REDACTED***", text)
    except Exception:
        return text


def _extract_trace_headers(headers: httpx.Headers) -> Dict[str, str]:
    """Selecteer de meest nuttige headers voor troubleshooting/correlatie."""
    keep = {
        "content-type",
        "content-length",
        "date",
        "server",
        "x-request-id",
        "x-correlation-id",
        "traceparent",
        "tracestate",
        "x-amzn-trace-id",
    }
    out: Dict[str, str] = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in keep:
            out[k] = v
            continue
        if lk.startswith("x-") and any(t in lk for t in ("request", "correlation", "trace", "span")):
            out[k] = v
    return out


def _summarize_operation_outcome(body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Maak een compacte samenvatting van een FHIR OperationOutcome."""
    if not isinstance(body, dict) or body.get("resourceType") != "OperationOutcome":
        return None
    issues = body.get("issue") or []
    if not isinstance(issues, list):
        return None
    summarized: List[Dict[str, Any]] = []
    for it in issues[:10]:
        if not isinstance(it, dict):
            continue
        details_txt = None
        details_obj = it.get("details")
        if isinstance(details_obj, dict):
            details_txt = details_obj.get("text")
        summarized.append({
            "severity": it.get("severity"),
            "code": it.get("code"),
            "details": details_txt,
            "diagnostics": it.get("diagnostics"),
            "location": it.get("location"),
            "expression": it.get("expression"),
        })
    return {"resourceType": "OperationOutcome", "issue": summarized}


def _operation_outcome_to_message(oo: Dict[str, Any]) -> Optional[str]:
    try:
        issues = (oo or {}).get("issue") or []
        if not isinstance(issues, list):
            return None
        parts: List[str] = []
        for it in issues[:3]:
            if not isinstance(it, dict):
                continue
            details_txt = None
            d = it.get("details")
            if isinstance(d, dict):
                details_txt = d.get("text")
            diag = it.get("diagnostics")
            msg = details_txt or diag
            if isinstance(msg, str) and msg.strip():
                parts.append(msg.strip())
        return "; ".join(parts) if parts else None
    except Exception:
        return None


def _extract_receiver_error_debug(response: httpx.Response, *, max_body_chars: int = 2000) -> Dict[str, Any]:
    """Extraheer bruikbare debug-informatie uit een fout response van een receiver."""
    content_type = str(response.headers.get("content-type") or "").strip() or None
    trace_headers = _extract_trace_headers(response.headers)
    body_json: Any = None
    body_text: Optional[str] = None
    receiver_message: Optional[str] = None
    operation_outcome: Optional[Dict[str, Any]] = None

    try:
        body_json = response.json()
        body_json = _redact_debug_payload(body_json)
        if isinstance(body_json, dict):
            if body_json.get("resourceType") == "OperationOutcome":
                operation_outcome = _summarize_operation_outcome(body_json) or body_json
                receiver_message = _operation_outcome_to_message(operation_outcome or {})
            else:
                # Veel voorkomende velden in non-FHIR error bodies
                for k in ("message", "detail", "error"):  # pragma: no cover
                    v = body_json.get(k)
                    if isinstance(v, str) and v.strip():
                        receiver_message = v.strip()
                        break
    except Exception:
        body_json = None

    if body_json is not None:
        try:
            body_text = json.dumps(body_json, ensure_ascii=False)
        except Exception:
            body_text = None
    if body_text is None:
        try:
            body_text = response.text
        except Exception:
            body_text = None

    snippet = None
    if isinstance(body_text, str) and body_text:
        snippet = _truncate_text(_redact_text_minimal(body_text), max_body_chars)

    return {
        "receiver_http_status": int(response.status_code),
        "receiver_content_type": content_type,
        "receiver_headers": trace_headers or None,
        "receiver_message": receiver_message,
        "receiver_operation_outcome": operation_outcome,
        "receiver_body_snippet": snippet,
    }

async def _get_json(client: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # print("UPSTREAM GET " + url + (("?" + str(httpx.QueryParams(params))) if params else ""))
    r = await client.get(url, params=params, headers=_auth_headers())
    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail={"reason": "invalid_json", "message": "Upstream did not return valid JSON."})

async def fetch_first_page(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Retrieve a single FHIR Bundle page (no 'next' following); returns entry.resource dicts.
    resources: List[Dict[str, Any]] = []
    try:
        data = await _get_json(app.state.http_client, url, params)
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)
    for e in data.get("entry", []) or []:
        res = e.get("resource")
        if res:
            resources.append(res)
    return resources

async def fetch_all_pages(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retrieve a FHIR Bundle and follow 'next' links until all pages are fetched.
    Returns a list with all entry.resource dicts.
    """
    resources: List[Dict[str, Any]] = []
    try:
        current_url = url
        current_params = params
        seen_urls: set[str] = set()
        client = app.state.http_client
        while True:
            seen_key = current_url + "|" + repr(current_params)
            if seen_key in seen_urls:
                break
            seen_urls.add(seen_key)
            data = await _get_json(client, current_url, current_params)
            for e in data.get("entry", []) or []:
                res = e.get("resource")
                if res:
                    resources.append(res)
            next_url = None
            for l in data.get("link", []) or []:
                if l.get("relation") == "next" and l.get("url"):
                    next_url = l["url"]
                    break
            if not next_url:
                break
            current_url = next_url
            current_params = {}
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)
    return resources

async def try_fetch_first_page(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Best-effort single-page fetch: return [] on upstream errors.
    try:
        return await fetch_first_page(url, params)
    except HTTPException as e:
        logger.warning("Upstream HTTP-fout bij try_fetch_first_page %s: status=%s", url, e.status_code)
        return []
    except Exception as e:
        logger.warning("Upstream fout bij try_fetch_first_page %s: type=%s", url, type(e).__name__)
        return []

async def try_fetch_all_pages(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Best-effort paged fetch: return [] on upstream errors.
    try:
        return await fetch_all_pages(url, params)
    except HTTPException as e:
        logger.warning("Upstream HTTP-fout bij try_fetch_all_pages %s: status=%s", url, e.status_code)
        return []
    except Exception as e:
        logger.warning("Upstream fout bij try_fetch_all_pages %s: type=%s", url, type(e).__name__)
        return []

def _telecom_value(resource: Dict[str, Any], system: str) -> Optional[str]:
    for t in resource.get("telecom", []) or []:
        if t.get("system") == system and t.get("value"):
            return t["value"]
    return None

def _name_to_string(human_name: Dict[str, Any]) -> str:
    if not human_name:
        return ""
    if human_name.get("text"):
        return human_name["text"]
    given = " ".join(human_name.get("given", []) or [])
    family = human_name.get("family") or ""
    prefix = " ".join(human_name.get("prefix", []) or [])
    parts = [p for p in [prefix, given, family] if p]
    return " ".join(parts)

def _primary_name(prac: Dict[str, Any]) -> str:
    names = prac.get("name", []) or []
    return _name_to_string(names[0]) if names else ""

def _address_to_string(addr: Dict[str, Any]) -> str:
    if not addr:
        return ""
    parts = []
    if addr.get("line"):
        parts.append(" ".join(addr["line"]))
    if addr.get("postalCode"):
        parts.append(addr["postalCode"])
    if addr.get("city"):
        parts.append(addr["city"])
    if addr.get("country"):
        parts.append(addr["country"])
    return ", ".join([p for p in parts if p])

def _first_address(resource: Dict[str, Any]) -> str:
    addr = resource.get("address")
    if not addr:
        return ""
    if isinstance(addr, list):
        return _address_to_string(addr[0]) if addr else ""
    if isinstance(addr, dict):
        return _address_to_string(addr)
    return ""

def _display(coding_like: Dict[str, Any]) -> str:
    if not coding_like:
        return ""
    if coding_like.get("text"):
        return coding_like["text"]
    coding = coding_like.get("coding", []) or []
    if coding:
        disp = coding[0].get("display") or ""
        code = coding[0].get("code") or ""
        if disp:
            return disp
        return code
    return ""

def _index_by_id(resources: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    # key: (resourceType, id)
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in resources:
        rt = r.get("resourceType")
        rid = r.get("id")
        if rt and rid:
            out[(rt, rid)] = r
    return out

def _chunks(seq: List[str], size: int) -> List[List[str]]:
    # Split a list into fixed-size chunks to keep query strings small for upstream servers.
    return [seq[i:i+size] for i in range(0, len(seq), size)]

def _http_status_to_http_exception(e: httpx.HTTPStatusError) -> HTTPException:
    status_code = e.response.status_code
    try:
        payload = e.response.json()
    except ValueError:
        logger.error(
            "Upstream HTTP-fout %s zonder JSON-body (content-type=%s, length=%s)",
            status_code,
            e.response.headers.get("content-type"),
            e.response.headers.get("content-length"),
        )
        payload = {"reason": "upstream_http_error", "status_code": status_code}
    return HTTPException(status_code=status_code, detail=payload)

@app.get("/health")
def health():
    """
    Simple readiness/liveness endpoint.
    Returns 200 OK if the FastAPI app is running.
    Does not probe the upstream mCSD server.
    """
    return {"status": "ok"}

@app.get("/mcsd/search/{resource}", dependencies=[Depends(verify_api_key)])
async def mcsd_search(resource: str, request: Request):
    """
    Pass-through FHIR search with allow-list filtering.
    Accepts arbitrary query parameters and forwards only allowed ones.
    """
    raw = request.query_params
    raw_items = list(raw.multi_items())
    if len(raw_items) > settings.max_query_params:
        raise HTTPException(status_code=400, detail="Te veel query parameters.")
    incoming: Dict[str, Any] = {}
    for k, v in raw_items:
        if v is not None and len(v) > settings.max_query_value_length:
            raise HTTPException(status_code=400, detail="Query parameter value is te lang.")
        if k in incoming:
            if isinstance(incoming[k], list):
                incoming[k].append(v)
            else:
                incoming[k] = [incoming[k], v]
        else:
            incoming[k] = v

    for k, v in incoming.items():
        if isinstance(v, list) and len(v) > settings.max_query_param_values:
            raise HTTPException(status_code=400, detail="Te veel waarden voor query parameter.")

    params = build_params(resource, incoming)
    url = f"{MCSDBASE.rstrip('/')}/{resource}"
    try:
        return await _get_json(app.state.http_client, url, params)
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)

@app.get("/addressbook/find-practitionerrole", dependencies=[Depends(verify_api_key)])
async def find_practitionerrole(name: str = Query(..., description="Search by practitioner name (string)", max_length=64),
                                organization: str | None = Query(None, max_length=128),
                                specialty: str | None = Query(None, max_length=128)):
    # Convenience endpoint; keep logic minimal
    try:
        client = app.state.http_client
        p_data = await _get_json(client, f"{MCSDBASE.rstrip('/')}/Practitioner", {"name": name})
        practitioners: List[str] = []
        for e in p_data.get("entry", []) or []:
            res = (e or {}).get("resource") or {}
            if res.get("resourceType") == "Practitioner" and res.get("id"):
                practitioners.append(res["id"])
        if not practitioners:
            return {"total": 0, "entry": []}
        pr_params = {"practitioner": ",".join(f"Practitioner/{pid}" for pid in practitioners), "_include": "PractitionerRole:practitioner"}
        if organization:
            pr_params["organization"] = organization
        if specialty:
            pr_params["specialty"] = specialty
        return await _get_json(client, f"{MCSDBASE.rstrip('/')}/PractitionerRole", pr_params)
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)

def _validate_near(near: str):
    parts = (near or "").split("|")
    if len(parts) != 4:
        raise HTTPException(status_code=400, detail="Ongeldig 'near' formaat.")
    try:
        lat = float(parts[0])
        lng = float(parts[1])
        dist = float(parts[2])
    except ValueError:
        raise HTTPException(status_code=400, detail="Ongeldig 'near' formaat.")
    if lat < -90.0 or lat > 90.0 or lng < -180.0 or lng > 180.0 or dist < 0.0:
        raise HTTPException(status_code=400, detail="Ongeldig 'near' formaat.")
    unit = parts[3]
    if unit not in {"km", "m", "cm", "mm", "mi", "yd", "ft", "in"}:
        raise HTTPException(status_code=400, detail="Ongeldig 'near' formaat.")

@app.get("/addressbook/search", dependencies=[Depends(verify_api_key)])
async def addressbook_search(
    name: str | None = Query(None, description="Free-text Practitioner name", max_length=64),
    family: str | None = Query(None, max_length=64),
    given: str | None = Query(None, max_length=64),
    organization: str | None = Query(None, description="e.g., Organization/123 or via org_name", max_length=128),
    org_name: str | None = Query(None, max_length=128),
    specialty: str | None = Query(None, max_length=128),
    city: str | None = Query(None, max_length=64),
    postal: str | None = Query(None, max_length=16),
    near: str | None = Query(None, description="lat|lng|distance|units (FHIR near)", max_length=64),
    limit: int = Query(200, ge=1, le=2000),
    mode: str = Query("fast", description="Result breadth: 'fast' (default) or 'full'", max_length=8),
    *, request: Request
):
    """
    1) Find Practitioners (name/given/family) -> ids
    2) Find PractitionerRole with _include=practitioner,organization,location
    3) Optionally filter on org_name, city/postal/near (server- or client-side depending on server features)
    4) Enrich rows:
       - HealthcareService offered by the same Organization or attached Location
       - OrganizationAffiliation relationships of the Organization (with org names via _include)
    5) Flatten into VBA-friendly rows
    """
    try:
        base = MCSDBASE.rstrip('/')
        fast_mode = (mode or "fast").lower() != "full"

        # Support chained-style query keys as aliases for addressbook/search facade params
        qp = request.query_params

        if name is None:
            name = qp.get("practitioner.name")
        if family is None:
            family = qp.get("practitioner.family")
        if given is None:
            given = qp.get("practitioner.given")

        practitioner_identifier = qp.get("practitioner.identifier")

        if org_name is None:
            org_name = qp.get("organization.name:contains") or qp.get("organization.name")

        if city is None:
            city = qp.get("location.address-city") or qp.get("organization.address-city")

        if postal is None:
            postal = qp.get("location.address-postalcode")

        if near is None:
            near_raw = qp.get("location.near")
            if near_raw:
                near_raw = near_raw.strip()
                if "|" in near_raw:
                    near = near_raw
                else:
                    parts = [p.strip() for p in near_raw.split(",")]
                    if len(parts) == 2 and parts[0] and parts[1]:
                        lat = parts[0]
                        lng = parts[1]
                        dist = "0"
                        unit = "km"
                        near_dist_raw = qp.get("location.near-distance")
                        if near_dist_raw:
                            s = near_dist_raw.strip()
                            i = len(s) - 1
                            while i >= 0 and s[i].isalpha():
                                i -= 1
                            num = s[:i+1].strip()
                            u = s[i+1:].strip()
                            if num:
                                dist = num
                            if u:
                                unit = u
                        near = f"{lat}|{lng}|{dist}|{unit}"

        if fast_mode:
            pr_seed_count = min(max(limit, 1) * 2, 50)     # Practitioner _count (first page)
            prrole_count = min(max(limit, 1), 20)          # PractitionerRole _count (first page)
            max_prac_refs = pr_seed_count                  # cap practitioner refs passed to roles
            max_ids = min(max(limit, 1) * 3, 60)           # cap org_ids / loc_ids list length
            batch_size = 10                                # chunk size for HS/Affiliation queries
        else:
            pr_seed_count = min(max(limit, 1) * 10, 500)
            prrole_count = min(max(limit, 1) * 5, 200)
            max_prac_refs = min(pr_seed_count, 500)
            max_ids = min(max(limit, 1) * 20, 1000)
            batch_size = 50

        # 1) Practitioners (with paging)
        prac_params: Dict[str, Any] = {}
        if name:    prac_params["name"] = name
        if family:  prac_params["family"] = family
        if given:   prac_params["given"] = given
        if practitioner_identifier: prac_params["identifier"] = practitioner_identifier
        prac_url = f"{base}/Practitioner"
        # Fetch only enough practitioners to seed practitioner-role lookups
        # (we cap later via max_prac_refs; use that here as _count too)
        prac_params["_count"] = pr_seed_count
        practitioners = await fetch_first_page(prac_url, prac_params) if prac_params else []
        if prac_params and not practitioners:
            return {"total": 0, "rows": []}
        prac_refs = [f"Practitioner/{p['id']}" for p in practitioners if p.get("id")]
        if len(prac_refs) > max_prac_refs:
            prac_refs = prac_refs[:max_prac_refs]

        # 2) PractitionerRole with includes (and optional filters)
        pr_params: Dict[str, Any] = {
            "_include": ["PractitionerRole:practitioner", "PractitionerRole:organization", "PractitionerRole:location"]
        }
        if prac_refs:
            pr_params["practitioner"] = ",".join(prac_refs)
        if organization:
            pr_params["organization"] = organization
        if specialty:
            pr_params["specialty"] = specialty
        if near:
            _validate_near(near)
            pr_params["location.near"] = near

        pr_url = f"{base}/PractitionerRole"
        pr_query_params: Dict[str, Any] = {}
        for k, v in pr_params.items():
            pr_query_params[k] = v
        # Ask only a small first page; we break once we built 'limit' rows anyway
        pr_query_params["_count"] = prrole_count
        resources = await try_fetch_first_page(pr_url, pr_query_params)
        if not fast_mode and len(resources) < limit: resources = await try_fetch_all_pages(pr_url, pr_query_params)

        # 3) Build indexes so we can join locally
        idx = _index_by_id(resources + practitioners)

        # Collect unique Organization and Location ids present in PractitionerRole results
        org_ids: List[str] = []
        loc_ids: List[str] = []
        for r in resources:
            if r.get("resourceType") == "PractitionerRole":
                if r.get("organization", {}).get("reference"):
                    ref = r["organization"]["reference"]
                    if "/" in ref:
                        org_ids.append(ref.split("/")[1])
                if r.get("location"):
                    for lr in r["location"]:
                        ref = lr.get("reference") or ""
                        if "/" in ref:
                            loc_ids.append(ref.split("/")[1])

        org_ids = list(dict.fromkeys(org_ids))
        loc_ids = list(dict.fromkeys(loc_ids))

        # Keep upstream query strings small
        org_ids = org_ids[:max_ids]
        loc_ids = loc_ids[:max_ids]

        # 4a) Fetch HealthcareService linked to Organizations or Locations (server-side filtering where possible)
        hs_all: List[Dict[str, Any]] = []
        if org_ids or loc_ids:
            # Batch size keeps query strings compact for public HAPI
            for chunk in _chunks(org_ids, batch_size):
                if not chunk:
                    continue
                hs_org_params = {"organization": ",".join(f"Organization/{i}" for i in chunk)}
                hs_org = await try_fetch_all_pages(f"{base}/HealthcareService", hs_org_params)
                hs_all.extend(hs_org)
            for chunk in _chunks(loc_ids, batch_size):
                if not chunk:
                    continue
                hs_loc_params = {"location": ",".join(f"Location/{i}" for i in chunk)}
                hs_loc = await try_fetch_all_pages(f"{base}/HealthcareService", hs_loc_params)
                hs_all.extend(hs_loc)
        hs_by_id = _index_by_id(hs_all)

        hs_by_org_id: Dict[str, List[Dict[str, Any]]] = {}
        hs_by_loc_id: Dict[str, List[Dict[str, Any]]] = {}
        for h in hs_all:
            if h.get("resourceType") != "HealthcareService":
                continue
            prov_ref = (h.get("providedBy") or {}).get("reference") or ""
            if prov_ref.startswith("Organization/"):
                oid = prov_ref.split("/", 1)[1]
                hs_by_org_id.setdefault(oid, []).append(h)
            for lr in h.get("location", []) or []:
                lref = (lr or {}).get("reference") or ""
                if lref.startswith("Location/"):
                    lid = lref.split("/", 1)[1]
                    hs_by_loc_id.setdefault(lid, []).append(h)

        # 4b) Fetch OrganizationAffiliation for Organizations (both directions) including linked Organization resources
        aff_all: List[Dict[str, Any]] = []
        if org_ids:
            include_orgs = ["OrganizationAffiliation:organization", "OrganizationAffiliation:participating-organization"]
            for chunk in _chunks(org_ids, batch_size):
                if not chunk:
                    continue
                aff_primary_params = {
                    "primary-organization": ",".join(f"Organization/{i}" for i in chunk),
                    "_include": include_orgs
                }
                aff_part_params = {
                    "participating-organization": ",".join(f"Organization/{i}" for i in chunk),
                    "_include": include_orgs
                }
                aff_primary = await try_fetch_all_pages(f"{base}/OrganizationAffiliation", aff_primary_params)
                aff_part = await try_fetch_all_pages(f"{base}/OrganizationAffiliation", aff_part_params)
                aff_all.extend(aff_primary)
                aff_all.extend(aff_part)
        aff_by_id = _index_by_id(aff_all)

        aff_by_org_id: Dict[str, List[Dict[str, Any]]] = {}
        for a in aff_all:
            if a.get("resourceType") != "OrganizationAffiliation":
                continue
            p_org_ref = (a.get("organization") or {}).get("reference") or ""
            part_org_ref = (a.get("participatingOrganization") or {}).get("reference") or ""
            if p_org_ref.startswith("Organization/"):
                aff_by_org_id.setdefault(p_org_ref.split("/", 1)[1], []).append(a)
            if part_org_ref.startswith("Organization/"):
                aff_by_org_id.setdefault(part_org_ref.split("/", 1)[1], []).append(a)

        # 5) Client-side additional filters on org_name/city/postal if needed
        rows: List[Dict[str, Any]] = []
        for r in resources:
            if r.get("resourceType") != "PractitionerRole":
                continue

            prac_ref = r.get("practitioner", {}).get("reference") or ""
            prac = idx.get(tuple(prac_ref.split("/") if "/" in prac_ref else ("", "")))

            org_ref = r.get("organization", {}).get("reference") or ""
            org = idx.get(tuple(org_ref.split("/") if "/" in org_ref else ("", "")))

            loc_ref = ""
            loc: Optional[Dict[str, Any]] = None
            if r.get("location"):
                loc_ref = r["location"][0].get("reference") or ""
                loc = idx.get(tuple(loc_ref.split("/") if "/" in loc_ref else ("", "")))

            if org_name:
                org_nm = (org.get("name") if org else "") if isinstance(org, dict) else ""
                if not org_nm or org_name.lower() not in org_nm.lower():
                    continue

            if city or postal:
                addr_ok = False

                if loc and loc.get("address"):
                    a = loc["address"]
                    if city and (a.get("city", "") or "").lower() != city.lower():
                        addr_ok = False
                    elif postal and (a.get("postalCode", "") or "").replace(" ", "").lower() != postal.replace(" ", "").lower():
                        addr_ok = False
                    else:
                        addr_ok = True
                elif org and isinstance(org, dict) and org.get("address"):
                    addrs = org.get("address")
                    if isinstance(addrs, dict):
                        addrs = [addrs]
                    if isinstance(addrs, list):
                        for a in addrs:
                            if not isinstance(a, dict):
                                continue
                            if city and (a.get("city", "") or "").lower() != city.lower():
                                continue
                            if postal and (a.get("postalCode", "") or "").replace(" ", "").lower() != postal.replace(" ", "").lower():
                                continue
                            addr_ok = True
                            break

                if not addr_ok:
                    continue

            prac_name = _primary_name(prac or {})
            org_name_out = (org.get("name") if org else "") if isinstance(org, dict) else ""

            specialty_out = ""
            if r.get("specialty"):
                specialty_out = _display(r["specialty"][0])

            role_out = ""
            if r.get("code"):
                role_out = _display(r["code"][0])

            phone = _telecom_value(prac or {}, "phone") or _telecom_value(org or {}, "phone") or ""
            email = _telecom_value(prac or {}, "email") or _telecom_value(org or {}, "email") or ""
            address_str = _first_address(loc or org or prac or {})

            # Enrichment: HealthcareService linked to same org or location (first match summarized)
            hs_name = ""
            hs_type = ""
            hs_contact = ""
            service_org_name = ""
            hs_candidates: List[Dict[str, Any]] = []

            if isinstance(org, dict) and org.get("id"):
                hs_candidates = hs_by_org_id.get(org["id"], []) or []
            if not hs_candidates and isinstance(loc, dict) and loc.get("id"):
                hs_candidates = hs_by_loc_id.get(loc["id"], []) or []
            if hs_candidates:
                h0 = hs_candidates[0]
                hs_name = h0.get("name") or ""
                if h0.get("serviceType"):
                    hs_type = _display(h0["serviceType"][0])
                hs_contact = _telecom_value(h0, "phone") or _telecom_value(h0, "email") or ""
                # Resolve provider org name from HealthcareService.providedBy (best-effort using already fetched orgs)
                prov_ref = h0.get("providedBy", {}).get("reference") or ""
                if prov_ref.startswith("Organization/"):
                    prov_id = prov_ref.split("/")[1]
                    o = idx.get(("Organization", prov_id)) or aff_by_id.get(("Organization", prov_id))
                    if isinstance(o, dict):
                        service_org_name = o.get("name") or ""

            # Enrichment: OrganizationAffiliation for the org (first match summarized) + resolve org names through included Organizations
            aff_primary_org = ""
            aff_primary_org_name = ""
            aff_participating_org = ""
            aff_participating_org_name = ""
            aff_role = ""

            if isinstance(org, dict) and org.get("id"):
                for a in aff_by_org_id.get(org["id"], []) or []:
                    if a.get("resourceType") != "OrganizationAffiliation":
                        continue
                    p_org_ref = a.get("organization", {}).get("reference") or ""
                    part_org_ref = a.get("participatingOrganization", {}).get("reference") or ""
                    if p_org_ref == f"Organization/{org['id']}" or part_org_ref == f"Organization/{org['id']}":
                        aff_primary_org = p_org_ref.replace("Organization/", "") if p_org_ref else ""
                        aff_participating_org = part_org_ref.replace("Organization/", "") if part_org_ref else ""
                        if aff_primary_org:
                            o = aff_by_id.get(("Organization", aff_primary_org))
                            if isinstance(o, dict):
                                aff_primary_org_name = o.get("name") or ""
                        if aff_participating_org:
                            o = aff_by_id.get(("Organization", aff_participating_org))
                            if isinstance(o, dict):
                                aff_participating_org_name = o.get("name") or ""
                        if a.get("role"):
                            aff_role = _display(a["role"][0])
                        break

            rows.append({
                "practitioner_id": (prac.get("id") if isinstance(prac, dict) else ""),
                "practitioner_name": prac_name,
                "organization_id": (org.get("id") if isinstance(org, dict) else ""),
                "organization_name": org_name_out,
                "role": role_out,
                "specialty": specialty_out,
                "city": (loc.get("address", {}).get("city") if isinstance(loc, dict) else ""),
                "postal": (loc.get("address", {}).get("postalCode") if isinstance(loc, dict) else ""),
                "address": address_str,
                "phone": phone,
                "email": email,
                "service_name": hs_name,
                "service_type": hs_type,
                "service_contact": hs_contact,
                "service_org_name": service_org_name,
                "affiliation_primary_org": aff_primary_org,
                "affiliation_primary_org_name": aff_primary_org_name,
                "affiliation_participating_org": aff_participating_org,
                "affiliation_participating_org_name": aff_participating_org_name,
                "affiliation_role": aff_role,
            })
            if len(rows) >= limit:
                break

        return {"total": len(rows), "rows": rows}

    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)

@app.get("/addressbook/organization", dependencies=[Depends(verify_api_key)])
async def addressbook_search_organization(name: str | None = Query(None, description="Organization name (string)", max_length=128), active: bool = True, limit: int = 20, contains: bool = False, *, request: Request):
    """
    Zoek organisaties met functionele mailboxen.
    - mCSD ITI-90: Organization?active=true&name=<...>&_include=Organization:endpoint
    - Retourneert e-mail uit Organization.telecom (system=email) en uit meegeleverde Endpoint.address (mailto:...)
    - Geen Practitioner/PractitionerRole contactgegevens.
    """
    base = MCSDBASE.rstrip('/')

    eff_name = request.query_params.get("name:contains") or name
    if eff_name is not None and len(eff_name) > 128:
        raise HTTPException(status_code=400, detail="Parameter 'name' is te lang.")
    eff_contains = contains or (request.query_params.get("name:contains") is not None)
    params: Dict[str, Any] = {("name:contains" if eff_contains else "name"): eff_name, "active": "true" if active else "false", "_include": "Organization:endpoint", "_count": max(min(limit, 100), 1)}
    # voorkom dat een lege naam als name= of name:contains= wordt gestuurd (server kan dat als �match alles� interpreteren).
    params = {k: v for k, v in params.items() if v is not None}

    def _emails_from_org(org: Dict[str, Any]) -> List[str]:
        emails: List[str] = []
        for t in org.get("telecom", []) or []:
            if t.get("system") == "email" and t.get("value"):
                emails.append(t["value"])
        return emails

    def _email_from_endpoint(ep: Dict[str, Any]) -> Optional[str]:
        addr = (ep or {}).get("address") or ""
        if isinstance(addr, str) and addr.lower().startswith("mailto:") and len(addr) > 7:
            return addr[7:]
        return None

    try:
        data = await _get_json(app.state.http_client, f"{base}/Organization", params)
        included: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for e in data.get("entry", []) or []:
            res = e.get("resource") or {}
            if res.get("resourceType") == "Endpoint" and res.get("id"):
                included[("Endpoint", res["id"])] = res

        rows: List[Dict[str, Any]] = []
        for e in data.get("entry", []) or []:
            org = e.get("resource") or {}
            if org.get("resourceType") != "Organization":
                continue
            org_id = org.get("id") or ""
            org_name = org.get("name") or ""
            seen: set[str] = set()
            for em in _emails_from_org(org):
                if em not in seen:
                    rows.append({"resourceType": "Organization", "id": org_id, "name": org_name, "email": em, "source": "organization.telecom"})
                    seen.add(em)
            for ref in org.get("endpoint", []) or []:
                ref_str = (ref or {}).get("reference") or ""
                if ref_str.startswith("Endpoint/"):
                    ep = included.get(("Endpoint", ref_str.split("/", 1)[1]))
                    em = _email_from_endpoint(ep or {})
                    if em and em not in seen:
                        rows.append({"resourceType": "Organization", "id": org_id, "name": org_name, "email": em, "source": "organization.endpoint"})
                        seen.add(em)
        return {"count": len(rows), "items": rows}
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)

@app.get("/addressbook/location", dependencies=[Depends(verify_api_key)])
async def addressbook_search_location(name: str | None = Query(None, description="Location name (string)", max_length=128), limit: int = 20, contains: bool = False, *, request: Request):
    """
    Zoek locaties en geef de functionele mailbox van de zorgverlener (organisatie) terug.
    - Primair: Location?name=<...>&_include=Location:organization (indien ondersteund)
    - Val terug: fetch Organization?_id=<ids>&_include=Organization:endpoint
    - E-mail komt uit Organization.telecom of Endpoint.address (mailto:...).
    - Geen Practitioner/PractitionerRole contactgegevens.
    """
    base = MCSDBASE.rstrip('/')
    eff_name = request.query_params.get("name:contains") or name
    if eff_name is not None and len(eff_name) > 128:
        raise HTTPException(status_code=400, detail="Parameter 'name' is te lang.")
    eff_contains = contains or (request.query_params.get("name:contains") is not None)
    loc_params: Dict[str, Any] = {("name:contains" if eff_contains else "name"): eff_name, "_count": max(min(limit, 100), 1), "_include": "Location:organization"}
    # voorkom dat een lege naam als name= of name:contains= wordt gestuurd (server kan dat als �match alles� interpreteren).
    loc_params = {k: v for k, v in loc_params.items() if v is not None}

    def _emails_from_org(org: Dict[str, Any]) -> List[str]:
        emails: List[str] = []
        for t in org.get("telecom", []) or []:
            if t.get("system") == "email" and t.get("value"):
                emails.append(t["value"])
        return emails

    def _email_from_endpoint(ep: Dict[str, Any]) -> Optional[str]:
        addr = (ep or {}).get("address") or ""
        if isinstance(addr, str) and addr.lower().startswith("mailto:") and len(addr) > 7:
            return addr[7:]
        return None

    try:
        loc_bundle = await _get_json(app.state.http_client, f"{base}/Location", loc_params)
        orgs_included: Dict[str, Dict[str, Any]] = {}
        for e in loc_bundle.get("entry", []) or []:
            res = e.get("resource") or {}
            if res.get("resourceType") == "Organization" and res.get("id"):
                orgs_included[res["id"]] = res

        locs: List[Dict[str, Any]] = []
        org_ids: List[str] = []
        for e in loc_bundle.get("entry", []) or []:
            res = e.get("resource") or {}
            if res.get("resourceType") == "Location":
                locs.append(res)
                mo = ((res.get("managingOrganization") or {}).get("reference") or "")
                if mo.startswith("Organization/"):
                    org_ids.append(mo.split("/", 1)[1])

        org_ids = sorted({oid for oid in org_ids})
        org_bundle = None
        if org_ids:
            try:
                org_bundle = await _get_json(app.state.http_client, f"{base}/Organization", {"_id": ",".join(org_ids), "_include": "Organization:endpoint", "_count": len(org_ids)})
            except httpx.HTTPStatusError:
                org_bundle = None

        endpoints_included: Dict[str, Dict[str, Any]] = {}
        org_by_id: Dict[str, Dict[str, Any]] = {}

        def _scan_org_bundle(bundle: Dict[str, Any]):
            for e in bundle.get("entry", []) or []:
                r = e.get("resource") or {}
                if r.get("resourceType") == "Endpoint" and r.get("id"):
                    endpoints_included[r["id"]] = r
                if r.get("resourceType") == "Organization" and r.get("id"):
                    org_by_id[r["id"]] = r

        if org_bundle:
            _scan_org_bundle(org_bundle)
        for oid, o in orgs_included.items():
            if oid not in org_by_id:
                org_by_id[oid] = o

        rows: List[Dict[str, Any]] = []
        for loc in locs:
            loc_id = loc.get("id") or ""
            loc_name = loc.get("name") or ""
            org_ref = ((loc.get("managingOrganization") or {}).get("reference") or "")
            org_id = org_ref.split("/", 1)[1] if org_ref.startswith("Organization/") else ""
            org = org_by_id.get(org_id) or {}
            org_name = org.get("name") or ""
            seen: set[str] = set()

            # eerst e-mails direct op de Location pakken (Location.telecom)
            for em in _emails_from_org(loc or {}):
                if em not in seen:
                    rows.append({
                        "resourceType": "Location",
                        "id": loc_id,
                        "name": loc_name,
                        "organizationId": org_id,
                        "organizationName": org_name,
                        "email": em,
                        "source": "location.telecom",
                    })
                    seen.add(em)

            # daarna e-mails op de gekoppelde Organization
            for em in _emails_from_org(org or {}):
                if em not in seen:
                    rows.append({"resourceType": "Location", "id": loc_id, "name": loc_name, "organizationId": org_id, "organizationName": org_name, "email": em, "source": "organization.telecom"})
                    seen.add(em)

            # tenslotte e-mail via Organization.endpoint (mailto:...)
            for ref in (org.get("endpoint") or []):
                ref_str = (ref or {}).get("reference") or ""
                if ref_str.startswith("Endpoint/"):
                    ep = endpoints_included.get(ref_str.split("/", 1)[1])
                    em = _email_from_endpoint(ep or {})
                    if em and em not in seen:
                        rows.append({"resourceType": "Location", "id": loc_id, "name": loc_name, "organizationId": org_id, "organizationName": org_name, "email": em, "source": "organization.endpoint"})
                        seen.add(em)
        return {"count": len(rows), "items": rows}
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)


def _endpoint_to_technical(ep: Dict[str, Any]) -> Dict[str, Any]:
    # Convert an Endpoint resource to a compact "technical endpoint" representation (PoC 8/9).
    # NOTE: Keep this separate from _email_from_endpoint() (which is mailbox-focused).
    def _coding_to_dict(c: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "system": c.get("system"),
            "code": c.get("code"),
            "display": c.get("display"),
        }

    ct = (ep or {}).get("connectionType") or {}
    payload_types: List[Dict[str, Any]] = []
    for pt in (ep or {}).get("payloadType", []) or []:
        for c in pt.get("coding", []) or []:
            payload_types.append(_coding_to_dict(c or {}))

    return {
        "id": (ep or {}).get("id"),
        "address": (ep or {}).get("address"),
        "status": (ep or {}).get("status"),
        "connectionType": _coding_to_dict(ct or {}),
        "payloadType": payload_types,
        "payloadMimeType": (ep or {}).get("payloadMimeType") or [],
        "header": (ep or {}).get("header") or [],
    }

def _split_ref(ref: str) -> Tuple[str, str]:
    """Split a FHIR Reference into (resourceType, id).

    Supports relative references ("ResourceType/id"), versioned references
    ("ResourceType/id/_history/vid") and absolute URLs.
    """
    if not isinstance(ref, str):
        return ("", "")
    ref = ref.strip()
    if not ref:
        return ("", "")

    # Absolute URL reference
    if "://" in ref:
        try:
            p = urlparse(ref)
            path = (p.path or "").strip("/")
            parts = [x for x in path.split("/") if x]
        except Exception:
            return ("", "")
        if len(parts) >= 4 and parts[-2] == "_history":
            return (parts[-4], parts[-3])
        if len(parts) >= 2:
            return (parts[-2], parts[-1])
        return ("", "")

    # Relative reference
    parts = [x for x in ref.strip("/").split("/") if x]
    if len(parts) >= 4 and parts[-2] == "_history":
        return (parts[-4], parts[-3])
    if len(parts) >= 2:
        return (parts[0], parts[1])
    return ("", "")

def _parse_token(token: str) -> Tuple[Optional[str], str]:
    # FHIR token search values are typically "system|code" or just "code".
    if not token:
        return (None, "")
    if "|" in token:
        sys_part, code_part = token.split("|", 1)
        return (sys_part or None, code_part or "")
    return (None, token)

def _coding_matches_token(coding: Dict[str, Any], token: str) -> bool:
    sys_part, code_part = _parse_token(token)
    if not code_part:
        return False
    if sys_part is None and (code_part.startswith("http://") or code_part.startswith("https://")):
        if (coding or {}).get("code") == code_part:
            return True
        return (coding or {}).get("system") == code_part
    if (coding or {}).get("code") != code_part:
        return False
    if sys_part is None:
        return True
    return (coding or {}).get("system") == sys_part

def _endpoint_matches_filters(
    ep: Dict[str, Any],
    *,
    endpoint_kind: Optional[str] = None,
    connection_type: Optional[str] = None,
    payload_type: Optional[str] = None,
    payload_mime_type: Optional[str] = None,
) -> bool:
    if connection_type:
        ct = ((ep or {}).get("connectionType") or {}).get("coding") or []
        if not any(_coding_matches_token(c or {}, connection_type) for c in ct):
            return False

    if payload_type:
        pts = (ep or {}).get("payloadType") or []
        ok = False
        for pt in pts:
            for c in (pt or {}).get("coding") or []:
                if _coding_matches_token(c or {}, payload_type):
                    ok = True
                    break
            if ok:
                break
        if not ok:
            return False

    if payload_mime_type:
        mimes = (ep or {}).get("payloadMimeType") or []
        if payload_mime_type not in mimes:
            return False

    # endpoint_kind is intentionally heuristic; prefer exact filtering using token parameters above.
    if endpoint_kind:
        k = endpoint_kind.strip().lower()
        addr = ((ep or {}).get("address") or "").lower() if isinstance((ep or {}).get("address"), str) else ""
        ct_codes: List[str] = []
        for c in (((ep or {}).get("connectionType") or {}).get("coding") or []):
            if (c or {}).get("code"):
                ct_codes.append(str((c or {}).get("code")).lower())
            if (c or {}).get("system"):
                ct_codes.append(str((c or {}).get("system")).lower())

        pt_codes: List[str] = []
        for pt in (ep or {}).get("payloadType") or []:
            for c in (pt or {}).get("coding") or []:
                if (c or {}).get("code"):
                    pt_codes.append(str((c or {}).get("code")).lower())
                if (c or {}).get("system"):
                    pt_codes.append(str((c or {}).get("system")).lower())

        mime_codes = [str(x).lower() for x in ((ep or {}).get("payloadMimeType") or [])]

        haystack = " ".join([addr] + ct_codes + pt_codes + mime_codes)

        if k == "fhir":
            return ("fhir" in haystack) or ("/fhir" in addr)
        if k == "notification":
            return ("notify" in haystack) or ("notification" in haystack) or ("subscription" in haystack) or ("rest-hook" in haystack)
        if k == "auth":
            return ("oauth" in haystack) or ("openid" in haystack) or ("jwt" in haystack) or ("auth" in haystack)
        # Unknown kind -> do not filter out.
    return True

async def _fetch_bundle(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return await _get_json(app.state.http_client, url, params)
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)


def _capability_supports_search_param(capability_statement: Dict[str, Any], resource_type: str, param_name: str) -> bool:
    try:
        for r in (capability_statement or {}).get("rest", []) or []:
            for res in (r or {}).get("resource", []) or []:
                if (res or {}).get("type") == resource_type:
                    for sp in (res or {}).get("searchParam", []) or []:
                        if (sp or {}).get("name") == param_name:
                            return True
        return False
    except Exception:
        return False


def _ttl_cache_get(cache: Any, key: Any) -> Optional[Any]:
    """Get a cached value (with TTL) from a plain dict.

    Stored format: cache[key] = (value, expires_epoch_seconds)
    """
    if not isinstance(cache, dict):
        return None
    entry = cache.get(key)
    if entry is None:
        return None
    if isinstance(entry, tuple) and len(entry) == 2:
        value, expires_at = entry
        try:
            if float(expires_at) >= time.time():
                return value
        except Exception:
            pass
        # expired or invalid -> delete
        cache.pop(key, None)
        return None
    # Backwards compatible: treat raw values as non-expiring.
    return entry


def _ttl_cache_set(cache: Any, key: Any, value: Any, ttl_seconds: int) -> None:
    if not isinstance(cache, dict):
        return
    ttl = int(ttl_seconds or 0)
    if ttl <= 0:
        cache[key] = value
        return
    cache[key] = (value, time.time() + ttl)

async def _upstream_supports_location_managing_organization(base: str) -> bool:
    base_norm = (base or "").rstrip("/")
    cache = getattr(app.state, "capability_cache", None)
    cache_key = (base_norm, "Location", "managing-organization")
    cached = _ttl_cache_get(cache, cache_key)
    if cached is not None:
        return bool(cached)

    supported = False
    try:
        cap = await _get_json(app.state.http_client, f"{base_norm}/metadata", {})
        supported = _capability_supports_search_param(cap, "Location", "managing-organization")
    except Exception:
        supported = False

    _ttl_cache_set(cache, cache_key, supported, settings.capability_cache_ttl_seconds)
    return supported
def _index_included(bundle: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    included: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in (bundle or {}).get("entry", []) or []:
        res = (e or {}).get("resource") or {}
        rt = res.get("resourceType")
        rid = res.get("id")
        if rt and rid:
            included[(rt, rid)] = res
    return included


# --- Pagination helpers (cursor-based) ------------------------------------

def _bundle_link_url(bundle: Dict[str, Any], rel: str) -> Optional[str]:
    for l in (bundle or {}).get("link", []) or []:
        if (l or {}).get("relation") == rel and (l or {}).get("url"):
            return str((l or {}).get("url"))
    return None

def _bundle_next_url(bundle: Dict[str, Any]) -> Optional[str]:
    return _bundle_link_url(bundle, "next")

def _cursor_encode(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def _cursor_decode(token: str) -> Dict[str, Any]:
    if not token or not isinstance(token, str):
        raise HTTPException(status_code=400, detail="Ongeldige cursor.")
    # restore base64 padding
    padded = token + ("=" * (-len(token) % 4))
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Ongeldige cursor.")
    if not isinstance(obj, dict):
        raise HTTPException(status_code=400, detail="Ongeldige cursor.")
    return obj

def _assert_safe_upstream_url(url: str) -> str:
    """
    Prevent SSRF: only allow paging URLs that stay within MCSDBASE origin and path prefix.

    Returns a normalized absolute URL (so we can also accept relative 'next' links).
    """
    if not url or not isinstance(url, str):
        raise HTTPException(status_code=400, detail="Ongeldige cursor-url.")

    base = MCSDBASE.rstrip("/")
    p_base = urlparse(base)
    p_url = urlparse(url)

    # Some FHIR servers return relative 'next' links; normalize to absolute using the base origin.
    if not p_url.scheme or not p_url.netloc:
        if url.startswith("/"):
            url = f"{p_base.scheme}://{p_base.netloc}{url}"
            p_url = urlparse(url)
        else:
            raise HTTPException(status_code=400, detail="Ongeldige cursor-url.")

    # Same origin
    if (p_url.scheme, p_url.netloc) != (p_base.scheme, p_base.netloc):
        raise HTTPException(status_code=400, detail="Cursor-url verwijst naar een andere host.")

    # Same path prefix
    base_path = p_base.path.rstrip("/")
    if base_path and not p_url.path.startswith(base_path):
        raise HTTPException(status_code=400, detail="Cursor-url valt buiten het toegestane pad.")

    return url


# --- Response models (PoC 9 frontend/backend contract) --------------------


class CodingModel(BaseModel):
    system: Optional[str] = None
    code: Optional[str] = None
    display: Optional[str] = None


class TechnicalEndpointModel(BaseModel):
    id: Optional[str] = None
    address: Optional[str] = None
    status: Optional[str] = None
    connectionType: Optional[CodingModel] = None
    payloadType: List[CodingModel] = Field(default_factory=list)
    payloadMimeType: List[str] = Field(default_factory=list)
    header: List[str] = Field(default_factory=list)
    # Used by capability-mapping previews
    source: Optional[str] = None
    sourceRef: Optional[str] = None


class OrganizationSearchItem(BaseModel):
    resourceType: Literal["Organization"] = "Organization"
    id: str
    name: Optional[str] = None
    identifier: List[Dict[str, Any]] = Field(default_factory=list)
    type: List[Dict[str, Any]] = Field(default_factory=list)
    endpoints: List[TechnicalEndpointModel] = Field(default_factory=list)


class PagedOrganizationsResponse(BaseModel):
    count: int
    items: List[OrganizationSearchItem] = Field(default_factory=list)
    next: Optional[str] = None
    total: Optional[int] = None


class OrgUnitSearchItem(BaseModel):
    # UI gebruikt minimaal resourceType/id/name
    resourceType: str
    id: str
    name: Optional[str] = None
    # Extra velden (voor debug/demo)
    organization: Optional[str] = None
    partOf: Optional[str] = None
    specialty: List[Dict[str, Any]] = Field(default_factory=list)
    endpoints: List[TechnicalEndpointModel] = Field(default_factory=list)


class PagedOrgUnitsResponse(BaseModel):
    count: int
    items: List[OrgUnitSearchItem] = Field(default_factory=list)
    next: Optional[str] = None
    total: Optional[int] = None


class CapabilityCandidatesModel(BaseModel):
    target_count: int = 0
    organization_count: int = 0
    target: List[TechnicalEndpointModel] = Field(default_factory=list)
    organization: List[TechnicalEndpointModel] = Field(default_factory=list)


class CapabilityMappingItemModel(BaseModel):
    label: Optional[str] = None
    code: Optional[str] = None
    tokens: List[str] = Field(default_factory=list)
    required: bool = False
    candidates: CapabilityCandidatesModel
    chosen: Optional[TechnicalEndpointModel] = None


class CapabilityEndpointInfoModel(BaseModel):
    address: Optional[str] = None
    base: Optional[str] = None
    endpoint_id: Optional[str] = None
    valid_http_base: Optional[bool] = None
    source: Optional[str] = None


class CapabilityMappingResponseModel(BaseModel):
    target: Dict[str, Any]
    organization: Optional[Dict[str, Any]] = None
    decision: str
    decision_explanation: str
    supported: bool
    missing: List[str] = Field(default_factory=list)
    notification: CapabilityEndpointInfoModel
    bgz_fhir_server: CapabilityEndpointInfoModel
    mapping: Dict[str, CapabilityMappingItemModel]


class BgzNotifyResponseModel(BaseModel):
    success: bool
    target: str
    task_id: Optional[str] = None
    task_status: Optional[str] = None
    group_identifier: Optional[str] = None
    workflow_task_id: Optional[str] = None
    sender_bgz_base: Optional[str] = None
    resolved_receiver_base: Optional[str] = None




class BgzTaskPreviewResponseModel(BaseModel):
    success: bool
    task: Dict[str, Any]
    sender_bgz_base: Optional[str] = None
    resolved_receiver_base: Optional[str] = None
    notification_endpoint_id: Optional[str] = None
    workflow_task_id: Optional[str] = None


@app.get(
    "/poc9/msz/organizations",
    dependencies=[Depends(verify_api_key)],
    response_model=PagedOrganizationsResponse,
)
async def poc9_msz_organizations(
    name: str | None = Query(None, max_length=128),
    contains: bool = False,
    identifier: str | None = Query(None, max_length=128),
    org_type: str | None = Query(None, alias="type", max_length=128),
    limit: int = Query(20, ge=1, le=200),
    cursor: str | None = Query(None, max_length=4096),
    *,
    request: Request,
):
    """
    PoC 8/9: zoek MSZ-zorgorganisaties (ziekenhuizen) in een (ITI-130) gevuld adresboek (Query Directory).

    Nieuw (punten 1/2): cursor-based paginering via `next` + `cursor`, zodat de UI een
    'meer laden' knop kan tonen.
    """
    base = MCSDBASE.rstrip("/")

    # If cursor is present, we ignore all other filters and continue from the upstream 'next' URL.
    if cursor:
        payload = _cursor_decode(cursor)
        next_url = payload.get("next")
        if not next_url:
            raise HTTPException(status_code=400, detail="Cursor mist 'next'.")
        next_url = _assert_safe_upstream_url(str(next_url))
        bundle = await _fetch_bundle(next_url, {})
    else:
        eff_name = (name or "").strip() if name else None
        eff_contains = bool(contains)

        if not eff_name:
            qp_name_contains = request.query_params.get("name:contains")
            if qp_name_contains:
                eff_name = qp_name_contains.strip() or None
                if eff_name:
                    eff_contains = True

        if eff_contains and not eff_name:
            eff_contains = False

        params: Dict[str, Any] = {
            "active": "true",
            "_count": max(min(limit, 200), 1),
            "_include": "Organization:endpoint",
        }

        if identifier:
            params["identifier"] = identifier

        if org_type:
            params["type"] = org_type

        if eff_name:
            params["name:contains" if eff_contains else "name"] = eff_name

        bundle = await _fetch_bundle(f"{base}/Organization", params)

    # Paging token
    next_url2 = _bundle_next_url(bundle)
    next_cursor: Optional[str] = None
    if next_url2:
        try:
            next_url2 = _assert_safe_upstream_url(str(next_url2))
            next_cursor = _cursor_encode({"next": next_url2})
        except HTTPException:
            # Don't fail the request if upstream returns a weird next URL; just omit paging.
            next_cursor = None

    included = _index_included(bundle)

    items: List[Dict[str, Any]] = []
    for e in (bundle or {}).get("entry", []) or []:
        org = (e or {}).get("resource") or {}
        if org.get("resourceType") != "Organization":
            continue
        org_id = org.get("id")
        if not org_id:
            continue

        ep_refs = (org or {}).get("endpoint") or []
        eps: List[Dict[str, Any]] = []
        for r in ep_refs:
            ref_str = (r or {}).get("reference") or ""
            rt, rid = _split_ref(ref_str)
            if rt == "Endpoint" and rid:
                ep = included.get(("Endpoint", rid))
                if ep:
                    eps.append(_endpoint_to_technical(ep))

        items.append(
            {
                "resourceType": "Organization",
                "id": org_id,
                "name": org.get("name"),
                "identifier": org.get("identifier") or [],
                "type": org.get("type") or [],
                "endpoints": eps,
            }
        )

    total = (bundle or {}).get("total")
    return {"count": len(items), "items": items, "next": next_cursor, "total": total}

@app.get(
    "/poc9/msz/orgunits",
    dependencies=[Depends(verify_api_key)],
    response_model=PagedOrgUnitsResponse,
)
async def poc9_msz_orgunits(
    organization: str | None = Query(None, max_length=128),
    kind: str = Query("all", max_length=32),
    name: str | None = Query(None, max_length=128),
    contains: bool = False,
    limit: int = 50,
    cursor: str | None = Query(None, max_length=4096),
    *,
    request: Request,
):
    """
    PoC 8/9: zoek organisatieonderdelen (afdelingen/specialismen/klinieken) binnen een MSZ-organisatie.
    kind = location | service | suborg | all (of leeg = all)

    Nieuw (punten 1/2): cursor-based paginering via `next` + `cursor`, zodat de UI een
    'meer laden' knop kan tonen.
    """
    base = MCSDBASE.rstrip('/')

    # --- Cursor mode (load more) -------------------------------------------
    if cursor:
        payload = _cursor_decode(cursor)
        next_loc = payload.get("next_location")
        next_svc = payload.get("next_service")
        next_sub = payload.get("next_suborg")
        org_ref = (payload.get("org_ref") or "").strip()
        total_out = payload.get("total") if isinstance(payload.get("total"), int) else None

        if not (next_loc or next_svc or next_sub):
            raise HTTPException(status_code=400, detail="Cursor mist 'next'.")

        items: List[Dict[str, Any]] = []
        loc_next_url = None
        svc_next_url = None
        sub_next_url = None

        if next_loc:
            bundle = await _fetch_bundle(_assert_safe_upstream_url(str(next_loc)), {})
            loc_next_url = _bundle_next_url(bundle)
            included = _index_included(bundle)

            for e in (bundle or {}).get("entry", []) or []:
                loc = (e or {}).get("resource") or {}
                if loc.get("resourceType") != "Location" or not loc.get("id"):
                    continue
                if loc.get("status") and loc.get("status") != "active":
                    continue
                loc_eps: List[Dict[str, Any]] = []
                for r in (loc or {}).get("endpoint") or []:
                    ref_str = (r or {}).get("reference") or ""
                    rt, rid = _split_ref(ref_str)
                    if rt == "Endpoint" and rid:
                        ep = included.get(("Endpoint", rid))
                        if ep and ep.get("status") == "active":
                            loc_eps.append(_endpoint_to_technical(ep))
                items.append(
                    {
                        "resourceType": "Location",
                        "id": loc.get("id"),
                        "name": loc.get("name"),
                        "organization": org_ref,
                        "endpoints": loc_eps,
                    }
                )

        if next_svc:
            bundle = await _fetch_bundle(_assert_safe_upstream_url(str(next_svc)), {})
            svc_next_url = _bundle_next_url(bundle)
            included = _index_included(bundle)

            for e in (bundle or {}).get("entry", []) or []:
                svc = (e or {}).get("resource") or {}
                if svc.get("resourceType") != "HealthcareService" or not svc.get("id"):
                    continue
                if svc.get("active") is False:
                    continue
                svc_eps: List[Dict[str, Any]] = []
                for r in (svc or {}).get("endpoint") or []:
                    ref_str = (r or {}).get("reference") or ""
                    rt, rid = _split_ref(ref_str)
                    if rt == "Endpoint" and rid:
                        ep = included.get(("Endpoint", rid))
                        if ep and ep.get("status") == "active":
                            svc_eps.append(_endpoint_to_technical(ep))
                items.append(
                    {
                        "resourceType": "HealthcareService",
                        "id": svc.get("id"),
                        "name": svc.get("name"),
                        "specialty": svc.get("specialty") or [],
                        "organization": org_ref,
                        "endpoints": svc_eps,
                    }
                )

        if next_sub:
            bundle = await _fetch_bundle(_assert_safe_upstream_url(str(next_sub)), {})
            sub_next_url = _bundle_next_url(bundle)
            included = _index_included(bundle)

            for e in (bundle or {}).get("entry", []) or []:
                sub = (e or {}).get("resource") or {}
                if sub.get("resourceType") != "Organization" or not sub.get("id"):
                    continue
                if sub.get("active") is False:
                    continue
                sub_eps: List[Dict[str, Any]] = []
                for r in (sub or {}).get("endpoint") or []:
                    ref_str = (r or {}).get("reference") or ""
                    rt, rid = _split_ref(ref_str)
                    if rt == "Endpoint" and rid:
                        ep = included.get(("Endpoint", rid))
                        if ep and ep.get("status") == "active":
                            sub_eps.append(_endpoint_to_technical(ep))
                items.append(
                    {
                        "resourceType": "Organization",
                        "id": sub.get("id"),
                        "name": sub.get("name"),
                        "partOf": org_ref,
                        "endpoints": sub_eps,
                    }
                )

        next_cursor: Optional[str] = None
        if loc_next_url or svc_next_url or sub_next_url:
            payload2: Dict[str, Any] = {"org_ref": org_ref}
            if total_out is not None:
                payload2["total"] = total_out

            if loc_next_url:
                try:
                    payload2["next_location"] = _assert_safe_upstream_url(str(loc_next_url))
                except HTTPException:
                    pass
            if svc_next_url:
                try:
                    payload2["next_service"] = _assert_safe_upstream_url(str(svc_next_url))
                except HTTPException:
                    pass
            if sub_next_url:
                try:
                    payload2["next_suborg"] = _assert_safe_upstream_url(str(sub_next_url))
                except HTTPException:
                    pass

            if any(k in payload2 for k in ("next_location", "next_service", "next_suborg")):
                next_cursor = _cursor_encode(payload2)

        type_order = {"Location": 0, "HealthcareService": 1, "Organization": 2}
        items.sort(key=lambda it: (type_order.get(it.get("resourceType") or "", 99), (it.get("name") or "").lower()))

        return {"count": len(items), "items": items, "next": next_cursor, "total": total_out}

    # --- First page (initial fetch) ----------------------------------------
    if not organization:
        raise HTTPException(status_code=400, detail="Parameter 'organization' is verplicht als er geen cursor wordt meegegeven.")

    eff_name = request.query_params.get("name:contains") or name
    eff_contains = contains or (request.query_params.get("name:contains") is not None)

    org_ref = organization
    if not org_ref.startswith("Organization/"):
        org_ref = f"Organization/{org_ref}"

    k_raw = (kind or "").strip().lower()
    if not k_raw:
        k_raw = "all"

    # Normaliseer varianten (UI kan bijv. "alle types" of "sub-organisatie" sturen).
    k_norm = k_raw.replace(" ", "").replace("-", "").replace("_", "")
    if k_norm in {"all", "alle", "alles", "alltypes", "alletypes"}:
        k = "all"
    elif k_norm in {"location", "locatie", "locations"}:
        k = "location"
    elif k_norm in {"service", "zorgdienst", "healthcareservice"}:
        k = "service"
    elif k_norm in {"suborg", "suborganisatie", "suborganization", "organization", "organisation"}:
        k = "suborg"
    else:
        k = k_raw

    if k not in {"location", "service", "suborg", "all"}:
        raise HTTPException(status_code=400, detail="Ongeldige 'kind' (gebruik: location, service, suborg, all).")

    items: List[Dict[str, Any]] = []

    do_location = (k == "location") or (k == "all")
    do_service = (k == "service") or (k == "all")
    do_suborg = (k == "suborg") or (k == "all")

    loc_next_url = None
    svc_next_url = None
    sub_next_url = None
    loc_total = None
    svc_total = None
    sub_total = None

    if do_location:
        params: Dict[str, Any] = {"organization": org_ref, "status": "active", "_count": max(min(limit, 200), 1), "_include": "Location:endpoint"}
        if eff_name:
            params["name:contains" if eff_contains else "name"] = eff_name

        bundle = await _fetch_bundle(f"{base}/Location", params)

        # Fallback: sommige servers gebruiken 'managing-organization' als search-parameternaam.
        if not (bundle.get("entry") or []):
            if await _upstream_supports_location_managing_organization(base):
                params2 = dict(params)
                params2.pop("organization", None)
                params2["managing-organization"] = org_ref
                try:
                    bundle = await _fetch_bundle(f"{base}/Location", params2)
                except HTTPException as e:
                    # Upstream ondersteunt managing-organization niet -> behandel als "geen resultaten"
                    if e.status_code in (400, 404, 422):
                        bundle = {"resourceType": "Bundle", "type": "searchset", "total": 0, "entry": []}
                    else:
                        raise
            else:
                bundle = {"resourceType": "Bundle", "type": "searchset", "total": 0, "entry": []}

        loc_total = bundle.get("total") if isinstance(bundle.get("total"), int) else None
        loc_next_url = _bundle_next_url(bundle)

        included = _index_included(bundle)

        for e in (bundle or {}).get("entry", []) or []:
            loc = (e or {}).get("resource") or {}
            if loc.get("resourceType") != "Location" or not loc.get("id"):
                continue
            if loc.get("status") and loc.get("status") != "active":
                continue
            loc_eps: List[Dict[str, Any]] = []
            for r in (loc or {}).get("endpoint") or []:
                ref_str = (r or {}).get("reference") or ""
                rt, rid = _split_ref(ref_str)
                if rt == "Endpoint" and rid:
                    ep = included.get(("Endpoint", rid))
                    if ep and ep.get("status") == "active":
                        loc_eps.append(_endpoint_to_technical(ep))
            items.append(
                {
                    "resourceType": "Location",
                    "id": loc.get("id"),
                    "name": loc.get("name"),
                    "organization": org_ref,
                    "endpoints": loc_eps,
                }
            )

    if do_service:
        params = {"organization": org_ref, "active": "true", "_count": max(min(limit, 200), 1), "_include": "HealthcareService:endpoint"}
        if eff_name:
            params["name:contains" if eff_contains else "name"] = eff_name
        bundle = await _fetch_bundle(f"{base}/HealthcareService", params)
        svc_total = bundle.get("total") if isinstance(bundle.get("total"), int) else None
        svc_next_url = _bundle_next_url(bundle)
        included = _index_included(bundle)

        for e in (bundle or {}).get("entry", []) or []:
            svc = (e or {}).get("resource") or {}
            if svc.get("resourceType") != "HealthcareService" or not svc.get("id"):
                continue
            if svc.get("active") is False:
                continue
            svc_eps: List[Dict[str, Any]] = []
            for r in (svc or {}).get("endpoint") or []:
                ref_str = (r or {}).get("reference") or ""
                rt, rid = _split_ref(ref_str)
                if rt == "Endpoint" and rid:
                    ep = included.get(("Endpoint", rid))
                    if ep and ep.get("status") == "active":
                        svc_eps.append(_endpoint_to_technical(ep))
            items.append(
                {
                    "resourceType": "HealthcareService",
                    "id": svc.get("id"),
                    "name": svc.get("name"),
                    "specialty": svc.get("specialty") or [],
                    "organization": org_ref,
                    "endpoints": svc_eps,
                }
            )

    if do_suborg:
        params = {"partof": org_ref, "active": "true", "_count": max(min(limit, 200), 1), "_include": "Organization:endpoint"}
        if eff_name:
            params["name:contains" if eff_contains else "name"] = eff_name
        bundle = await _fetch_bundle(f"{base}/Organization", params)
        sub_total = bundle.get("total") if isinstance(bundle.get("total"), int) else None
        sub_next_url = _bundle_next_url(bundle)
        included = _index_included(bundle)

        for e in (bundle or {}).get("entry", []) or []:
            sub = (e or {}).get("resource") or {}
            if sub.get("resourceType") != "Organization" or not sub.get("id"):
                continue
            if sub.get("active") is False:
                continue
            sub_eps: List[Dict[str, Any]] = []
            for r in (sub or {}).get("endpoint") or []:
                ref_str = (r or {}).get("reference") or ""
                rt, rid = _split_ref(ref_str)
                if rt == "Endpoint" and rid:
                    ep = included.get(("Endpoint", rid))
                    if ep and ep.get("status") == "active":
                        sub_eps.append(_endpoint_to_technical(ep))
            items.append(
                {
                    "resourceType": "Organization",
                    "id": sub.get("id"),
                    "name": sub.get("name"),
                    "partOf": org_ref,
                    "endpoints": sub_eps,
                }
            )

    type_order = {"Location": 0, "HealthcareService": 1, "Organization": 2}
    items.sort(key=lambda it: (type_order.get(it.get("resourceType") or "", 99), (it.get("name") or "").lower()))

    total_out = None
    if k == "location":
        total_out = loc_total
    elif k == "service":
        total_out = svc_total
    elif k == "suborg":
        total_out = sub_total
    elif k == "all":
        if isinstance(loc_total, int) and isinstance(svc_total, int) and isinstance(sub_total, int):
            total_out = int(loc_total) + int(svc_total) + int(sub_total)

    next_cursor: Optional[str] = None
    if loc_next_url or svc_next_url or sub_next_url:
        payload2: Dict[str, Any] = {"org_ref": org_ref}
        if total_out is not None:
            payload2["total"] = total_out

        if loc_next_url:
            try:
                payload2["next_location"] = _assert_safe_upstream_url(str(loc_next_url))
            except HTTPException:
                pass
        if svc_next_url:
            try:
                payload2["next_service"] = _assert_safe_upstream_url(str(svc_next_url))
            except HTTPException:
                pass
        if sub_next_url:
            try:
                payload2["next_suborg"] = _assert_safe_upstream_url(str(sub_next_url))
            except HTTPException:
                pass

        if any(k in payload2 for k in ("next_location", "next_service", "next_suborg")):
            next_cursor = _cursor_encode(payload2)

    return {"count": len(items), "items": items, "next": next_cursor, "total": total_out}


@app.get("/poc9/msz/endpoints", dependencies=[Depends(verify_api_key)])
async def poc9_msz_endpoints(
    target: str | None = Query(None, max_length=256),
    endpoint_kind: str | None = Query(None, max_length=32),
    connection_type: str | None = Query(None, max_length=128),
    payload_type: str | None = Query(None, max_length=256),
    payload_mime_type: str | None = Query(None, max_length=128),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None, max_length=4096),
):
    """
    PoC 8/9: haal technische endpoints op voor een organisatieonderdeel (Location/HealthcareService/Organization)
    en filter optioneel op FHIR / notificatie / authenticatie.

    Nieuw (punten 1/2): cursor-based paginering via `next` + `cursor`, zodat de UI een
    'meer laden' knop kan tonen (ook als de FHIR server de resultaten pagineert).
    """
    base = MCSDBASE.rstrip("/")

    async def _fetch_resource(url: str) -> Dict[str, Any]:
        try:
            return await _get_json(app.state.http_client, url, {})
        except httpx.HTTPStatusError as e:
            raise _http_status_to_http_exception(e)
        except HTTPException:
            raise
        except Exception as e:
            _raise_502_with_reason(e)

    def _endpoint_ids_from_resource(res: Dict[str, Any]) -> List[str]:
        ids: List[str] = []
        for r in (res or {}).get("endpoint") or []:
            ref_str = (r or {}).get("reference") or ""
            rt2, rid2 = _split_ref(ref_str)
            if rt2 == "Endpoint" and rid2 and rid2 not in ids:
                ids.append(rid2)
        return ids

    # --- Cursor mode (load more) -------------------------------------------
    if cursor:
        payload = _cursor_decode(cursor)
        next_url = payload.get("next")
        if not next_url:
            raise HTTPException(status_code=400, detail="Cursor mist 'next'.")
        next_url = _assert_safe_upstream_url(str(next_url))

        # Freeze filters to the cursor values (so the user can't change filters mid-page by accident)
        ek = payload.get("endpoint_kind") if payload.get("endpoint_kind") is not None else endpoint_kind
        ct = payload.get("connection_type") if payload.get("connection_type") is not None else connection_type
        pt = payload.get("payload_type") if payload.get("payload_type") is not None else payload_type
        pm = payload.get("payload_mime_type") if payload.get("payload_mime_type") is not None else payload_mime_type

        bundle = await _fetch_bundle(str(next_url), {})

        next_url2 = _bundle_next_url(bundle)
        next_cursor: Optional[str] = None
        if next_url2:
            try:
                next_url2 = _assert_safe_upstream_url(str(next_url2))
                next_cursor = _cursor_encode(
                    {
                        "next": str(next_url2),
                        "endpoint_kind": ek,
                        "connection_type": ct,
                        "payload_type": pt,
                        "payload_mime_type": pm,
                    }
                )
            except HTTPException:
                next_cursor = None

        eps: List[Dict[str, Any]] = []
        for e in (bundle or {}).get("entry", []) or []:
            res = (e or {}).get("resource") or {}
            if res.get("resourceType") == "Endpoint":
                eps.append(res)

        technical: List[Dict[str, Any]] = []
        for ep in eps:
            if ep.get("status") != "active":
                continue
            if _endpoint_matches_filters(
                ep,
                endpoint_kind=ek,
                connection_type=ct,
                payload_type=pt,
                payload_mime_type=pm,
            ):
                technical.append(_endpoint_to_technical(ep))

        total = (bundle or {}).get("total")
        # endpoint_kind is heuristic and applied locally -> total would be misleading
        if ek:
            total = None

        return {"count": len(technical), "items": technical, "next": next_cursor, "total": total}

    # --- First page (initial fetch) ----------------------------------------
    if not target:
        raise HTTPException(status_code=400, detail="Parameter 'target' is verplicht als er geen cursor wordt meegegeven.")

    rt, rid = _split_ref(target)
    if not rt or not rid:
        raise HTTPException(status_code=400, detail="Parameter 'target' moet de vorm ResourceType/id hebben.")

    # 1) Read the target resource and collect endpoint IDs from endpoint references.
    tgt = await _fetch_resource(f"{base}/{rt}/{rid}")
    endpoint_ids = _endpoint_ids_from_resource(tgt)

    # 2) Fallback: als het orgunit geen endpoint refs heeft, probeer via parent Organization endpoints.
    if not endpoint_ids:
        parent_org_ref: Optional[str] = None
        if rt == "Location":
            mo = (tgt or {}).get("managingOrganization") or {}
            parent_org_ref = mo.get("reference")
        elif rt == "HealthcareService":
            pb = (tgt or {}).get("providedBy") or {}
            parent_org_ref = pb.get("reference")
        elif rt == "Organization":
            # Child organizations often don't carry endpoints themselves; use the parent (partOf) if present.
            po = (tgt or {}).get("partOf") or {}
            parent_org_ref = po.get("reference") or f"Organization/{rid}"

        # Follow the Organization.partOf chain until we find endpoints (or we can't go further).
        # This supports selecting sub-organizations that don't have endpoints of their own, while
        # still being able to route via the parent organization's technical endpoint(s).
        if parent_org_ref:
            org_rt, org_id = _split_ref(parent_org_ref)
            if org_rt == "Organization" and org_id:
                visited: set[str] = set()
                current_id: Optional[str] = org_id
                for _ in range(10):
                    if not current_id or current_id in visited:
                        break
                    visited.add(current_id)
                    org = await _fetch_resource(f"{base}/Organization/{current_id}")
                    endpoint_ids = _endpoint_ids_from_resource(org)
                    if endpoint_ids:
                        break
                    po2 = (org or {}).get("partOf") or {}
                    ref2 = (po2 or {}).get("reference") or ""
                    rt2, rid2 = _split_ref(ref2)
                    if rt2 != "Organization" or not rid2:
                        break
                    current_id = rid2

    if not endpoint_ids:
        return {"count": 0, "items": [], "next": None, "total": 0}

    # 3) Query Endpoint resources (paged), optionally with token filters.
    params: Dict[str, Any] = {
        "_id": ",".join(endpoint_ids),
        "_count": max(min(limit, 200), 1),
        "status": "active",
    }
    if connection_type:
        params["connection-type"] = connection_type
    if payload_type:
        params["payload-type"] = payload_type
    if payload_mime_type:
        params["payload-mime-type"] = payload_mime_type

    bundle = await _fetch_bundle(f"{base}/Endpoint", params)

    next_url2 = _bundle_next_url(bundle)
    next_cursor: Optional[str] = None
    if next_url2:
        try:
            next_url2 = _assert_safe_upstream_url(str(next_url2))
            next_cursor = _cursor_encode(
                {
                    "next": str(next_url2),
                    "endpoint_kind": endpoint_kind,
                    "connection_type": connection_type,
                    "payload_type": payload_type,
                    "payload_mime_type": payload_mime_type,
                }
            )
        except HTTPException:
            next_cursor = None

    eps: List[Dict[str, Any]] = []
    for e in (bundle or {}).get("entry", []) or []:
        res = (e or {}).get("resource") or {}
        if res.get("resourceType") == "Endpoint":
            eps.append(res)

    technical: List[Dict[str, Any]] = []
    for ep in eps:
        if ep.get("status") != "active":
            continue
        if _endpoint_matches_filters(
            ep,
            endpoint_kind=endpoint_kind,
            connection_type=connection_type,
            payload_type=payload_type,
            payload_mime_type=payload_mime_type,
        ):
            technical.append(_endpoint_to_technical(ep))

    total = (bundle or {}).get("total")
    if endpoint_kind:
        total = None

    return {"count": len(technical), "items": technical, "next": next_cursor, "total": total}

# --- Capability mapping (PoC 9) -------------------------------------------

# IG CodeSystem used in Endpoint.payloadType to declare data-exchange capabilities.
IG_CAPABILITY_SYSTEM = "http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities"

# PoC 9 extension: explicit sender BgZ FHIR base URL for receivers that do not resolve sender endpoints via URA/mCSD.
TASK_EXT_SENDER_BGZ_BASE_URL = "http://example.org/fhir/StructureDefinition/sender-bgz-base"

# NL Generic Functions: extensions to support routing references on FHIR STU3 Task
# - Location for Task in STU3
# - HealthcareService for Task in STU3
# See: https://build.fhir.org/ig/nuts-foundation/nl-generic-functions-ig/StructureDefinition-task-stu3-location.html
# See: https://build.fhir.org/ig/nuts-foundation/nl-generic-functions-ig/StructureDefinition-task-stu3-healthcareservice.html
TASK_EXT_TASK_STU3_LOCATION_URL = "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/task-stu3-location"
TASK_EXT_TASK_STU3_HEALTHCARESERVICE_URL = "http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/task-stu3-healthcareservice"

def _normalize_fhir_base(address: str) -> str:
    """Normalize an Endpoint.address to a FHIR base URL.

    - trims trailing slashes
    - if the address ends with '/Task' (common for notification endpoints), strip it
    """
    if not address or not isinstance(address, str):
        return ""
    a = address.strip()
    while a.endswith("/"):
        a = a[:-1]
    if a.lower().endswith("/task"):
        a = a[:-5]
        while a.endswith("/"):
            a = a[:-1]
    return a


def _validate_http_base_url(base: str, *, field_name: str = "base") -> str:
    """Validate that a (FHIR) base URL is a safe http(s) URL.

    This is used to reduce SSRF risk for outbound requests:
    - only allow absolute http/https URLs
    - require hostname/netloc
    - reject embedded credentials (userinfo) and URL fragments
    """
    b = (base or "").strip()
    if not b:
        raise HTTPException(status_code=400, detail=f"Ongeldige {field_name}.")
    p = urlparse(b)
    if p.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail=f"Ongeldige {field_name}: alleen http/https is toegestaan.")
    if not p.netloc:
        raise HTTPException(status_code=400, detail=f"Ongeldige {field_name}: hostname ontbreekt.")
    if p.username or p.password:
        raise HTTPException(status_code=400, detail=f"Ongeldige {field_name}: userinfo is niet toegestaan.")
    if p.fragment:
        raise HTTPException(status_code=400, detail=f"Ongeldige {field_name}: fragment is niet toegestaan.")
    return b


def _normalize_relative_ref(ref: Optional[str]) -> str:
    """Normalize a FHIR relative reference (ResourceType/id).

    - Handles full URLs by extracting the last two path segments
    - Returns empty string for empty/None input
    """
    if not ref:
        return ""
    r = str(ref).strip()
    if not r:
        return ""
    # If a full URL is provided, keep only the last two path segments (ResourceType/id)
    parts = [p for p in r.split("/") if p]
    if len(parts) >= 2:
        return parts[-2] + "/" + parts[-1]
    return r


@app.get(
    "/poc9/msz/capability-mapping",
    dependencies=[Depends(verify_api_key)],
    response_model=CapabilityMappingResponseModel,
)
async def poc9_msz_capability_mapping(
    target: str = Query(..., max_length=256, description="Target resource reference (ResourceType/id)."),
    organization: str | None = Query(
        None, max_length=256, description="Owning organization reference (Organization/id). Optional; if omitted, inferred where possible."
    ),
    include_oauth: bool = Query(False, description="Include Nuts-OAuth in the response (optional capability)."),
    limit: int = Query(200, ge=1, le=200, description="Max number of Endpoint resources to fetch per scope (target/org)."),
):
    """Resolve endpoints for key capabilities using decision tree A-D.

    Decision tree:
    - A: all required capabilities found directly on the target (via target.endpoint references)
    - B: all required capabilities found directly on the organization (via Organization.endpoint references)
    - C: required capabilities found by combining target + organization (per capability: prefer target, else organization)
    - D: incomplete even after combining

    Required capabilities for *sender-side* BgZ notified pull (TA Notified Pull):
    - Twiin TA notification (Endpoint.payloadType = Twiin-TA-notification)  [receiver Task endpoint]

Optional capabilities (informational / for other flows):
    - BgZ FHIR server (Endpoint.payloadType = http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities)

    OAuth is optional and only included when include_oauth=true.
    """
    base = MCSDBASE.rstrip("/")

    # --- Helpers -----------------------------------------------------------
    def _endpoint_ids_from_resource(res: Dict[str, Any] | None) -> List[str]:
        ids: List[str] = []
        for ref in (res or {}).get("endpoint") or []:
            r = None
            if isinstance(ref, dict):
                r = ref.get("reference")
            elif isinstance(ref, str):
                r = ref
            if not r:
                continue
            rt, rid = _split_ref(str(r))
            if rt == "Endpoint" and rid:
                ids.append(rid)
        # dedup, keep order
        seen: set[str] = set()
        out: List[str] = []
        for x in ids:
            if x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    async def _fetch_resource(url: str) -> Dict[str, Any]:
        try:
            return await _get_json(app.state.http_client, url, {})
        except httpx.HTTPStatusError as e:
            raise _http_status_to_http_exception(e)
        except HTTPException:
            raise
        except Exception as e:
            _raise_502_with_reason(e)

    async def _fetch_endpoints_by_ids(endpoint_ids: List[str]) -> List[Dict[str, Any]]:
        if not endpoint_ids:
            return []

        params: Dict[str, Any] = {
            "_id": ",".join(endpoint_ids),
            "_count": max(min(limit, 200), 1),
        }
        bundle = await _fetch_bundle(f"{base}/Endpoint", params)

        eps: List[Dict[str, Any]] = []
        for e in (bundle or {}).get("entry", []) or []:
            res = (e or {}).get("resource") or {}
            if res.get("resourceType") == "Endpoint":
                eps.append(res)

        # Follow paging links if present
        next_url = _bundle_next_url(bundle)
        while next_url:
            next_url = _assert_safe_upstream_url(str(next_url))
            bundle = await _fetch_bundle(next_url, {})
            for e in (bundle or {}).get("entry", []) or []:
                res = (e or {}).get("resource") or {}
                if res.get("resourceType") == "Endpoint":
                    eps.append(res)
            next_url = _bundle_next_url(bundle)

        # Dedup by id
        seen: set[str] = set()
        out: List[Dict[str, Any]] = []
        for ep in eps:
            eid = str((ep or {}).get("id") or "")
            if not eid:
                continue
            if eid in seen:
                continue
            seen.add(eid)
            out.append(ep)
        return out

    def _technical_matches_any_token(tech_ep: Dict[str, Any], tokens: List[str]) -> bool:
        for c in (tech_ep or {}).get("payloadType") or []:
            for t in tokens:
                if _coding_matches_token(c or {}, t):
                    return True
        return False

    def _pick_best(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        # Prefer active endpoints with an address
        for ep in candidates or []:
            if (ep or {}).get("status") == "active" and (ep or {}).get("address"):
                return ep
        # Then any endpoint with an address
        for ep in candidates or []:
            if (ep or {}).get("address"):
                return ep
        return candidates[0] if candidates else None

    def _tokens_for_code(code: str) -> List[str]:
        # Accept both strict token (system|code) and loose token (code only).
        return [f"{IG_CAPABILITY_SYSTEM}|{code}", code]

    # --- Parse inputs ------------------------------------------------------
    rt, rid = _split_ref(target)
    if not rt or not rid:
        raise HTTPException(status_code=400, detail="Parameter 'target' moet de vorm ResourceType/id hebben.")

    tgt = await _fetch_resource(f"{base}/{rt}/{rid}")

    # Determine organization ref.
    # Voor routing op onderdeel/afdeling-niveau geven we de voorkeur aan de owning organisatie
    # die aan het target gekoppeld is:
    # - Location.managingOrganization
    # - HealthcareService.providedBy
    # Als die relatie ontbreekt, vallen we terug op de expliciete 'organization' parameter.
    inferred_org_ref: Optional[str] = None
    if rt == "Organization":
        inferred_org_ref = f"Organization/{rid}"
    elif rt == "Location":
        mo = (tgt or {}).get("managingOrganization") or {}
        inferred_org_ref = mo.get("reference")
    elif rt == "HealthcareService":
        pb = (tgt or {}).get("providedBy") or {}
        inferred_org_ref = pb.get("reference")

    # Normalize inferred org ref to relative Organization/<id> (or None if invalid)
    inf_rt, inf_id = _split_ref(inferred_org_ref or "")
    if inf_rt == "Organization" and inf_id:
        inferred_org_ref = f"Organization/{inf_id}"
    else:
        inferred_org_ref = None

    explicit_org_ref: Optional[str] = None
    if organization:
        org_rt, org_id = _split_ref(organization)
        if org_rt != "Organization" or not org_id:
            raise HTTPException(status_code=400, detail="Parameter 'organization' moet de vorm Organization/id hebben.")
        explicit_org_ref = f"Organization/{org_id}"

    # Prefer inferred owning org; fall back to explicit param if inference isn't possible.
    org_ref: Optional[str] = inferred_org_ref or explicit_org_ref

    org: Optional[Dict[str, Any]] = None
    org_rt, org_id = _split_ref(org_ref or "")
    if org_rt == "Organization" and org_id:
        # Only fetch if not the same as target (avoids double call)
        if not (rt == "Organization" and rid == org_id):
            org = await _fetch_resource(f"{base}/Organization/{org_id}")
        else:
            org = tgt

    # --- Collect endpoints per scope --------------------------------------
    target_endpoint_ids = _endpoint_ids_from_resource(tgt)

    # Resolve Organization endpoints (follow Organization.partOf chain if needed).
    org_endpoint_ids: List[str] = []
    org_endpoint_ref_used: Optional[str] = org_ref or None

    async def _resolve_org_endpoint_ids_with_partof(
        org_res: Dict[str, Any] | None,
        org_ref_in: Optional[str],
    ) -> Tuple[List[str], Optional[str]]:
        """Return endpoint ids from org_res, or from its ancestors via partOf."""
        if not org_res:
            return [], org_ref_in or None

        current: Dict[str, Any] = org_res
        current_ref: str = str(org_ref_in or "")

        rt0, rid0 = _split_ref(current_ref)
        if rt0 != "Organization" or not rid0:
            rid0 = str((current or {}).get("id") or "")
            if rid0:
                current_ref = f"Organization/{rid0}"

        visited: set[str] = set()
        if rid0:
            visited.add(rid0)

        for _ in range(10):
            ids = _endpoint_ids_from_resource(current)
            if ids:
                return ids, current_ref or None

            po = (current or {}).get("partOf") or {}
            ref = (po or {}).get("reference") or ""
            rt1, rid1 = _split_ref(ref)
            if rt1 != "Organization" or not rid1:
                break
            if rid1 in visited:
                break

            visited.add(rid1)
            current_ref = f"Organization/{rid1}"
            current = await _fetch_resource(f"{base}/Organization/{rid1}")

        return [], org_ref_in or None

    if org:
        org_endpoint_ids, org_endpoint_ref_used = await _resolve_org_endpoint_ids_with_partof(org, org_ref)

    # Avoid duplicates: if an Endpoint is directly referenced by target, treat it as target-level.
    if target_endpoint_ids and org_endpoint_ids:
        target_set = set(target_endpoint_ids)
        org_endpoint_ids = [x for x in org_endpoint_ids if x not in target_set]

    tgt_eps = await _fetch_endpoints_by_ids(target_endpoint_ids)
    org_eps = await _fetch_endpoints_by_ids(org_endpoint_ids)

    tgt_tech: List[Dict[str, Any]] = []
    for ep in tgt_eps:
        te = _endpoint_to_technical(ep)
        te["source"] = "target"
        te["sourceRef"] = f"{rt}/{rid}"
        tgt_tech.append(te)

    org_tech: List[Dict[str, Any]] = []
    for ep in org_eps:
        te = _endpoint_to_technical(ep)
        te["source"] = "organization"
        te["sourceRef"] = org_endpoint_ref_used or org_ref or None
        org_tech.append(te)

    # --- Capability mapping ------------------------------------------------
    capabilities: Dict[str, Dict[str, Any]] = {
        "twiin_ta_notification": {
            "label": "Twiin TA notificatie",
            "code": "Twiin-TA-notification",
            "required": True,
        },
        "bgz_fhir_server": {
            "label": "BgZ FHIR server",
            "code": "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities",
            "required": False,
        },
    }
    if include_oauth:
        capabilities["nuts_oauth"] = {
            "label": "Nuts OAuth",
            "code": "Nuts-OAuth",
            "required": False,
        }

    mapping: Dict[str, Any] = {}
    missing: List[str] = []

    # Cap candidate lists in the response to keep payload reasonable.
    PREVIEW_MAX = 25

    for key, info in capabilities.items():
        tokens = _tokens_for_code(str(info["code"]))
        target_candidates = [e for e in tgt_tech if _technical_matches_any_token(e, tokens)]
        org_candidates = [e for e in org_tech if _technical_matches_any_token(e, tokens)]
        combined_candidates = target_candidates if target_candidates else org_candidates
        chosen = _pick_best(combined_candidates)

        if info.get("required") and not chosen:
            missing.append(key)

        mapping[key] = {
            "label": info.get("label"),
            "code": info.get("code"),
            "tokens": tokens,
            "required": bool(info.get("required")),
            "candidates": {
                "target_count": len(target_candidates),
                "organization_count": len(org_candidates),
                "target": target_candidates[:PREVIEW_MAX],
                "organization": org_candidates[:PREVIEW_MAX],
            },
            "chosen": chosen,
        }

    required_keys = [k for k, v in capabilities.items() if v.get("required")]

    target_ok = all(mapping[k]["candidates"]["target_count"] > 0 for k in required_keys)
    org_ok = all(mapping[k]["candidates"]["organization_count"] > 0 for k in required_keys)
    combined_ok = all(mapping[k]["chosen"] is not None for k in required_keys)

    if target_ok:
        decision = "A"
        decision_explanation = "Alle vereiste capabilities zijn gevonden op het geselecteerde onderdeel (target.endpoint)."
    elif org_ok:
        decision = "B"
        decision_explanation = "Alle vereiste capabilities zijn gevonden op de organisatie (Organization.endpoint)."
    elif combined_ok:
        decision = "C"
        decision_explanation = "Vereiste capabilities zijn gevonden door target + organisatie te combineren (per capability: target → organisatie)."
    else:
        decision = "D"
        decision_explanation = "Onvoldoende: één of meer vereiste capabilities ontbreken (ook na fallback naar organisatie)."

    # Convenience fields for the frontend (used by BgZ notified pull demo)
    notif_chosen = (mapping.get("twiin_ta_notification") or {}).get("chosen") or None
    bgz_chosen = (mapping.get("bgz_fhir_server") or {}).get("chosen") or None

    notification_address = str((notif_chosen or {}).get("address") or "")
    bgz_address = str((bgz_chosen or {}).get("address") or "")

    notification_base = _normalize_fhir_base(notification_address)
    bgz_base = _normalize_fhir_base(bgz_address)

    # Validate that the resolved bases are safe http(s) URLs (aligns with /bgz/notify validation).
    notification_base_valid = False
    if notification_base:
        try:
            notification_base = _validate_http_base_url(notification_base, field_name="notification_base")
            notification_base_valid = True
        except HTTPException:
            notification_base = ""
            notification_base_valid = False

    bgz_base_valid = False
    if bgz_base:
        try:
            bgz_base = _validate_http_base_url(bgz_base, field_name="bgz_base")
            bgz_base_valid = True
        except HTTPException:
            bgz_base = ""
            bgz_base_valid = False

    supported = bool(combined_ok and notification_base_valid and notification_base)

    # Include lightweight display/identifier hints (used for routing extensions + UI).
    target_info: Dict[str, Any] = {"reference": f"{rt}/{rid}"}
    try:
        target_display = str((tgt or {}).get("name") or (tgt or {}).get("title") or "").strip() or None
        if target_display:
            target_info["display"] = target_display
        tgt_identifiers = (tgt or {}).get("identifier")
        if isinstance(tgt_identifiers, list) and tgt_identifiers:
            target_info["identifier"] = tgt_identifiers
    except Exception:
        pass

    org_info: Optional[Dict[str, Any]] = None
    if org_ref:
        org_info = {"reference": org_ref}
        try:
            org_display = str((org or {}).get("name") or (org or {}).get("title") or "").strip() or None
            if org_display:
                org_info["display"] = org_display
            org_identifiers = (org or {}).get("identifier")
            if isinstance(org_identifiers, list) and org_identifiers:
                org_info["identifier"] = org_identifiers
        except Exception:
            pass

    return {
        "target": target_info,
        "organization": org_info,
        "decision": decision,
        "decision_explanation": decision_explanation,
        "supported": supported,
        "missing": missing,
        "notification": {
            "address": notification_address or None,
            "base": notification_base or None,
            "endpoint_id": (notif_chosen or {}).get("id") if notif_chosen else None,
            "valid_http_base": notification_base_valid,
            "source": (notif_chosen or {}).get("source") if notif_chosen else None,
        },
        "bgz_fhir_server": {
            "address": bgz_address or None,
            "base": bgz_base or None,
            "endpoint_id": (bgz_chosen or {}).get("id") if bgz_chosen else None,
            "valid_http_base": bgz_base_valid,
            "source": (bgz_chosen or {}).get("source") if bgz_chosen else None,
        },
        "mapping": mapping,
    }

# --- BgZ Endpoints ---

class BgzNotifyRequest(BaseModel):
    receiver_ura: str = Field(..., max_length=64, description="Receiver organization URA")
    receiver_name: str = Field(..., max_length=128, description="Receiver organization/department display name")
    receiver_org_ref: str | None = Field(None, max_length=256, description="(PoC 9) Receiver organization reference (Organization/<id>).")
    receiver_org_name: str | None = Field(None, max_length=256, description="(PoC 9) Receiver organization display name.")
    receiver_target_ref: str = Field(..., max_length=256, description="(PoC 9) Receiver target reference (Organization/<id>, HealthcareService/<id>, Location/<id>).")
    receiver_notification_endpoint_id: str | None = Field(None, max_length=64, description="(Optional) Endpoint.id van het gekozen Twiin-TA notification endpoint (preflight).")
    patient_bsn: str = Field(..., max_length=32, description="Patient BSN")
    patient_name: str | None = Field(None, max_length=128, description="Patient display name (optional)")
    description: str | None = Field(None, max_length=256, description="Notification description (optional)")
    workflow_task_id: str | None = Field(None, max_length=64, description="Workflow Task logical id (optional). If omitted, server generates one and sets Task.basedOn to Task/<id>.")

@lru_cache(maxsize=2)
def _load_bgz_template(filename: str) -> dict:
    """Load a BgZ JSON template from the data directory."""
    path = Path(__file__).parent / "data" / filename
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _validate_notification_task_template() -> None:
    """Validate notification-task.json at startup.

    Doel:
    - fail fast als template ontbreekt of geen dict is
    - voorkom runtime-KeyErrors bij het vullen van Task velden
    """
    try:
        template = _load_bgz_template("notification-task.json")
    except FileNotFoundError as exc:
        raise RuntimeError(
            "notification-task.json ontbreekt. Verwacht bestand in <app_dir>/data/notification-task.json"
        ) from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("notification-task.json is geen geldige JSON.") from exc

    if not isinstance(template, dict):
        raise RuntimeError("notification-task.json moet een JSON object (dict) zijn.")

    # Probeer een minimale build om paden zeker te stellen.
    tb = TaskBuilder(template)
    now = datetime.now(timezone.utc)
    tb.set_group_identifier(f"urn:uuid:{uuid.uuid4()}")
    tb.set_task_identifier(f"urn:uuid:{uuid.uuid4()}")
    tb.set_authored_on(now)
    tb.set_restriction_end(now + timedelta(days=1))
    # Dummy values (worden tijdens runtime overschreven)
    tb.set_requester_agent(uzi_sys="DUMMY", system_name="DUMMY")
    tb.set_sender(ura="DUMMY", display="DUMMY")
    tb.set_receiver_owner_identifier(ura="DUMMY")
    tb.set_patient(bsn="DUMMY")
    tb.set_authorization_base("DUMMY")
    # Valideer constraints per TA spec
    tb.validate_fhir_constraints(allow_missing_refs=True)


def _validate_workflow_task_template() -> None:
    """Validate workflow-task.json at startup.

    Doel:
    - fail fast als template ontbreekt of geen dict is
    - voorkom runtime-KeyErrors bij het vullen van Workflow Task velden
    """
    try:
        template = _load_bgz_template("workflow-task.json")
    except FileNotFoundError as exc:
        raise RuntimeError(
            "workflow-task.json ontbreekt. Verwacht bestand in <app_dir>/data/workflow-task.json"
        ) from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("workflow-task.json is geen geldige JSON.") from exc

    if not isinstance(template, dict):
        raise RuntimeError("workflow-task.json moet een JSON object (dict) zijn.")

    tb = TaskBuilder(template)
    tb.task["id"] = "DUMMY"
    now = datetime.now(timezone.utc)
    tb.set_group_identifier(f"urn:uuid:{uuid.uuid4()}")
    tb.set_task_identifier(f"urn:uuid:{uuid.uuid4()}")
    tb.set_authored_on(now)
    tb.set_restriction_end(now + timedelta(days=1))
    tb.set_requester_agent(uzi_sys="DUMMY", system_name="DUMMY")
    tb.set_sender(ura="DUMMY", display="DUMMY")
    tb.set_receiver_owner_identifier(ura="DUMMY")
    tb.set_patient(bsn="DUMMY")
    tb.set_description("DUMMY")
    tb.validate_fhir_constraints(allow_missing_refs=True)


# URA identifier system used in Dutch healthcare for organization identification
URA_IDENTIFIER_SYSTEM = "http://fhir.nl/fhir/NamingSystem/ura"


def _extract_ura_from_organization(org: Dict[str, Any] | None) -> str | None:
    """Extract URA identifier from Organization.identifier[] array.

    Per Dutch healthcare standards, URA is the unique identifier for healthcare organizations.
    Returns None if no URA identifier is found.
    """
    if not org or not isinstance(org, dict):
        return None

    identifiers = org.get("identifier")
    if not identifiers or not isinstance(identifiers, list):
        return None

    for ident in identifiers:
        if not isinstance(ident, dict):
            continue
        system = (ident.get("system") or "").strip()
        value = (ident.get("value") or "").strip()
        if system == URA_IDENTIFIER_SYSTEM and value:
            return value

    return None


def _ensure_dict(obj: Dict[str, Any], key: str) -> Dict[str, Any]:
    v = obj.get(key)
    if not isinstance(v, dict):
        v = {}
        obj[key] = v
    return v


def _ensure_list(obj: Dict[str, Any], key: str) -> List[Any]:
    v = obj.get(key)
    if not isinstance(v, list):
        v = []
        obj[key] = v
    return v


def _ensure_list_item(lst: List[Any], idx: int, default: Dict[str, Any]) -> Dict[str, Any]:
    while len(lst) <= idx:
        lst.append(copy.deepcopy(default))
    if not isinstance(lst[idx], dict):
        lst[idx] = copy.deepcopy(default)
    return lst[idx]


def _pick_author_assigned_identifier(identifiers: Any) -> Optional[Dict[str, Any]]:
    """Pick an identifier suitable for putting on Reference.identifier.

    NL-GF Location/HealthcareService profiles require at least one identifier.
    The routing spec recommends including an author-assigned identifier on the
    Reference so the receiver can route without dereferencing the resource.

    We pick the first identifier that has both `system` and `value`.
    """
    if not isinstance(identifiers, list):
        return None
    for ident in identifiers:
        if not isinstance(ident, dict):
            continue
        system = str(ident.get("system") or "").strip()
        value = str(ident.get("value") or "").strip()
        if not system or not value:
            continue
        out: Dict[str, Any] = {"system": system, "value": value}
        # Carry over common optional Identifier fields if present.
        for k in ("use", "type", "period", "assigner"):
            if ident.get(k) is not None:
                out[k] = copy.deepcopy(ident.get(k))
        return out
    return None


class TaskBuilder:
    """Build/patch a FHIR Task safely (avoid runtime KeyErrors).

    Dit is expliciet bedoeld voor PoC 9 BgZ notification Task.
    """

    def __init__(self, template: Dict[str, Any]):
        if not isinstance(template, dict):
            raise ValueError("Task template moet een dict zijn")
        self.task: Dict[str, Any] = copy.deepcopy(template)
        # Ensure baseline
        self.task.setdefault("resourceType", "Task")

        # Ensure all paths we write to
        self.group_identifier = _ensure_dict(self.task, "groupIdentifier")

        ident_list = _ensure_list(self.task, "identifier")
        _ensure_list_item(ident_list, 0, {"system": "urn:ietf:rfc:3986", "value": ""})

        self.restriction = _ensure_dict(self.task, "restriction")
        self.restriction_period = _ensure_dict(self.restriction, "period")

        self.requester = _ensure_dict(self.task, "requester")
        self.requester_on_behalf = _ensure_dict(self.requester, "onBehalfOf")
        self.requester_on_behalf_ident = _ensure_dict(self.requester_on_behalf, "identifier")

        self.owner = _ensure_dict(self.task, "owner")
        self.owner_ident = _ensure_dict(self.owner, "identifier")

        self.for_ = _ensure_dict(self.task, "for")
        self.for_ident = _ensure_dict(self.for_, "identifier")

        # Extensions are optional
        if "extension" in self.task and not isinstance(self.task.get("extension"), list):
            self.task["extension"] = []

    def set_group_identifier(self, value: str) -> None:
        self.group_identifier["value"] = str(value)

    def set_task_identifier(self, value: str) -> None:
        self.task.setdefault("identifier", [])
        ident_list = _ensure_list(self.task, "identifier")
        first = _ensure_list_item(ident_list, 0, {"system": "urn:ietf:rfc:3986", "value": ""})
        first["value"] = str(value)

    def set_authored_on(self, dt: datetime) -> None:
        self.task["authoredOn"] = dt.isoformat()

    def set_restriction_end(self, dt: datetime) -> None:
        self.restriction_period["end"] = dt.isoformat()

    def set_sender(self, *, ura: str, display: str) -> None:
        self.requester_on_behalf_ident["value"] = str(ura)
        self.requester_on_behalf["display"] = str(display)

    def set_requester_agent(self, *, uzi_sys: str, system_name: str) -> None:
        """Set requester.agent identifier (sending system identity)."""
        agent = _ensure_dict(self.requester, "agent")
        agent_ident = _ensure_dict(agent, "identifier")
        agent_ident["value"] = str(uzi_sys)
        agent["display"] = str(system_name)

    def set_authorization_base(self, value: str) -> None:
        """Set input:authorization-base value."""
        inputs = _ensure_list(self.task, "input")
        for inp in inputs:
            if not isinstance(inp, dict):
                continue
            type_coding = (inp.get("type") or {}).get("coding") or []
            for coding in type_coding:
                if coding.get("code") == "authorization-base":
                    inp["valueString"] = str(value)
                    return
        # If not found, append new input
        inputs.append({
            "type": {
                "coding": [{
                    "system": "http://fhir.nl/fhir/NamingSystem/TaskParameter",
                    "code": "authorization-base"
                }]
            },
            "valueString": str(value)
        })


    def set_get_workflow_task(self, value: bool) -> None:
        """Set input:get-workflow-task valueBoolean."""
        inputs = _ensure_list(self.task, "input")
        for inp in inputs:
            if not isinstance(inp, dict):
                continue
            type_coding = (inp.get("type") or {}).get("coding") or []
            for coding in type_coding:
                if coding.get("code") == "get-workflow-task":
                    inp["valueBoolean"] = bool(value)
                    return
        inputs.append({
            "type": {
                "coding": [{
                    "system": "http://fhir.nl/fhir/NamingSystem/TaskParameter",
                    "code": "get-workflow-task"
                }]
            },
            "valueBoolean": bool(value)
        })
    def set_sender_bgz_base_extension(self, *, ext_url: str, base_url: str) -> None:
        if not base_url:
            return
        if not isinstance(self.task.get("extension"), list):
            self.task["extension"] = []
        # remove old
        self.task["extension"] = [
            e for e in self.task["extension"]
            if not (isinstance(e, dict) and e.get("url") == ext_url)
        ]
        self.task["extension"].append({"url": ext_url, "valueUrl": str(base_url)})

    def set_reference_extension(
        self,
        *,
        ext_url: str,
        reference: str,
        display: Optional[str] = None,
        identifier: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Upsert a simple extension with valueReference.

        Used for NL-GF routing extensions on FHIR STU3 Task:
        - task-stu3-location
        - task-stu3-healthcareservice
        """
        if not isinstance(self.task.get("extension"), list):
            self.task["extension"] = []
        # Remove any existing slice with the same url
        self.task["extension"] = [
            e
            for e in self.task["extension"]
            if not (isinstance(e, dict) and e.get("url") == ext_url)
        ]
        if not reference:
            return
        value_ref: Dict[str, Any] = {"reference": str(reference)}
        if display:
            value_ref["display"] = str(display)
        if identifier:
            value_ref["identifier"] = copy.deepcopy(identifier)
        self.task["extension"].append({"url": ext_url, "valueReference": value_ref})

    def set_task_stu3_location_extension(
        self,
        reference: str,
        *,
        display: Optional[str] = None,
        identifier: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.set_reference_extension(
            ext_url=TASK_EXT_TASK_STU3_LOCATION_URL,
            reference=reference,
            display=display,
            identifier=identifier,
        )

    def set_task_stu3_healthcareservice_extension(
        self,
        reference: str,
        *,
        display: Optional[str] = None,
        identifier: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.set_reference_extension(
            ext_url=TASK_EXT_TASK_STU3_HEALTHCARESERVICE_URL,
            reference=reference,
            display=display,
            identifier=identifier,
        )

    def set_receiver_owner_identifier(self, *, ura: str) -> None:
        """Set owner.identifier (receiving organization URA). Per spec, owner only contains identifier."""
        self.owner_ident["value"] = str(ura)
        # Per TA Notified Pull spec, owner should only contain identifier - no reference or display
        self.owner.pop("reference", None)
        self.owner.pop("display", None)

    def set_owner_reference(self, reference: str, *, display: Optional[str] = None) -> None:
        if reference:
            self.owner["reference"] = str(reference)
        else:
            self.owner.pop("reference", None)
        if display is not None:
            self.owner["display"] = str(display)

    def set_location_reference(self, reference: str, *, display: Optional[str] = None) -> None:
        if reference:
            loc = {"reference": str(reference)}
            if display:
                loc["display"] = str(display)
            self.task["location"] = loc
        else:
            self.task.pop("location", None)

    def set_patient(self, *, bsn: str, display: Optional[str] = None) -> None:
        self.for_ident["value"] = str(bsn)
        if display:
            self.for_["display"] = str(display)
        else:
            self.for_.pop("display", None)

    def set_description(self, description: Optional[str]) -> None:
        if description:
            self.task["description"] = str(description)
        else:
            self.task.pop("description", None)

    def validate_fhir_constraints(self, allow_missing_refs: bool = False) -> None:
        """Validate constraints per TA Notified Pull spec.

        - Task.owner.reference (if present) must be Organization/... (routing to a specific
          HealthcareService/Location is expressed using NL-GF STU3 Task extensions).
        - If NL-GF STU3 routing extensions are present, their valueReference.reference must
          match the expected resource type.
        - (Legacy/R4) Task.location.reference (if present) must be Location/...
        """
        # Validate owner.identifier is set
        owner_ident_value = None
        try:
            owner_ident_value = (self.task.get("owner") or {}).get("identifier", {}).get("value")
        except Exception:
            owner_ident_value = None
        owner_ref = None
        try:
            owner_ref = (self.task.get("owner") or {}).get("reference")
        except Exception:
            owner_ref = None

        if owner_ref:
            rt, _ = _split_ref(str(owner_ref))
            if rt and rt != "Organization":
                raise RuntimeError("Task.owner.reference moet Organization/... zijn.")
        elif not allow_missing_refs:
            # For our use-cases we always set a reference (except in template validation).
            raise RuntimeError("Task.owner.reference ontbreekt.")

        # Validate NL-GF STU3 routing extensions (if present)
        ext_list = self.task.get("extension") if isinstance(self.task.get("extension"), list) else []
        for ext in ext_list or []:
            if not isinstance(ext, dict):
                continue
            url = str(ext.get("url") or "")
            if url not in (TASK_EXT_TASK_STU3_LOCATION_URL, TASK_EXT_TASK_STU3_HEALTHCARESERVICE_URL):
                continue
            value_ref = ext.get("valueReference")
            if not isinstance(value_ref, dict):
                continue
            ref = str(value_ref.get("reference") or "").strip()
            if not ref:
                # Reference.identifier-only is technically possible; do not hard-fail.
                continue
            rt, _ = _split_ref(ref)
            if url == TASK_EXT_TASK_STU3_LOCATION_URL and rt and rt != "Location":
                raise RuntimeError("Task.extension(task-stu3-location) moet refereren naar Location/... .")
            if url == TASK_EXT_TASK_STU3_HEALTHCARESERVICE_URL and rt and rt != "HealthcareService":
                raise RuntimeError(
                    "Task.extension(task-stu3-healthcareservice) moet refereren naar HealthcareService/... ."
                )

        loc_ref = None
        try:
            loc_ref = (self.task.get("location") or {}).get("reference")
        except Exception:
            loc_ref = None
        if loc_ref:
            rt, _ = _split_ref(str(loc_ref))
            if rt and rt != "Location":
                raise RuntimeError("Task.location.reference moet Location/... zijn.")

    def set_based_on_reference(self, reference: str, display: str | None = None) -> None:
        self.task.setdefault("basedOn", [])
        based_on_list = _ensure_list(self.task, "basedOn")
        first = _ensure_list_item(based_on_list, 0, default={})
        if not isinstance(first, dict):
            first = {}
            based_on_list[0] = first
        first["reference"] = str(reference)
        if display is not None:
            first["display"] = str(display)

    def build(self) -> Dict[str, Any]:
        # Ensure resourceType is Task
        if self.task.get("resourceType") != "Task":
            self.task["resourceType"] = "Task"
        return self.task




def _keep_task_inputs(*, task: Dict[str, Any], allowed_taskparameter_codes: set[str]) -> None:
    """Keep only Task.input entries whose TaskParameter.code is in allowed_taskparameter_codes."""
    inputs = (task or {}).get("input")
    if not isinstance(inputs, list):
        return
    kept = []
    for inp in inputs:
        if not isinstance(inp, dict):
            continue
        type_obj = inp.get("type") or {}
        coding_list = (type_obj.get("coding") or []) if isinstance(type_obj, dict) else []
        code_val = None
        for coding in coding_list:
            if isinstance(coding, dict) and coding.get("system") == "http://fhir.nl/fhir/NamingSystem/TaskParameter":
                code_val = coding.get("code")
                break
        if code_val and code_val in allowed_taskparameter_codes:
            kept.append(inp)
    task["input"] = kept
async def _resolve_bgz_notify_destination(
    *,
    receiver_target_ref: str,
    receiver_org_ref: str | None,
    receiver_notification_endpoint_id: str | None,
) -> tuple[Dict[str, Any], str, str | None, str, str | None, str, str]:
    """Resolve receiver destination from mCSD: notification endpoint base URL and URA."""
    receiver_org_ref_norm = _normalize_relative_ref(receiver_org_ref)
    receiver_target_ref_norm = _normalize_relative_ref(receiver_target_ref)
    if receiver_org_ref_norm and not receiver_org_ref_norm.startswith("Organization/"):
        raise HTTPException(status_code=400, detail="receiver_org_ref moet de vorm Organization/<id> hebben.")
    target_type = receiver_target_ref_norm.split("/", 1)[0] if "/" in receiver_target_ref_norm else ""
    if target_type not in ("Organization", "HealthcareService", "Location"):
        raise HTTPException(
            status_code=400,
            detail="receiver_target_ref moet Organization/<id>, HealthcareService/<id> of Location/<id> zijn.",
        )
    mapping = await poc9_msz_capability_mapping(
        target=receiver_target_ref_norm,
        organization=receiver_org_ref_norm or None,
        include_oauth=False,
        limit=200,
    )
    resolved_notification_endpoint_id = None
    try:
        resolved_notification_endpoint_id = (
            ((((mapping or {}).get("mapping") or {}).get("twiin_ta_notification") or {}).get("chosen") or {}).get("id")
        )
    except Exception:
        resolved_notification_endpoint_id = None
    if receiver_notification_endpoint_id:
        if not resolved_notification_endpoint_id:
            logger.warning(
                "Timing race detected: frontend endpoint_id=%s not resolvable anymore for target=%s",
                receiver_notification_endpoint_id, receiver_target_ref_norm,
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "reason": "stale_endpoint_resolution",
                    "message": "Het notification endpoint is niet meer beschikbaar in het adresboek. "
                               "Dit kan gebeuren als het adresboek is gewijzigd tussen selectie en verzenden. "
                               "Selecteer de organisatie opnieuw en probeer het nogmaals.",
                    "action": "refresh_selection",
                },
            )
        if receiver_notification_endpoint_id != str(resolved_notification_endpoint_id):
            logger.warning(
                "Timing race detected: endpoint changed from %s to %s for target=%s",
                receiver_notification_endpoint_id, resolved_notification_endpoint_id, receiver_target_ref_norm,
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "reason": "stale_endpoint_resolution",
                    "message": "Het notification endpoint is gewijzigd in het adresboek sinds uw selectie. "
                               "Selecteer de organisatie opnieuw en probeer het nogmaals.",
                    "action": "refresh_selection",
                    "expected_endpoint_id": str(resolved_notification_endpoint_id),
                    "provided_endpoint_id": str(receiver_notification_endpoint_id),
                },
            )
    notification_base = str(((mapping or {}).get("notification") or {}).get("base") or "")
    if not notification_base:
        raise HTTPException(
            status_code=400,
            detail={
                "reason": "no_notification_endpoint",
                "message": "Geen Twiin TA notification endpoint gevonden voor het gekozen target/organisatie.",
            },
        )
    receiver_base_norm = _normalize_fhir_base(notification_base)
    receiver_base_norm = _validate_http_base_url(receiver_base_norm, field_name="receiver_base")
    if not receiver_base_norm:
        raise HTTPException(status_code=400, detail="Ongeldige receiver_base (resolved).")

    base = MCSDBASE.rstrip("/")
    resolved_receiver_ura: str | None = None

    effective_org_ref: str | None = None
    try:
        raw_org_ref = str(((mapping or {}).get("organization") or {}).get("reference") or "").strip()
        if raw_org_ref:
            tmp = _normalize_relative_ref(raw_org_ref)
            if tmp and tmp.startswith("Organization/"):
                effective_org_ref = tmp
    except Exception:
        effective_org_ref = None

    if not effective_org_ref and target_type == "Organization":
        effective_org_ref = receiver_target_ref_norm

    if not effective_org_ref and receiver_org_ref_norm:
        effective_org_ref = receiver_org_ref_norm

    if effective_org_ref:
        try:
            org_resource = await _get_json(app.state.http_client, f"{base}/{effective_org_ref}", {})
            resolved_receiver_ura = _extract_ura_from_organization(org_resource)
            if resolved_receiver_ura:
                logger.debug(
                    "Resolved receiver URA from mCSD: org_ref=%s, ura=%s",
                    effective_org_ref, resolved_receiver_ura,
                )
        except Exception as e:
            logger.warning(
                "Failed to fetch Organization from mCSD for URA extraction: org_ref=%s, error=%s",
                effective_org_ref, str(e),
            )

    if not resolved_receiver_ura:
        raise HTTPException(
            status_code=400,
            detail={
                "reason": "no_receiver_ura",
                "message": "Geen URA-identifier gevonden in de mCSD Organization resource. "
                           "De ontvangende organisatie moet een geldig URA-nummer hebben in het adresboek.",
                "organization_ref": effective_org_ref,
            },
        )

    return mapping, receiver_base_norm, resolved_notification_endpoint_id, receiver_target_ref_norm, receiver_org_ref_norm, target_type, resolved_receiver_ura

def _extract_effective_org_from_mapping(mapping: Dict[str, Any]) -> tuple[str | None, str | None]:
    """Extract effective organization reference and name from capability mapping.
    
    Returns: (effective_org_ref_norm, effective_org_name)
    """
    effective_org_ref_norm: str | None = None
    effective_org_name: str | None = None
    try:
        raw_org_ref = str(((mapping or {}).get("organization") or {}).get("reference") or "").strip()
        if raw_org_ref:
            tmp = _normalize_relative_ref(raw_org_ref)
            if tmp and tmp.startswith("Organization/"):
                effective_org_ref_norm = tmp
        effective_org_name = str(((mapping or {}).get("organization") or {}).get("display") or "").strip() or None
    except Exception:
        effective_org_ref_norm = None
        effective_org_name = None
    return effective_org_ref_norm, effective_org_name
def _determine_task_routing(
    *,
    target_type: str,
    receiver_target_ref_norm: str,
    receiver_org_ref_norm: str | None,
    effective_org_ref_norm: str | None,
    effective_org_name: str | None,
    target_display: str | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Determine Task.owner and Task.location references based on target type.
    
    Returns: (task_owner_ref, task_owner_display, task_location_ref, task_location_display)
    """
    task_owner_ref: str | None = None
    task_owner_display: str | None = None
    task_location_ref: str | None = None
    task_location_display: str | None = None
    if target_type == "Organization":
        task_owner_ref = receiver_target_ref_norm
        task_owner_display = target_display
    elif target_type == "HealthcareService":
        # In FHIR STU3, HealthcareService is referenced through the dedicated extension.
        # Task.owner should remain an Organization reference (if known).
        if effective_org_ref_norm or receiver_org_ref_norm:
            task_owner_ref = effective_org_ref_norm or receiver_org_ref_norm
            task_owner_display = effective_org_name
        else:
            task_owner_ref = None
            task_owner_display = None
    elif target_type == "Location":
        task_location_ref = receiver_target_ref_norm
        task_location_display = target_display
        task_owner_ref = effective_org_ref_norm or receiver_org_ref_norm or None
        task_owner_display = effective_org_name
    return task_owner_ref, task_owner_display, task_location_ref, task_location_display
def _build_bgz_notification_task(
    *,
    sender_ura: str,
    sender_name: str,
    sender_uzi_sys: str,
    sender_system_name: str,
    sender_bgz_base: str | None,
    authorization_base: str,
    receiver_ura: str,
    receiver_name: str,
    receiver_org_ref_norm: str | None,
    receiver_org_name: str | None,
    receiver_target_ref_norm: str,
    receiver_target_display: str | None = None,
    receiver_target_identifiers: List[Dict[str, Any]] | None = None,
    receiver_effective_org_ref_norm: str | None = None,
    receiver_effective_org_name: str | None = None,
    patient_bsn: str,
    patient_name: str | None,
    description: str | None,
    workflow_task_id: str | None = None,
) -> tuple[Dict[str, Any], str | None, str]:
    """Build a Notification Task per TA Notified Pull v1.0.1 spec.

    Per spec, the Task only contains identifiers (URA/BSN), not references or display names.
    """
    template = _load_bgz_template("notification-task.json")
    tb = TaskBuilder(template)
    tb.set_group_identifier(f"urn:uuid:{uuid.uuid4()}")
    tb.set_task_identifier(f"urn:uuid:{uuid.uuid4()}")
    workflow_task_id_norm = (workflow_task_id or "").strip()
    workflow_task_logical_id = ""
    if workflow_task_id_norm:
        if workflow_task_id_norm.startswith("Task/"):
            workflow_task_logical_id = workflow_task_id_norm.split("/", 1)[1].strip()
        elif "://" in workflow_task_id_norm:
            _rt_tmp, _rid_tmp = _split_ref(workflow_task_id_norm)
            workflow_task_logical_id = (_rid_tmp or "").strip()
        else:
            workflow_task_logical_id = workflow_task_id_norm
    if not workflow_task_logical_id:
        workflow_task_logical_id = str(uuid.uuid4())
    workflow_task_id_norm = workflow_task_logical_id
    workflow_task_ref = f"Task/{workflow_task_logical_id}"
    tb.set_based_on_reference(workflow_task_ref)
    now = datetime.now(timezone.utc)
    tb.set_authored_on(now)
    tb.set_restriction_end(now + timedelta(days=365))

    # Requester: sending system (agent) and organization (onBehalfOf)
    tb.set_requester_agent(uzi_sys=sender_uzi_sys, system_name=sender_system_name)
    tb.set_sender(ura=sender_ura, display=sender_name)
    sender_bgz_base_norm = _normalize_fhir_base(sender_bgz_base) if sender_bgz_base else ""
    if sender_bgz_base_norm:
        sender_bgz_base_norm = _validate_http_base_url(sender_bgz_base_norm, field_name="sender_bgz_base")
        tb.set_sender_bgz_base_extension(ext_url=TASK_EXT_SENDER_BGZ_BASE_URL, base_url=sender_bgz_base_norm)
    tb.set_receiver_owner_identifier(ura=receiver_ura)

    receiver_org_name_norm = (receiver_org_name or "").strip()
    effective_org_ref_norm = (receiver_effective_org_ref_norm or "").strip() or None
    if effective_org_ref_norm:
        effective_org_ref_norm = _normalize_relative_ref(effective_org_ref_norm)
        if not effective_org_ref_norm.startswith("Organization/"):
            effective_org_ref_norm = None
    effective_org_name_norm = (receiver_effective_org_name or "").strip()
    if not effective_org_name_norm:
        effective_org_name_norm = receiver_org_name_norm

    # Normalize target hints (used for STU3 routing extensions)
    target_display_norm = (receiver_target_display or "").strip() or None
    target_identifier = _pick_author_assigned_identifier(receiver_target_identifiers or [])

    target_ref = receiver_target_ref_norm or receiver_org_ref_norm
    target_type = target_ref.split("/", 1)[0] if target_ref and "/" in target_ref else ""
    if target_type not in ("Location", "Organization", "HealthcareService"):
        raise HTTPException(
            status_code=400,
            detail={
                "reason": "invalid_reference",
                "message": "receiver_target_ref moet verwijzen naar Location, Organization of HealthcareService.",
            },
        )

    # Compute the Task routing information that the receiver will see:
    # - Organization target -> Task.owner = Organization
    # - Location target (onderdeel) -> Task.extension(task-stu3-location)
    # - HealthcareService target (onderdeel) -> Task.extension(task-stu3-healthcareservice)
    #
    # NOTE: For STU3, these routing references SHOULD include an author-assigned identifier
    # (Reference.identifier). We add it when the directory returns one.
    owner_ref_used: Optional[str] = None

    # Ensure we don't leak R4-only fields into a STU3 Task.
    tb.task.pop("location", None)
    # Clear routing extensions first (idempotent builds)
    tb.set_task_stu3_location_extension("", display=None, identifier=None)
    tb.set_task_stu3_healthcareservice_extension("", display=None, identifier=None)

    if target_type == "Location":
        if not target_identifier:
            logger.warning(
                "Building STU3 Task with task-stu3-location extension but without Reference.identifier (target=%s)",
                target_ref,
            )
        tb.set_task_stu3_location_extension(
            target_ref,
            display=(target_display_norm or receiver_name),
            identifier=target_identifier,
        )
        owner_ref_used = effective_org_ref_norm or receiver_org_ref_norm
        if owner_ref_used:
            tb.set_owner_reference(owner_ref_used, display=(effective_org_name_norm or receiver_org_name_norm or receiver_name))
        else:
            tb.set_owner_reference("", display=receiver_name)
    elif target_type == "Organization":
        owner_ref_used = target_ref
        tb.set_owner_reference(owner_ref_used, display=receiver_name)
    elif target_type == "HealthcareService":
        if not target_identifier:
            logger.warning(
                "Building STU3 Task with task-stu3-healthcareservice extension but without Reference.identifier (target=%s)",
                target_ref,
            )
        tb.set_task_stu3_healthcareservice_extension(
            target_ref,
            display=(target_display_norm or receiver_name),
            identifier=target_identifier,
        )

        owner_ref_used = effective_org_ref_norm or receiver_org_ref_norm
        if owner_ref_used:
            # If we mapped HealthcareService -> Organization, use the org name as display (if known).
            if owner_ref_used.startswith("Organization/") and (effective_org_name_norm or receiver_org_name_norm):
                tb.set_owner_reference(owner_ref_used, display=(effective_org_name_norm or receiver_org_name_norm))
            else:
                tb.set_owner_reference(owner_ref_used, display=receiver_name)
        else:
            # Owner identifier is still present (URA), but we don't have a resolvable Organization/<id>.
            tb.set_owner_reference("", display=receiver_name)

    # Patient and description
    tb.set_patient(bsn=patient_bsn, display=patient_name)
    tb.set_description(description)
    tb.validate_fhir_constraints(allow_missing_refs=(target_type in ("Location", "HealthcareService") and not owner_ref_used))
    # Authorization base for the pull
    tb.set_authorization_base(authorization_base)
    tb.set_get_workflow_task(True)
    _keep_task_inputs(task=tb.task, allowed_taskparameter_codes={"authorization-base", "get-workflow-task"})
    task = tb.build()
    return task, (sender_bgz_base_norm or None), workflow_task_id_norm

def _build_bgz_workflow_task(
    *,
    workflow_task_id: str,
    group_identifier: str,
    sender_ura: str,
    sender_name: str,
    sender_uzi_sys: str,
    sender_system_name: str,
    receiver_ura: str,
    patient_bsn: str,
    patient_name: str | None,
    description: str | None,
) -> Dict[str, Any]:
    """Build a Workflow Task that lists BgZ queries/resources in Task.input."""
    template = _load_bgz_template("workflow-task.json")
    tb = TaskBuilder(template)
    tb.task["id"] = str(workflow_task_id)
    if group_identifier:
        tb.set_group_identifier(str(group_identifier))
    tb.set_task_identifier(f"urn:uuid:{uuid.uuid4()}")
    now = datetime.now(timezone.utc)
    tb.set_authored_on(now)
    tb.set_restriction_end(now + timedelta(days=365))
    tb.set_requester_agent(uzi_sys=sender_uzi_sys, system_name=sender_system_name)
    tb.set_sender(ura=sender_ura, display=sender_name)
    tb.set_receiver_owner_identifier(ura=receiver_ura)
    tb.set_patient(bsn=patient_bsn, display=patient_name)
    tb.set_description(description)
    tb.validate_fhir_constraints(allow_missing_refs=True)
    task = tb.build()
    # Workflow Task should not carry the notification-only profile
    try:
        meta = task.get("meta")
        if isinstance(meta, dict):
            prof = meta.get("profile")
            if isinstance(prof, list) and "http://fhir.nl/fhir/StructureDefinition/nl-vzvz-TaskNotifiedPull" in prof:
                meta.pop("profile", None)
                if not meta:
                    task.pop("meta", None)
    except Exception:
        pass
    return task


async def _upsert_workflow_task_to_sender_bgz(
    *,
    sender_bgz_base: str,
    workflow_task: Dict[str, Any],
) -> str:
    """Create/update the Workflow Task on the Sending System (HAPI) using PUT /Task/{id}.

    Sommige servers staan geen client-assigned id toe via PUT-create en geven dan 422/400.
    In dat geval vallen we terug op POST /Task en gebruiken we het server-assigned id.
    """
    base_url = _validate_http_base_url(_normalize_fhir_base(sender_bgz_base), field_name="sender_bgz_base")
    task_id = str((workflow_task or {}).get("id") or "").strip()
    if not task_id:
        raise HTTPException(status_code=500, detail={"reason": "internal_error", "message": "Workflow Task mist een id."})
    put_url = f"{base_url}/Task/{task_id}"
    _dump_debug_json("bgz_workflow_task_upsert", {"method": "PUT", "url": put_url, "json": workflow_task})

    def _loc_to_task_id(loc: str | None) -> str:
        if not isinstance(loc, str):
            return ""
        s = loc.strip()
        if not s:
            return ""
        # Best-effort: extract "Task/{id}" tail
        i = s.lower().rfind("/task/")
        if i == -1:
            return ""
        tail = s[i + 6:]
        tail = tail.split("?")[0].split("#")[0]
        return tail.strip("/")

    try:
        r = await app.state.http_client.put(
            put_url,
            json=workflow_task,
            headers={"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"},
        )
        r.raise_for_status()
        return task_id
    except httpx.HTTPStatusError as e:
        debug: Dict[str, Any] = {}
        try:
            if getattr(e, "response", None) is not None:
                debug = _extract_receiver_error_debug(e.response, max_body_chars=2000)
        except Exception:
            debug = {}
        _dump_debug_json("bgz_workflow_task_upsert_error", {"method": "PUT", "url": put_url, "error": debug})

        http_status = debug.get("receiver_http_status")
        receiver_msg = (debug.get("receiver_message") or "").strip() if isinstance(debug.get("receiver_message"), str) else ""

        # Fallback: server does not accept client-assigned id via PUT-create
        should_try_post = False
        try:
            hs = int(http_status or 0)
            if hs in (400, 405, 409, 422):
                should_try_post = True
        except Exception:
            should_try_post = True

        if should_try_post:
            post_url = f"{base_url}/Task"
            post_task = copy.deepcopy(workflow_task)
            try:
                post_task.pop("id", None)
            except Exception:
                pass
            _dump_debug_json("bgz_workflow_task_create", {"method": "POST", "url": post_url, "json": post_task})
            try:
                r2 = await app.state.http_client.post(
                    post_url,
                    json=post_task,
                    headers={"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"},
                )
                r2.raise_for_status()
                created_id = ""
                try:
                    body = r2.json()
                    if isinstance(body, dict):
                        created_id = str(body.get("id") or "").strip()
                except Exception:
                    created_id = ""
                if not created_id:
                    try:
                        created_id = _loc_to_task_id(r2.headers.get("location") or r2.headers.get("content-location"))
                    except Exception:
                        created_id = ""
                if not created_id:
                    created_id = task_id
                return created_id
            except httpx.HTTPStatusError as e2:
                debug2: Dict[str, Any] = {}
                try:
                    if getattr(e2, "response", None) is not None:
                        debug2 = _extract_receiver_error_debug(e2.response, max_body_chars=2000)
                except Exception:
                    debug2 = {}
                _dump_debug_json("bgz_workflow_task_create_error", {"method": "POST", "url": post_url, "error": debug2})
                http_status2 = debug2.get("receiver_http_status")
                receiver_msg2 = (debug2.get("receiver_message") or "").strip() if isinstance(debug2.get("receiver_message"), str) else ""
                msg2 = "Workflow Task kon niet worden opgeslagen op de Sending FHIR server."
                if receiver_msg2 and not settings.is_production:
                    msg2 = f"Workflow Task kon niet worden opgeslagen op de Sending FHIR server (HTTP {http_status2}): {receiver_msg2}"
                raise HTTPException(
                    status_code=502,
                    detail={
                        "reason": "sender_workflow_task_error",
                        "message": msg2,
                        "details": {
                            "sender_bgz_base": base_url,
                            "workflow_task_id": task_id,
                            "http_status": http_status2,
                            "response": debug2,
                        },
                    },
                )
            except httpx.RequestError as e2:
                info2 = _classify_upstream_exception(e2)
                raise HTTPException(
                    status_code=502,
                    detail={
                        "reason": info2.get("reason") or "network",
                        "message": info2.get("message") or "Sending FHIR server is niet bereikbaar.",
                        "details": {
                            "sender_bgz_base": base_url,
                            "workflow_task_id": task_id,
                        },
                    },
                )

        msg = "Workflow Task kon niet worden opgeslagen op de Sending FHIR server."
        if receiver_msg and not settings.is_production:
            msg = f"Workflow Task kon niet worden opgeslagen op de Sending FHIR server (HTTP {http_status}): {receiver_msg}"
        raise HTTPException(
            status_code=502,
            detail={
                "reason": "sender_workflow_task_error",
                "message": msg,
                "details": {
                    "sender_bgz_base": base_url,
                    "workflow_task_id": task_id,
                    "http_status": http_status,
                    "response": debug,
                },
            },
        )
    except httpx.RequestError as e:
        info = _classify_upstream_exception(e)
        raise HTTPException(
            status_code=502,
            detail={
                "reason": info.get("reason") or "network",
                "message": info.get("message") or "Sending FHIR server is niet bereikbaar.",
                "details": {
                    "sender_bgz_base": base_url,
                    "workflow_task_id": task_id,
                },
            },
        )
@app.post("/bgz/load-data", dependencies=[Depends(verify_api_key)])
async def bgz_load_data(
    hapi_base: str = Query(..., description="Target HAPI FHIR server base URL"),
    sender_ura: str = Query(..., description="Sender organization URA"),
):
    """
    Load BgZ sample data (Patient, Conditions, Allergies, Medications, etc.)
    to a target HAPI FHIR server using PUT for each resource.
    """
    template = _load_bgz_template("bgz-sample-bundle.json")
    bundle = copy.deepcopy(template)

    # Update sender URA in Organization resource
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Organization" and resource.get("id") == "organization-sender":
            for ident in resource.get("identifier", []):
                if ident.get("system") == "http://fhir.nl/fhir/NamingSystem/ura":
                    ident["value"] = sender_ura
    base_url = _validate_http_base_url(_normalize_fhir_base(hapi_base), field_name="hapi_base")
    headers = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
    results = []

    try:
        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            resource_type = resource.get("resourceType")
            resource_id = resource.get("id")
            if not resource_type or not resource_id:
                continue

            url = f"{base_url}/{resource_type}/{resource_id}"
            _dump_debug_json(f"bgz_load_data_{resource_type}_{resource_id}", {"method": "PUT", "url": url, "json": resource})
            response = await app.state.http_client.put(url, json=resource, headers=headers)
            response.raise_for_status()

            results.append({
                "resource": f"{resource_type}/{resource_id}",
                "status": response.status_code,
            })

        return {
            "success": True,
            "target": base_url,
            "resources_created": len(results),
            "details": results,
        }
    except httpx.HTTPStatusError as e:
        raise _http_status_to_http_exception(e)
    except HTTPException:
        raise
    except Exception as e:
        _raise_502_with_reason(e)



class BgzPreflightRequest(BaseModel):
    receiver_org_ref: str | None = Field(None, max_length=256, description="(PoC 9) Receiver organization reference (Organization).")
    receiver_target_ref: str = Field(..., max_length=256, description="(PoC 9) Receiver target reference (Organization, HealthcareService, Location).")
    receiver_notification_endpoint_id: str | None = Field(None, max_length=64, description="(Optional) Frontend-chosen notification Endpoint.id; backend checks for staleness.")
    check_receiver: bool = Field(True, description="Probe receiver FHIR base (/metadata) for reachability and Task create support.")
    include_oauth: bool = Field(False, description="(Debug/demo) Include OAuth/NUTS endpoints in the capability mapping response.")


@app.post("/bgz/preflight", dependencies=[Depends(verify_api_key)])
async def bgz_preflight(payload: BgzPreflightRequest = Body(...)):
    """Preflight check before sending a BgZ notification Task.

    Doel: geef the frontend maximale zekerheid dat the backend *kan* verzenden:
    - validate backend sender config
    - resolve receiver notification base via PoC 9 capability mapping (same logic as /bgz/notify)
    - (optional) probe receiver /metadata to check reachability and Task create support
    """
    sender_ura = (settings.sender_ura or "").strip()
    sender_name = (settings.sender_name or "").strip()
    if not sender_ura or not sender_name:
        raise HTTPException(
            status_code=500,
            detail={"reason": "misconfigured", "message": "MCSD_SENDER_URA en/of MCSD_SENDER_NAME is niet ingesteld."},
        )

    receiver_org_ref = (payload.receiver_org_ref or "").strip() or None
    receiver_target_ref = (payload.receiver_target_ref or "").strip()
    receiver_notification_endpoint_id = (payload.receiver_notification_endpoint_id or "").strip() or None
    check_receiver = bool(payload.check_receiver)

    if not receiver_target_ref:
        raise HTTPException(status_code=400, detail="receiver_target_ref is verplicht.")

    receiver_org_ref_norm = _normalize_relative_ref(receiver_org_ref)
    receiver_target_ref_norm = _normalize_relative_ref(receiver_target_ref)

    if receiver_org_ref_norm and not receiver_org_ref_norm.startswith("Organization/"):
        raise HTTPException(status_code=400, detail="receiver_org_ref moet de vorm Organization hebben.")

    target_type = receiver_target_ref_norm.split("/", 1)[0] if "/" in receiver_target_ref_norm else ""
    if target_type not in ("Organization", "HealthcareService", "Location"):
        raise HTTPException(
            status_code=400,
            detail="receiver_target_ref moet Organization, HealthcareService of Location zijn.",
        )

    mapping = await poc9_msz_capability_mapping(
        target=receiver_target_ref_norm,
        organization=receiver_org_ref_norm or None,
        include_oauth=bool(payload.include_oauth),
        limit=200,
    )

    # Extract resolved base (already normalized/validated in capability-mapping response)
    resolved_base = str(((mapping or {}).get("notification") or {}).get("base") or "").strip()
    supported = bool((mapping or {}).get("supported")) and bool(resolved_base)

    # Extract resolved Endpoint.id (if present) for staleness detection
    resolved_notification_endpoint_id = None
    try:
        resolved_notification_endpoint_id = (
            ((((mapping or {}).get("mapping") or {}).get("twiin_ta_notification") or {}).get("chosen") or {}).get("id")
        )
    except Exception:
        resolved_notification_endpoint_id = None

    endpoint_id_match: Optional[bool] = None
    if receiver_notification_endpoint_id:
        endpoint_id_match = bool(resolved_notification_endpoint_id) and (
            str(resolved_notification_endpoint_id) == str(receiver_notification_endpoint_id)
        )
        if not endpoint_id_match:
            supported = False

    base = MCSDBASE.rstrip("/")
    resolved_receiver_ura: str | None = None
    effective_org_ref_for_ura: str | None = None

    try:
        raw_org_ref = str(((mapping or {}).get("organization") or {}).get("reference") or "").strip()
        if raw_org_ref:
            tmp = _normalize_relative_ref(raw_org_ref)
            if tmp and tmp.startswith("Organization/"):
                effective_org_ref_for_ura = tmp
    except Exception:
        pass

    if not effective_org_ref_for_ura and target_type == "Organization":
        effective_org_ref_for_ura = receiver_target_ref_norm

    if not effective_org_ref_for_ura and receiver_org_ref_norm:
        effective_org_ref_for_ura = receiver_org_ref_norm

    if effective_org_ref_for_ura:
        try:
            org_resource = await _get_json(app.state.http_client, f"{base}/{effective_org_ref_for_ura}", {})
            resolved_receiver_ura = _extract_ura_from_organization(org_resource)
        except Exception as e:
            logger.warning("preflight: Failed to fetch Organization for URA: %s", str(e))

    if not resolved_receiver_ura:
        supported = False

    # Optional receiver probe
    receiver_probe: Dict[str, Any] = {
        "attempted": False,
        "ok": None,
        "reachable": None,
        "http_status": None,
        "task_create_supported": None,
        "reason": None,
        "message": None,
        "url": None,
    }

    def _capability_supports_task_create(capability_statement: Dict[str, Any]) -> bool:
        try:
            for r in (capability_statement or {}).get("rest", []) or []:
                for res in (r or {}).get("resource", []) or []:
                    if (res or {}).get("type") == "Task":
                        for it in (res or {}).get("interaction", []) or []:
                            if (it or {}).get("code") == "create":
                                return True
            return False
        except Exception:
            return False

    if check_receiver and resolved_base:
        receiver_probe["attempted"] = True
        receiver_probe["url"] = f"{resolved_base.rstrip('/')}/metadata"
        try:
            r = await app.state.http_client.get(
                receiver_probe["url"],
                headers={"Accept": "application/fhir+json"},
                timeout=5.0,
            )
            receiver_probe["http_status"] = r.status_code
            receiver_probe["reachable"] = True

            if r.status_code >= 400:
                receiver_probe["ok"] = False
                receiver_probe["reason"] = f"http_{r.status_code}"
            else:
                try:
                    cap = r.json()
                except Exception:
                    receiver_probe["ok"] = False
                    receiver_probe["task_create_supported"] = None
                    receiver_probe["reason"] = "invalid_capability_statement"
                    receiver_probe["message"] = "Receiver /metadata response kon niet als JSON worden gelezen."
                else:
                    tcs = _capability_supports_task_create(cap)
                    receiver_probe["task_create_supported"] = tcs

                    if tcs is True:
                        receiver_probe["ok"] = True
                    else:
                        receiver_probe["ok"] = False
                        receiver_probe["reason"] = "task_create_not_supported"
                        receiver_probe["message"] = "Receiver CapabilityStatement ondersteunt geen Task.create (POST /Task)."
        except Exception as e:
            info = _classify_upstream_exception(e)
            receiver_probe["reachable"] = False
            receiver_probe["ok"] = False
            receiver_probe["reason"] = info.get("reason")
            receiver_probe["message"] = info.get("message")

    ready_to_send = supported and ((receiver_probe["ok"] is True) if receiver_probe["attempted"] else True)

    # Bepaal expliciet welke routing-refs de backend zal gebruiken in /bgz/notify,
    # zodat de frontend dit transparant kan tonen (en geen 'fallback' hoeft te doen).
    # NB: Voor FHIR STU3 worden Location/HealthcareService als routinglabel opgenomen via extensies
    # (task-stu3-location / task-stu3-healthcareservice), i.p.v. Task.location / Task.owner=HealthcareService.
    effective_org_ref_norm, effective_org_name = _extract_effective_org_from_mapping(mapping)

    target_display = str(((mapping or {}).get("target") or {}).get("display") or "").strip() or None

    task_owner_ref, task_owner_display, task_location_ref, task_location_display = _determine_task_routing(
        target_type=target_type,
        receiver_target_ref_norm=receiver_target_ref_norm,
        receiver_org_ref_norm=receiver_org_ref_norm,
        effective_org_ref_norm=effective_org_ref_norm,
        effective_org_name=effective_org_name,
        target_display=target_display,
    )

    task_routing = {
        "target_type": target_type,
        "owner_ref": task_owner_ref,
        "owner_display": task_owner_display,
        "location_ref": task_location_ref,
        "location_display": task_location_display,
        # STU3 routing extensions (used when 'onderdeel' is a Location or HealthcareService)
        "extension_location_ref": receiver_target_ref_norm if target_type == "Location" else None,
        "extension_location_display": target_display if target_type == "Location" else None,
        "extension_healthcareservice_ref": receiver_target_ref_norm if target_type == "HealthcareService" else None,
        "extension_healthcareservice_display": target_display if target_type == "HealthcareService" else None,
    }

    # Return mapping + extra fields for the UI.
    out = dict(mapping or {})
    out["task_routing"] = task_routing
    out["resolved_receiver_base"] = resolved_base or None
    out["resolved_receiver_ura"] = resolved_receiver_ura or None
    out["notification_endpoint_id"] = str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None
    out["frontend_endpoint_id_match"] = endpoint_id_match
    out["receiver_probe"] = receiver_probe
    out["ready_to_send"] = ready_to_send
    return out


@app.post(
    "/bgz/task-preview",
    dependencies=[Depends(verify_api_key)],
    response_model=BgzTaskPreviewResponseModel,
)
async def bgz_task_preview(payload: BgzNotifyRequest = Body(...)):
    """Build a BgZ notification Task without sending it (for UI preview/tests)."""
    if settings.is_production and not settings.allow_task_preview_in_production:
        raise HTTPException(
            status_code=403,
            detail={"reason": "forbidden", "message": "task-preview is niet toegestaan in productie."},
        )

    sender_ura = (settings.sender_ura or "").strip()
    sender_name = (settings.sender_name or "").strip()
    sender_uzi_sys = (settings.sender_uzi_sys or "").strip()
    sender_system_name = (settings.sender_system_name or "").strip()
    sender_bgz_base = (settings.sender_bgz_base or "").strip() or None

    if not sender_ura or not sender_name:
        raise HTTPException(
            status_code=500,
            detail={"reason": "misconfigured", "message": "MCSD_SENDER_URA en/of MCSD_SENDER_NAME is niet ingesteld."},
        )
    if not sender_uzi_sys or not sender_system_name:
        raise HTTPException(
            status_code=500,
            detail={"reason": "misconfigured", "message": "MCSD_SENDER_UZI_SYS en/of MCSD_SENDER_SYSTEM_NAME is niet ingesteld."},
        )

    client_receiver_ura = (payload.receiver_ura or "").strip()
    receiver_name = (payload.receiver_name or "").strip()
    receiver_org_ref = (payload.receiver_org_ref or "").strip() or None
    receiver_org_name = (payload.receiver_org_name or "").strip() or None
    receiver_target_ref = (payload.receiver_target_ref or "").strip()
    receiver_notification_endpoint_id = (payload.receiver_notification_endpoint_id or "").strip() or None
    patient_bsn = (payload.patient_bsn or "").strip()
    patient_name = (payload.patient_name or "").strip() if payload.patient_name else None
    description = (payload.description or "").strip() if payload.description else None

    if not receiver_name:
        raise HTTPException(status_code=400, detail="receiver_name is verplicht.")
    if not receiver_target_ref:
        raise HTTPException(status_code=400, detail="receiver_target_ref is verplicht.")
    if not patient_bsn:
        raise HTTPException(status_code=400, detail="patient_bsn is verplicht.")

    mapping, receiver_base_norm, resolved_notification_endpoint_id, receiver_target_ref_norm, receiver_org_ref_norm, target_type, resolved_receiver_ura = await _resolve_bgz_notify_destination(
        receiver_target_ref=receiver_target_ref,
        receiver_org_ref=receiver_org_ref,
        receiver_notification_endpoint_id=receiver_notification_endpoint_id,
    )

    if client_receiver_ura and client_receiver_ura != resolved_receiver_ura:
        logger.info(
            "task_preview: Client URA differs from mCSD URA. client=%s, mcsd=%s, target=%s",
            client_receiver_ura, resolved_receiver_ura, receiver_target_ref_norm,
        )

    # Effective organisatie voor routing (bijv. HealthcareService.providedBy, Location.managingOrganization)
    effective_org_ref_norm: str | None = None
    effective_org_name: str | None = None
    try:
        raw_org_ref = str(((mapping or {}).get("organization") or {}).get("reference") or "").strip()
        if raw_org_ref:
            tmp = _normalize_relative_ref(raw_org_ref)
            if tmp and tmp.startswith("Organization/"):
                effective_org_ref_norm = tmp
        effective_org_name = str(((mapping or {}).get("organization") or {}).get("display") or "").strip() or None
    except Exception:
        effective_org_ref_norm = None
        effective_org_name = None

    authorization_base = base64.b64encode(str(uuid.uuid4()).encode()).decode()

    task, sender_bgz_base_norm, workflow_task_id_norm = _build_bgz_notification_task(
        sender_ura=sender_ura,
        sender_name=sender_name,
        sender_uzi_sys=sender_uzi_sys,
        sender_system_name=sender_system_name,
        sender_bgz_base=sender_bgz_base,
        authorization_base=authorization_base,
        receiver_ura=resolved_receiver_ura,
        receiver_name=receiver_name,
        receiver_org_ref_norm=receiver_org_ref_norm,
        receiver_org_name=receiver_org_name,
        receiver_target_ref_norm=receiver_target_ref_norm,
        receiver_target_display=str(((mapping or {}).get("target") or {}).get("display") or "").strip() or None,
        receiver_target_identifiers=(
            ((mapping or {}).get("target") or {}).get("identifier")
            if isinstance(((mapping or {}).get("target") or {}).get("identifier"), list)
            else None
        ),
        receiver_effective_org_ref_norm=effective_org_ref_norm,
        receiver_effective_org_name=effective_org_name,
        patient_bsn=patient_bsn,
        patient_name=patient_name,
        description=description,
        workflow_task_id=payload.workflow_task_id,
    )

    audit_event(
        "bgz.task_preview",
        sender_ura=sender_ura,
        receiver_ura=resolved_receiver_ura,
        receiver_target_ref=receiver_target_ref_norm,
        receiver_org_ref=receiver_org_ref_norm,
        resolved_receiver_base=receiver_base_norm,
        notification_endpoint_id=(str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
        task_group_identifier=(task.get("groupIdentifier") or {}).get("value"),
        patient=_audit_hash(patient_bsn),
    )

    return {
        "success": True,
        "task": task,
        "sender_bgz_base": sender_bgz_base_norm or None,
        "resolved_receiver_base": receiver_base_norm,
        "notification_endpoint_id": (str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
        "workflow_task_id": workflow_task_id_norm,
    }


@app.post(
    "/bgz/notify",
    dependencies=[Depends(verify_api_key)],
    response_model=BgzNotifyResponseModel,
)
async def bgz_notify(payload: BgzNotifyRequest = Body(...)):
    """
    Send a BgZ notification Task to an external receiver's FHIR server.

    SSRF-mitigatie:
    - de backend accepteert geen vrije `receiver_base` meer vanuit de client.
    - de backend resolveert de notification endpoint base opnieuw o.b.v. receiver_target_ref (+ optioneel receiver_org_ref)
      via het mCSD-adresboek (capability mapping).
    """
    sender_ura = (settings.sender_ura or "").strip()
    sender_name = (settings.sender_name or "").strip()
    sender_uzi_sys = (settings.sender_uzi_sys or "").strip()
    sender_system_name = (settings.sender_system_name or "").strip()
    sender_bgz_base = (settings.sender_bgz_base or "").strip() or None

    if not sender_ura or not sender_name:
        raise HTTPException(
            status_code=500,
            detail={"reason": "misconfigured", "message": "MCSD_SENDER_URA en/of MCSD_SENDER_NAME is niet ingesteld."},
        )
    if not sender_uzi_sys or not sender_system_name:
        raise HTTPException(
            status_code=500,
            detail={"reason": "misconfigured", "message": "MCSD_SENDER_UZI_SYS en/of MCSD_SENDER_SYSTEM_NAME is niet ingesteld."},
        )

    client_receiver_ura = (payload.receiver_ura or "").strip()
    receiver_name = (payload.receiver_name or "").strip()
    receiver_org_ref = (payload.receiver_org_ref or "").strip() or None
    receiver_org_name = (payload.receiver_org_name or "").strip() or None
    receiver_target_ref = (payload.receiver_target_ref or "").strip()
    receiver_notification_endpoint_id = (payload.receiver_notification_endpoint_id or "").strip() or None
    patient_bsn = (payload.patient_bsn or "").strip()
    patient_name = (payload.patient_name or "").strip() if payload.patient_name else None
    description = (payload.description or "").strip() if payload.description else None

    if not receiver_name:
        raise HTTPException(status_code=400, detail="receiver_name is verplicht.")
    if not receiver_target_ref:
        raise HTTPException(status_code=400, detail="receiver_target_ref is verplicht.")
    if not patient_bsn:
        raise HTTPException(status_code=400, detail="patient_bsn is verplicht.")

    # Log request context for traceability
    logger.info(
        "bgz_notify: client_receiver_ura=%s receiver_name=%s receiver_org_name=%s target_ref=%s",
        client_receiver_ura or "(none)", receiver_name, receiver_org_name or "(none)", receiver_target_ref,
    )

    mapping, receiver_base_norm, resolved_notification_endpoint_id, receiver_target_ref_norm, receiver_org_ref_norm, target_type, resolved_receiver_ura = await _resolve_bgz_notify_destination(
        receiver_target_ref=receiver_target_ref,
        receiver_org_ref=receiver_org_ref,
        receiver_notification_endpoint_id=receiver_notification_endpoint_id,
    )

    if client_receiver_ura and client_receiver_ura != resolved_receiver_ura:
        logger.warning(
            "bgz_notify: Client URA differs from mCSD URA. client=%s, mcsd=%s, target=%s",
            client_receiver_ura, resolved_receiver_ura, receiver_target_ref_norm,
        )

    # Effective organisatie voor routing (bijv. HealthcareService.providedBy, Location.managingOrganization)
    effective_org_ref_norm, effective_org_name = _extract_effective_org_from_mapping(mapping)

    authorization_base = base64.b64encode(str(uuid.uuid4()).encode()).decode()

    task, sender_bgz_base_norm, workflow_task_id_norm = _build_bgz_notification_task(
        sender_ura=sender_ura,
        sender_name=sender_name,
        sender_uzi_sys=sender_uzi_sys,
        sender_system_name=sender_system_name,
        sender_bgz_base=sender_bgz_base,
        authorization_base=authorization_base,
        receiver_ura=resolved_receiver_ura,
        receiver_name=receiver_name,
        receiver_org_ref_norm=receiver_org_ref_norm,
        receiver_org_name=receiver_org_name,
        receiver_target_ref_norm=receiver_target_ref_norm,
        receiver_target_display=str(((mapping or {}).get("target") or {}).get("display") or "").strip() or None,
        receiver_target_identifiers=(
            ((mapping or {}).get("target") or {}).get("identifier")
            if isinstance(((mapping or {}).get("target") or {}).get("identifier"), list)
            else None
        ),
        receiver_effective_org_ref_norm=effective_org_ref_norm,
        receiver_effective_org_name=effective_org_name,
        patient_bsn=patient_bsn,
        patient_name=patient_name,
        description=description,
        workflow_task_id=payload.workflow_task_id,
    )

    if not sender_bgz_base_norm:
        raise HTTPException(
            status_code=500,
            detail={"reason": "misconfigured", "message": "MCSD_SENDER_BGZ_BASE is niet ingesteld; dit is nodig om de Workflow Task te hosten."},
        )
    task_group_identifier = (task.get("groupIdentifier") or {}).get("value") or ""
    workflow_task = _build_bgz_workflow_task(
        workflow_task_id=workflow_task_id_norm,
        group_identifier=task_group_identifier,
        sender_ura=sender_ura,
        sender_name=sender_name,
        sender_uzi_sys=sender_uzi_sys,
        sender_system_name=sender_system_name,
        receiver_ura=resolved_receiver_ura,
        patient_bsn=patient_bsn,
        patient_name=patient_name,
        description=description,
    )
    workflow_task_id_actual = await _upsert_workflow_task_to_sender_bgz(sender_bgz_base=sender_bgz_base_norm, workflow_task=workflow_task)
    if workflow_task_id_actual and workflow_task_id_actual != workflow_task_id_norm:
        workflow_task_id_norm = workflow_task_id_actual
        try:
            based_on = task.get("basedOn")
            if isinstance(based_on, list) and based_on:
                if isinstance(based_on[0], dict):
                    based_on[0]["reference"] = f"Task/{workflow_task_id_actual}"
            elif isinstance(based_on, dict):
                based_on["reference"] = f"Task/{workflow_task_id_actual}"
        except Exception:
            pass
    patient_hash = _audit_hash(patient_bsn)
    audit_event(
        "bgz.notify.attempt",
        sender_ura=sender_ura,
        receiver_ura=resolved_receiver_ura,
        receiver_target_ref=receiver_target_ref_norm,
        receiver_org_ref=receiver_org_ref_norm,
        resolved_receiver_base=receiver_base_norm,
        notification_endpoint_id=(str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
        task_group_identifier=task_group_identifier,
        patient=patient_hash,
    )

    # --- Send ---------------------------------------------------------------
    target_url = f"{receiver_base_norm}/Task"
    _dump_debug_json("bgz_notify_task", {"method": "POST", "url": target_url, "json": task})
    try:
        response = await app.state.http_client.post(
            target_url,
            json=task,
            headers={"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"},
        )
        response.raise_for_status()
        result = response.json()

        audit_event(
            "bgz.notify.result",
            success=True,
            http_status=response.status_code,
            receiver_target_ref=receiver_target_ref_norm,
            receiver_org_ref=receiver_org_ref_norm,
            resolved_receiver_base=receiver_base_norm,
            notification_endpoint_id=(str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
            task_group_identifier=task_group_identifier,
            task_id=result.get("id"),
            task_status=result.get("status"),
        )

        return {
            "success": True,
            "target": target_url,
            "task_id": result.get("id"),
            "task_status": result.get("status"),
            "group_identifier": (task.get("groupIdentifier") or {}).get("value"),
            "sender_bgz_base": sender_bgz_base_norm or None,
            "resolved_receiver_base": receiver_base_norm,
            "workflow_task_id": workflow_task_id_norm,
        }
    except httpx.HTTPStatusError as e:
        # Receiver gaf een HTTP fout terug (4xx/5xx). Log zoveel mogelijk context voor troubleshooting.
        http_status = 0
        try:
            http_status = int(getattr(getattr(e, "response", None), "status_code", 0) or 0)
        except Exception:
            http_status = 0

        reason = "receiver_http_error"
        base_message = f"De receiver server gaf een HTTP {http_status} fout terug."

        receiver_debug: Dict[str, Any] = {}
        receiver_headers_for_audit: Dict[str, str] = {}
        try:
            if getattr(e, "response", None) is not None:
                receiver_debug = _extract_receiver_error_debug(e.response, max_body_chars=2000)
                receiver_headers_for_audit = dict((receiver_debug.get("receiver_headers") or {}) or {})
        except Exception:
            receiver_debug = {}
            receiver_headers_for_audit = {}

        # Human friendly message for the UI (optionally enriched in non-production).
        message = base_message
        receiver_msg = (receiver_debug.get("receiver_message") or "").strip() if isinstance(receiver_debug.get("receiver_message"), str) else ""
        if receiver_msg and not settings.is_production:
            message = f"Receiver server fout (HTTP {http_status}): {receiver_msg}"

        # Extra console logging for root-cause analysis (without dumping full Task/BSN).
        logger.error(
            "bgz_notify: receiver_http_error rid=%s http=%s url=%s group=%s headers=%s body=%s",
            _fmt_rid(REQUEST_ID_CTX.get() or "-"),
            http_status,
            target_url,
            task_group_identifier or "-",
            receiver_debug.get("receiver_headers") or {},
            receiver_debug.get("receiver_body_snippet") or "(no body)",
        )

        # Optional debug dump to file (controlled via MCSD_DEBUG_DUMP_JSON).
        _dump_debug_json(
            "bgz_notify_receiver_error",
            {
                "url": target_url,
                "http_status": http_status,
                "headers": receiver_debug.get("receiver_headers"),
                "operation_outcome": receiver_debug.get("receiver_operation_outcome"),
                "body_snippet": receiver_debug.get("receiver_body_snippet"),
            },
        )

        # Keep audit log compact; do NOT include full bodies.
        audit_event(
            "bgz.notify.result",
            success=False,
            http_status=(http_status or None),
            reason=reason,
            message=base_message,
            receiver_target_ref=receiver_target_ref_norm,
            receiver_org_ref=receiver_org_ref_norm,
            resolved_receiver_base=receiver_base_norm,
            notification_endpoint_id=(str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
            task_group_identifier=task_group_identifier,
            receiver_request_id=receiver_headers_for_audit.get("X-Request-ID") or receiver_headers_for_audit.get("x-request-id"),
            receiver_traceparent=receiver_headers_for_audit.get("traceparent"),
        )

        # Return a user-friendly error response; include debug details to the frontend in non-production.
        details: Dict[str, Any] = {
            "receiver_http_status": http_status,
            "receiver_url": target_url,
            "receiver_content_type": receiver_debug.get("receiver_content_type"),
            "receiver_headers": receiver_debug.get("receiver_headers"),
            "receiver_operation_outcome": receiver_debug.get("receiver_operation_outcome"),
            "receiver_body_snippet": receiver_debug.get("receiver_body_snippet"),
        }

        raise HTTPException(
            status_code=502,
            detail={
                "reason": reason,
                "message": message,
                "details": details,
            },
        )
    except httpx.RequestError as e:
        info = _classify_upstream_exception(e)
        audit_event(
            "bgz.notify.result",
            success=False,
            reason=info.get("reason"),
            message=info.get("message"),
            receiver_target_ref=receiver_target_ref_norm,
            receiver_org_ref=receiver_org_ref_norm,
            resolved_receiver_base=receiver_base_norm,
            notification_endpoint_id=(str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
            task_group_identifier=task_group_identifier,
        )
        _raise_502_with_reason(e)
    except HTTPException:
        raise
    except Exception as e:
        info = _classify_upstream_exception(e)
        audit_event(
            "bgz.notify.result",
            success=False,
            reason=info.get("reason"),
            message=info.get("message"),
            receiver_target_ref=receiver_target_ref_norm,
            receiver_org_ref=receiver_org_ref_norm,
            resolved_receiver_base=receiver_base_norm,
            notification_endpoint_id=(str(resolved_notification_endpoint_id) if resolved_notification_endpoint_id else None),
            task_group_identifier=task_group_identifier,
        )
        _raise_502_with_reason(e)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower(),
    )
