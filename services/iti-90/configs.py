# configs.py
import json
import logging
import os
from pathlib import Path
from typing import Any, List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("mcsd.app")


class Settings(BaseSettings):
    base_url: str = Field("https://example-fhir/mcsd", validation_alias="MCSD_BASE")
    upstream_timeout: float = Field(30.0, validation_alias="MCSD_UPSTREAM_TIMEOUT")
    bearer_token: Optional[str] = Field(None, validation_alias="MCSD_BEARER_TOKEN")
    verify_tls: bool = Field(True, validation_alias="MCSD_VERIFY_TLS")
    ca_certs_file: Optional[str] = Field(None, validation_alias="MCSD_CA_CERTS_FILE")
    allow_origins: Optional[str] = Field("*", validation_alias="MCSD_ALLOW_ORIGINS")
    allowed_hosts: Optional[str] = Field("*", validation_alias="MCSD_ALLOWED_HOSTS")
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
        case_sensitive=False,
    )


def _is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _load_dotenv_once() -> None:
    # Load .env early so its values (like MCSD_DEBUG_DUMP_DIR) are applied consistently.
    try:
        from dotenv import dotenv_values

        dotenv_path = (Path(__file__).parent / ".env").resolve()
        if not dotenv_path.exists():
            logger.info("[mCSD] .env not found file=%s", str(dotenv_path))
            return

        file_values = dotenv_values(str(dotenv_path))
        is_prod = _is_truthy(os.getenv("MCSD_IS_PRODUCTION") or file_values.get("MCSD_IS_PRODUCTION"))
        override = not is_prod
        for key, value in file_values.items():
            if value is None:
                continue
            if override or key not in os.environ:
                os.environ[key] = value
        logger.info("[mCSD] .env loaded file=%s override=%s", str(dotenv_path), "on" if override else "off")
    except Exception:
        logger.exception("[mCSD] .env load failed")


def _parse_csv_list(value: Any, *, default: List[str]) -> List[str]:
    if value is None:
        return list(default)
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    raw = str(value).strip()
    if not raw:
        return list(default)
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    return [v.strip() for v in raw.split(",") if v.strip()]


_load_dotenv_once()
settings = Settings()
ALLOW_ORIGINS = _parse_csv_list(settings.allow_origins, default=["*"])
ALLOWED_HOSTS = _parse_csv_list(settings.allowed_hosts, default=["*"])


__all__ = ["settings", "ALLOW_ORIGINS", "ALLOWED_HOSTS"]
