from enum import Enum
import configparser
import re
from os import environ
from os.path import exists
from typing import Any, Optional
import logging

from pydantic import BaseModel, Field, ValidationError, computed_field, field_validator

logger = logging.getLogger(__name__)

_PATH = "app{suffix}.conf"
_CONFIG = None

def _convert_conf_to_sec(value: str) -> int:
    conversion_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    match = re.match(r"^(\d+)([smhd])$", value)
    if not match:
        raise ValidationError(
            f"Incorrect input, must be digits with {conversion_map.keys()}"
        )

    number = int(match.group(1))
    unit = match.group(2)

    return number * conversion_map[unit]

class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"
    critical = "critical"


class ConfigApp(BaseModel):
    loglevel: LogLevel = Field(default=LogLevel.info)
    override_authentication_ura: str | None


class Scheduler(BaseModel):
    delay_input: str = Field(default="30s")
    max_logs_entries: int = Field(default=1000, ge=0)
    # Whether the scheduler should automatically run background tasks
    automatic_background_update: bool = Field(default=True)
    automatic_background_cleanup: bool = Field(default=True)
    # Parallelism for updating multiple directories (1 = sequential)
    max_concurrent_directory_updates: int = Field(default=1, ge=1, le=32)

    @computed_field
    def delay_input_in_sec(self) -> int:
        return _convert_conf_to_sec(self.delay_input)

    @field_validator("max_logs_entries", mode="before")
    def validate_max_log_entries(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 1000
        return int(v)

    @field_validator(
        "automatic_background_update", "automatic_background_cleanup", mode="before"
    )
    def validate_automatic_background_action(cls, v: Any) -> Any:
        if v in (None, "", " "):
            return True
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)

class ConfigDatabase(BaseModel):
    dsn: str
    create_tables: bool = Field(default=False)
    retry_backoff: list[float] = Field(
        default=[0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 4.8, 6.4, 10.0]
    )
    pool_size: int = Field(default=5, ge=0, lt=100)
    max_overflow: int = Field(default=10, ge=0, lt=100)
    pool_pre_ping: bool = Field(default=False)
    pool_recycle: int = Field(default=3600, ge=0)

    @field_validator("create_tables", mode="before")
    def validate_create_tables(cls, v: Any) -> bool:
        if v in (None, "", " "):
            return False
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)

    @field_validator("pool_size", mode="before")
    def validate_pool_size(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 5
        return int(v)

    @field_validator("max_overflow", mode="before")
    def validate_max_overflow(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 10
        return int(v)

    @field_validator("pool_recycle", mode="before")
    def validate_pool_recycle(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 3600
        return int(v)


class ConfigClientDirectory(BaseModel):
    # either provider_url or urls_path should be set from config
    directories_provider_url: str | None = Field(default=None)
    directories_file_path: str | None = Field(default=None)
    timeout: int = Field(default=1)
    backoff: float = Field(default=0.1)
    retries: int = Field(default=5)
    directory_marked_as_unhealthy_after_success_timeout: str = Field(
        default="5m",
        description="Time since last successful update before marking directory as unhealthy",
    )

    ignore_client_directory_after_success_timeout: str = Field(
        default="30m",
        description="Time since last successful update before ignoring directory",
    )

    ignore_client_directory_after_failed_attempts_threshold: int = Field(
        default=20,
        description="Consecutive failed attempts before ignoring directory",
    )

    mark_client_directory_as_deleted_after_success_timeout: str = Field(
        default="10d",
        description="Time since last successful update before marking directory as to be deleted",
    )

    mark_client_directory_as_deleted_after_lrza_delete: bool = Field(
        default=True,
        description="Mark directory as to be deleted when removed from LRZA",
    )

    cleanup_delay_after_client_directory_marked_deleted: str = Field(
        default="30d",
        description="Delay after directory marked as to be deleted before permanent cleanup",
    )

    @field_validator("timeout", mode="before")
    def validate_timeout(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 10
        return int(v)

    @field_validator("backoff", mode="before")
    def validate_backoff(cls, v: Any) -> float:
        if v in (None, "", " "):
            return 0.5
        return float(v)

    @field_validator("ignore_client_directory_after_failed_attempts_threshold", mode="before")
    def validate_ignore_client_directory_after_failed_attempts_threshold(cls, v: Any) -> Any:
        if v in (None, "", " "):
            return 20
        return int(v)

    @computed_field
    def directory_marked_as_unhealthy_after_success_timeout_in_sec(self) -> int:
        return _convert_conf_to_sec(self.directory_marked_as_unhealthy_after_success_timeout)

    @computed_field
    def mark_client_directory_as_deleted_after_success_timeout_in_sec(self) -> int:
        return _convert_conf_to_sec(self.mark_client_directory_as_deleted_after_success_timeout)

    @computed_field
    def ignore_client_directory_after_success_timeout_in_sec(self) -> int:
        return _convert_conf_to_sec(self.ignore_client_directory_after_success_timeout)

    @computed_field
    def cleanup_delay_after_client_directory_marked_deleted_in_sec(self) -> int:
        return _convert_conf_to_sec(self.cleanup_delay_after_client_directory_marked_deleted)


class ConfigUvicorn(BaseModel):
    swagger_enabled: bool = Field(default=False)
    docs_url: str = Field(default="/docs")
    redoc_url: str = Field(default="/redoc")
    host: str = Field(default="127.0.0.1")
    port: Optional[int] = Field(default=8000, gt=0, lt=65535)
    reload: bool = Field(default=True)
    reload_delay: float = Field(default=1)
    reload_dirs: list[str] = Field(default=["app"])
    use_ssl: bool = Field(default=False)
    ssl_base_dir: str | None
    ssl_cert_file: str | None
    ssl_key_file: str | None

    @field_validator("host", mode="before")
    def validate_host(cls, v: Any) -> str:
        if v in (None, "", " "):
            return "127.0.0.1"
        return str(v)

    @field_validator("port", mode="before")
    def validate_port(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 8000
        return int(v)

    @field_validator("reload", mode="before")
    def validate_reload(cls, v: Any) -> bool:
        if v in (None, "", " "):
            return True
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)

    @field_validator("reload_delay", mode="before")
    def validate_reload_delay(cls, v: Any) -> float:
        if v in (None, "", " "):
            return 1.0
        return float(v)

    @field_validator("reload_dirs", mode="before")
    def validate_reload_dirs(cls, v: Any) -> list[str]:
        if v in (None, "", " "):
            return ["app"]
        if isinstance(v, str):
            return [d.strip() for d in v.split(",")]
        return v  # type: ignore

    @field_validator("use_ssl", mode="before")
    def validate_use_ssl(cls, v: Any) -> bool:
        if v in (None, "", " "):
            return False
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)


class ConfigExternalCache(BaseModel):
    host: str | None = Field(default=None)
    port: int | None = Field(default=None)
    enable_debugger: bool = Field(default=False)
    key: str | None = Field(default=None)
    ssl: bool = Field(default=False)
    cert: str | None = Field(default=None)
    cafile: str | None = Field(default=None)
    check_hostname: bool = Field(default=True)
    object_ttl: int = Field(default=600)
    default_cache_namespace: str = Field(default="mcsd")


class ConfigTelemetry(BaseModel):
    enabled: bool = Field(default=False)
    endpoint: str | None
    service_name: str | None
    tracer_name: str | None

    @field_validator("enabled", mode="before")
    def validate_enabled(cls, v: Any) -> bool:
        if v in (None, "", " "):
            return False
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)


class ConfigStats(BaseModel):
    enabled: bool = Field(default=False)
    host: str | None
    port: int | None
    module_name: str | None

    @field_validator("enabled", mode="before")
    def validate_enabled(cls, v: Any) -> bool:
        if v in (None, "", " "):
            return False
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)

    @field_validator("port", mode="before")
    def validate_port(cls, v: Any) -> int | None:
        if v in (None, "", " "):
            return None
        return int(v)


class ConfigMcsd(BaseModel):
    update_client_url: str
    authentication: str = Field(
        default="off",
        description="Enable authentication, can be 'off', 'oauth2', or 'azure_oauth2'",
    )
    request_count: int = Field(default=20)
    fill_required_fields: bool = Field(default=False)
    mtls_client_cert_path: str | None = Field(default=None)
    mtls_client_key_path: str | None = Field(default=None)
    verify_ca: str | bool = Field(default=True)
    check_capability_statement: bool = Field(default=False, description="Whether to check the CapabilityStatement of client directories")

    @field_validator("request_count", mode="before")
    def validate_request_count(cls, v: Any) -> int:
        if v in (None, "", " "):
            return 20
        return int(v)

    @field_validator("authentication")
    def validate_authentication(cls, value: Any) -> str | bool:
        if value not in {"off", "oauth2", "azure_oauth2", "aws"}:
            raise ValueError(
                "authentication must be either 'off', 'oauth2', 'azure_oauth2' or 'aws'"
            )
        return str(value)

    @field_validator("fill_required_fields", mode="before")
    def validate_fill_required_fields(cls, v: Any) -> bool:
        if v in (None, "", " "):
            return False
        if isinstance(v, str):
            return v.lower() in ("yes", "true", "t", "1")
        return bool(v)


class ConfigAws(BaseModel):
    profile: str
    region: str


class ConfigAzureOauth2(BaseModel):
    token_url: str
    client_id: str
    client_secret: str
    resource: str


class Config(BaseModel):
    app: ConfigApp
    database: ConfigDatabase
    uvicorn: ConfigUvicorn
    external_cache: ConfigExternalCache
    mcsd: ConfigMcsd
    telemetry: ConfigTelemetry
    stats: ConfigStats
    azure_oauth2: ConfigAzureOauth2 | None
    aws: ConfigAws | None
    client_directory: ConfigClientDirectory
    scheduler: Scheduler


def read_ini_file(path: str) -> Any:
    ini_data = configparser.ConfigParser()
    ini_data.read(path)

    ret = {}
    for section in ini_data.sections():
        ret[section] = dict(ini_data[section])

    return ret


def reset_config() -> None:
    global _CONFIG
    _CONFIG = None


def set_config(config: Config) -> None:
    global _CONFIG
    _CONFIG = config


def get_config(path: str | None = None) -> Config:
    global _CONFIG
    global _PATH

    if _CONFIG is not None:
        return _CONFIG

    if path is None:
        suffix = environ.get("APP_ENV", "")
        if suffix:
            suffix = f".{suffix}"
        path = _PATH.replace("{suffix}", suffix)
        logger.info(f"Reading configuration using file: {path}")

    # check if path exists
    if not exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    # To be inline with other python code, we use INI-type files for configuration. Since this isn't
    # a standard format for pydantic, we need to do some manual parsing first.
    ini_data = read_ini_file(path)

    try:
        # Convert database.retry_backoff to a list of floats
        if "retry_backoff" in ini_data["database"] and isinstance(
            ini_data["database"]["retry_backoff"], str
        ):
            # convert the string to a list of floats
            ini_data["database"]["retry_backoff"] = [
                float(i) for i in ini_data["database"]["retry_backoff"].split(",")
            ]

            if "azure_oauth2" not in ini_data:
                ini_data["azure_oauth2"] = None
            if "aws" not in ini_data:
                ini_data["aws"] = None

        _CONFIG = Config(**ini_data)
    except ValidationError as e:
        logger.error(f"Configuration validation error: {e}")
        raise e

    return _CONFIG
