from enum import Enum
import configparser
import re
from typing import Any

from pydantic import BaseModel, Field, ValidationError, computed_field, field_validator

_PATH = "app.conf"
_CONFIG = None


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
    automatic_background_update: bool = Field(default=True)
    automatic_background_cleanup: bool = Field(default=True)
    directory_stale_timeout: str = Field(default="5m")
    cleanup_client_directory_after_success_timeout: str = Field(default="30d")
    cleanup_client_directory_after_directory_delete: bool = Field(
        default=True,
    )
    ignore_directory_after_success_timeout: str = Field(default="10m")
    ignore_directory_after_failed_attempts_threshold: int = Field(default=20)

    @computed_field
    def delay_input_in_sec(self) -> int:
        return self._convert_to_sec(self.delay_input)

    @computed_field
    def directory_stale_timeout_in_sec(self) -> int:
        return self._convert_to_sec(self.directory_stale_timeout)

    @computed_field
    def cleanup_client_directory_after_success_timeout_in_sec(self) -> int:
        return self._convert_to_sec(self.cleanup_client_directory_after_success_timeout)

    @computed_field
    def ignore_directory_after_success_timeout_in_sec(self) -> int:
        return self._convert_to_sec(self.ignore_directory_after_success_timeout)

    def _convert_to_sec(self, value: str) -> int:
        conversion_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        match = re.match(r"^(\d+)([smhd])$", value)
        if not match:
            raise ValidationError(
                f"Incorrect input, must be digits with {conversion_map.keys()}"
            )

        number = int(match.group(1))
        unit = match.group(2)

        return number * conversion_map[unit]


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


class ConfigDirectoryApi(BaseModel):
    # either provider_url or urls_path should be set from config
    directories_provider_url: str | None = Field(default=None)
    directory_urls_path: str | None = Field(default=None)
    timeout: int = Field(default=1)
    backoff: float = Field(default=0.1)
    retries: int = Field(default=5)


class ConfigUvicorn(BaseModel):
    swagger_enabled: bool = Field(default=False)
    docs_url: str = Field(default="/docs")
    redoc_url: str = Field(default="/redoc")
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8509, gt=0, lt=65535)
    reload: bool = Field(default=True)
    reload_delay: float = Field(default=1)
    reload_dirs: list[str] = Field(default=["app"])
    use_ssl: bool = Field(default=False)
    ssl_base_dir: str | None
    ssl_cert_file: str | None
    ssl_key_file: str | None


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


class ConfigStats(BaseModel):
    enabled: bool = Field(default=False)
    host: str | None
    port: int | None
    module_name: str | None


class ConfigMcsd(BaseModel):
    update_client_url: str
    authentication: str = Field(
        default="off",
        description="Enable authentication, can be 'off', 'oauth2', or 'azure_oauth2'",
    )
    request_count: int = Field(default=20)
    strict_validation: bool = Field(default=False)

    @field_validator("authentication")
    def validate_authentication(cls, value: Any) -> str | bool:
        if value not in {"off", "oauth2", "azure_oauth2", "aws"}:
            raise ValueError(
                "authentication must be either 'off', 'oauth2', 'azure_oauth2' or 'aws'"
            )
        return str(value)


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
    directory_api: ConfigDirectoryApi
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
        path = _PATH

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
        raise e

    return _CONFIG
