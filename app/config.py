from enum import Enum
import configparser
from typing import Any, Union

from pydantic import BaseModel, ValidationError, field_validator
from pydantic import Field

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
    consumer_url: str = Field(default="http://addressing-app:8502")
    authentication: Union[str] = Field(
        default="off",
        description="Enable authentication, can be 'off', 'oauth2', or 'azure_oauth2'"
    )

    @field_validator("authentication")
    def validate_authentication(cls, value: Any) -> str|bool:
        if value not in {"off", "oauth2", "azure_oauth2"}:
            raise ValueError("authentication must be either 'off', 'oauth2', or 'azure_oauth2'")
        return str(value)


class ConfigMockSeeder(BaseModel):
    mock_supplier_url: str | None


class ConfigAzureOauth2(BaseModel):
    token_url: str
    client_id: str
    client_secret: str
    resource: str

class Config(BaseModel):
    app: ConfigApp
    database: ConfigDatabase
    uvicorn: ConfigUvicorn
    mcsd: ConfigMcsd
    mock_seeder: ConfigMockSeeder
    telemetry: ConfigTelemetry
    stats: ConfigStats
    azure_oauth2: ConfigAzureOauth2 | None


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

        _CONFIG = Config(**ini_data)
    except ValidationError as e:
        raise e

    return _CONFIG
