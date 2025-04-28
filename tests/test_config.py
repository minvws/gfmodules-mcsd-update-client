from app.config import (
    Config,
    ConfigApp,
    ConfigDatabase,
    ConfigUvicorn,
    ConfigMcsd,
    ConfigMockSeeder,
    ConfigTelemetry,
    ConfigStats,
    ConfigSupplierApi,
    Scheduler,
    LogLevel,
)


def get_test_config() -> Config:
    return Config(
        app=ConfigApp(
            loglevel=LogLevel.error,
            override_authentication_ura=None,
        ),
        database=ConfigDatabase(
            dsn="sqlite:///:memory:",
            create_tables=True,
            retry_backoff=[0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 4.8, 6.4, 10.0],
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=False,
            pool_recycle=1,
        ),
        uvicorn=ConfigUvicorn(
            swagger_enabled=False,
            docs_url="/docs",
            redoc_url="/redoc",
            host="0.0.0.0",
            port=8503,
            reload=True,
            use_ssl=False,
            ssl_base_dir=None,
            ssl_cert_file=None,
            ssl_key_file=None,
        ),
        mcsd=ConfigMcsd(
            consumer_url="http://testserver/consumer/test",
            authentication="off",
            request_count=20,
            strict_validation=False,
        ),
        mock_seeder=ConfigMockSeeder(
            mock_supplier_url=None,
        ),
        telemetry=ConfigTelemetry(
            enabled=False,
            endpoint=None,
            service_name=None,
            tracer_name=None,
        ),
        stats=ConfigStats(
            enabled=True, host=None, port=None, module_name="mcsd", keep_in_memory=True
        ),
        azure_oauth2=None,
        aws=None,
        supplier_api=ConfigSupplierApi(
            base_url="http://testserver/supplier",
            timeout=1,
            backoff=0.1,
        ),
        scheduler=Scheduler(
            automatic_background_update=False,
            delay_input="0s",
            max_logs_entries=10,
        ),
    )
