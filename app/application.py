import logging

from typing import Any

from fastapi import FastAPI
import uvicorn

from app.container import get_cleanup_scheduler, get_update_scheduler, setup_container
from app.routers.default import router as default_router
from app.routers.health import router as health_router
from app.routers.supplier_router import router as supplier_router
from app.routers.resource_map_router import router as resource_map_router
from app.routers.update_router import router as update_router
from app.routers.consumer import router as consumer_router
from app.routers.update_scheduler_router import router as update_scheduler_router
from app.routers.directory_health import router as directory_health_router
from app.routers.cleanup_scheduler_router import router as cleanup_scheduler_router
from app.routers.ignore_list_router import router as ignore_list_router
from app.config import get_config
from app.stats import setup_stats
from app.telemetry import setup_telemetry


def get_uvicorn_params() -> dict[str, Any]:
    config = get_config()
    kwargs = {
        "host": config.uvicorn.host,
        "port": config.uvicorn.port,
        "reload": config.uvicorn.reload,
        "reload_delay": config.uvicorn.reload_delay,
        "reload_dirs": config.uvicorn.reload_dirs,
    }
    if (
        config.uvicorn.use_ssl
        and config.uvicorn.ssl_base_dir is not None
        and config.uvicorn.ssl_cert_file is not None
        and config.uvicorn.ssl_key_file is not None
    ):
        kwargs["ssl_keyfile"] = (
            config.uvicorn.ssl_base_dir + "/" + config.uvicorn.ssl_key_file
        )
        kwargs["ssl_certfile"] = (
            config.uvicorn.ssl_base_dir + "/" + config.uvicorn.ssl_cert_file
        )

    return kwargs


def run() -> None:
    uvicorn.run("app.application:create_fastapi_app", **get_uvicorn_params())


def create_fastapi_app() -> FastAPI:
    application_init()
    fastapi = setup_fastapi()

    if get_config().stats.enabled:
        setup_stats()

    if get_config().telemetry.enabled:
        setup_telemetry(fastapi)

    return fastapi


def application_init() -> None:
    config = get_config()
    setup_logging()
    setup_container()
    if config.scheduler.automatic_background_update:
        update_scheduler = get_update_scheduler()
        update_scheduler.start()
    if config.scheduler.automatic_background_cleanup:
        cleanup_scheduler = get_cleanup_scheduler()
        cleanup_scheduler.start()


def setup_logging() -> None:
    loglevel = logging.getLevelName(get_config().app.loglevel.upper())

    if isinstance(loglevel, str):
        raise ValueError(f"Invalid loglevel {loglevel.upper()}")
    logging.basicConfig(
        level=loglevel,
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )


def setup_fastapi() -> FastAPI:
    config = get_config()

    fastapi = (
        FastAPI(docs_url=config.uvicorn.docs_url, redoc_url=config.uvicorn.redoc_url)
        if config.uvicorn.swagger_enabled
        else FastAPI(docs_url=None, redoc_url=None)
    )

    routers = [
        default_router,
        health_router,
        supplier_router,
        resource_map_router,
        update_router,
        consumer_router,
        update_scheduler_router,
        directory_health_router,
        cleanup_scheduler_router,
        ignore_list_router,
    ]
    for router in routers:
        fastapi.include_router(router)

    return fastapi
