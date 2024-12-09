import logging

from typing import Any

from fastapi import FastAPI
import uvicorn

from app.container import setup_container
from app.routers.default import router as default_router
from app.routers.health import router as health_router
from app.routers.supplier_router import router as supplier_router
from app.routers.resource_map_router import router as resource_map_router
from app.routers.update_router import router as update_router
from app.config import get_config


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

    return fastapi


def application_init() -> None:
    setup_container()
    setup_logging()


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
    ]
    for router in routers:
        fastapi.include_router(router)

    return fastapi
