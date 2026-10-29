import logging
import logging.config
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import yaml

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import internal, process
from app.api.internal import INTERNAL_TAG
from app.api.process import PROCESS_TAG
from app.core.authorization import initialize_authorizer
from app.core.database import check_database_connection, close_database, initialize_database
from app.core.exceptions import register_exception_handlers
from app.openapi import get_published_doc_path, setup_openapi
from app.otel import initialize_instrumentation, shutdown_otel
from app.settings import get_settings
from app.version import __version__

logger = logging.getLogger(__name__)

settings = get_settings()


def get_logging_cfg(config_file: Path) -> dict:  # pragma: no cover
    """Load and parse logging configuration from the given file"""
    config = yaml.safe_load(config_file.read_text())

    logger.info("Loaded logging configuration from file %s", config_file)
    return config


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    await initialize_database(app, settings)
    initialize_authorizer(app, settings)

    try:
        # Fail fast on startup if the database is not reachable
        await check_database_connection(app.state.database_engine)
        logger.info("Startup tasks completed")
        yield
    finally:
        await close_database(app)
        shutdown_otel()
        logger.info("Shutdown tasks completed")


# First configure logging for local server if needed
if settings.logging_enable_dev_server_logging:  # pragma: no cover
    if settings.logging_config_file:
        log_config = get_logging_cfg(settings.logging_config_file)
        logging.config.dictConfig(log_config)
    else:
        logging.basicConfig(level=logging.INFO)

if settings.logging_handlers_level is not None:  # pragma: no cover
    for handler in logging.getLogger().handlers:
        handler.setLevel(settings.logging_handlers_level)


app = FastAPI(
    title="Service Processes",
    summary="Start and view data pipelines",
    description="""This services allows users to manage and monitor data pipelines.""",
    version=__version__,
    contact={"name": "swissgeo", "url": "https://www.swissgeo.ch/infos"},
    license_info={
        "name": "BSD 3-Clause License",
        "identifier": "BSD-3-Clause",
    },
    openapi_url=get_published_doc_path("/openapi.json"),
    docs_url=get_published_doc_path("/docs"),
    swagger_ui_oauth2_redirect_url=get_published_doc_path("/docs/oauth2-redirect"),
    redoc_url=get_published_doc_path("/redoc"),
    openapi_tags=[
        {"name": INTERNAL_TAG, "description": "Internal APIs not for external users"},
        {"name": PROCESS_TAG, "description": "Process operations"},
    ],
    lifespan=lifespan,
    root_path=settings.root_path,
)
if settings.publish_openapi_spec:  # pragma: no cover
    setup_openapi(app)

# Register exceptions handlers
register_exception_handlers(app)

# Add middlewares
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_origin_regex=settings.cors_origin_regex,
    allow_methods=settings.cors_method,
    allow_headers=settings.cors_headers,
    max_age=settings.cors_max_age,
)


# Register routes
app.include_router(internal.router)
app.include_router(process.router)

# Setup OTEL instrumentation
initialize_instrumentation(app)
