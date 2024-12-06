import logging
from typing import Any

from fastapi import APIRouter, Depends

from app.container import get_database
from app.db.db import Database

logger = logging.getLogger(__name__)
router = APIRouter()


def ok_or_error(value: bool) -> str:
    return "ok" if value else "error"


@router.get("/health")
def health(db: Database = Depends(get_database)) -> dict[str, Any]:
    logger.info("Checking database health")

    components = {
        "database": ok_or_error(db.is_healthy()),
    }
    healthy = ok_or_error(all(value == "ok" for value in components.values()))

    return {"status": healthy, "components": components}
