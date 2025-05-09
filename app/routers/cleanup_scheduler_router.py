from typing import Any
from app.services.scheduler import Scheduler
from fastapi import APIRouter, Depends

from app.container import get_cleanup_scheduler

router = APIRouter(prefix="/cleanup_scheduler", tags=["Scheduled background cleanup"])


@router.post("/start", summary="Starts background scheduled cleanups")
def start_scheduled_update(service: Scheduler = Depends(get_cleanup_scheduler)) -> None:
    return service.start()


@router.post("/stop", summary="Stops background scheduled cleanups")
def stop_scheduled_updates(service: Scheduler = Depends(get_cleanup_scheduler)) -> None:
    return service.stop()


@router.get("/runner_logs")
def get_runner_history_logs(
    service: Scheduler = Depends(get_cleanup_scheduler),
) -> list[dict[str, Any]]:
    return service.get_runner_history()
