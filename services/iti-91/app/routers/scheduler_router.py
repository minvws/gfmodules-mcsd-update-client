from typing import Any
from app.services.scheduler import Scheduler
from fastapi import APIRouter, Depends

from app.container import get_cleanup_scheduler, get_update_scheduler

router = APIRouter(prefix="/scheduler", tags=["Scheduled background updates and cleanups"])


@router.post("/update/start", summary="Starts background scheduled updates")
def start_scheduled_update(service: Scheduler = Depends(get_update_scheduler)) -> None:
    return service.start()


@router.post("/update/stop", summary="Stops background scheduled updates")
def stop_scheduled_updates(service: Scheduler = Depends(get_update_scheduler)) -> None:
    return service.stop()


@router.get("/update/runner_logs")
def get_update_runner_history_logs(
    service: Scheduler = Depends(get_update_scheduler),
) -> list[dict[str, Any]]:
    return service.get_runner_history()

@router.post("/cleanup/start", summary="Starts background scheduled cleanups")
def start_scheduled_cleanup(service: Scheduler = Depends(get_cleanup_scheduler)) -> None:
    return service.start()


@router.post("/cleanup/stop", summary="Stops background scheduled cleanups")
def stop_scheduled_cleanups(service: Scheduler = Depends(get_cleanup_scheduler)) -> None:
    return service.stop()


@router.get("/cleanup/runner_logs")
def get_cleanup_runner_history_logs(
    service: Scheduler = Depends(get_cleanup_scheduler),
) -> list[dict[str, Any]]:
    return service.get_runner_history()
