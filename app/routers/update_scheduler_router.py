from typing import Any
from app.services.scheduler import Scheduler
from fastapi import APIRouter, Depends

from app.container import get_update_scheduler

router = APIRouter(prefix="/update_scheduler", tags=["Scheduled background updates"])


@router.post("/start", summary="Starts background scheduled updates")
def start_scheduled_update(service: Scheduler = Depends(get_update_scheduler)) -> None:
    return service.start()


@router.post("/stop", summary="Stops background scheduled updates")
def stop_scheduled_updates(service: Scheduler = Depends(get_update_scheduler)) -> None:
    return service.stop()


@router.get("/runner_logs")
def get_runner_history_logs(
    service: Scheduler = Depends(get_update_scheduler),
) -> list[dict[str, Any]]:
    return service.get_runner_history()
