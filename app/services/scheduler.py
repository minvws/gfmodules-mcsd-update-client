import logging
import time
from datetime import datetime
from threading import Event, Thread
from typing import Any, Callable


logger = logging.getLogger("Scheduler")


class Scheduler:
    """Run a function at fixed intervals in a background thread."""

    def __init__(self, function: Callable[..., Any], delay: int, max_logs_entries: int) -> None:
        self.__function = function
        self.__delay = delay
        self.__max_logs_entries = max_logs_entries
        self.__thread: Thread | None = None
        self.__stop_event = Event()
        self.__runner_id = 1
        self.__runner_logs: list[dict[str, Any]] = []

    def start(self) -> None:
        """Start the scheduler in a separate daemon thread."""
        if self.__thread is not None:
            return

        if self.__stop_event.is_set():
            self.__stop_event.clear()

        self.__thread = Thread(target=self.__run, daemon=True, name="scheduler")
        self.__thread.start()

    def stop(self) -> None:
        """Stop the scheduler and wait for the thread to finish."""
        if self.__thread is not None:
            self.__stop_event.set()
            self.__thread.join()
            self.__thread = None

    def __run(self) -> None:
        """The main loop that runs the scheduled function at the specified interval."""
        while not self.__stop_event.is_set():
            start_time = time.time()
            success = True
            error: str | None = None

            try:
                self.__function()
            except Exception as e:
                success = False
                error = str(e)
                logger.exception("Got an error while scheduling task")
            finally:
                end_time = time.time()
                self.update_runner(start_time, end_time, success=success, error=error)

            # Wait for delay, but exit early if stop was requested.
            self.__stop_event.wait(self.__delay)

    def update_runner(self, start_time: float, end_time: float, *, success: bool, error: str | None) -> None:
        """Update the runner logs with the latest execution details."""
        data: dict[str, Any] = {
            "runner_id": self.__runner_id,
            "started_at": datetime.fromtimestamp(start_time).isoformat(),
            "finished_at": datetime.fromtimestamp(end_time).isoformat(),
            "time_delta": end_time - start_time,
            "success": success,
        }
        if error is not None:
            data["error"] = error

        if self.__thread is not None:
            data["thread_name"] = self.__thread.name
            data["thread_id"] = self.__thread.ident

        self.__runner_logs.append(data)
        self.__runner_id += 1

        if len(self.__runner_logs) > self.__max_logs_entries:
            self.__runner_logs.pop(0)

    def get_runner_history(self) -> list[dict[str, Any]]:
        return self.__runner_logs
