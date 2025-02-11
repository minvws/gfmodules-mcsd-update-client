import time
from datetime import datetime
from threading import Thread, Event
from typing import Callable, Any


class Scheduler:
    def __init__(
        self, function: Callable[..., Any], delay: int, max_logs_entries: int
    ) -> None:
        self.__function = function
        self.__delay = delay
        self.__max_logs_entries = max_logs_entries
        self.__thread: Thread | None = None
        self.__stop_event = Event()
        self.__runner_id = 1
        self.__runner_logs: list[dict[str, Any]] = []

    def start(self) -> None:
        if self.__thread is not None:
            return

        if self.__stop_event.is_set() is True:
            self.__stop_event.clear()

        self.__thread = Thread(target=self.__run)
        self.__thread.start()

    def stop(self) -> None:
        if self.__thread is not None:
            self.__stop_event.set()
            self.__thread.join()
            self.__thread = None

    def __run(self) -> None:
        while self.__stop_event.is_set() is False:
            start_time = time.time()
            self.__function()
            self.__stop_event.wait(self.__delay)
            end_time = time.time()
            self.update_runner(start_time, end_time)

    def update_runner(self, start_time: float, end_time: float) -> None:

        data = {
            "runner_id": self.__runner_id,
            "started_at": datetime.fromtimestamp(start_time).isoformat(),
            "finished_at": datetime.fromtimestamp(end_time).isoformat(),
            "time_delta": end_time - start_time,
        }
        if self.__thread is not None:
            data.update({"thread_name": self.__thread.name})
            data.update({"thread_id": self.__thread.ident})

        self.__runner_logs.append(data)
        self.__runner_id += 1

        if len(self.__runner_logs) > self.__max_logs_entries:
            self.__runner_logs.pop(0)

    def get_runner_history(self) -> list[dict[str, Any]]:
        return self.__runner_logs
