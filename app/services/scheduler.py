from threading import Thread, Event
from typing import Callable, Any


class Scheduler:
    def __init__(self, function: Callable[..., Any], delay: int) -> None:
        self.__function = function
        self.__delay = delay  # maybe consider a cap on the delay
        self.__thread: Thread | None = None
        self.__stop_event = Event()

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
            self.__function()
            self.__stop_event.wait(self.__delay)
