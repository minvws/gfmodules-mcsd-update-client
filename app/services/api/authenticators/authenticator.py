from abc import ABC, abstractmethod
from typing import Any


class Authenticator(ABC):
    @abstractmethod
    def get_authentication_header(self) -> str: ...

    @abstractmethod
    def get_auth(self) -> Any: ...
