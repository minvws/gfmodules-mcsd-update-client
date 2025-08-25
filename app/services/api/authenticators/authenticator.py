from abc import ABC, abstractmethod
from typing import Any


class Authenticator(ABC):
    """
    Abstract base class for authentication providers.

    This clas defines the common interface for authentication strategies
    that can be used to sign requests or provide credentials. Concrete
    implementations should return the appropriate authentication data
    depending on the target system.
    """

    @abstractmethod
    def get_authentication_header(self) -> str:
        """
        Returns an authentication header value as a string.

        This is typically used for HTTP `Authorization` headers or other
        header-based authentication mechanisms.

        Returns:
            str: The formatted header string, e.g., ``"Bearer <token>"``.
        """
        ...

    @abstractmethod
    def get_auth(self) -> Any:
        """
        Return authentication data in a raw or library-specific format.

        This allows compatibility with libraries that expect authentication
        objects or dictionaries (e.g., ``requests``'s ``auth`` parameter,
        or API SDKs requiring credential objects).

        Returns:
            Any: Authentication object, dictionary, or other structure as
            required by the consumer of this authenticator.
        """
        ...
