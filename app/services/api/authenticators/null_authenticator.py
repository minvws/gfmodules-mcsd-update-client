from typing import Any
from app.services.api.authenticators.authenticator import Authenticator


class NullAuthenticator(Authenticator):
    """
    Null Authenticator that performs no authentication. This is the default when authentication is turned off.
    """
    def get_authentication_header(self) -> str:
        return ""

    def get_auth(self) -> Any:
        return None
