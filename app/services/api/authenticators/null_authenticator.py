from typing import Any
from app.services.api.authenticators.authenticator import Authenticator


class NullAuthenticator(Authenticator):
    def get_authentication_header(self) -> str:
        return ""

    def get_auth(self) -> Any:
        return None
