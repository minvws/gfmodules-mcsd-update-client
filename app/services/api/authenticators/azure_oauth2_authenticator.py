import time
import requests
from typing import Any
from requests.exceptions import RequestException
from app.services.api.authenticators.authenticator import Authenticator


class AzureOAuth2Authenticator(Authenticator):
    """
    Authenticator for Azure services using OAuth2 client credentials flow.
    """
    def __init__(
        self, token_url: str, client_id: str, client_secret: str, resource: str
    ) -> None:
        self.__token_url = token_url
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__resource = resource
        self.token = None
        self.expiry = 0.0

    def get_auth(self) -> Any:
        """
        OAuth2 does not use a simple auth object.
        """
        return None

    def get_authentication_header(self) -> str:
        """
        Returns a Bearer token for the Authorization header.
        """
        if self.token is None or time.time() >= self.expiry:
            token_data = self.__get_token()
            self.token = str(token_data["access_token"])  # type: ignore
            self.expiry = time.time() + float(token_data["expires_in"]) - 60.0
        return f"Bearer {self.token}"

    def __get_token(self) -> dict[str, Any]:
        try:
            response = requests.post(
                self.__token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.__client_id,
                    "client_secret": self.__client_secret,
                    "scope": self.__resource + "/.default",
                },
            )
        except RequestException as e:
            raise ConnectionError(f"Failed to connect to token endpoint: {e}")

        if response.status_code >= 400:
            try:
                error_details = response.json()
                raise ValueError(f"Authentication failed with status {response.status_code}: {error_details}")
            except requests.JSONDecodeError:
                raise ValueError(f"Authentication failed with status {response.status_code}: {response.text}")

        try:
            return response.json()  # type: ignore
        except requests.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response from token endpoint: {e}")
