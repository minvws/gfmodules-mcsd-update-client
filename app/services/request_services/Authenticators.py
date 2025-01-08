import time
from typing import Any

from requests_aws4auth import AWS4Auth
import requests
import boto3

class Authenticator:
    def get_authentication_header(self) -> str: # type: ignore
        pass
    def get_auth(self) -> Any:
        pass


class NullAuthenticator(Authenticator):
    def get_authentication_header(self) -> str:
        return ""

    def get_auth(self) -> Any:
        return None


class AwsV4Authenticator(Authenticator):
    def __init__(self, profile: str, region: str) -> None:
        self.__profile = profile
        self.__region = region

    def get_authentication_header(self) -> str:
        return ""

    def get_auth(self) -> Any:
        session = boto3.Session(profile_name = self.__profile, region_name=self.__region)
        credentials = session.get_credentials()
        auth = AWS4Auth(refreshable_credentials=credentials, service="healthlake", region=self.__region)
        return auth

class AzureOAuth2Authenticator(Authenticator):
    def __init__(self, token_url: str, client_id: str, client_secret: str, resource: str) -> None:
        self.__token_url = token_url
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__resource = resource
        self.token = None
        self.expiry = 0.0

    def get_auth(self) -> Any:
        return None

    def get_authentication_header(self) -> str:
        if self.token is None or time.time() >= self.expiry:
            token_data = self.__get_token()
            self.token = str(token_data["access_token"])     # type: ignore
            self.expiry = time.time() + float(token_data["expires_in"]) - 60.0
        return f"Bearer {self.token}"

    def __get_token(self) -> dict[str, Any]:
        response = requests.post(
            self.__token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.__client_id,
                "client_secret": self.__client_secret,
                "scope": self.__resource + "/.default",
            },
        )
        if response.status_code > 300:
            raise Exception(response.json())

        return response.json() # type: ignore
