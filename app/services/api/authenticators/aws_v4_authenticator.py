from typing import Any
from app.services.api.authenticators.authenticator import Authenticator

from requests_aws4auth import AWS4Auth
import boto3


class AwsV4Authenticator(Authenticator):
    def __init__(self, profile: str, region: str) -> None:
        self.__profile = profile
        self.__region = region

    def get_authentication_header(self) -> str:
        return ""

    def get_auth(self) -> Any:
        session = boto3.Session(profile_name=self.__profile, region_name=self.__region)
        credentials = session.get_credentials()
        auth = AWS4Auth(
            refreshable_credentials=credentials,
            service="healthlake",
            region=self.__region,
        )
        return auth
