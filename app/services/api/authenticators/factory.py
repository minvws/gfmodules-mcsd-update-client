from app.config import Config
from app.services.api.authenticators.authenticator import Authenticator
from app.services.api.authenticators.null_authenticator import NullAuthenticator
from app.services.api.authenticators.aws_v4_authenticator import AwsV4Authenticator
from app.services.api.authenticators.azure_oauth2_authenticator import (
    AzureOAuth2Authenticator,
)


class AuthenticatorFactory:
    def __init__(self, config: Config) -> None:
        self.__config = config

    def create_authenticator(self) -> Authenticator:
        auth_type = self.__config.mcsd.authentication

        match auth_type:
            case "off":
                return NullAuthenticator()
            case "azure_oauth2":
                if self.__config.azure_oauth2 is None:
                    error_msg = self.__error_message("azure_oauth2")
                    raise ValueError(error_msg)

                return AzureOAuth2Authenticator(
                    token_url=self.__config.azure_oauth2.token_url,
                    client_id=self.__config.azure_oauth2.client_id,
                    client_secret=self.__config.azure_oauth2.client_secret,
                    resource=self.__config.azure_oauth2.resource,
                )
            case "aws":
                if self.__config.aws is None:
                    error_msg = self.__error_message("aws")
                    raise ValueError(error_msg)
                return AwsV4Authenticator(
                    profile=self.__config.aws.profile,
                    region=self.__config.aws.region,
                )
            case _:
                raise ValueError(
                    "incorrect value for authenticator, supported types are 'aws', 'azure_oauth2' or 'off'. Please fix in app.conf"
                )

    def __error_message(self, auth_type: str) -> str:
        return f"{auth_type} cannot be None when authenticator type is set, please fix in app.conf"
