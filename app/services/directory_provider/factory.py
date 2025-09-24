import json
from typing import Any

from app.services.api.authenticators.authenticator import Authenticator
from app.services.api.fhir_api import FhirApi
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.api.directory_api_service import DirectoryApiService
from app.services.directory_provider.caching_provider import CachingDirectoryProvider
from app.services.directory_provider.json_provider import DirectoryJsonProvider
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.config import Config


class DirectoryProviderFactory:
    """
    Factory to create a DirectoryProvider based on configuration.
    """

    def __init__(self, config: Config, auth: Authenticator, directory_info_service: DirectoryInfoService) -> None:
        self.__directory_config = config.client_directory
        self.__mcsd_config = config.mcsd
        self.__auth = auth
        self.__directory_info_service = directory_info_service

    def create(self) -> DirectoryProvider:
        # Use JSON file-based provider if lrza_output_path is provided
        if (
            self.__directory_config.lrza_output_path is not None
            and len(self.__directory_config.lrza_output_path) > 1
        ):
            return DirectoryJsonProvider(
                lrza_output=DirectoryProviderFactory._read_lrza_output_file(
                    self.__directory_config.lrza_output_path
                ),
                directory_info_service=self.__directory_info_service,
            )
        elif self.__directory_config.directories_provider_url is not None:
            # Use API-based provider if directories_provider_url is provided
            api_service = DirectoryApiService(
                fhir_api=FhirApi(
                    timeout=self.__directory_config.timeout,
                    backoff=self.__directory_config.backoff,
                    auth=self.__auth,
                    base_url=self.__directory_config.directories_provider_url,
                    request_count=5,
                    fill_required_fields=False,
                    retries=10,
                    mtls_cert=self.__mcsd_config.mtls_client_cert_path,
                    mtls_key=self.__mcsd_config.mtls_client_key_path,
                    mtls_ca=self.__mcsd_config.mtls_server_ca_path,
                ),
                provider_url=self.__directory_config.directories_provider_url,
            )
            
            return CachingDirectoryProvider(
                api_service=api_service,
                directory_info_service=self.__directory_info_service,
            )
        else:
            raise ValueError(
                "Configuration error: Either 'lrza_output_path' or 'directories_provider_url' must be provided. "
                f"Provided values - lrza_output_path: {self.__directory_config.lrza_output_path}, "
                f"directories_provider_url: {self.__directory_config.directories_provider_url}."
            )

    @staticmethod
    def _read_lrza_output_file(lrza_output_path: str) -> Any:
        try:
            with open(lrza_output_path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error processing lrza output file: {e}")
