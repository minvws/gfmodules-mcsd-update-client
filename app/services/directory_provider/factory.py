import json
from typing import List

from app.models.directory.dto import DirectoryDto
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
        # Use JSON file-based provider if directories_file_path is provided
        if (
            self.__directory_config.directories_file_path is not None
            and len(self.__directory_config.directories_file_path) > 1
        ):
            return DirectoryJsonProvider(
                directories=self._read_directories_file(
                    self.__directory_config.directories_file_path
                ),
                directory_info_service=self.__directory_info_service,
                validate_capability_statement=self.__mcsd_config.check_capability_statement,
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
                validate_capability_statement=self.__mcsd_config.check_capability_statement,
            )
        else:
            raise ValueError(
                "Configuration error: Either 'directories_file_path' or 'directories_provider_url' must be provided. "
                f"Provided values - directories_file_path: {self.__directory_config.directories_file_path}, "
                f"directories_provider_url: {self.__directory_config.directories_provider_url}."
            )

    def _read_directories_file(self, directory_urls_path: str) -> List[DirectoryDto]:
        try:
            with open(directory_urls_path) as f:
                data = json.load(f)
                return [DirectoryDto(**item) for item in data["directories"]]
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error processing directory URLs file: {e}")
