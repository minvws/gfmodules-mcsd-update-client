import json
from typing import List

from app.models.directory.dto import DirectoryDto
from app.services.api.authenticators.authenticator import Authenticator
from app.services.api.fhir_api import FhirApi
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.directory_provider.api_provider import DirectoryApiProvider
from app.services.directory_provider.caching_provider import CachingDirectoryProvider
from app.services.directory_provider.json_provider import DirectoryJsonProvider
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.config import Config
from app.db.db import Database


class DirectoryProviderFactory:
    """
    Factory to create a DirectoryProvider based on configuration.
    """

    def __init__(self, config: Config, database: Database, auth: Authenticator) -> None:
        self.__directory_config = config.directory_api
        self.__mcsd_config = config.mcsd
        self.__db = database
        self.__auth = auth

    def create(self) -> DirectoryProvider:
        # Use JSON file-based provider if directory_urls_path is provided
        if (
            self.__directory_config.directory_urls_path is not None
            and len(self.__directory_config.directory_urls_path) > 1
        ):
            return DirectoryJsonProvider(
                directories_json_data=DirectoryProviderFactory._read_directories_file(
                    self.__directory_config.directory_urls_path
                ),
                ignored_directory_service=IgnoredDirectoryService(self.__db),
                validate_capability_statement=self.__mcsd_config.check_capability_statement,
            )
        elif self.__directory_config.directories_provider_url is not None:
            # Use API-based provider if directories_provider_url is provided
            directory_api_provider = DirectoryApiProvider(
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
                ignored_directory_service=IgnoredDirectoryService(self.__db),
                provider_url=self.__directory_config.directories_provider_url,
            )
            directory_cache_service = DirectoryCacheService(self.__db)
            return CachingDirectoryProvider(
                directory_provider=directory_api_provider,
                directory_cache_service=directory_cache_service,
                validate_capability_statement=self.__mcsd_config.check_capability_statement,
            )
        else:
            raise ValueError(
                "Configuration error: Either 'directory_urls_path' or 'directories_provider_url' must be provided. "
                f"Provided values - directory_urls_path: {self.__directory_config.directory_urls_path}, "
                f"directories_provider_url: {self.__directory_config.directories_provider_url}."
            )

    @staticmethod
    def _read_directories_file(directory_urls_path: str) -> List[DirectoryDto]:
        try:
            with open(directory_urls_path) as f:
                directory_urls: List[DirectoryDto] = []
                directory_data = json.load(f)
                for directory in directory_data["directories"]:
                    directory_urls.append(
                        DirectoryDto(
                            id=directory["id"],
                            name=directory["name"],
                            endpoint=directory["endpoint"],
                        )
                    )
                return directory_urls
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error processing directories file: {e}")
