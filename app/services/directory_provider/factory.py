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
    def __init__(self, config: Config, database: Database, auth: Authenticator) -> None:
        self.__directory_config = config.directory_api
        self.__db = database
        self.__auth = auth

    def create(self) -> DirectoryProvider:
        if self.__directory_config.directory_urls_path is not None and len(
            self.__directory_config.directory_urls_path
        ) > 1:
            return DirectoryJsonProvider(
                directories_json_data=DirectoryProviderFactory._read_directories_file(
                    self.__directory_config.directory_urls_path
                ),
                ignored_directory_service=IgnoredDirectoryService(self.__db)

            )
        elif self.__directory_config.directories_provider_url is not None:
            directory_api_provider = DirectoryApiProvider(
                fhir_api=FhirApi(
                    timeout=self.__directory_config.timeout,
                    backoff=self.__directory_config.backoff,
                    auth=self.__auth,
                    url=self.__directory_config.directories_provider_url,
                    request_count=5,
                    strict_validation=False
                ),
                ignored_directory_service=IgnoredDirectoryService(self.__db),
                provider_url=self.__directory_config.directories_provider_url,
            )
            directory_cache_service = DirectoryCacheService(self.__db)
            return CachingDirectoryProvider(
                directory_provider=directory_api_provider,
                directory_cache_service=directory_cache_service,
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
                            endpoint=directory["endpoint"]
                        )
                    )
                return directory_urls
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error processing directories file: {e}")
