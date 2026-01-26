from app.services.api.authenticators.authenticator import Authenticator
from app.services.api.fhir_api import FhirApi, FhirApiConfig
from app.services.directory_provider.capability_provider import CapabilityProvider
from app.services.directory_provider.db_provider import DbProvider
from app.services.directory_provider.fhir_provider import FhirDirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.api.directory_api_service import DirectoryApiService
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
        provider: DirectoryProvider|None = None

        # Use JSON file-based provider if directories_file_path is provided
        if (
            self.__directory_config.directories_file_path is not None
            and len(self.__directory_config.directories_file_path) > 1
        ):
            provider = DirectoryJsonProvider(
                json_path=self.__directory_config.directories_file_path,
                directory_info_service=self.__directory_info_service,
            )

        elif self.__directory_config.directories_provider_urls:
            # Use API-based provider if directories_provider_urls is provided

            # Api service to interact with the FHIR server
            provider_url = self.__directory_config.directories_provider_urls[0]
            config = FhirApiConfig(
                timeout=self.__directory_config.timeout,
                backoff=self.__directory_config.backoff,
                auth=self.__auth,
                base_url=provider_url,
                request_count=5,
                fill_required_fields=False,
                retries=3,
                mtls_cert=self.__mcsd_config.mtls_client_cert_path,
                mtls_key=self.__mcsd_config.mtls_client_key_path,
                verify_ca=self.__mcsd_config.verify_ca,
                require_mcsd_profiles=self.__mcsd_config.require_mcsd_profiles,
            )
            api_service = DirectoryApiService(
                fhir_api=FhirApi(config),
                provider_url=provider_url,
            )

            provider = FhirDirectoryProvider(
                api_provider=api_service,
                directory_info_service=self.__directory_info_service,
            )

        else:
            raise ValueError(
                "Configuration error: Either 'directories_file_path' or 'directories_provider_urls' must be provided. "
                f"Provided values - directories_file_path: {self.__directory_config.directories_file_path}, "
                f"directories_provider_urls: {self.__directory_config.directories_provider_urls}."
            )

        # Wrap with capability filter provider
        capability_provider = CapabilityProvider(
            inner=provider,
            validate_capability_statement=self.__mcsd_config.check_capability_statement,
            require_mcsd_profiles=self.__mcsd_config.require_mcsd_profiles,
        )

        db_provider = DbProvider(
            inner=capability_provider,
            directory_info_service=self.__directory_info_service,
        )
        return db_provider
