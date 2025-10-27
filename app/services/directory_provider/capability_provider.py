import logging
from typing import List

from app.models.directory.dto import DirectoryDto
from app.services.api.authenticators.null_authenticator import NullAuthenticator
from app.services.api.fhir_api import FhirApi, FhirApiConfig
from app.services.directory_provider.directory_provider import DirectoryProvider

logger = logging.getLogger(__name__)

class CapabilityProvider(DirectoryProvider):
    """
    Filters out directories that do not meet certain capability statement criteria.
    """

    def __init__(
        self,
        inner: DirectoryProvider,
        validate_capability_statement: bool = True,
    ) -> None:
        self.__inner = inner
        self.__validate_capability_statement = validate_capability_statement


    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        dirs = self.__inner.get_all_directories(include_ignored)

        dirs = self.filter_on_capability(dirs)

        return dirs

    def get_all_directories_include_ignored_ids(
        self, include_ignored_ids: List[str]
    ) -> List[DirectoryDto]:
        dirs = self.__inner.get_all_directories_include_ignored_ids(include_ignored_ids)

        dirs = self.filter_on_capability(dirs)

        return dirs

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        the_dir = self.__inner.get_one_directory(directory_id)

        dirs = self.filter_on_capability([the_dir])
        if not dirs or len(dirs) == 0:
            logger.error(f"Requested directory {directory_id} does not meet capability requirements")
            raise Exception(f"Requested directory {directory_id} does not meet capability requirements")

        return dirs[0]

    def filter_on_capability(self, dirs: List[DirectoryDto]) -> List[DirectoryDto]:
        if self.__validate_capability_statement:
            dirs = [d for d in dirs if self.check_capability_statement(d)]

        return dirs

    @staticmethod
    def check_capability_statement(dir_dto: DirectoryDto) -> bool:
        """
        Validates the capability statement of a directory to ensure it meets mCSD requirements.
        Logs the process and returns True if valid, False otherwise.
        """
        logger.info(f"Checking capability statement for {dir_dto.id}")
        try:
            config = FhirApiConfig(
                timeout=5,
                backoff=5,
                auth=NullAuthenticator(),
                base_url=dir_dto.endpoint_address,
                request_count=5,
                fill_required_fields=False,
                retries=5,
                # We assume no MTLS is needed for capability check. Otherwise we need to add this info to the dir_dto
                mtls_cert=None,
                mtls_key=None,
                mtls_ca=None,
            )
            fhir_api=FhirApi(config)
            if not fhir_api.validate_capability_statement():
                logger.warning(
                    f"Directory {dir_dto.id} at {dir_dto.endpoint_address} does not support mCSD requirements"
                )
                return False
        except Exception as e:
            logger.error(f"Error checking capability statement for {dir_dto.id}: {e}")
            return False

        return True
