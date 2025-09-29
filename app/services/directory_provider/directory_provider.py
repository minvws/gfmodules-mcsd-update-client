from abc import ABC
import abc
from typing import List
import logging
from app.models.directory.dto import DirectoryDto
from app.services.api.authenticators.null_authenticator import NullAuthenticator
from app.services.api.fhir_api import FhirApi

logger = logging.getLogger(__name__)

class DirectoryProvider(ABC):
    """
    Abstract base class for directory provider services.
    This class defines the interface for interacting with directory data.
    Implementations of this class should provide concrete logic for the
    following methods:
    Methods:
        get_all_directories(include_ignored: bool = False) -> List[DirectoryDto]:
            Retrieve a list of all directories. If `include_ignored` is True, ignored directories are included; otherwise, they are filtered out.
        get_all_directories_include_ignored_ids(include_ignored_ids: List[str]) -> List[DirectoryDto]:
            Retrieve a list of all directories, including those specified in the ignore list.
        get_one_directory(directory_id: str) -> DirectoryDto:
            Retrieve a specific directory by their unique identifier.
    """

    @abc.abstractmethod
    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        """
        Returns a list of all directories, also including ignored ones if specified, otherwise these are filtered out.
        or raises Exception if the directory provider could not be reached.
        """
        pass

    @abc.abstractmethod
    def get_all_directories_include_ignored_ids(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        """
        Returns a list of all directories including, if specified, the directories which id is in the ignore list, otherwise these are filtered out.
        Raises Exception if the directory provider could not be reached.
        """
        pass

    @abc.abstractmethod
    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        """
        Returns a specific directory by their unique identifier or raises Exception if the directory provider could not be reached.
        """
        pass

    @staticmethod
    def check_capability_statement(dir_dto: DirectoryDto) -> bool:
        """
        Validates the capability statement of a directory to ensure it meets mCSD requirements.
        Logs the process and returns True if valid, False otherwise.
        """
        logger.info(f"Checking capability statement for {dir_dto.id}")
        try:
            fhir_api=FhirApi(
                    timeout=5,
                    backoff=5,
                    auth=NullAuthenticator(),
                    base_url=dir_dto.endpoint_address,
                    request_count=5,
                    fill_required_fields=False,
                    retries=5,
                )
            if not fhir_api.validate_capability_statement():
                logger.warning(
                    f"Directory {dir_dto.id} at {dir_dto.endpoint_address} does not support mCSD requirements"
                )
                return False
        except Exception as e:
            logger.error(f"Error checking capability statement for {dir_dto.id}: {e}")
            return False
        return True
