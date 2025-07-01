from abc import ABC
import abc
from typing import List

from app.models.directory.dto import DirectoryDto


class DirectoryProvider(ABC):
    """
    Abstract base class for directory provider services.
    This class defines the interface for interacting with directory data.
    Implementations of this class should provide concrete logic for the
    following methods:
    Methods:
        get_all_directories(include_ignored: bool = False) -> List[DirectoryDto]:
            Retrieve a list of all directories. If `include_ignored` is True, ignored directories are included; otherwise, they are filtered out.
        get_all_directories_include_ignored(include_ignored_ids: List[str]) -> List[DirectoryDto]:
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
    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
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
