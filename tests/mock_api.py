import logging
from typing import List

from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.directory_provider import DirectoryProvider

logger = logging.getLogger(__name__)


class MockApi(DirectoryProvider):
    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        if directory_id == "1":
            return DirectoryDto(
                id="1",
                name="Test Directory",
                endpoint="http://test1.directory.example.org",
            )
        elif directory_id == "2":
            return DirectoryDto(
                id="2",
                name="Test Directory 2",
                endpoint="http://test2.directory.example.org",
            )
        else:
            raise ValueError(f"DirectoryDto with ID {directory_id} not found")

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        return [
            DirectoryDto(
                id="1",
                name="Test Directory",
                endpoint="http://test1.directory.example.org",
            ),
            DirectoryDto(
                id="2",
                name="Test Directory 2",
                endpoint="http://test2.directory.example.org",
            ),
        ]
