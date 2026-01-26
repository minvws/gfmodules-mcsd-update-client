from typing import Any, Type, Dict

from app.db.entities.base import Base
from app.db.repositories.repository_base import RepositoryBase

repository_registry: Dict[Type[Base], Type[RepositoryBase]] = {}


def repository(model_class: Type[Base]) -> Any:
    def decorator(repo_class: Type[RepositoryBase]) -> Type[RepositoryBase]:
        """
        Decorator to register a repository for a model class

        :param repo_class:
        :return:
        """
        repository_registry[model_class] = repo_class
        return repo_class

    return decorator
