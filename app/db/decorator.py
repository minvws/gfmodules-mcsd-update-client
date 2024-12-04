from typing import Any

from app.db.models import Base

repository_registry = {}


def repository(model_class: Base) -> Any:
    def decorator(repo_class: Any) -> Any:
        """
        Decorator to register a repository for a model class

        :param repo_class:
        :return:
        """
        repository_registry[model_class] = repo_class
        return repo_class
    return decorator
