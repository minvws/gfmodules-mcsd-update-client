from typing import Any

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from app.db.decorator import repository_registry
from app.db.models import Base


class DbSession:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def __enter__(self) -> 'DbSession':
        """
        Create a new session when entering the context manager
        """
        self.session = Session(self._engine, expire_on_commit=False)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Close the session when exiting the context manager
        """
        self.session.close()

    def get_repository(self, model_class: Any) -> Any|None:
        """
        Returns an instantiated repository for the given model class

        :param model_class:
        :return:
        """
        repo_class = repository_registry.get(model_class)
        if repo_class:
            return repo_class(self.session)
        raise ValueError(f"No repository registered for model {model_class}")

    def add_resource(self, entry: Base) -> None:
        """
        Add a resource to the session, so it will be inserted/updated in the database on the next commit

        :param entry:
        :return:
        """
        self.session.add(entry)

    def delete_resource(self, entry: Base) -> None:
        """
        Delete a resource from the session, so it will be deleted from the database on the next commit

        :param entry:
        :return:
        """
        # database cascading will take care of the rest
        self.session.delete(entry)

    def commit(self) -> None:
        """
        Commits any pending work in the session to the database

        :return:
        """
        self.session.commit()

    def rollback(self) -> None:
        """
        Rollback the current transaction

        :return:
        """
        self.session.rollback()

