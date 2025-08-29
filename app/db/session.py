import logging
import random
from time import sleep
from typing import Callable, Any, TypeVar, Type

from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError, DatabaseError, PendingRollbackError
from sqlalchemy.orm import Session

from app.config import get_config
from app.db.entities.base import Base
from app.db.repositories import repository_base

"""
This module contains the DbSession class, which is a context manager that provides a session to interact with
the database. It also provides methods to add and delete resources from the session, and to commit or rollback the
current transaction.

Usage:

    with DbSession(engine) as session:
        repo = session.get_repository(MyModelRepository)
        repo.find_all()
        session.add(MyModel())
        session.commit()
"""


logger = logging.getLogger(__name__)

T = TypeVar("T")


class DbSession:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def __enter__(self) -> "DbSession":
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

    def get_repository(
        self, repository_class: Type["repository_base.TRepositoryBase"]
    ) -> "repository_base.TRepositoryBase":
        """
        Returns an instantiated repository
        """
        if issubclass(repository_class, repository_base.RepositoryBase):
            return repository_class(self)
        raise ValueError(f"No repository registered for model {repository_class}")

    def add(self, entry: Base) -> None:
        """
        Add a resource to the session, so it will be inserted/updated in the database on the next commit

        :param entry:
        :return:
        """
        self._retry(self.session.add, entry)

    def delete(self, entry: Base) -> None:
        """
        Delete a resource from the session, so it will be deleted from the database on the next commit

        :param entry:
        :return:
        """
        # database cascading will take care of the rest
        self._retry(self.session.delete, entry)

    def commit(self) -> None:
        """
        Commits any pending work in the session to the database

        :return:
        """
        self._retry(self.session.commit)

    def rollback(self) -> None:
        """
        Rollback the current transaction

        :return:
        """
        self._retry(self.session.rollback)

    def query(self, *entities: Any) -> Any:
        """
        Create a new query object

        :param entities:
        :return:
        """
        return self._retry(self.session.query, *entities)

    def execute(self, stmt: Any) -> Any:
        """
        Execute a statement in the current session

        :param stmt:
        :return:
        """
        return self._retry(self.session.execute, stmt)

    def begin(self) -> Any:
        """
        Begin a new transaction

        :return:
        """
        return self._retry(self.session.begin)

    def _retry(self, f: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Retry a function call in case of database errors
        """
        backoff = get_config().database.retry_backoff

        while True:
            try:
                return f(*args, **kwargs)
            except PendingRollbackError as e:
                logger.warning("Retrying operation due to PendingRollbackError: %s", e)
                self.session.rollback()
            except OperationalError as e:
                logger.warning("Retrying operation due to OperationalError: %s", e)
            except DatabaseError as e:
                logger.warning("Retrying operation due to DatabaseError: %s", e)
                raise e
            except Exception as e:
                logger.warning("Generic Exception during operation: %s", e)
                raise e

            if len(backoff) == 0:
                logger.error("Operation failed after all retries")
                raise DatabaseError("Operation failed after all retries", None, BaseException())

            logger.info("Retrying operation in %s seconds", backoff[0])
            sleep(backoff[0] + random.uniform(0, 0.1))
            backoff = backoff[1:]
