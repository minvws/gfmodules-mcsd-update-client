from typing import TypeVar

from app.db import session


class RepositoryBase:
    def __init__(self, db_session: session.DbSession):
        self.db_session = db_session


TRepositoryBase = TypeVar("TRepositoryBase", bound=RepositoryBase, covariant=True)
