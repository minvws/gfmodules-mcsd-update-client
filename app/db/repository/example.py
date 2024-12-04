from typing import Tuple

from sqlalchemy import ScalarResult, select, func
from sqlalchemy.orm import Session

from db.decorator import repository
from db.models import Example


@repository(Example)
class ExampleRepository:
    def __init__(self, session: Session):
        self.session = session

    def find_example(self, prefix: str, offset: int = 0, limit: int = 25) -> Tuple[int, ScalarResult[Example]]:
        """
        Find examples by prefix

        :param prefix: Prefix to search
        :param offset: Offset to start (in case of pagination)
        :param limit: Limit of items to return
        :return: Tuple with total number of items and a list of examples
        """
        data_stmt = select(Example).where(Example.name.istartswith(prefix, autoescape=True)).offset(offset).limit(limit)
        count_stmt = select(func.count()).where(Example.name.istartswith(prefix, autoescape=True))

        return self.session.scalar(count_stmt) or 0, self.session.scalars(data_stmt)

    def find_all_examples(self) -> Tuple[int, ScalarResult[Example]]:
        """
        Returns all examples

        :return: Tuple with total number of items and a list of examples
        """
        data_stmt = select(Example)
        count_stmt = select(func.count())

        return self.session.scalar(count_stmt) or 0, self.session.scalars(data_stmt)
