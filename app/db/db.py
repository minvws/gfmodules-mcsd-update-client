import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from db.db_session import DbSession
from db.models import Base

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, dsn: str):
        try:
            self.engine = create_engine(dsn, echo=False)
        except BaseException as e:
            logger.error("Error while connecting to database: %s", e)
            raise e

    def generate_tables(self) -> None:
        logger.info("Generating tables...")
        Base.metadata.create_all(self.engine)

    def is_healthy(self) -> bool:
        """
        Check if the database is healthy

        :return: True if the database is healthy, False otherwise
        """
        try:
            with Session(self.engine) as session:
                session.execute(text('SELECT 1'))
            return True
        except Exception as e:
            logger.info("Database is not healthy: %s", e)
            return False

    def get_db_session(self) -> DbSession:
        return DbSession(self.engine)
