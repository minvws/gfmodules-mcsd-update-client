import logging

from sqlalchemy import MetaData, StaticPool, create_engine, text
from sqlalchemy.orm import Session

from app.db.entities.base import Base
from app.db.session import DbSession

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, dsn: str):
        try:
            if "sqlite://" in dsn:
                self.engine = create_engine(
                    dsn,
                    connect_args={'check_same_thread': False},
                    # This + static pool is needed for sqlite in-memory tables
                    poolclass=StaticPool,
                    echo=False,
                )
            else:
                self.engine = create_engine(dsn, echo=False)
        except BaseException as e:
            logger.error("Error while connecting to database: %s", e)
            raise e

    def generate_tables(self) -> None:
        logger.info("Generating tables...")
        Base.metadata.create_all(self.engine)

    def truncate_tables(self) -> None:
        logger.info("Truncating all tables...")
        try:
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            with Session(self.engine) as session:
                for table in reversed(metadata.sorted_tables):
                    session.execute(text(f"DELETE FROM {table.name}"))
                session.commit()
            logger.info("All tables truncated successfully.")
        except Exception as e:
            logger.error("Error while truncating tables: %s", e)
            raise e

    def is_healthy(self) -> bool:
        """
        Check if the database is healthy

        :return: True if the database is healthy, False otherwise
        """
        try:
            with Session(self.engine) as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.info("Database is not healthy: %s", e)
            return False

    def get_db_session(self) -> DbSession:
        return DbSession(self.engine)
