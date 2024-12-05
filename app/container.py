import inject

from app.db.db import Database
from app.config import get_config


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)


def get_database() -> Database:
    return inject.instance(Database)


def setup_container() -> None:
    inject.configure(container_config, once=True)
