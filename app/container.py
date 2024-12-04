
import inject
from db.db import Database
from config import get_config, Config


class ExampleService:
    def __init__(self, argument1: str, argument2: bool) -> None:
        self.argument1 = argument1
        self.argument2 = argument2


def configure_example_service(config: Config) -> ExampleService:
    # get arguments from config and configure the service
    return ExampleService(
        config.example.argument1,
        config.example.argument2
    )


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    binder.bind(ExampleService, configure_example_service(config))


def get_database() -> Database:
    return inject.instance(Database)


def get_example_service() -> ExampleService:
    return inject.instance(ExampleService)


inject.configure(container_config)
