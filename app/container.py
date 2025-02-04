import inject

from app.db.db import Database
from app.config import get_config
from app.services.entity_services.http_api import HttpApi
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.Authenticators import AzureOAuth2Authenticator, NullAuthenticator, AwsV4Authenticator
from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)


    supplier_service = SupplierService(
        HttpApi(
            config.supplier_api.base_url,
            config.supplier_api.timeout,
            config.supplier_api.backoff,
        )
    )
    binder.bind(SupplierService, supplier_service)

    resource_map_service = ResourceMapService(db)
    binder.bind(ResourceMapService, resource_map_service)

    if config.mcsd.authentication == "off":
        auth = NullAuthenticator()
    elif config.mcsd.authentication == "aws":
        auth = AwsV4Authenticator(
            profile=config.aws.profile,        # type: ignore
            region=config.aws.region,            # type: ignore
        )
    elif config.mcsd.authentication == "azure_oauth2":
        auth = AzureOAuth2Authenticator(
            token_url=config.azure_oauth2.token_url,        # type: ignore
            client_id=config.azure_oauth2.client_id,        # type: ignore
            client_secret=config.azure_oauth2.client_secret,   # type: ignore
            resource=config.azure_oauth2.resource,       # type: ignore
        )
    else:
        raise ValueError(
            "authentication must be either False, or 'azure_oauth2'"
        )
    consumer_request_service = ConsumerRequestService(config.mcsd.consumer_url, auth)
    supplier_request_service = SupplierRequestsService(supplier_service, NullAuthenticator())

    update_consumer_service = UpdateConsumerService(
        consumer_request_service=consumer_request_service,
        supplier_request_service=supplier_request_service,
        resource_map_service=resource_map_service,
    )
    binder.bind(UpdateConsumerService, update_consumer_service)


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_service() -> SupplierService:
    return inject.instance(SupplierService)


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_update_consumer_service() -> UpdateConsumerService:
    return inject.instance(UpdateConsumerService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
