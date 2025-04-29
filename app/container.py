from app.services.entity_services.supplier_info_service import SupplierInfoService
from app.services.mcsd_services.mass_update_consumer_service import (
    MassUpdateConsumerService,
)
from app.services.scheduler import Scheduler
import inject

from app.db.db import Database
from app.config import get_config
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.request_services.Authenticators import (
    AzureOAuth2Authenticator,
    NullAuthenticator,
    AwsV4Authenticator,
)
from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService
from app.services_new.api.suppliers_api import SupplierProvider, SuppliersApi
from app.services_new.update_service import UpdateConsumer


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    supplier_provider = SupplierProvider(
        supplier_urls=config.supplier_api.supplier_urls,
        supplier_provider_url=config.supplier_api.suppliers_provider_url,
    )

    supplier_api = SuppliersApi(
        timeout=config.supplier_api.timeout,
        backoff=config.supplier_api.backoff,
        supplier_provider=supplier_provider,
    )
    binder.bind(SuppliersApi, supplier_api)

    resource_map_service = ResourceMapService(db)
    binder.bind(ResourceMapService, resource_map_service)

    if config.mcsd.authentication == "off":
        auth = NullAuthenticator()
    elif config.mcsd.authentication == "aws":
        auth = AwsV4Authenticator(
            profile=config.aws.profile,  # type: ignore
            region=config.aws.region,  # type: ignore
        )
    elif config.mcsd.authentication == "azure_oauth2":
        auth = AzureOAuth2Authenticator(
            token_url=config.azure_oauth2.token_url,  # type: ignore
            client_id=config.azure_oauth2.client_id,  # type: ignore
            client_secret=config.azure_oauth2.client_secret,  # type: ignore
            resource=config.azure_oauth2.resource,  # type: ignore
        )
    else:
        raise ValueError("authentication must be either False, or 'azure_oauth2'")
    consumer_request_service = ConsumerRequestService(config.mcsd.consumer_url, auth)
    supplier_request_service = SupplierRequestsService(
        supplier_api, NullAuthenticator(), request_count=config.mcsd.request_count
    )

    # test
    update_consumer = UpdateConsumer(
        consumer_url=config.mcsd.consumer_url,
        strict_validation=config.mcsd.strict_validation,
        timeout=config.supplier_api.timeout,
        backoff=config.supplier_api.backoff,
        request_count=config.mcsd.request_count,
        suppliers_api=supplier_api,
        resource_map_service=resource_map_service,
        # auth=NullAuthenticator(),
    )
    binder.bind(UpdateConsumer, update_consumer)

    update_consumer_service = UpdateConsumerService(
        consumer_request_service=consumer_request_service,
        supplier_request_service=supplier_request_service,
        resource_map_service=resource_map_service,
        strict_validation=config.mcsd.strict_validation,
    )
    binder.bind(UpdateConsumerService, update_consumer_service)

    supplier_info_service = SupplierInfoService(db)

    mass_update_service = MassUpdateConsumerService(
        update_consumer_service=update_consumer_service,
        supplier_service=supplier_api,
        supplier_info_service=supplier_info_service,
    )
    scheduler = Scheduler(
        function=mass_update_service.update_all,
        delay=config.scheduler.delay,
        max_logs_entries=config.scheduler.max_logs_entries,
    )
    binder.bind(Scheduler, scheduler)


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_api() -> SuppliersApi:
    return inject.instance(SuppliersApi)


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_update_consumer_service() -> UpdateConsumerService:
    return inject.instance(UpdateConsumerService)


def get_scheduler() -> Scheduler:
    return inject.instance(Scheduler)


def get_update_consumer() -> UpdateConsumer:
    return inject.instance(UpdateConsumer)


def setup_container() -> None:
    inject.configure(container_config, once=True)
