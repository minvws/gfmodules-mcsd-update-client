from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

from app.config import Config
from app.db.db import Database
from app.db.repositories.directory_provider_directory_repository import (
    DirectoryProviderDirectoryRepository,
)
from app.db.repositories.directory_provider_repository import DirectoryProviderRepository
from app.models.directory.dto import DirectoryDto
from app.services.api.directory_api_service import DirectoryApiService
from app.services.api.fhir_api import FhirApi, FhirApiConfig
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.update.update_client_service import UpdateClientService

logger = logging.getLogger(__name__)


class DirectoryRegistryService:
    def __init__(
        self,
        database: Database,
        config: Config,
        directory_info_service: DirectoryInfoService,
        update_client_service: UpdateClientService,
        auth,
    ) -> None:
        self.__db = database
        self.__config = config
        self.__directory_info_service = directory_info_service
        self.__update_client_service = update_client_service
        self.__auth = auth

    @staticmethod
    def _default_manual_id(endpoint_address: str) -> str:
        return hashlib.sha256(endpoint_address.encode("utf-8")).hexdigest()[:32]

    def add_provider(self, url: str, enabled: bool = True) -> dict:
        with self.__db.get_db_session() as session:
            repo = session.get_repository(DirectoryProviderRepository)
            existing = repo.get_by_url(url)
            if existing is None:
                from app.db.entities.directory_provider import DirectoryProvider

                existing = DirectoryProvider(url=url, enabled=enabled)
                session.add(existing)
                session.commit()
                session.session.refresh(existing)
            else:
                existing.enabled = enabled
                session.commit()
                session.session.refresh(existing)
            return {"id": existing.id, "url": existing.url, "enabled": existing.enabled, "last_refresh_at": existing.last_refresh_at}

    def list_providers(self, include_disabled: bool = True) -> list[dict]:
        with self.__db.get_db_session() as session:
            repo = session.get_repository(DirectoryProviderRepository)
            providers = repo.get_all(include_disabled=include_disabled)
            return [
                {"id": p.id, "url": p.url, "enabled": p.enabled, "last_refresh_at": p.last_refresh_at}
                for p in providers
            ]

    def add_manual_directory(
        self,
        endpoint_address: str,
        directory_id: str | None = None,
        ura: str = "",
    ) -> DirectoryDto:
        if directory_id is None or directory_id.strip() == "":
            directory_id = self._default_manual_id(endpoint_address)
        dto = self.__directory_info_service.create_or_update(
            directory_id=directory_id,
            endpoint_address=endpoint_address,
            ura=ura,
            origin="manual",
        )
        return dto

    def ensure_config_providers(self) -> None:
        cfg = self.__config.client_directory
        urls = list(cfg.directories_provider_urls)
        urls = [u.strip() for u in urls if u and u.strip()]
        if not urls:
            return
        for url in urls:
            try:
                self.add_provider(url=url, enabled=True)
            except Exception:
                logger.exception("Failed to upsert provider from config. url=%s", url)

    def refresh_all_enabled_providers(self) -> dict:
        with self.__db.get_db_session() as session:
            provider_repo = session.get_repository(DirectoryProviderRepository)
            providers = provider_repo.get_all(include_disabled=False)
        results: dict = {"providers": [], "errors": []}
        for p in providers:
            try:
                out = self.refresh_provider(p.id)
                results["providers"].append(out)
            except Exception as e:
                logger.exception("Failed to refresh provider. provider_id=%s", p.id)
                results["errors"].append({"provider_id": p.id, "error": str(e)})
        return results

    def refresh_provider(self, provider_id: int) -> dict:
        now = datetime.now(timezone.utc)
        with self.__db.get_db_session() as session:
            provider_repo = session.get_repository(DirectoryProviderRepository)
            provider = provider_repo.get_by_id(provider_id)
            if provider is None:
                raise ValueError(f"Provider {provider_id} not found")
            if not provider.enabled:
                return {"provider_id": provider.id, "url": provider.url, "status": "disabled"}
            provider_url = provider.url

        api_config = FhirApiConfig(
            timeout=self.__config.client_directory.timeout,
            backoff=self.__config.client_directory.backoff,
            auth=self.__auth,
            base_url=provider_url,
            request_count=5,
            fill_required_fields=False,
            retries=self.__config.client_directory.retries,
            mtls_cert=self.__config.mcsd.mtls_client_cert_path,
            mtls_key=self.__config.mcsd.mtls_client_key_path,
            verify_ca=self.__config.mcsd.verify_ca,
        )
        api_service = DirectoryApiService(fhir_api=FhirApi(api_config), provider_url=provider_url)
        fetched = api_service.fetch_directories()

        seen_directory_ids: set[str] = set()
        endpoint_to_id: dict[str, str] = {}
        with self.__db.get_db_session() as session:
            link_repo = session.get_repository(DirectoryProviderDirectoryRepository)
            provider_repo = session.get_repository(DirectoryProviderRepository)
            provider = provider_repo.get_by_id(provider_id)
            if provider is None:
                raise ValueError(f"Provider {provider_id} not found")

            for dto in fetched:
                existing_id = self.__directory_info_service.get_id_by_endpoint_address(dto.endpoint_address)
                dir_id = existing_id or dto.id
                endpoint_to_id[dto.endpoint_address] = dir_id
                origin = "provider"
                if existing_id is not None:
                    try:
                        existing = self.__directory_info_service.get_one_by_id(existing_id)
                        if existing.origin == "manual":
                            origin = None
                    except Exception:
                        pass
                stored = self.__directory_info_service.create_or_update(
                    directory_id=dir_id,
                    endpoint_address=dto.endpoint_address,
                    ura=dto.ura,
                    origin=origin,
                )
                seen_directory_ids.add(stored.id)
                link_repo.upsert_seen(provider_id=provider.id, directory_id=stored.id, now=now)

            removed = link_repo.mark_removed_if_not_seen(provider_id=provider.id, seen_directory_ids=seen_directory_ids, now=now)

            provider.last_refresh_at = now
            session.commit()

        archived: list[str] = []
        if not self.__config.client_directory.mark_client_directory_as_deleted_after_lrza_delete:
            return {
                "provider_id": provider_id,
                "url": provider_url,
                "fetched": len(fetched),
                "removed": len(removed),
                "archived": archived,
            }
        for removed_dir_id in removed:
            try:
                info = self.__directory_info_service.get_one_by_id(removed_dir_id)
                if info.origin != "provider":
                    continue
                with self.__db.get_db_session() as session:
                    link_repo = session.get_repository(DirectoryProviderDirectoryRepository)
                    if link_repo.count_active_for_directory(removed_dir_id) > 0:
                        continue
                self.__directory_info_service.set_deleted_at(removed_dir_id, specific_datetime=now)
                try:
                    self.__update_client_service.cleanup(removed_dir_id)
                except Exception:
                    logger.exception("Failed to cleanup update-client resources for removed directory. directory_id=%s", removed_dir_id)
                archived.append(removed_dir_id)
            except Exception:
                logger.exception("Failed to archive removed directory. directory_id=%s", removed_dir_id)

        return {
            "provider_id": provider_id,
            "url": provider_url,
            "fetched": len(fetched),
            "removed": len(removed),
            "archived": archived,
        }
