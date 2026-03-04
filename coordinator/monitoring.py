from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .config import AppConfig
from .debezium import DebeziumClient
from .repositories import CoordinatorRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MonitoringResult:
    bulk_transitions: list[dict[str, Any]]
    connectors: list[dict[str, Any]]
    released_owners: list[str]


class MonitoringService:
    def __init__(self, config: AppConfig, repository: CoordinatorRepository) -> None:
        self._config = config
        self._repository = repository
        self._debezium = DebeziumClient(config.debezium_connect_url)

    def run_once(self) -> MonitoringResult:
        transitions = self._coordinate_bulk_completion()
        connector_states = self._check_connectors()
        released = self._repository.release_stale_cdc_ownership(
            heartbeat_timeout_seconds=self._config.monitor_heartbeat_timeout_seconds
        )
        return MonitoringResult(
            bulk_transitions=transitions,
            connectors=connector_states,
            released_owners=released,
        )

    def _coordinate_bulk_completion(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        jobs = self._repository.list_bulk_in_progress_jobs()
        for job in jobs:
            if not self._repository.all_chunks_completed(job.job_id):
                continue

            if job.migration_mode == "cdc":
                new_status = "bulk_done"
                completed = False
            else:
                new_status = "completed"
                completed = True

            self._repository.update_job_status(job.job_id, new_status, completed=completed)
            results.append(
                {
                    "job_id": job.job_id,
                    "from": job.status,
                    "to": new_status,
                }
            )
        return results

    def _check_connectors(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for job in self._repository.list_cdc_jobs_for_monitoring():
            if not job.debezium_connector_name:
                continue
            status = self._debezium.get_connector_status(job.debezium_connector_name)
            results.append(
                {
                    "job_id": job.job_id,
                    "connector": job.debezium_connector_name,
                    "status": status,
                }
            )
        return results

