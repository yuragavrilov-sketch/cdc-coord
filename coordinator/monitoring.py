from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .config import AppConfig
from .debezium import DebeziumClient
from .repositories import CoordinatorRepository

if TYPE_CHECKING:
    from .notifications import VKTeamsNotifier

logger = logging.getLogger(__name__)

# Connector statuses that require an alert
_BAD_CONNECTOR_STATUSES = frozenset({"FAILED", "MISSING", "PAUSED"})

# Bulk-phase milestones (%) at which to send a progress notification
_BULK_MILESTONES = (25, 50, 75)


@dataclass(slots=True)
class MonitoringResult:
    bulk_transitions: list[dict[str, Any]]
    connectors: list[dict[str, Any]]
    released_owners: list[str]        # job_ids whose stale CDC ownership was released
    stale_chunks_reclaimed: int       # total chunks reset to pending this cycle


class MonitoringService:
    def __init__(
        self,
        config: AppConfig,
        repository: CoordinatorRepository,
        notifier: VKTeamsNotifier | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._debezium = DebeziumClient(config.debezium_connect_url)
        self._notifier = notifier

        # Phase-change tracking: job_id → last known status
        self._known_statuses: dict[str, str] = {}
        # Connector status tracking: connector_name → last known status
        self._known_connector_statuses: dict[str, str] = {}
        # job_ids for which a chunk-failure notification was already sent
        self._reported_error_jobs: set[str] = set()
        # job_id → last notified bulk milestone percent (25 / 50 / 75)
        self._notified_milestones: dict[str, int] = {}
        # Timestamp of the last periodic status report (monotonic)
        self._last_status_report: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_once(self) -> MonitoringResult:
        transitions = self._coordinate_bulk_completion()
        connector_states = self._check_connectors()
        released = self._repository.release_stale_cdc_ownership(
            heartbeat_timeout_seconds=self._config.monitor_heartbeat_timeout_seconds
        )
        reclaimed = self._repository.reclaim_stale_chunks(
            self._config.worker_chunk_timeout_seconds
        )

        if self._notifier:
            # Phase transitions (bulk_running → bulk_done; hybrid child activation)
            self._send_bulk_transition_notifications(transitions)
            # Completion with stats (bulk_running → completed, including hybrid children)
            self._notify_completed_jobs(transitions)
            # CDC phase changes (cdc_catchup → cdc_streaming, etc.)
            self._detect_cdc_phase_changes()
            # Shared summary used by the remaining checks
            summary = self._repository.get_active_jobs_summary()
            # Chunk failure alerts with error message sample
            self._detect_errors(summary)
            # Bulk progress milestones (25 / 50 / 75 %)
            self._check_milestones(summary)
            # Debezium connector FAILED / MISSING / recovered
            self._notify_connector_changes(connector_states)
            # CDC worker died — ownership forcibly released
            self._notify_stale_cdc_released(released)
            # Worker hung — stale chunks reset to pending
            self._notify_stale_chunks(reclaimed)
            # Periodic status digest
            self._maybe_send_status_report(summary)

        released_job_ids = [r["job_id"] for r in released]
        stale_total = sum(r["count"] for r in reclaimed)

        return MonitoringResult(
            bulk_transitions=transitions,
            connectors=connector_states,
            released_owners=released_job_ids,
            stale_chunks_reclaimed=stale_total,
        )

    # ------------------------------------------------------------------
    # Bulk completion coordination (also populates transition list)
    # ------------------------------------------------------------------

    def _coordinate_bulk_completion(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        jobs = self._repository.list_bulk_in_progress_jobs()
        for job in jobs:
            if not self._repository.all_chunks_completed(job.job_id):
                continue

            if job.migration_mode == "cdc":
                new_status = "bulk_done"
                completed = False
                # Activate pending child static jobs (hybrid mode)
                activated = self._repository.activate_child_jobs(job.job_id)
                if activated:
                    logger.info(
                        "Activated %d child static job(s) for hybrid parent",
                        len(activated),
                        extra={"parent_job_id": job.job_id},
                    )
                    # Emit a transition entry for each activated child so
                    # _send_bulk_transition_notifications can fire a hybrid child alert
                    for child in activated:
                        child_chunk_count = len(self._repository.list_chunks(child.job_id))
                        results.append({
                            "job_id": child.job_id,
                            "table_name": child.table_name,
                            "mode": child.migration_mode,
                            "from": "pending",
                            "to": "bulk_running",
                            "parent_job_id": job.job_id,
                            # child chunk count for the notification message
                            "_child_chunk_count": child_chunk_count,
                        })
            else:
                new_status = "completed"
                completed = True

            self._repository.update_job_status(job.job_id, new_status, completed=completed)
            results.append({
                "job_id": job.job_id,
                "table_name": job.table_name,
                "mode": job.migration_mode,
                "from": job.status,
                "to": new_status,
                "parent_job_id": job.parent_job_id,
            })
            # Pre-update known status so _detect_cdc_phase_changes won't double-notify
            self._known_statuses[job.job_id] = new_status

        return results

    # ------------------------------------------------------------------
    # Notification helpers
    # ------------------------------------------------------------------

    def _send_bulk_transition_notifications(self, transitions: list[dict[str, Any]]) -> None:
        assert self._notifier is not None
        for t in transitions:
            if t["to"] == "completed":
                # Handled separately by _notify_completed_jobs (includes stats)
                continue

            if t.get("parent_job_id") and t["from"] == "pending" and t["to"] == "bulk_running":
                # Hybrid child activated
                chunk_count = t.get("_child_chunk_count", 0)
                self._notifier.notify_hybrid_child_started(
                    table_name=t["table_name"],
                    child_job_id=t["job_id"],
                    parent_job_id=t["parent_job_id"],
                    chunk_count=chunk_count,
                )
            else:
                self._notifier.notify_phase_change(
                    table_name=t["table_name"],
                    job_id=t["job_id"],
                    from_status=t["from"],
                    to_status=t["to"],
                    mode=t["mode"],
                    parent_job_id=t.get("parent_job_id"),
                )

    def _notify_completed_jobs(self, transitions: list[dict[str, Any]]) -> None:
        """Send a completion notification with row count and duration for each completed job."""
        assert self._notifier is not None
        for t in transitions:
            if t["to"] != "completed":
                continue
            stats = self._repository.get_job_completion_stats(t["job_id"])
            self._notifier.notify_migration_completed(
                table_name=t["table_name"],
                job_id=t["job_id"],
                mode=t["mode"],
                total_rows=stats.get("total_rows", 0),
                duration_seconds=stats.get("duration_seconds"),
                parent_job_id=t.get("parent_job_id"),
            )

    def _detect_cdc_phase_changes(self) -> None:
        """Detect CDC job phase changes (cdc_catchup → cdc_streaming etc.) by comparing
        current DB state with last observed state."""
        assert self._notifier is not None
        jobs = self._repository.list_cdc_jobs_for_monitoring()
        for job in jobs:
            prev_status = self._known_statuses.get(job.job_id)
            if prev_status is not None and prev_status != job.status:
                self._notifier.notify_phase_change(
                    table_name=job.table_name,
                    job_id=job.job_id,
                    from_status=prev_status,
                    to_status=job.status,
                    mode=job.migration_mode,
                )
            self._known_statuses[job.job_id] = job.status

    def _detect_errors(self, summary: list[dict[str, Any]]) -> None:
        """Notify once per job when failed chunks are detected; include a sample error message."""
        assert self._notifier is not None
        for j in summary:
            if j["chunks_failed"] <= 0 or j["job_id"] in self._reported_error_jobs:
                continue
            errors = self._repository.get_failed_chunk_errors(j["job_id"])
            sample = errors[0] if errors else None
            msg = f"{j['chunks_failed']} чанк(ов) завершились с ошибкой"
            if sample:
                msg += f"\nПример: {sample[:300]}"
            self._notifier.notify_error(
                table_name=j["table_name"],
                job_id=j["job_id"],
                message=msg,
            )
            self._reported_error_jobs.add(j["job_id"])

    def _check_milestones(self, summary: list[dict[str, Any]]) -> None:
        """Notify when bulk progress crosses 25 / 50 / 75 % thresholds.

        Works for both standalone/hybrid-parent jobs and hybrid child static jobs.
        """
        assert self._notifier is not None
        for j in summary:
            if j["status"] not in ("bulk_running", "bulk_loading"):
                continue
            total = j["chunks_total"]
            done = j["chunks_done"]
            if total <= 0:
                continue
            pct = (done * 100) // total
            last = self._notified_milestones.get(j["job_id"], 0)
            for m in _BULK_MILESTONES:
                if pct >= m and last < m:
                    self._notifier.notify_bulk_milestone(
                        table_name=j["table_name"],
                        job_id=j["job_id"],
                        mode=j["mode"],
                        percent=m,
                        chunks_done=done,
                        chunks_total=total,
                        parent_job_id=j.get("parent_job_id"),
                    )
                    self._notified_milestones[j["job_id"]] = m
                    break  # one milestone per cycle to avoid message flood

    def _notify_connector_changes(self, connector_states: list[dict[str, Any]]) -> None:
        """Detect and alert on Debezium connector status changes (RUNNING → FAILED etc.)."""
        assert self._notifier is not None
        for cs in connector_states:
            connector = cs["connector"]
            new_status = cs["status"]
            prev_status = self._known_connector_statuses.get(connector)

            should_notify = False
            if prev_status is None:
                # First time seen: alert only if already in a bad state
                should_notify = new_status in _BAD_CONNECTOR_STATUSES
            elif prev_status != new_status:
                # Status changed: alert for degradation or recovery from bad state
                should_notify = (
                    new_status in _BAD_CONNECTOR_STATUSES
                    or (prev_status in _BAD_CONNECTOR_STATUSES and new_status == "RUNNING")
                )

            if should_notify:
                self._notifier.notify_connector_status_change(
                    table_name=cs["table_name"],
                    job_id=cs["job_id"],
                    connector=connector,
                    from_status=prev_status or "(неизвестно)",
                    to_status=new_status,
                )
            self._known_connector_statuses[connector] = new_status

    def _notify_stale_cdc_released(self, released: list[dict[str, Any]]) -> None:
        """Alert when CDC ownership is forcibly released due to stale heartbeat."""
        assert self._notifier is not None
        for r in released:
            self._notifier.notify_cdc_ownership_lost(
                table_name=r["table_name"],
                job_id=r["job_id"],
            )

    def _notify_stale_chunks(self, reclaimed: list[dict[str, Any]]) -> None:
        """Alert when stale running chunks are reset to pending."""
        assert self._notifier is not None
        if reclaimed:
            self._notifier.notify_stale_chunks_reclaimed(reclaimed)

    def _maybe_send_status_report(self, summary: list[dict[str, Any]]) -> None:
        """Send a periodic status digest if the configured interval has elapsed."""
        assert self._notifier is not None
        now = time.monotonic()
        if now - self._last_status_report < self._config.vkteams_status_interval_seconds:
            return
        self._last_status_report = now
        self._notifier.notify_status_report(self._build_report_items(summary))

    @staticmethod
    def _build_report_items(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Merge hybrid child jobs under their parent for a clean status digest.

        Top-level items are jobs without a parent_job_id.  If a top-level job
        has a child (hybrid static), the child's status is embedded inline.
        """
        result: list[dict[str, Any]] = []
        for j in summary:
            if j["parent_job_id"]:
                # Child job — shown under its parent
                continue
            item = dict(j)
            children = [c for c in summary if c.get("parent_job_id") == j["job_id"]]
            if children:
                child = children[0]
                item["child_status"] = child["status"]
                item["child_chunks_done"] = child["chunks_done"]
                item["child_chunks_total"] = child["chunks_total"]
            result.append(item)
        return result

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    def start_background_loop(self, interval_seconds: int = 30) -> threading.Thread:
        """Start a daemon thread that calls run_once() every interval_seconds."""
        stop_event = threading.Event()

        def _loop() -> None:
            logger.info("Monitoring loop started", extra={"interval_seconds": interval_seconds})
            while not stop_event.wait(interval_seconds):
                try:
                    result = self.run_once()
                    if result.bulk_transitions:
                        logger.info("Bulk transitions", extra={"transitions": result.bulk_transitions})
                    if result.released_owners:
                        logger.info("Released stale CDC owners", extra={"job_ids": result.released_owners})
                    if result.stale_chunks_reclaimed:
                        logger.info("Reclaimed stale chunks", extra={"count": result.stale_chunks_reclaimed})
                except Exception:
                    logger.exception("Monitoring loop error")

        thread = threading.Thread(target=_loop, daemon=True, name="monitoring-loop")
        thread._stop_event = stop_event  # type: ignore[attr-defined]
        thread.start()
        return thread

    # ------------------------------------------------------------------
    # Connector status (internal helper)
    # ------------------------------------------------------------------

    def _check_connectors(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for job in self._repository.list_cdc_jobs_for_monitoring():
            if not job.debezium_connector_name:
                continue
            status = self._debezium.get_connector_status(job.debezium_connector_name)
            results.append({
                "job_id": job.job_id,
                "table_name": job.table_name,
                "connector": job.debezium_connector_name,
                "status": status,
            })
        return results
