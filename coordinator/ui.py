from __future__ import annotations

import logging
from typing import Any

from flask import Blueprint, render_template

from .config import AppConfig
from .oracle import OracleClientConfig, OracleIntrospector
from .repositories import CoordinatorRepository, normalize_table_key
from .status_checks import collect_dashboard_service_statuses

logger = logging.getLogger(__name__)


def build_ui_blueprint(config: AppConfig, repository: CoordinatorRepository) -> Blueprint:
    bp = Blueprint("coordinator_ui", __name__, template_folder="templates")

    @bp.get("/")
    def dashboard():
        service_statuses = collect_dashboard_service_statuses(config, repository)

        table_statuses: list[dict[str, Any]] = []
        jobs_error: str | None = None
        source_tables_error: str | None = None

        jobs_by_table: dict[str, dict[str, Any]] = {}
        try:
            latest_jobs = [job.to_dict() for job in repository.list_latest_jobs_by_source_table()]
            jobs_by_table = {
                normalize_table_key(job.get("table_name")): job
                for job in latest_jobs
                if normalize_table_key(job.get("table_name"))
            }
        except Exception:
            jobs_error = "state db is unavailable"
            logger.exception("Failed to load jobs for dashboard")

        source_tables: list[str] = []
        try:
            source_tables = _list_source_tables(config)
        except Exception:
            source_tables_error = "не удалось загрузить список таблиц source"
            logger.exception("Failed to load source tables for dashboard")

        for source_table in source_tables:
            normalized_table = normalize_table_key(source_table)
            job = jobs_by_table.get(normalized_table)
            table_statuses.append(
                {
                    "source_table": source_table,
                    "target_table": job.get("target_table_name") if job else "—",
                    "mode": job.get("migration_mode") if job else "—",
                    "status": job.get("status") if job else "не мигрирована",
                    "job_id": job.get("job_id") if job else "—",
                }
            )

        return render_template(
            "dashboard.html",
            service_statuses=service_statuses,
            table_statuses=table_statuses,
            jobs_error=jobs_error,
            source_tables_error=source_tables_error,
        )

    return bp


def _list_source_tables(config: AppConfig) -> list[str]:
    source_cfg = (
        OracleClientConfig(
            dsn=config.oracle_source_dsn,
            user=config.oracle_source_user,
            password=config.oracle_source_password,
        )
        if config.oracle_source_dsn and config.oracle_source_user and config.oracle_source_password
        else None
    )
    introspector = OracleIntrospector(
        source=source_cfg,
        target=None,
        source_schema=config.oracle_source_schema,
        target_schema=None,
    )
    return introspector.list_source_tables()
