from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from .config import AppConfig, normalize_oracle_identifier
from .debezium import DebeziumClient
from .errors import ValidationError
from .models import JobRecord, OracleTableMetadata
from .oracle import OracleClientConfig, OracleIntrospector
from .repositories import CoordinatorRepository
from .sql_templates import build_sql_templates

logger = logging.getLogger(__name__)


def _resolve_table_identifier(table_name: str, default_schema: str | None) -> tuple[str, str, str]:
    raw = str(table_name or "").strip()
    if not raw:
        raise ValidationError("table_name is required")

    schema: str | None = None
    table = raw
    if "." in raw:
        schema, table = raw.split(".", 1)
    else:
        schema = default_schema

    normalized_schema = normalize_oracle_identifier(schema)
    normalized_table = normalize_oracle_identifier(table)
    if not normalized_schema:
        raise ValidationError(
            "table schema is required",
            details={"table_name": table_name, "expected": "SCHEMA.TABLE or configured schema"},
        )
    if not normalized_table:
        raise ValidationError("table name is invalid", details={"table_name": table_name})

    qualified = f"{normalized_schema}.{normalized_table}"
    return qualified, normalized_schema, normalized_table


def _runtime_table_token(schema: str, table: str) -> str:
    return f"{schema}_{table}"


@dataclass(slots=True)
class CreateJobRequest:
    table_name: str
    target_table_name: str | None
    migration_mode: str
    message_key_columns: list[str] | None
    idempotency_key: str | None
    chunk_count: int | None


class CoordinatorService:
    def __init__(self, config: AppConfig, repository: CoordinatorRepository) -> None:
        self._config = config
        self._repository = repository
        self._debezium = DebeziumClient(config.debezium_connect_url)
        source_cfg = (
            OracleClientConfig(
                dsn=config.oracle_source_dsn,
                user=config.oracle_source_user,
                password=config.oracle_source_password,
            )
            if config.oracle_source_dsn and config.oracle_source_user and config.oracle_source_password
            else None
        )
        target_cfg = (
            OracleClientConfig(
                dsn=config.oracle_target_dsn,
                user=config.oracle_target_user,
                password=config.oracle_target_password,
            )
            if config.oracle_target_dsn and config.oracle_target_user and config.oracle_target_password
            else None
        )
        self._oracle = OracleIntrospector(
            source_cfg,
            target_cfg,
            source_schema=config.oracle_source_schema,
            target_schema=config.oracle_target_schema,
        )

    def create_job(self, request: CreateJobRequest) -> JobRecord:
        mode = request.migration_mode.lower().strip()
        if mode not in {"cdc", "static"}:
            raise ValidationError("migration_mode must be 'cdc' or 'static'")

        source_table, source_schema, source_table_only = _resolve_table_identifier(
            request.table_name,
            self._config.oracle_source_schema,
        )
        target_table, _, _ = _resolve_table_identifier(
            request.target_table_name or request.table_name,
            self._config.oracle_target_schema,
        )
        chunk_count = request.chunk_count or self._config.default_chunk_count
        metadata = self._oracle.fetch_table_metadata(target_table)

        key_columns = self._resolve_key_columns(
            migration_mode=mode,
            metadata=metadata,
            message_key_columns=request.message_key_columns,
        )

        if mode == "cdc":
            return self._create_cdc_job(
                source_table=source_table,
                source_schema=source_schema,
                source_table_only=source_table_only,
                target_table=target_table,
                metadata=metadata,
                key_columns=key_columns,
                idempotency_key=request.idempotency_key,
                chunk_count=chunk_count,
            )

        return self._create_static_job(
            source_table=source_table,
            target_table=target_table,
            metadata=metadata,
            key_columns=key_columns,
            idempotency_key=request.idempotency_key,
            chunk_count=chunk_count,
        )

    def get_job(self, job_id: str) -> JobRecord:
        return self._repository.get_job(job_id)

    def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        return self._repository.list_jobs(limit=limit)

    def delete_job(self, job_id: str) -> dict:
        job = self._repository.get_job(job_id)
        debezium_warning: str | None = None
        if job.debezium_connector_name:
            err = self._debezium.delete_connector(job.debezium_connector_name)
            if err:
                debezium_warning = (
                    f"Коннектор Debezium «{job.debezium_connector_name}» "
                    f"не удалось удалить автоматически ({err}). "
                    f"Удалите его вручную через Kafka Connect."
                )
                logger.warning(
                    "Failed to delete Debezium connector during job deletion",
                    extra={"job_id": job_id, "connector": job.debezium_connector_name, "error": err},
                )
        self._repository.delete_job(job_id)
        return {"debezium_warning": debezium_warning}

    def _resolve_key_columns(
        self,
        *,
        migration_mode: str,
        metadata: OracleTableMetadata,
        message_key_columns: list[str] | None,
    ) -> list[str]:
        if metadata.pk_columns:
            return metadata.pk_columns

        if migration_mode == "cdc" and not message_key_columns:
            raise ValidationError(
                "Primary key not found; message_key_columns are required for CDC mode",
                details={"table_name": metadata.table_name},
            )

        if message_key_columns:
            normalized = [column.upper() for column in message_key_columns]
            self._oracle.validate_key_columns(metadata, normalized)
            return normalized

        if migration_mode == "static":
            if not metadata.insertable_columns:
                raise ValidationError("Cannot process table without insertable columns")
            return [metadata.insertable_columns[0]]

        raise ValidationError("Unable to determine key columns")

    def _create_cdc_job(
        self,
        *,
        source_table: str,
        source_schema: str,
        source_table_only: str,
        target_table: str,
        metadata: OracleTableMetadata,
        key_columns: list[str],
        idempotency_key: str | None,
        chunk_count: int,
    ) -> JobRecord:
        job_id = str(uuid.uuid4())
        scn_cutoff = self._oracle.read_current_scn()
        connector_name, topic_name, consumer_group = self._build_runtime_names(
            source_schema,
            source_table_only,
            job_id,
        )

        debezium_config = self._build_debezium_config(
            connector_name=connector_name,
            source_table=source_table,
            source_schema=source_schema,
            source_table_only=source_table_only,
            scn_cutoff=scn_cutoff,
            key_columns=key_columns,
            metadata=metadata,
        )

        self._debezium.create_connector(connector_name, debezium_config)

        job = self._repository.create_job(
            job_id=job_id,
            table_name=source_table,
            target_table_name=target_table,
            migration_mode="cdc",
            message_key_columns=None if metadata.pk_columns else key_columns,
            scn_cutoff=scn_cutoff,
            connector_name=connector_name,
            topic_name=topic_name,
            consumer_group_name=consumer_group,
            debezium_config=debezium_config,
            status="cdc_accumulating",
            idempotency_key=idempotency_key,
        )

        sql_templates = build_sql_templates(
            job_id=job.job_id,
            table_name=target_table,
            metadata=metadata,
            key_columns=key_columns,
            migration_mode="cdc",
        )
        self._repository.save_sql_templates(sql_templates)

        self._debezium.wait_for_running(
            connector_name,
            timeout_seconds=self._config.monitor_connector_start_timeout_seconds,
            poll_interval_seconds=self._config.monitor_connector_poll_interval_seconds,
        )

        chunks = self._oracle.build_rowid_chunks(source_table, chunk_count)
        self._repository.save_chunks(job.job_id, source_table, chunks)
        return self._repository.update_job_status(job.job_id, "bulk_running")

    def _create_static_job(
        self,
        *,
        source_table: str,
        target_table: str,
        metadata: OracleTableMetadata,
        key_columns: list[str],
        idempotency_key: str | None,
        chunk_count: int,
    ) -> JobRecord:
        job = self._repository.create_job(
            job_id=None,
            table_name=source_table,
            target_table_name=target_table,
            migration_mode="static",
            message_key_columns=None,
            scn_cutoff=None,
            connector_name=None,
            topic_name=None,
            consumer_group_name=None,
            debezium_config=None,
            status="pending",
            idempotency_key=idempotency_key,
        )

        sql_templates = build_sql_templates(
            job_id=job.job_id,
            table_name=target_table,
            metadata=metadata,
            key_columns=key_columns,
            migration_mode="static",
        )
        self._repository.save_sql_templates(sql_templates)

        chunks = self._oracle.build_rowid_chunks(source_table, chunk_count)
        self._repository.save_chunks(job.job_id, source_table, chunks)
        return self._repository.update_job_status(job.job_id, "bulk_running")

    def _build_runtime_names(self, source_schema: str, source_table: str, job_id: str) -> tuple[str, str, str]:
        source_token = _runtime_table_token(source_schema, source_table)
        connector_name = f"migration-{source_token}.{job_id}"
        topic_name = f"{self._config.topic_prefix}.{source_token}.{job_id}"
        consumer_group = f"migration-consumer-{job_id}"
        return connector_name, topic_name, consumer_group

    def _build_debezium_config(
        self,
        *,
        connector_name: str,
        source_table: str,
        source_schema: str,
        source_table_only: str,
        scn_cutoff: int,
        key_columns: list[str],
        metadata: OracleTableMetadata,
    ) -> dict[str, Any]:
        cfg_updates: dict[str, Any] = {
            "name": connector_name,
            "table.include.list": f"{source_schema}.{source_table_only}",
            "log.mining.start.scn": str(scn_cutoff),
        }
        if not metadata.pk_columns and key_columns:
            cfg_updates["message.key.columns"] = f"{source_table}:{','.join(key_columns)}"

        return self._config.merge_connector_template(cfg_updates)

