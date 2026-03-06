from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .config import AppConfig, normalize_oracle_identifier
from .debezium import DebeziumClient
from .errors import ValidationError
from .models import JobRecord, OracleTableMetadata
from .oracle import OracleClientConfig, OracleIntrospector
from .repositories import CoordinatorRepository
from .sql_templates import build_sql_templates

if TYPE_CHECKING:
    from .notifications import VKTeamsNotifier

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
    """Build a safe token for use in Kafka topic names and connector names.

    Kafka topic names allow only [a-zA-Z0-9._-].  Characters outside that set
    (e.g. '#', '$', '@', spaces) are replaced with underscores so that Oracle
    table names such as 'SCHEMA.TABLE#1' don't break connector/topic creation.
    """
    raw = f"{schema}_{table}"
    return re.sub(r"[^a-zA-Z0-9._\-]", "_", raw)


@dataclass(slots=True)
class CreateJobRequest:
    table_name: str
    target_table_name: str | None
    migration_mode: str
    message_key_columns: list[str] | None
    idempotency_key: str | None
    chunk_size: int | None
    recent_rows: int | None = None  # hybrid mode: rows for CDC phase (rest → static background)


class CoordinatorService:
    def __init__(
        self,
        config: AppConfig,
        repository: CoordinatorRepository,
        notifier: VKTeamsNotifier | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._notifier = notifier
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
        if mode not in {"cdc", "static", "hybrid"}:
            raise ValidationError("migration_mode must be 'cdc', 'static', or 'hybrid'")

        source_table, source_schema, source_table_only = _resolve_table_identifier(
            request.table_name,
            self._config.oracle_source_schema,
        )
        target_table, _, _ = _resolve_table_identifier(
            request.target_table_name or request.table_name,
            self._config.oracle_target_schema,
        )
        chunk_size = request.chunk_size or self._config.default_chunk_size
        metadata = self._oracle.fetch_table_metadata(target_table)

        key_columns = self._resolve_key_columns(
            migration_mode=mode if mode != "hybrid" else "cdc",
            metadata=metadata,
            message_key_columns=request.message_key_columns,
        )

        self._repository.upsert_migration_table(source_table)

        if mode == "hybrid":
            return self._create_hybrid_job(
                source_table=source_table,
                source_schema=source_schema,
                source_table_only=source_table_only,
                target_table=target_table,
                metadata=metadata,
                key_columns=key_columns,
                idempotency_key=request.idempotency_key,
                chunk_size=chunk_size,
                recent_rows=request.recent_rows or self._config.hybrid_recent_rows,
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
                chunk_size=chunk_size,
            )

        return self._create_static_job(
            source_table=source_table,
            target_table=target_table,
            metadata=metadata,
            key_columns=key_columns,
            idempotency_key=request.idempotency_key,
            chunk_size=chunk_size,
        )

    def get_job(self, job_id: str) -> JobRecord:
        return self._repository.get_job(job_id)

    def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        return self._repository.list_jobs(limit=limit)

    def delete_jobs_for_table(self, table_name: str) -> dict:
        """Delete all top-level jobs (and their children/connectors) for a source table."""
        jobs = self._repository.list_jobs_for_table(table_name)
        top_level = [j for j in jobs if not j.parent_job_id]
        warnings: list[str] = []
        for job in top_level:
            result = self.delete_job(job.job_id)
            if result.get("debezium_warning"):
                warnings.append(result["debezium_warning"])
        return {"deleted_jobs": len(top_level), "debezium_warnings": warnings}

    def delete_job(self, job_id: str) -> dict:
        job = self._repository.get_job(job_id)
        debezium_warning: str | None = None

        # For hybrid parent jobs: delete child static jobs first (FK constraint)
        child_jobs = self._repository.list_child_jobs(job_id)
        for child in child_jobs:
            self._repository.delete_job(child.job_id)
            logger.info(
                "Deleted child static job as part of hybrid parent deletion",
                extra={"parent_job_id": job_id, "child_job_id": child.job_id},
            )

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
        chunk_size: int,
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
            topic_name=topic_name,
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

        chunks = self._oracle.build_rowid_chunks(source_table, chunk_size)
        self._repository.save_chunks(job.job_id, source_table, chunks)
        result = self._repository.update_job_status(job.job_id, "bulk_running")
        if self._notifier:
            self._notifier.notify_job_added(
                table_name=source_table,
                mode="cdc",
                chunk_count=len(chunks),
                job_id=result.job_id,
            )
        return result

    def _create_hybrid_job(
        self,
        *,
        source_table: str,
        source_schema: str,
        source_table_only: str,
        target_table: str,
        metadata: OracleTableMetadata,
        key_columns: list[str],
        idempotency_key: str | None,
        chunk_size: int,
        recent_rows: int,
    ) -> JobRecord:
        """Create a hybrid migration: CDC job for recent rows + child static job for historical rows.

        The CDC parent job starts immediately (bulk_running).
        The child static job starts in 'pending' and is activated by the monitoring service
        when the parent transitions to 'bulk_done' (all recent chunks processed).
        """
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
            topic_name=topic_name,
            scn_cutoff=scn_cutoff,
            key_columns=key_columns,
            metadata=metadata,
        )

        self._debezium.create_connector(connector_name, debezium_config)

        parent_job = self._repository.create_job(
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
            parent_job_id=None,
        )

        sql_templates = build_sql_templates(
            job_id=parent_job.job_id,
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

        # Split chunks: recent → CDC parent, historical → static child
        recent_chunks, historical_chunks = self._oracle.build_hybrid_split(
            source_table, chunk_size, recent_rows
        )

        self._repository.save_chunks(parent_job.job_id, source_table, recent_chunks)
        parent_job = self._repository.update_job_status(parent_job.job_id, "bulk_running")

        child_job_id: str | None = None
        if historical_chunks:
            child_job = self._repository.create_job(
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
                # Starts pending — monitoring activates it when parent reaches bulk_done
                status="pending",
                idempotency_key=None,
                parent_job_id=parent_job.job_id,
            )
            child_sql_templates = build_sql_templates(
                job_id=child_job.job_id,
                table_name=target_table,
                metadata=metadata,
                key_columns=key_columns,
                migration_mode="static",
            )
            self._repository.save_sql_templates(child_sql_templates)
            self._repository.save_chunks(child_job.job_id, source_table, historical_chunks)
            child_job_id = child_job.job_id
            logger.info(
                "Hybrid job created: %d recent chunks (CDC parent) + %d historical chunks (static child)",
                len(recent_chunks), len(historical_chunks),
                extra={"parent_job_id": parent_job.job_id, "child_job_id": child_job.job_id},
            )
        else:
            logger.info(
                "Hybrid job created: table fits in recent window, no static child needed (%d chunks)",
                len(recent_chunks),
                extra={"parent_job_id": parent_job.job_id},
            )

        if self._notifier:
            total_chunks = len(recent_chunks) + len(historical_chunks)
            self._notifier.notify_job_added(
                table_name=source_table,
                mode="hybrid",
                chunk_count=total_chunks,
                job_id=parent_job.job_id,
                child_job_id=child_job_id,
            )

        return parent_job

    def _create_static_job(
        self,
        *,
        source_table: str,
        target_table: str,
        metadata: OracleTableMetadata,
        key_columns: list[str],
        idempotency_key: str | None,
        chunk_size: int,
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

        chunks = self._oracle.build_rowid_chunks(source_table, chunk_size)
        self._repository.save_chunks(job.job_id, source_table, chunks)
        result = self._repository.update_job_status(job.job_id, "bulk_running")
        if self._notifier:
            self._notifier.notify_job_added(
                table_name=source_table,
                mode="static",
                chunk_count=len(chunks),
                job_id=result.job_id,
            )
        return result

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
        topic_name: str,
        scn_cutoff: int,
        key_columns: list[str],
        metadata: OracleTableMetadata,
    ) -> dict[str, Any]:
        # Java regex for ByLogicalTableRouter.
        # We anchor only the SUFFIX (.SCHEMA.TABLE$) because in CDB mode Debezium
        # inserts the container/PDB name into the topic, e.g.:
        #   migration.TCBPAY.MERC_CONFIG          (non-CDB / PDB mode)
        #   migration.ORCLCDB.TCBPAY.MERC_CONFIG  (CDB mode)
        # Each connector captures exactly one table, so ".*" at the start is safe.
        # Debezium normalizes '#' in Oracle table names to '_' in emitted topic table tokens.
        normalized_topic_table = source_table_only.replace("#", "_")
        default_topic_regex = (
            ".*\\."
            + re.escape(source_schema)
            + "\\."
            + re.escape(normalized_topic_table)
            + "$"
        )
        cfg_updates: dict[str, Any] = {
            "name": connector_name,
            "table.include.list": f"{source_schema}.{source_table_only}",
            "schema.include.list": source_schema,
            "log.mining.start.scn": str(scn_cutoff),
            "schema.history.internal.kafka.topic": (
                f"{self._config.debezium_schema_history_topic_prefix}.{connector_name}"
            ),
            "transforms.route.topic.regex": default_topic_regex,
            "transforms.route.topic.replacement": topic_name,
        }
        if not metadata.pk_columns and key_columns:
            cfg_updates["message.key.columns"] = f"{source_table}:{','.join(key_columns)}"

        return self._config.merge_connector_template(cfg_updates)

