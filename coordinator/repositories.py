from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Json

from .db import Database
from .errors import ConflictError, NotFoundError
from .models import JobRecord, RowIdChunk, SqlTemplateRecord


def normalize_table_key(table_name: str | None) -> str | None:
    raw = str(table_name or "").strip()
    if not raw:
        return None
    normalized = raw.upper()
    if "." in normalized:
        _, normalized = normalized.rsplit(".", 1)
    normalized = normalized.strip().strip('"')
    return normalized or None


class CoordinatorRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def ping(self) -> bool:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True

    def ensure_schema(self) -> None:
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE SCHEMA IF NOT EXISTS migration_system")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS migration_system.migration_jobs (
                            job_id UUID PRIMARY KEY,
                            table_name VARCHAR(200) NOT NULL,
                            target_table_name VARCHAR(200),
                            migration_mode VARCHAR(20) NOT NULL,
                            message_key_columns JSONB,
                            scn_cutoff BIGINT,
                            catchup_target BIGINT,
                            debezium_connector_name VARCHAR(200),
                            kafka_topic_name VARCHAR(200),
                            consumer_group_name VARCHAR(200),
                            debezium_config JSONB,
                            cdc_owner_worker_id VARCHAR(100),
                            cdc_started_at TIMESTAMP,
                            cdc_heartbeat_at TIMESTAMP,
                            status VARCHAR(32) NOT NULL DEFAULT 'pending',
                            config JSONB NOT NULL DEFAULT '{}'::jsonb,
                            idempotency_key VARCHAR(200),
                            created_at TIMESTAMP DEFAULT NOW(),
                            completed_at TIMESTAMP,
                            parent_job_id UUID REFERENCES migration_system.migration_jobs(job_id)
                        )
                        """
                    )
                    cur.execute(
                        "ALTER TABLE migration_system.migration_jobs"
                        " ADD COLUMN IF NOT EXISTS parent_job_id UUID"
                        " REFERENCES migration_system.migration_jobs(job_id)"
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_jobs_mode_status ON migration_system.migration_jobs (migration_mode, status)"
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_jobs_connector ON migration_system.migration_jobs (debezium_connector_name) WHERE debezium_connector_name IS NOT NULL"
                    )
                    cur.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency ON migration_system.migration_jobs (idempotency_key) WHERE idempotency_key IS NOT NULL"
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS migration_system.migration_chunks (
                            chunk_id UUID PRIMARY KEY,
                            job_id UUID NOT NULL REFERENCES migration_system.migration_jobs(job_id),
                            table_name VARCHAR(200) NOT NULL,
                            start_rowid VARCHAR(100),
                            end_rowid VARCHAR(100),
                            assigned_worker_id VARCHAR(100),
                            status VARCHAR(20) NOT NULL DEFAULT 'pending',
                            rows_processed BIGINT DEFAULT 0,
                            error_message TEXT,
                            assigned_at TIMESTAMP,
                            completed_at TIMESTAMP
                        )
                        """
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_chunks_job_status ON migration_system.migration_chunks (job_id, status)"
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS migration_system.migration_sql_templates (
                            job_id UUID PRIMARY KEY REFERENCES migration_system.migration_jobs(job_id),
                            table_name VARCHAR(200) NOT NULL,
                            pk_columns JSONB NOT NULL,
                            all_columns JSONB NOT NULL,
                            insertable_columns JSONB NOT NULL,
                            bulk_merge_sql TEXT NOT NULL,
                            bulk_insert_sql TEXT,
                            cdc_merge_sql TEXT,
                            cdc_delete_sql TEXT,
                            has_lobs BOOLEAN DEFAULT FALSE,
                            has_timestamps BOOLEAN DEFAULT FALSE,
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS migration_system._migration_offsets (
                            consumer_group VARCHAR(200) NOT NULL,
                            topic VARCHAR(200) NOT NULL,
                            partition INTEGER NOT NULL,
                            "offset" BIGINT NOT NULL,
                            updated_at TIMESTAMP DEFAULT NOW(),
                            PRIMARY KEY (consumer_group, topic, partition)
                        )
                        """
                    )
        except psycopg.errors.UniqueViolation:
            pass  # Another worker already created the schema concurrently

    def create_job(
        self,
        *,
        job_id: str | None,
        table_name: str,
        target_table_name: str,
        migration_mode: str,
        message_key_columns: list[str] | None,
        scn_cutoff: int | None,
        connector_name: str | None,
        topic_name: str | None,
        consumer_group_name: str | None,
        debezium_config: dict[str, Any] | None,
        status: str,
        idempotency_key: str | None,
        parent_job_id: str | None = None,
    ) -> JobRecord:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                if idempotency_key:
                    cur.execute(
                        "SELECT * FROM migration_system.migration_jobs WHERE idempotency_key = %s",
                        (idempotency_key,),
                    )
                    existing = cur.fetchone()
                    if existing:
                        return self._to_job(existing)

                actual_job_id = job_id or str(uuid.uuid4())
                try:
                    cur.execute(
                        """
                        INSERT INTO migration_system.migration_jobs (
                            job_id, table_name, target_table_name, migration_mode, message_key_columns,
                            scn_cutoff, debezium_connector_name, kafka_topic_name, consumer_group_name,
                            debezium_config, status, config, idempotency_key, parent_job_id
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, '{}'::jsonb, %s, %s
                        )
                        RETURNING *
                        """,
                        (
                            actual_job_id,
                            table_name,
                            target_table_name,
                            migration_mode,
                            Json(message_key_columns) if message_key_columns is not None else None,
                            scn_cutoff,
                            connector_name,
                            topic_name,
                            consumer_group_name,
                            Json(debezium_config) if debezium_config is not None else None,
                            status,
                            idempotency_key,
                            parent_job_id,
                        ),
                    )
                except Exception as exc:
                    if "idx_jobs_idempotency" in str(exc):
                        raise ConflictError("Job with this idempotency key already exists") from exc
                    raise
                row = cur.fetchone()
                return self._to_job(row)

    def save_sql_templates(self, tpl: SqlTemplateRecord) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO migration_system.migration_sql_templates (
                        job_id, table_name, pk_columns, all_columns, insertable_columns,
                        bulk_merge_sql, bulk_insert_sql, cdc_merge_sql, cdc_delete_sql,
                        has_lobs, has_timestamps
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO UPDATE SET
                        pk_columns = EXCLUDED.pk_columns,
                        all_columns = EXCLUDED.all_columns,
                        insertable_columns = EXCLUDED.insertable_columns,
                        bulk_merge_sql = EXCLUDED.bulk_merge_sql,
                        bulk_insert_sql = EXCLUDED.bulk_insert_sql,
                        cdc_merge_sql = EXCLUDED.cdc_merge_sql,
                        cdc_delete_sql = EXCLUDED.cdc_delete_sql,
                        has_lobs = EXCLUDED.has_lobs,
                        has_timestamps = EXCLUDED.has_timestamps
                    """,
                    (
                        tpl.job_id,
                        tpl.table_name,
                        Json(tpl.pk_columns),
                        Json(tpl.all_columns),
                        Json(tpl.insertable_columns),
                        tpl.bulk_merge_sql,
                        tpl.bulk_insert_sql,
                        tpl.cdc_merge_sql,
                        tpl.cdc_delete_sql,
                        tpl.has_lobs,
                        tpl.has_timestamps,
                    ),
                )

    def save_chunks(self, job_id: str, table_name: str, chunks: list[RowIdChunk]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for chunk in chunks:
                    cur.execute(
                        """
                        INSERT INTO migration_system.migration_chunks (
                            chunk_id, job_id, table_name, start_rowid, end_rowid, status
                        ) VALUES (%s, %s, %s, %s, %s, 'pending')
                        """,
                        (str(uuid.uuid4()), job_id, table_name, chunk.start_rowid, chunk.end_rowid),
                    )

    def update_job_status(self, job_id: str, status: str, *, completed: bool = False) -> JobRecord:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET status = %s,
                        completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END
                    WHERE job_id = %s
                    RETURNING *
                    """,
                    (status, completed, job_id),
                )
                row = cur.fetchone()
                if not row:
                    raise NotFoundError(f"Job not found: {job_id}")
                return self._to_job(row)

    def update_job_connector_config(self, job_id: str, debezium_config: dict[str, Any]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE migration_system.migration_jobs SET debezium_config = %s WHERE job_id = %s",
                    (Json(debezium_config), job_id),
                )

    def get_job(self, job_id: str) -> JobRecord:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM migration_system.migration_jobs WHERE job_id = %s", (job_id,))
                row = cur.fetchone()
                if not row:
                    raise NotFoundError(f"Job not found: {job_id}")
                return self._to_job(row)

    def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migration_system.migration_jobs ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        return [self._to_job(row) for row in rows]

    def list_latest_jobs_by_source_table(self) -> list[JobRecord]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM migration_system.migration_jobs
                    ORDER BY created_at DESC NULLS LAST, job_id DESC
                    """
                )
                rows = cur.fetchall()

        latest_by_table: dict[str, JobRecord] = {}
        for row in rows:
            job = self._to_job(row)
            table_key = self._normalize_table_key(job.table_name)
            if table_key and table_key not in latest_by_table:
                latest_by_table[table_key] = job

        return list(latest_by_table.values())

    def list_bulk_in_progress_jobs(self) -> list[JobRecord]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migration_system.migration_jobs WHERE status IN ('bulk_running', 'bulk_loading')"
                )
                rows = cur.fetchall()
        return [self._to_job(row) for row in rows]

    def list_chunks(self, job_id: str) -> list[dict[str, Any]]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        chunk_id, status, assigned_worker_id,
                        rows_processed, error_message,
                        assigned_at, completed_at,
                        CASE
                            WHEN completed_at IS NOT NULL AND assigned_at IS NOT NULL
                                THEN EXTRACT(EPOCH FROM (completed_at - assigned_at))
                            WHEN assigned_at IS NOT NULL
                                THEN EXTRACT(EPOCH FROM (NOW() - assigned_at))
                            ELSE NULL
                        END AS elapsed_seconds
                    FROM migration_system.migration_chunks
                    WHERE job_id = %s
                    ORDER BY assigned_at NULLS LAST, chunk_id
                    """,
                    (job_id,),
                )
                rows = cur.fetchall()

        result = []
        for row in rows:
            elapsed = row.get("elapsed_seconds")
            rows_proc = int(row.get("rows_processed") or 0)
            speed = None
            if elapsed and float(elapsed) > 0:
                speed = round(rows_proc / float(elapsed), 1)
            result.append({
                "chunk_id": str(row["chunk_id"]),
                "status": row["status"],
                "assigned_worker_id": row.get("assigned_worker_id"),
                "rows_processed": rows_proc,
                "error_message": row.get("error_message"),
                "assigned_at": row["assigned_at"].isoformat() if row.get("assigned_at") else None,
                "completed_at": row["completed_at"].isoformat() if row.get("completed_at") else None,
                "elapsed_seconds": round(float(elapsed), 1) if elapsed is not None else None,
                "rows_per_second": speed,
            })
        return result

    def delete_job(self, job_id: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM migration_system.migration_chunks WHERE job_id = %s",
                    (job_id,),
                )
                cur.execute(
                    "DELETE FROM migration_system.migration_sql_templates WHERE job_id = %s",
                    (job_id,),
                )
                cur.execute(
                    "DELETE FROM migration_system.migration_jobs WHERE job_id = %s RETURNING job_id",
                    (job_id,),
                )
                if not cur.fetchone():
                    raise NotFoundError(f"Job not found: {job_id}")

    def all_chunks_completed(self, job_id: str) -> bool:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS pending_count
                    FROM migration_system.migration_chunks
                    WHERE job_id = %s AND status <> 'completed'
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
        return int(row["pending_count"]) == 0

    def list_cdc_jobs_for_monitoring(self) -> list[JobRecord]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM migration_system.migration_jobs
                    WHERE migration_mode = 'cdc' AND status IN ('bulk_done', 'cdc_catchup', 'cdc_streaming', 'bulk_running')
                    """
                )
                rows = cur.fetchall()
        return [self._to_job(row) for row in rows]

    def release_stale_cdc_ownership(self, heartbeat_timeout_seconds: int) -> list[str]:
        cutoff = datetime.now(tz=UTC) - timedelta(seconds=heartbeat_timeout_seconds)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET cdc_owner_worker_id = NULL,
                        cdc_started_at = NULL,
                        cdc_heartbeat_at = NULL,
                        status = CASE
                            WHEN status = 'cdc_streaming' THEN 'bulk_done'
                            ELSE status
                        END
                    WHERE migration_mode = 'cdc'
                      AND cdc_owner_worker_id IS NOT NULL
                      AND cdc_heartbeat_at IS NOT NULL
                      AND cdc_heartbeat_at < %s
                    RETURNING job_id
                    """,
                    (cutoff,),
                )
                rows = cur.fetchall()
        return [str(row["job_id"]) for row in rows]

    def _to_job(self, row: dict[str, Any]) -> JobRecord:
        raw_config = row.get("config") or {}
        if isinstance(raw_config, str):
            raw_config = json.loads(raw_config)
        raw_debezium = row.get("debezium_config")
        if isinstance(raw_debezium, str):
            raw_debezium = json.loads(raw_debezium)

        raw_parent = row.get("parent_job_id")
        return JobRecord(
            job_id=str(row["job_id"]),
            table_name=row["table_name"],
            target_table_name=row.get("target_table_name") or row["table_name"],
            migration_mode=row["migration_mode"],
            message_key_columns=row.get("message_key_columns"),
            scn_cutoff=row.get("scn_cutoff"),
            catchup_target=row.get("catchup_target"),
            debezium_connector_name=row.get("debezium_connector_name"),
            kafka_topic_name=row.get("kafka_topic_name"),
            consumer_group_name=row.get("consumer_group_name"),
            debezium_config=raw_debezium,
            cdc_owner_worker_id=row.get("cdc_owner_worker_id"),
            cdc_started_at=row.get("cdc_started_at"),
            cdc_heartbeat_at=row.get("cdc_heartbeat_at"),
            status=row["status"],
            config=raw_config,
            idempotency_key=row.get("idempotency_key"),
            created_at=row.get("created_at"),
            completed_at=row.get("completed_at"),
            parent_job_id=str(raw_parent) if raw_parent else None,
        )

    # -------------------------------------------------------------------------
    # Worker methods
    # -------------------------------------------------------------------------

    def claim_bulk_chunk(self, worker_id: str) -> dict[str, Any] | None:
        """Atomically claim one pending bulk chunk via SKIP LOCKED."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_chunks
                    SET status = 'running',
                        assigned_worker_id = %s,
                        assigned_at = NOW()
                    WHERE chunk_id = (
                        SELECT c.chunk_id
                        FROM migration_system.migration_chunks c
                        JOIN migration_system.migration_jobs j ON c.job_id = j.job_id
                        WHERE c.status = 'pending'
                          AND j.status IN ('bulk_running', 'bulk_loading')
                        ORDER BY c.chunk_id
                        LIMIT 1
                        FOR UPDATE OF c SKIP LOCKED
                    )
                    RETURNING chunk_id, job_id, table_name, start_rowid, end_rowid
                    """,
                    (worker_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def complete_chunk(self, chunk_id: str, rows_processed: int) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_chunks
                    SET status = 'completed', rows_processed = %s, completed_at = NOW()
                    WHERE chunk_id = %s
                    """,
                    (rows_processed, chunk_id),
                )

    def fail_chunk(self, chunk_id: str, error_message: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_chunks
                    SET status = 'failed', error_message = %s
                    WHERE chunk_id = %s
                    """,
                    (error_message[:2000], chunk_id),
                )

    def get_sql_templates(self, job_id: str) -> dict[str, Any] | None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migration_system.migration_sql_templates WHERE job_id = %s",
                    (job_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def try_claim_cdc_job(self, worker_id: str) -> JobRecord | None:
        """Claim a single CDC job (legacy, used by MigrationWorker)."""
        jobs = self.claim_cdc_jobs_batch(worker_id, limit=1)
        return jobs[0] if jobs else None

    def claim_cdc_jobs_batch(self, worker_id: str, limit: int = 20) -> list[JobRecord]:
        """Atomically claim up to `limit` unclaimed CDC jobs for this worker.

        Picks jobs with migration_mode='cdc' that have no current owner.
        bulk_done → cdc_catchup on claim.
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET cdc_owner_worker_id = %s,
                        cdc_started_at = COALESCE(cdc_started_at, NOW()),
                        cdc_heartbeat_at = NOW(),
                        status = CASE WHEN status = 'bulk_done' THEN 'cdc_catchup' ELSE status END
                    WHERE job_id IN (
                        SELECT job_id FROM migration_system.migration_jobs
                        WHERE migration_mode = 'cdc'
                          AND status IN ('bulk_done', 'cdc_catchup')
                          AND cdc_owner_worker_id IS NULL
                        ORDER BY created_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *
                    """,
                    (worker_id, limit),
                )
                rows = cur.fetchall()
        return [self._to_job(r) for r in rows]

    def release_all_cdc_jobs(self, worker_id: str) -> None:
        """Release ownership of all CDC jobs held by this worker (called on CDCWorker shutdown)."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET cdc_owner_worker_id = NULL,
                        cdc_started_at = NULL,
                        cdc_heartbeat_at = NULL
                    WHERE cdc_owner_worker_id = %s
                    """,
                    (worker_id,),
                )

    def list_child_jobs(self, parent_job_id: str) -> list[JobRecord]:
        """Return all jobs whose parent_job_id matches (e.g. hybrid static children)."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migration_system.migration_jobs WHERE parent_job_id = %s",
                    (parent_job_id,),
                )
                rows = cur.fetchall()
        return [self._to_job(r) for r in rows]

    def activate_child_jobs(self, parent_job_id: str) -> list[JobRecord]:
        """Transition pending child static jobs to bulk_running (called when parent reaches bulk_done)."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET status = 'bulk_running'
                    WHERE parent_job_id = %s
                      AND migration_mode = 'static'
                      AND status = 'pending'
                    RETURNING *
                    """,
                    (parent_job_id,),
                )
                rows = cur.fetchall()
        activated = [self._to_job(r) for r in rows]
        return activated

    def release_cdc_ownership(self, job_id: str, worker_id: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET cdc_owner_worker_id = NULL,
                        cdc_started_at = NULL,
                        cdc_heartbeat_at = NULL
                    WHERE job_id = %s AND cdc_owner_worker_id = %s
                    """,
                    (job_id, worker_id),
                )

    def update_cdc_heartbeat(self, job_id: str, worker_id: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_jobs
                    SET cdc_heartbeat_at = NOW()
                    WHERE job_id = %s AND cdc_owner_worker_id = %s
                    """,
                    (job_id, worker_id),
                )

    def set_catchup_target(self, job_id: str, catchup_target: int) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE migration_system.migration_jobs SET catchup_target = %s WHERE job_id = %s",
                    (catchup_target, job_id),
                )

    def get_kafka_offsets(self, consumer_group: str, topic: str) -> dict[int, int]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT partition, "offset"
                    FROM migration_system._migration_offsets
                    WHERE consumer_group = %s AND topic = %s
                    """,
                    (consumer_group, topic),
                )
                rows = cur.fetchall()
        return {int(row["partition"]): int(row["offset"]) for row in rows}

    def save_kafka_offsets(self, consumer_group: str, topic: str, offsets: dict[int, int]) -> None:
        if not offsets:
            return
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for partition, offset in offsets.items():
                    cur.execute(
                        """
                        INSERT INTO migration_system._migration_offsets
                            (consumer_group, topic, partition, "offset", updated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (consumer_group, topic, partition) DO UPDATE
                        SET "offset" = EXCLUDED."offset", updated_at = NOW()
                        """,
                        (consumer_group, topic, partition, offset),
                    )

    def reclaim_stale_chunks(self, timeout_seconds: int) -> int:
        """Reset running chunks stuck longer than timeout back to pending."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE migration_system.migration_chunks
                    SET status = 'pending',
                        assigned_worker_id = NULL,
                        assigned_at = NULL,
                        error_message = 'Reclaimed after timeout'
                    WHERE status = 'running'
                      AND assigned_at < NOW() - (%s * INTERVAL '1 second')
                    """,
                    (timeout_seconds,),
                )
                return cur.rowcount

    @staticmethod
    def _normalize_table_key(table_name: str | None) -> str | None:
        return normalize_table_key(table_name)

