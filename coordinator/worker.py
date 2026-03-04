from __future__ import annotations

import json
import logging
import signal
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from .config import AppConfig
from .errors import ExternalServiceError, NotFoundError, ValidationError
from .models import JobRecord
from .repositories import CoordinatorRepository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Oracle helpers
# ---------------------------------------------------------------------------


@contextmanager
def _oracle_connect(dsn: str, user: str, password: str) -> Iterator[Any]:
    try:
        import oracledb
    except ImportError as exc:
        raise ExternalServiceError("oracledb package is required for worker") from exc
    try:
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as exc:
        raise ExternalServiceError(
            "Cannot connect to Oracle", details={"dsn": dsn}
        ) from exc
    try:
        yield conn
    finally:
        conn.close()


def _source_cfg(config: AppConfig) -> tuple[str, str, str]:
    if not (config.oracle_source_dsn and config.oracle_source_user and config.oracle_source_password):
        raise ValidationError("Oracle source connection is not fully configured")
    return config.oracle_source_dsn, config.oracle_source_user, config.oracle_source_password


def _target_cfg(config: AppConfig) -> tuple[str, str, str]:
    if not (config.oracle_target_dsn and config.oracle_target_user and config.oracle_target_password):
        raise ValidationError("Oracle target connection is not fully configured")
    return config.oracle_target_dsn, config.oracle_target_user, config.oracle_target_password


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------


def _qualified_table(table_name: str) -> str:
    schema, table = table_name.split(".", 1)
    return f'"{schema}"."{table}"'


def _build_source_select(
    table_name: str,
    columns: list[str],
    scn_cutoff: int | None,
    start_rowid: str | None,
    end_rowid: str | None,
) -> tuple[str, dict[str, Any]]:
    qualified = _qualified_table(table_name)
    cols_str = ", ".join(f'"{c}"' for c in columns)
    params: dict[str, Any] = {}

    from_clause = f"{qualified} AS OF SCN :scn" if scn_cutoff else qualified
    if scn_cutoff:
        params["scn"] = scn_cutoff

    if start_rowid:
        where = " WHERE ROWID BETWEEN :start_rowid AND :end_rowid"
        params["start_rowid"] = start_rowid
        params["end_rowid"] = end_rowid
    else:
        where = ""

    return f"SELECT {cols_str} FROM {from_clause}{where}", params


def _build_bulk_merge_sql(table_name: str, columns: list[str], pk_columns: list[str]) -> str:
    """MERGE with WHEN NOT MATCHED only — idempotent bulk load."""
    qualified = _qualified_table(table_name)
    dual_select = ", ".join(f':{c} AS "{c}"' for c in columns)
    merge_on = " AND ".join(f't."{k}" = s."{k}"' for k in pk_columns)
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f's."{c}"' for c in columns)
    return (
        f"MERGE INTO {qualified} t "
        f"USING (SELECT {dual_select} FROM DUAL) s "
        f"ON ({merge_on}) "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def _build_bulk_insert_sql(table_name: str, columns: list[str]) -> str:
    """Plain INSERT for bulk load — use with executemany(batcherrors=True) for idempotency.
    Much faster than MERGE-via-DUAL: Oracle can apply array-INSERT optimisation."""
    qualified = _qualified_table(table_name)
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f':{c}' for c in columns)
    return f"INSERT INTO {qualified} ({insert_cols}) VALUES ({insert_vals})"


def _build_cdc_merge_sql(table_name: str, columns: list[str], pk_columns: list[str]) -> str:
    """Full MERGE (update existing + insert new) for CDC apply."""
    qualified = _qualified_table(table_name)
    non_key = [c for c in columns if c not in pk_columns]
    dual_select = ", ".join(f':{c} AS "{c}"' for c in columns)
    merge_on = " AND ".join(f't."{k}" = s."{k}"' for k in pk_columns)
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f's."{c}"' for c in columns)
    matched = ""
    if non_key:
        update_set = ", ".join(f't."{c}" = s."{c}"' for c in non_key)
        matched = f"WHEN MATCHED THEN UPDATE SET {update_set} "
    return (
        f"MERGE INTO {qualified} t "
        f"USING (SELECT {dual_select} FROM DUAL) s "
        f"ON ({merge_on}) "
        f"{matched}"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def _build_cdc_delete_sql(table_name: str, pk_columns: list[str]) -> str:
    qualified = _qualified_table(table_name)
    conditions = " AND ".join(f'"{k}" = :{k}' for k in pk_columns)
    return f"DELETE FROM {qualified} WHERE {conditions}"


# ---------------------------------------------------------------------------
# Heartbeat thread
# ---------------------------------------------------------------------------


class _HeartbeatThread(threading.Thread):
    def __init__(
        self,
        repository: CoordinatorRepository,
        job_id: str,
        worker_id: str,
        interval_seconds: int,
    ) -> None:
        super().__init__(daemon=True, name=f"heartbeat-{job_id[:8]}")
        self._repository = repository
        self._job_id = job_id
        self._worker_id = worker_id
        self._interval = interval_seconds
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._repository.update_cdc_heartbeat(self._job_id, self._worker_id)
                logger.debug("Heartbeat sent", extra={"job_id": self._job_id})
            except Exception:
                logger.warning("Heartbeat update failed", extra={"job_id": self._job_id})

    def stop(self) -> None:
        self._stop.set()


# ---------------------------------------------------------------------------
# BulkChunkProcessor
# ---------------------------------------------------------------------------


class BulkChunkProcessor:
    """Transfers one ROWID chunk from Oracle source to Oracle target."""

    def __init__(self, config: AppConfig, repository: CoordinatorRepository) -> None:
        self._config = config
        self._repository = repository

    def process(self, chunk: dict[str, Any]) -> int:
        """Process chunk and return number of rows transferred."""
        job = self._repository.get_job(str(chunk["job_id"]))
        templates = self._repository.get_sql_templates(job.job_id)
        if not templates:
            raise RuntimeError(f"No SQL templates found for job {job.job_id}")

        insertable_cols: list[str] = templates["insertable_columns"]
        pk_cols: list[str] = templates["pk_columns"]
        source_table = job.table_name
        target_table = job.target_table_name or job.table_name
        scn_cutoff = job.scn_cutoff  # None for STATIC mode
        start_rowid: str | None = chunk.get("start_rowid")
        end_rowid: str | None = chunk.get("end_rowid")

        select_sql, select_params = _build_source_select(
            source_table, insertable_cols, scn_cutoff, start_rowid, end_rowid
        )
        insert_sql = _build_bulk_insert_sql(target_table, insertable_cols)

        src_dsn, src_user, src_pwd = _source_cfg(self._config)
        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)
        rows_processed = 0
        batch_size = self._config.worker_bulk_batch_size

        with _oracle_connect(src_dsn, src_user, src_pwd) as src_conn:
            with _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:
                with src_conn.cursor() as src_cur:
                    src_cur.arraysize = batch_size
                    src_cur.execute(select_sql, select_params)
                    col_names = [d[0].upper() for d in src_cur.description]
                    with tgt_conn.cursor() as tgt_cur:
                        while True:
                            rows = src_cur.fetchmany(batch_size)
                            if not rows:
                                break
                            batch = [dict(zip(col_names, row)) for row in rows]
                            tgt_cur.executemany(insert_sql, batch, batcherrors=True)
                            tgt_conn.commit()
                            rows_processed += len(batch)
                            logger.debug(
                                "Bulk batch applied",
                                extra={"chunk_id": str(chunk["chunk_id"]), "batch": len(batch), "total": rows_processed},
                            )

        return rows_processed


# ---------------------------------------------------------------------------
# CDCConsumer
# ---------------------------------------------------------------------------


class CDCConsumer:
    """Applies Debezium/Kafka CDC events to Oracle target. Blocking."""

    def __init__(
        self,
        config: AppConfig,
        repository: CoordinatorRepository,
        worker_id: str,
        shutdown_event: threading.Event,
    ) -> None:
        self._config = config
        self._repository = repository
        self._worker_id = worker_id
        self._shutdown_event = shutdown_event

    def run(self, job: JobRecord) -> None:
        try:
            from confluent_kafka import Consumer, TopicPartition
        except ImportError as exc:
            raise ExternalServiceError("confluent_kafka is required for CDC processing") from exc

        templates = self._repository.get_sql_templates(job.job_id)
        if not templates:
            raise RuntimeError(f"No SQL templates found for job {job.job_id}")

        insertable_cols: list[str] = templates["insertable_columns"]
        pk_cols: list[str] = templates["pk_columns"]
        topic: str = job.kafka_topic_name  # type: ignore[assignment]
        group_id: str = job.consumer_group_name  # type: ignore[assignment]
        target_table = job.target_table_name or job.table_name

        merge_sql = _build_cdc_merge_sql(target_table, insertable_cols, pk_cols)
        delete_sql = _build_cdc_delete_sql(target_table, pk_cols)

        consumer = Consumer(
            {
                "bootstrap.servers": self._config.kafka_bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
            }
        )
        logger.info("CDC consumer created", extra={"job_id": job.job_id, "topic": topic})

        try:
            saved = self._repository.get_kafka_offsets(group_id, topic)
            if saved and 0 in saved:
                resume_offset = saved[0] + 1
                logger.info("CDC resuming", extra={"job_id": job.job_id, "offset": resume_offset})
            else:
                resume_offset = 0
                logger.info("CDC starting from beginning", extra={"job_id": job.job_id})

            tp = TopicPartition(topic, 0, resume_offset)
            consumer.assign([tp])

            # Determine catchup target (end offset at CDC start time)
            catchup_target = job.catchup_target
            if catchup_target is None:
                consumer.list_topics(topic, timeout=10)  # ensure broker metadata is fetched
                _low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
                catchup_target = high
                self._repository.set_catchup_target(job.job_id, catchup_target)
                logger.info("Catchup target set", extra={"job_id": job.job_id, "target": catchup_target})

            is_streaming = job.status == "cdc_streaming"

            tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)
            with _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:
                while not self._shutdown_event.is_set():
                    # Periodically check if job was externally completed/cancelled/deleted
                    try:
                        current_job = self._repository.get_job(job.job_id)
                    except NotFoundError:
                        logger.info("Job deleted externally, stopping CDC", extra={"job_id": job.job_id})
                        break
                    if current_job.status == "completed":
                        logger.info("Job completed externally, stopping CDC", extra={"job_id": job.job_id})
                        break

                    msgs = consumer.consume(
                        num_messages=self._config.worker_cdc_batch_size,
                        timeout=1.0,
                    )
                    if not msgs:
                        continue

                    current_offsets: dict[int, int] = {}
                    for msg in msgs:
                        if msg.error():
                            logger.warning(
                                "Kafka message error: %s",
                                msg.error(),
                                extra={"job_id": job.job_id},
                            )
                            continue
                        self._apply_event(tgt_conn, msg, merge_sql, delete_sql, pk_cols)
                        current_offsets[msg.partition()] = msg.offset()

                    # Commit Oracle then save offsets to PG then commit Kafka
                    tgt_conn.commit()
                    self._repository.save_kafka_offsets(group_id, topic, current_offsets)
                    consumer.commit()

                    # Check catchup completion
                    if not is_streaming and current_offsets:
                        max_offset = max(current_offsets.values())
                        if max_offset + 1 >= catchup_target:
                            self._repository.update_job_status(job.job_id, "cdc_streaming")
                            is_streaming = True
                            logger.info(
                                "CDC catchup complete → streaming",
                                extra={"job_id": job.job_id, "offset": max_offset},
                            )
        finally:
            consumer.close()
            logger.info("CDC consumer closed", extra={"job_id": job.job_id})

    def _apply_event(
        self,
        conn: Any,
        msg: Any,
        merge_sql: str,
        delete_sql: str,
        pk_cols: list[str],
    ) -> None:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            logger.warning("Cannot parse Kafka message at offset %s: %s", msg.offset(), exc)
            return

        # Debezium envelope: {"payload": {"op": ..., "before": ..., "after": ...}}
        data = payload.get("payload") or payload
        op = data.get("op")
        after: dict[str, Any] | None = data.get("after")
        before: dict[str, Any] | None = data.get("before")

        with conn.cursor() as cur:
            if op in ("c", "r", "u") and after is not None:
                cur.execute(merge_sql, after)
            elif op == "d" and before is not None:
                key_row = {k: before[k] for k in pk_cols if k in before}
                cur.execute(delete_sql, key_row)
            else:
                logger.debug("Skipping Kafka event op=%s offset=%s", op, msg.offset())


# ---------------------------------------------------------------------------
# MigrationWorker
# ---------------------------------------------------------------------------


class MigrationWorker:
    """
    Main worker process. Homogeneous — dynamically takes either a bulk chunk
    or CDC ownership depending on what's available.
    """

    def __init__(
        self,
        worker_id: str,
        config: AppConfig,
        repository: CoordinatorRepository,
    ) -> None:
        self._worker_id = worker_id
        self._config = config
        self._repository = repository
        self._shutdown_event = threading.Event()
        self._bulk = BulkChunkProcessor(config, repository)
        self._cdc = CDCConsumer(config, repository, worker_id, self._shutdown_event)

    def shutdown(self) -> None:
        logger.info("Shutdown requested", extra={"worker_id": self._worker_id})
        self._shutdown_event.set()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        signal.signal(signal.SIGINT, lambda *_: self.shutdown())

        logger.info("Worker started", extra={"worker_id": self._worker_id})

        while not self._shutdown_event.is_set():
            try:
                if self._try_cdc_work():
                    continue
                if self._try_bulk_work():
                    continue
                # Nothing to do — idle wait
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)
            except Exception:
                logger.exception("Unexpected error in worker loop", extra={"worker_id": self._worker_id})
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)

        logger.info("Worker stopped", extra={"worker_id": self._worker_id})

    def _try_cdc_work(self) -> bool:
        """Try to claim a CDC job. Returns True if work was done (blocking until CDC ends)."""
        job = self._repository.try_claim_cdc_job(self._worker_id)
        if not job:
            return False

        logger.info(
            "CDC ownership claimed",
            extra={"job_id": job.job_id, "status": job.status, "worker_id": self._worker_id},
        )
        heartbeat = _HeartbeatThread(
            self._repository,
            job.job_id,
            self._worker_id,
            self._config.worker_heartbeat_interval_seconds,
        )
        heartbeat.start()
        try:
            self._cdc.run(job)
        except Exception:
            logger.exception("CDC consumer error", extra={"job_id": job.job_id})
        finally:
            heartbeat.stop()
            try:
                self._repository.release_cdc_ownership(job.job_id, self._worker_id)
                logger.info("CDC ownership released", extra={"job_id": job.job_id})
            except Exception:
                logger.warning("Failed to release CDC ownership", extra={"job_id": job.job_id})
        return True

    def _try_bulk_work(self) -> bool:
        """Try to claim and process one bulk chunk. Returns True if a chunk was taken."""
        chunk = self._repository.claim_bulk_chunk(self._worker_id)
        if not chunk:
            return False

        chunk_id = str(chunk["chunk_id"])
        logger.info("Bulk chunk claimed", extra={"chunk_id": chunk_id, "worker_id": self._worker_id})
        try:
            rows = self._bulk.process(chunk)
            self._repository.complete_chunk(chunk_id, rows)
            logger.info("Bulk chunk completed", extra={"chunk_id": chunk_id, "rows": rows})
        except Exception as exc:
            logger.exception("Bulk chunk failed", extra={"chunk_id": chunk_id})
            self._repository.fail_chunk(chunk_id, str(exc))
        return True
