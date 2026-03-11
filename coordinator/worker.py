from __future__ import annotations

import json
import logging
import signal
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .config import AppConfig
from .errors import ExternalServiceError, NotFoundError, ValidationError
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
    oracledb.defaults.fetch_lobs = False
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
    chunk_meta: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    qualified = _qualified_table(table_name)
    cols_str = ", ".join(f'"{c}"' for c in columns)
    params: dict[str, Any] = {}

    # cdc_pk_split chunks: filter by PK range; use_scn flag overrides job-level scn_cutoff
    if chunk_meta and "pk_col" in chunk_meta:
        pk_col = chunk_meta["pk_col"]
        pk_from = chunk_meta.get("pk_from")
        pk_to = chunk_meta.get("pk_to")
        effective_scn = scn_cutoff if chunk_meta.get("use_scn", True) else None

        from_clause = f"{qualified} AS OF SCN :scn" if effective_scn else qualified
        if effective_scn:
            params["scn"] = effective_scn

        where_parts: list[str] = []
        if pk_from is not None:
            where_parts.append(f'"{pk_col}" >= :pk_from')
            params["pk_from"] = pk_from
        if pk_to is not None:
            where_parts.append(f'"{pk_col}" <= :pk_to')
            params["pk_to"] = pk_to
        where = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        return f"SELECT {cols_str} FROM {from_clause}{where}", params

    # Standard ROWID-based chunk
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


def _build_bulk_insert_sql(table_name: str, columns: list[str]) -> str:
    """Plain INSERT for bulk load — use with executemany(batcherrors=True) for idempotency.
    Positional binds (:1,:2,...) + tuple rows avoids per-row dict creation overhead.
    Note: APPEND_VALUES hint is incompatible with BATCH ERROR mode (ORA-38910)."""
    qualified = _qualified_table(table_name)
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f":{i + 1}" for i in range(len(columns)))
    return f"INSERT INTO {qualified} ({insert_cols}) VALUES ({insert_vals})"


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


def _detect_lob_column_type_codes(description: Any) -> dict[int, Any]:
    """Return {column_index: db_type_code} for CLOB/BLOB/NCLOB columns."""
    if not description:
        return {}
    try:
        import oracledb
    except ImportError:
        return {}

    lob_types = {
        oracledb.DB_TYPE_CLOB,
        oracledb.DB_TYPE_BLOB,
        oracledb.DB_TYPE_NCLOB,
    }
    lob_columns: dict[int, Any] = {}
    for idx, col in enumerate(description):
        type_code = getattr(col, "type_code", None)
        if type_code is None and isinstance(col, (tuple, list)) and len(col) > 1:
            type_code = col[1]
        if type_code in lob_types:
            lob_columns[idx] = type_code
    return lob_columns


def _apply_lob_input_sizes(cursor: Any, column_count: int, lob_type_codes: dict[int, Any]) -> None:
    """Set bind sizes for positional executemany() LOB columns when known."""
    if not lob_type_codes:
        return
    input_sizes: list[Any] = [None] * column_count
    for idx, type_code in lob_type_codes.items():
        if 0 <= idx < column_count:
            input_sizes[idx] = type_code
    if any(size is not None for size in input_sizes):
        cursor.setinputsizes(*input_sizes)


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
        source_table = job.table_name
        target_table = job.target_table_name or job.table_name
        scn_cutoff = job.scn_cutoff
        start_rowid: str | None = chunk.get("start_rowid")
        end_rowid: str | None = chunk.get("end_rowid")
        chunk_meta: dict[str, Any] | None = chunk.get("chunk_meta")

        select_sql, select_params = _build_source_select(
            source_table, insertable_cols, scn_cutoff, start_rowid, end_rowid, chunk_meta
        )
        insert_sql = _build_bulk_insert_sql(target_table, insertable_cols)

        src_dsn, src_user, src_pwd = _source_cfg(self._config)
        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)
        rows_processed = 0
        batch_size = self._config.worker_bulk_batch_size

        with _oracle_connect(src_dsn, src_user, src_pwd) as src_conn:
            with _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:
                chunk_id_str = str(chunk["chunk_id"])
                with src_conn.cursor() as src_cur:
                    src_cur.arraysize = batch_size
                    src_cur.prefetchrows = batch_size
                    t0 = time.monotonic()
                    src_cur.execute(select_sql, select_params)
                    lob_type_codes = _detect_lob_column_type_codes(src_cur.description)
                    lob_indexes = tuple(lob_type_codes.keys())
                    logger.info(
                        "Bulk SELECT executed: elapsed_ms=%d lob_cols=%d",
                        round((time.monotonic() - t0) * 1000),
                        len(lob_indexes),
                        extra={"chunk_id": chunk_id_str},
                    )
                    with tgt_conn.cursor() as tgt_cur:
                        _apply_lob_input_sizes(tgt_cur, len(insertable_cols), lob_type_codes)
                        batch_no = 0
                        while True:
                            t_fetch = time.monotonic()
                            rows = src_cur.fetchmany(batch_size)
                            fetch_ms = round((time.monotonic() - t_fetch) * 1000)
                            if not rows:
                                break
                            batch_no += 1
                            t_insert = time.monotonic()
                            tgt_cur.executemany(insert_sql, rows, batcherrors=True)
                            insert_ms = round((time.monotonic() - t_insert) * 1000)
                            t_commit = time.monotonic()
                            tgt_conn.commit()
                            commit_ms = round((time.monotonic() - t_commit) * 1000)
                            rows_processed += len(rows)
                            logger.info(
                                "Bulk batch applied: batch=%d rows=%d total=%d fetch_ms=%d insert_ms=%d commit_ms=%d",
                                batch_no, len(rows), rows_processed, fetch_ms, insert_ms, commit_ms,
                                extra={"chunk_id": chunk_id_str},
                            )

        return rows_processed

# ---------------------------------------------------------------------------
# Per-job state for MultiTableCDCConsumer
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _CDCJobState:
    job_id: str
    topic: str
    consumer_group: str
    merge_sql: str
    delete_sql: str
    pk_cols: list[str]
    catchup_target: int
    is_streaming: bool


# ---------------------------------------------------------------------------
# MultiTableCDCConsumer
# ---------------------------------------------------------------------------


class MultiTableCDCConsumer:
    """Handles multiple CDC jobs simultaneously in a single Kafka consumer session.

    Dynamically discovers and subscribes to new jobs without restart.
    Uses manual partition assignment (assign()) — no consumer-group rebalancing.
    All jobs share one Oracle target connection.
    """

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
        self._jobs: dict[str, _CDCJobState] = {}        # job_id → state
        self._jobs_by_topic: dict[str, _CDCJobState] = {}  # topic  → state

    def run(self) -> None:
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise ExternalServiceError("confluent_kafka is required for CDC processing") from exc

        consumer = Consumer(
            {
                "bootstrap.servers": self._config.kafka_bootstrap_servers,
                # Unique group per worker — offsets are managed in PG, not Kafka
                "group.id": f"cdc-worker-{self._worker_id}",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
            }
        )
        logger.info("MultiTableCDCConsumer started", extra={"worker_id": self._worker_id})

        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)
        refresh_interval = self._config.cdc_worker_refresh_interval_seconds
        heartbeat_interval = self._config.worker_heartbeat_interval_seconds
        last_refresh = 0.0
        last_heartbeat = 0.0
        # Prune completed jobs less frequently to reduce DB load
        last_prune = 0.0
        prune_interval = 60.0

        try:
            with _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:
                while not self._shutdown_event.is_set():
                    now = time.monotonic()

                    # Claim new CDC jobs periodically
                    if now - last_refresh >= refresh_interval:
                        self._refresh_jobs(consumer)
                        last_refresh = now

                    if not self._jobs:
                        self._shutdown_event.wait(refresh_interval)
                        continue

                    msgs = consumer.consume(
                        num_messages=self._config.worker_cdc_batch_size,
                        timeout=1.0,
                    )

                    if not msgs:
                        if now - last_heartbeat >= heartbeat_interval:
                            self._send_heartbeats()
                            last_heartbeat = now
                        if now - last_prune >= prune_interval:
                            self._prune_completed_jobs(consumer)
                            last_prune = now
                        continue

                    # Route and apply events
                    topic_max_offsets: dict[str, int] = {}
                    for msg in msgs:
                        if msg.error():
                            logger.warning(
                                "Kafka message error: %s",
                                msg.error(),
                                extra={"worker_id": self._worker_id},
                            )
                            continue
                        state = self._jobs_by_topic.get(msg.topic())
                        if state:
                            self._apply_event(tgt_conn, msg, state)
                            topic_max_offsets[msg.topic()] = max(
                                topic_max_offsets.get(msg.topic(), -1), msg.offset()
                            )

                    # Commit Oracle (covers all tables in this batch)
                    tgt_conn.commit()

                    # Save offsets per job and check catchup completion
                    for topic, max_offset in topic_max_offsets.items():
                        state = self._jobs_by_topic.get(topic)
                        if not state:
                            continue
                        self._repository.save_kafka_offsets(
                            state.consumer_group, topic, {0: max_offset}
                        )
                        if not state.is_streaming and max_offset + 1 >= state.catchup_target:
                            self._repository.update_job_status(state.job_id, "cdc_streaming")
                            state.is_streaming = True
                            logger.info(
                                "CDC catchup complete → streaming",
                                extra={"job_id": state.job_id, "offset": max_offset},
                            )

                    consumer.commit()

                    now = time.monotonic()
                    if now - last_heartbeat >= heartbeat_interval:
                        self._send_heartbeats()
                        last_heartbeat = now
                    if now - last_prune >= prune_interval:
                        self._prune_completed_jobs(consumer)
                        last_prune = now

        finally:
            consumer.close()
            self._release_all_jobs()
            logger.info("MultiTableCDCConsumer stopped", extra={"worker_id": self._worker_id})

    def _refresh_jobs(self, consumer: Any) -> None:
        """Claim new unclaimed CDC jobs and add their Kafka topics to the consumer."""
        from confluent_kafka import TopicPartition

        available_slots = self._config.cdc_worker_job_limit - len(self._jobs)
        if available_slots <= 0:
            return

        new_jobs = self._repository.claim_cdc_jobs_batch(self._worker_id, limit=available_slots)
        if not new_jobs:
            return

        new_tps: list[Any] = []
        newly_added: list[_CDCJobState] = []

        for job in new_jobs:
            if job.job_id in self._jobs:
                continue

            templates = self._repository.get_sql_templates(job.job_id)
            if not templates:
                logger.warning(
                    "No SQL templates for CDC job %s, skipping", job.job_id,
                    extra={"worker_id": self._worker_id},
                )
                continue

            topic: str = job.kafka_topic_name  # type: ignore[assignment]
            insertable_cols: list[str] = templates["insertable_columns"]
            pk_cols: list[str] = templates["pk_columns"]
            target_table = job.target_table_name or job.table_name

            saved = self._repository.get_kafka_offsets(job.consumer_group_name, topic)  # type: ignore[arg-type]
            resume_offset = (saved[0] + 1) if saved and 0 in saved else 0

            state = _CDCJobState(
                job_id=job.job_id,
                topic=topic,
                consumer_group=job.consumer_group_name,  # type: ignore[arg-type]
                merge_sql=_build_cdc_merge_sql(target_table, insertable_cols, pk_cols),
                delete_sql=_build_cdc_delete_sql(target_table, pk_cols),
                pk_cols=pk_cols,
                catchup_target=job.catchup_target or 0,
                is_streaming=(job.status == "cdc_streaming"),
            )

            self._jobs[job.job_id] = state
            self._jobs_by_topic[topic] = state
            new_tps.append(TopicPartition(topic, 0, resume_offset))
            newly_added.append(state)
            logger.info(
                "CDCWorker claimed job",
                extra={"job_id": job.job_id, "topic": topic, "worker_id": self._worker_id},
            )

        if not new_tps:
            return

        current = list(consumer.assignment())
        consumer.assign(current + new_tps)

        # Resolve catchup targets for newly added jobs that don't have one yet
        for state in newly_added:
            if state.catchup_target == 0 and not state.is_streaming:
                try:
                    consumer.list_topics(state.topic, timeout=10)
                    _, high = consumer.get_watermark_offsets(TopicPartition(state.topic, 0), timeout=10)
                    state.catchup_target = high
                    self._repository.set_catchup_target(state.job_id, high)
                    logger.info(
                        "Catchup target set",
                        extra={"job_id": state.job_id, "target": high},
                    )
                except Exception as exc:
                    logger.warning(
                        "Cannot resolve catchup target for %s: %s", state.topic, exc,
                        extra={"job_id": state.job_id},
                    )

    def _prune_completed_jobs(self, consumer: Any) -> None:
        """Remove externally completed or deleted jobs from local state."""
        to_remove: list[_CDCJobState] = []
        for job_id, state in list(self._jobs.items()):
            try:
                job = self._repository.get_job(job_id)
                if job.status == "completed":
                    to_remove.append(state)
            except NotFoundError:
                to_remove.append(state)

        if not to_remove:
            return

        remove_topics = {s.topic for s in to_remove}
        for state in to_remove:
            self._jobs.pop(state.job_id, None)
            self._jobs_by_topic.pop(state.topic, None)
            logger.info(
                "CDCWorker removed completed/deleted job",
                extra={"job_id": state.job_id, "worker_id": self._worker_id},
            )

        remaining = [tp for tp in consumer.assignment() if tp.topic not in remove_topics]
        consumer.assign(remaining)

    def _send_heartbeats(self) -> None:
        for job_id in list(self._jobs.keys()):
            try:
                self._repository.update_cdc_heartbeat(job_id, self._worker_id)
            except Exception:
                logger.warning("Heartbeat failed for job %s", job_id)

    def _release_all_jobs(self) -> None:
        try:
            self._repository.release_all_cdc_jobs(self._worker_id)
        except Exception:
            logger.warning("Failed to release CDC jobs for worker %s", self._worker_id)
        self._jobs.clear()
        self._jobs_by_topic.clear()

    def _apply_event(self, conn: Any, msg: Any, state: _CDCJobState) -> None:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            logger.warning("Cannot parse Kafka message at offset %s: %s", msg.offset(), exc)
            return

        data = payload.get("payload") or payload
        op = data.get("op")
        after: dict[str, Any] | None = data.get("after")
        before: dict[str, Any] | None = data.get("before")

        try:
            with conn.cursor() as cur:
                if op in ("c", "r", "u") and after is not None:
                    cur.execute(state.merge_sql, after)
                elif op == "d" and before is not None:
                    missing = [k for k in state.pk_cols if k not in before]
                    if missing:
                        logger.warning(
                            "Delete event missing PK columns %s, skipping (topic=%s offset=%s)",
                            missing, state.topic, msg.offset(),
                            extra={"job_id": state.job_id},
                        )
                        return
                    key_row = {k: before[k] for k in state.pk_cols}
                    cur.execute(state.delete_sql, key_row)
                else:
                    logger.debug(
                        "Skipping CDC event op=%s offset=%s topic=%s", op, msg.offset(), state.topic,
                    )
        except Exception as exc:
            logger.warning(
                "Failed to apply CDC event op=%s offset=%s topic=%s: %s",
                op, msg.offset(), state.topic, exc,
                extra={"job_id": state.job_id},
            )


# ---------------------------------------------------------------------------
# CDCWorker — dedicated multi-table CDC worker
# ---------------------------------------------------------------------------


class CDCWorker:
    """Dedicated CDC worker. Manages multiple tables simultaneously via one Kafka consumer.

    Start multiple CDCWorker processes (e.g. 2) for CDC redundancy and throughput.
    Each worker claims up to cdc_worker_job_limit jobs.
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
        self._consumer = MultiTableCDCConsumer(
            config, repository, worker_id, self._shutdown_event
        )

    def shutdown(self) -> None:
        logger.info("CDCWorker shutdown requested", extra={"worker_id": self._worker_id})
        self._shutdown_event.set()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        signal.signal(signal.SIGINT, lambda *_: self.shutdown())
        logger.info("CDCWorker started", extra={"worker_id": self._worker_id})
        try:
            self._consumer.run()
        except Exception:
            logger.exception("CDCWorker fatal error", extra={"worker_id": self._worker_id})
        logger.info("CDCWorker stopped", extra={"worker_id": self._worker_id})


# ---------------------------------------------------------------------------
# BulkWorker — dedicated bulk chunk worker
# ---------------------------------------------------------------------------


class BulkWorker:
    """Dedicated bulk chunk worker. Processes ROWID chunks from any job.

    Start multiple BulkWorker processes (e.g. 4) to parallelise bulk loading.
    Workers have no CDC responsibility — they only process chunks.
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

    def shutdown(self) -> None:
        logger.info("BulkWorker shutdown requested", extra={"worker_id": self._worker_id})
        self._shutdown_event.set()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        signal.signal(signal.SIGINT, lambda *_: self.shutdown())
        logger.info("BulkWorker started", extra={"worker_id": self._worker_id})

        while not self._shutdown_event.is_set():
            try:
                if self._try_bulk_work():
                    continue
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)
            except Exception:
                logger.exception("BulkWorker error", extra={"worker_id": self._worker_id})
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)

        logger.info("BulkWorker stopped", extra={"worker_id": self._worker_id})

    def _try_bulk_work(self) -> bool:
        chunk = self._repository.claim_bulk_chunk(
            self._worker_id, self._config.worker_bulk_max_workers_per_job
        )
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


# ---------------------------------------------------------------------------
# MigrationWorker — legacy mixed worker (CDC + bulk in one process)
# ---------------------------------------------------------------------------


class MigrationWorker:
    """Legacy mixed worker: tries CDC work first, then bulk.

    Use CDCWorker + BulkWorker separately for production deployments.
    This class is kept for backward compatibility and simple single-process setups.
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
        self._cdc_consumer = MultiTableCDCConsumer(
            config, repository, worker_id, self._shutdown_event
        )
        self._cdc_thread: threading.Thread | None = None

    def shutdown(self) -> None:
        logger.info("Shutdown requested", extra={"worker_id": self._worker_id})
        self._shutdown_event.set()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        signal.signal(signal.SIGINT, lambda *_: self.shutdown())
        logger.info("MigrationWorker started", extra={"worker_id": self._worker_id})

        # Run CDC consumer in a background thread so bulk work can proceed in parallel
        self._cdc_thread = threading.Thread(
            target=self._run_cdc, daemon=True, name=f"cdc-{self._worker_id[:8]}"
        )
        self._cdc_thread.start()

        while not self._shutdown_event.is_set():
            try:
                if self._try_bulk_work():
                    continue
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)
            except Exception:
                logger.exception("MigrationWorker bulk error", extra={"worker_id": self._worker_id})
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)

        if self._cdc_thread:
            self._cdc_thread.join(timeout=10)
        logger.info("MigrationWorker stopped", extra={"worker_id": self._worker_id})

    def _run_cdc(self) -> None:
        try:
            self._cdc_consumer.run()
        except Exception:
            logger.exception("MigrationWorker CDC thread error", extra={"worker_id": self._worker_id})

    def _try_bulk_work(self) -> bool:
        chunk = self._repository.claim_bulk_chunk(
            self._worker_id, self._config.worker_bulk_max_workers_per_job
        )
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


# ---------------------------------------------------------------------------
# CompareWorker — compares table contents between source and target Oracle
# ---------------------------------------------------------------------------


class CompareWorker:
    """Compares table data between source and target Oracle databases.

    For each claimed compare task, the worker:
      1. Counts rows in source and target.
      2. If PK columns are specified, identifies rows missing on each side.
      3. Stores a result summary and sample diffs back to the state DB.

    Start with WORKER_TYPE=compare.
    """

    _MAX_SAMPLE = 20

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

    def shutdown(self) -> None:
        logger.info("CompareWorker shutdown requested", extra={"worker_id": self._worker_id})
        self._shutdown_event.set()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        signal.signal(signal.SIGINT, lambda *_: self.shutdown())
        logger.info("CompareWorker started", extra={"worker_id": self._worker_id})

        while not self._shutdown_event.is_set():
            try:
                if self._try_compare_work():
                    continue
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)
            except Exception:
                logger.exception("CompareWorker error", extra={"worker_id": self._worker_id})
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)

        logger.info("CompareWorker stopped", extra={"worker_id": self._worker_id})

    def _try_compare_work(self) -> bool:
        task = self._repository.claim_compare_task(self._worker_id)
        if not task:
            return False

        task_id = str(task["task_id"])
        source_table = str(task["source_table"])
        target_table = str(task["target_table"])
        pk_columns: list[str] = list(task.get("pk_columns") or [])
        compare_mode: str = str(task.get("compare_mode") or "full")
        pk_filter_value: str | None = task.get("pk_filter_value") or None

        logger.info(
            "Compare task claimed",
            extra={"task_id": task_id, "source_table": source_table,
                   "compare_mode": compare_mode, "worker_id": self._worker_id},
        )
        try:
            result_summary, sample_diffs = self._compare_tables(
                task_id, source_table, target_table, pk_columns, compare_mode, pk_filter_value
            )
            self._repository.complete_compare_task(task_id, result_summary, sample_diffs)
            logger.info("Compare task completed", extra={"task_id": task_id, "summary": result_summary})
        except Exception as exc:
            logger.exception("Compare task failed", extra={"task_id": task_id})
            self._repository.fail_compare_task(task_id, str(exc))
        return True

    @staticmethod
    def _qualify(table_name: str, default_schema: str | None) -> str:
        """Return "SCHEMA"."TABLE" — adds default_schema if no dot in table_name."""
        if "." not in table_name:
            if not default_schema:
                raise ValidationError(
                    f"Table '{table_name}' has no schema prefix and no default schema is configured"
                )
            return f'"{default_schema.upper()}"."{table_name.upper()}"'
        return _qualified_table(table_name)

    def _progress(self, task_id: str, message: str) -> None:
        logger.info("Compare progress: %s", message, extra={"task_id": task_id})
        self._repository.update_compare_task_progress(task_id, message)

    @staticmethod
    def _build_filter(
        pk_columns: list[str],
        compare_mode: str,
        pk_filter_value: str | None,
    ) -> tuple[str, dict[str, Any]]:
        """Return (WHERE clause, bind params) for the given mode.

        Modes:
          full       — no filter
          before_pk  — WHERE first_pk_col < :pk_filter
          after_pk   — WHERE first_pk_col > :pk_filter
        """
        if compare_mode == "full" or not pk_columns or not pk_filter_value:
            return "", {}
        pk_col = pk_columns[0]
        op = "<" if compare_mode == "before_pk" else ">"
        return f' WHERE "{pk_col}" {op} :pk_filter', {"pk_filter": pk_filter_value}

    def _compare_tables(
        self,
        task_id: str,
        source_table: str,
        target_table: str,
        pk_columns: list[str],
        compare_mode: str = "full",
        pk_filter_value: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        src_dsn, src_user, src_pwd = _source_cfg(self._config)
        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)

        src_qual = self._qualify(source_table, self._config.oracle_source_schema)
        tgt_qual = self._qualify(target_table, self._config.oracle_target_schema)

        where_clause, filter_params = self._build_filter(pk_columns, compare_mode, pk_filter_value)

        mode_label = {
            "full": "полное",
            "before_pk": f"до PK={pk_filter_value}",
            "after_pk": f"после PK={pk_filter_value}",
        }.get(compare_mode, compare_mode)

        self._progress(task_id, f"Подключение к базам данных… (режим: {mode_label})")

        with _oracle_connect(src_dsn, src_user, src_pwd) as src_conn, \
             _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:

            self._progress(task_id, "Подсчёт строк в source…")
            src_count = self._count_rows(src_conn, src_qual, where_clause, filter_params)

            self._progress(task_id, f"Source: {src_count:,} строк. Подсчёт строк в target…")
            tgt_count = self._count_rows(tgt_conn, tgt_qual, where_clause, filter_params)

            count_diff = src_count - tgt_count
            count_msg = "совпадает" if count_diff == 0 else f"разница {count_diff:+,}"
            self._progress(
                task_id,
                f"Source: {src_count:,} | Target: {tgt_count:,} ({count_msg}). "
                + ("Сравнение PK…" if pk_columns else "PK не указаны."),
            )

            result_summary: dict[str, Any] = {
                "compare_mode": compare_mode,
                "pk_filter_value": pk_filter_value,
                "source_count": src_count,
                "target_count": tgt_count,
                "count_match": src_count == tgt_count,
                "count_diff": count_diff,
            }
            sample_diffs: list[dict[str, Any]] = []

            if not pk_columns:
                result_summary["pk_comparison"] = "skipped — no PK columns specified"
                return result_summary, sample_diffs

            self._progress(task_id, "Загрузка PK из source (до 10 000)…")
            missing_in_target = self._find_missing_pks(
                src_conn, tgt_conn, src_qual, tgt_qual, pk_columns,
                where_clause=where_clause, filter_params=filter_params,
                progress_cb=lambda n: self._progress(
                    task_id, f"Поиск отсутствующих в target: проверено ~{n:,} PK…"
                ),
            )

            self._progress(
                task_id,
                f"Отсутствует в target: {len(missing_in_target)}. Проверка обратного направления…",
            )
            missing_in_source = self._find_missing_pks(
                tgt_conn, src_conn, tgt_qual, src_qual, pk_columns,
                where_clause=where_clause, filter_params=filter_params,
                progress_cb=lambda n: self._progress(
                    task_id, f"Поиск отсутствующих в source: проверено ~{n:,} PK…"
                ),
            )

            result_summary["missing_in_target_count"] = len(missing_in_target)
            result_summary["missing_in_source_count"] = len(missing_in_source)
            result_summary["pk_comparison"] = "done"

            for pk_val in missing_in_target[: self._MAX_SAMPLE]:
                sample_diffs.append({"type": "missing_in_target", "pk": pk_val})
            for pk_val in missing_in_source[: self._MAX_SAMPLE]:
                sample_diffs.append({"type": "missing_in_source", "pk": pk_val})

            # Phase 3: column-level comparison for matched rows
            self._progress(task_id, "Сравнение значений колонок (выборка совпадающих строк)…")
            col_diffs, differing_cols, rows_compared = self._compare_row_values(
                src_conn, tgt_conn, src_qual, tgt_qual, pk_columns,
                where_clause=where_clause, filter_params=filter_params,
                progress_cb=lambda n: self._progress(
                    task_id, f"Сравнение значений: проверено {n} строк…"
                ),
            )
            result_summary["rows_compared"] = rows_compared
            result_summary["rows_with_diffs"] = len(col_diffs)
            result_summary["differing_columns"] = differing_cols
            for diff in col_diffs[: self._MAX_SAMPLE]:
                sample_diffs.append({"type": "data_mismatch", **diff})

        return result_summary, sample_diffs

    @staticmethod
    def _count_rows(
        conn: Any,
        qualified_table: str,
        where_clause: str = "",
        filter_params: dict[str, Any] | None = None,
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {qualified_table}{where_clause}",  # noqa: S608
                filter_params or {},
            )
            row = cur.fetchone()
        return int(row[0])

    def _find_missing_pks(
        self,
        source_conn: Any,
        check_conn: Any,
        source_qual: str,
        check_qual: str,
        pk_columns: list[str],
        where_clause: str = "",
        filter_params: dict[str, Any] | None = None,
        progress_cb: Any = None,
    ) -> list[Any]:
        """Return PK values present in source_conn but absent in check_conn (up to 2×MAX_SAMPLE)."""
        pk_cols_sql = ", ".join(f'"{c}"' for c in pk_columns)

        with source_conn.cursor() as cur:
            cur.execute(
                f"SELECT {pk_cols_sql} FROM {source_qual}{where_clause}",  # noqa: S608
                filter_params or {},
            )
            source_pks = cur.fetchmany(10000)

        if not source_pks:
            return []

        missing: list[Any] = []
        batch_size = 500
        checked = 0
        for i in range(0, len(source_pks), batch_size):
            batch = source_pks[i : i + batch_size]
            if len(pk_columns) == 1:
                placeholders = ", ".join(f":{j + 1}" for j in range(len(batch)))
                sql = f'SELECT {pk_cols_sql} FROM {check_qual} WHERE "{pk_columns[0]}" IN ({placeholders})'  # noqa: S608
                params: list[Any] = [r[0] for r in batch]
            else:
                conditions = " OR ".join(
                    "(" + " AND ".join(
                        f'"{c}" = :{j * len(pk_columns) + k + 1}'
                        for k, c in enumerate(pk_columns)
                    ) + ")"
                    for j in range(len(batch))
                )
                sql = f"SELECT {pk_cols_sql} FROM {check_qual} WHERE {conditions}"  # noqa: S608
                params = [val for row in batch for val in row]

            with check_conn.cursor() as cur:
                cur.execute(sql, params)
                found = {tuple(r) for r in cur.fetchall()}

            for row in batch:
                pk_key = tuple(row)
                if pk_key not in found:
                    missing.append(list(row) if len(pk_columns) > 1 else row[0])
                    if len(missing) >= self._MAX_SAMPLE * 2:
                        return missing

            checked += len(batch)
            if progress_cb:
                progress_cb(checked)

        return missing

    # Maximum number of rows to fetch per side for column-level comparison.
    _MAX_ROWS_COMPARE = 1000

    def _compare_row_values(
        self,
        src_conn: Any,
        tgt_conn: Any,
        src_qual: str,
        tgt_qual: str,
        pk_columns: list[str],
        where_clause: str = "",
        filter_params: dict[str, Any] | None = None,
        progress_cb: Any = None,
    ) -> tuple[list[dict[str, Any]], list[str], int]:
        """Compare column values for rows present on both sides.

        Returns:
            col_diffs   — list of per-row diffs (pk + column differences)
            diff_cols   — sorted list of column names that had any difference
            rows_compared — how many rows were actually compared
        """
        pk_cols_sql = ", ".join(f'"{c}"' for c in pk_columns)

        # Fetch column names from source
        src_schema, src_table = (src_qual.strip('"').split('"."') + [""])[:2]
        with src_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM all_tab_columns"
                " WHERE owner = :owner AND table_name = :tbl"
                " ORDER BY column_id",
                {"owner": src_schema.upper(), "tbl": src_table.upper()},
            )
            all_columns: list[str] = [r[0] for r in cur.fetchall()]

        if not all_columns:
            # Fallback: derive columns from a SELECT * with zero rows
            with src_conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {src_qual} WHERE 1=0")  # noqa: S608
                all_columns = [d[0] for d in cur.description]

        non_pk = [c for c in all_columns if c.upper() not in {p.upper() for p in pk_columns}]
        all_cols_sql = ", ".join(f'"{c}"' for c in all_columns)
        col_idx = {c.upper(): i for i, c in enumerate(all_columns)}

        # Fetch a sample of rows from source (up to _MAX_ROWS_COMPARE), respecting the filter
        if where_clause:
            sample_sql = (
                f"SELECT {all_cols_sql} FROM {src_qual}{where_clause}"  # noqa: S608
                f" AND ROWNUM <= :_rownum_limit"
            )
            sample_params = {**(filter_params or {}), "_rownum_limit": self._MAX_ROWS_COMPARE}
        else:
            sample_sql = f"SELECT {all_cols_sql} FROM {src_qual} WHERE ROWNUM <= :_rownum_limit"  # noqa: S608
            sample_params = {"_rownum_limit": self._MAX_ROWS_COMPARE}

        with src_conn.cursor() as cur:
            cur.execute(sample_sql, sample_params)
            src_rows = cur.fetchall()

        if not src_rows:
            return [], [], 0

        # Build PK → full row dict for source
        def pk_key(row: Any) -> tuple:
            return tuple(row[col_idx[p.upper()]] for p in pk_columns)

        src_by_pk: dict[tuple, Any] = {pk_key(r): r for r in src_rows}

        # Fetch matching rows from target in one query
        if len(pk_columns) == 1:
            placeholders = ", ".join(f":{j + 1}" for j in range(len(src_rows)))
            pk_vals = [r[col_idx[pk_columns[0].upper()]] for r in src_rows]
            fetch_sql = (
                f'SELECT {all_cols_sql} FROM {tgt_qual}'  # noqa: S608
                f' WHERE "{pk_columns[0]}" IN ({placeholders})'
            )
            fetch_params: list[Any] = pk_vals
        else:
            conditions = " OR ".join(
                "(" + " AND ".join(
                    f'"{c}" = :{j * len(pk_columns) + k + 1}'
                    for k, c in enumerate(pk_columns)
                ) + ")"
                for j in range(len(src_rows))
            )
            fetch_sql = f"SELECT {all_cols_sql} FROM {tgt_qual} WHERE {conditions}"  # noqa: S608
            fetch_params = [r[col_idx[c.upper()]] for r in src_rows for c in pk_columns]

        with tgt_conn.cursor() as cur:
            cur.execute(fetch_sql, fetch_params)
            tgt_rows = cur.fetchall()

        tgt_by_pk: dict[tuple, Any] = {pk_key(r): r for r in tgt_rows}

        col_diffs: list[dict[str, Any]] = []
        diff_col_set: set[str] = set()
        rows_compared = 0

        for pk, src_row in src_by_pk.items():
            tgt_row = tgt_by_pk.get(pk)
            if tgt_row is None:
                continue  # already captured as missing_in_target

            rows_compared += 1
            row_diffs: dict[str, dict[str, Any]] = {}
            for col in non_pk:
                idx = col_idx[col.upper()]
                sv = src_row[idx]
                tv = tgt_row[idx]
                # Normalize: compare string representations to handle LOB / numeric edge cases
                if str(sv) != str(tv):
                    row_diffs[col] = {"src": str(sv)[:200], "tgt": str(tv)[:200]}
                    diff_col_set.add(col)

            if row_diffs:
                pk_repr = list(pk) if len(pk_columns) > 1 else pk[0]
                col_diffs.append({"pk": pk_repr, "columns": row_diffs})

            if progress_cb and rows_compared % 100 == 0:
                progress_cb(rows_compared)

            if len(col_diffs) >= self._MAX_SAMPLE * 2:
                break

        return col_diffs, sorted(diff_col_set), rows_compared


# ---------------------------------------------------------------------------
# SchemaCompareWorker — compares DDL objects between source and target Oracle
# ---------------------------------------------------------------------------


class SchemaCompareWorker:
    """Compares table DDL objects (columns, indexes, constraints, triggers)
    between source and target Oracle databases.

    Start with WORKER_TYPE=schema_compare.
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

    def shutdown(self) -> None:
        logger.info("SchemaCompareWorker shutdown requested", extra={"worker_id": self._worker_id})
        self._shutdown_event.set()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        signal.signal(signal.SIGINT, lambda *_: self.shutdown())
        logger.info("SchemaCompareWorker started", extra={"worker_id": self._worker_id})

        while not self._shutdown_event.is_set():
            try:
                if self._try_work():
                    continue
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)
            except Exception:
                logger.exception("SchemaCompareWorker error", extra={"worker_id": self._worker_id})
                self._shutdown_event.wait(self._config.worker_poll_interval_seconds)

        logger.info("SchemaCompareWorker stopped", extra={"worker_id": self._worker_id})

    def _try_work(self) -> bool:
        task = self._repository.claim_schema_task(self._worker_id)
        if not task:
            return False

        task_id = str(task["task_id"])
        source_table = str(task["source_table"])
        target_table = str(task["target_table"])

        logger.info(
            "Schema task claimed",
            extra={"task_id": task_id, "source_table": source_table, "worker_id": self._worker_id},
        )
        try:
            result = self._compare_schema(task_id, source_table, target_table)
            self._repository.complete_schema_task(task_id, result)
            logger.info("Schema task completed", extra={"task_id": task_id})
        except Exception as exc:
            logger.exception("Schema task failed", extra={"task_id": task_id})
            self._repository.fail_schema_task(task_id, str(exc))
        return True

    def _progress(self, task_id: str, message: str) -> None:
        logger.info("Schema compare progress: %s", message, extra={"task_id": task_id})
        self._repository.update_schema_task_progress(task_id, message)

    # ------------------------------------------------------------------
    # Oracle introspection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve(table_name: str, default_schema: str | None) -> tuple[str, str]:
        if "." in table_name:
            owner, tbl = table_name.split(".", 1)
        else:
            if not default_schema:
                raise ValidationError(
                    f"Table '{table_name}' has no schema prefix and no default schema is configured"
                )
            owner, tbl = default_schema, table_name
        return owner.strip().strip('"').upper(), tbl.strip().strip('"').upper()

    @staticmethod
    def _fetch_columns(conn: Any, owner: str, table_name: str) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, data_length, data_precision, data_scale,
                       nullable, data_default, char_length, column_id
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :tbl
                ORDER BY column_id
                """,
                {"owner": owner, "tbl": table_name},
            )
            cols = [d[0].lower() for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def _fetch_indexes(conn: Any, owner: str, table_name: str) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.index_name, i.uniqueness, i.index_type, i.status,
                       LISTAGG(c.column_name || ':' || c.descend, ',')
                           WITHIN GROUP (ORDER BY c.column_position) AS columns
                FROM all_indexes i
                JOIN all_ind_columns c
                  ON c.index_owner = i.owner AND c.index_name = i.index_name
                WHERE i.owner = :owner AND i.table_name = :tbl
                GROUP BY i.index_name, i.uniqueness, i.index_type, i.status
                ORDER BY i.index_name
                """,
                {"owner": owner, "tbl": table_name},
            )
            cols = [d[0].lower() for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def _fetch_constraints(conn: Any, owner: str, table_name: str) -> list[dict[str, Any]]:
        # Two separate queries because search_condition is LONG in all_constraints.
        # LONG columns cannot appear in GROUP BY, ORDER BY, subqueries, or with TO_CHAR()
        # in most Oracle query forms. We fetch the LONG column in a plain SELECT and
        # merge the result into the main rows in Python.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.constraint_name, c.constraint_type, c.status,
                       c.validated, c.r_owner, c.r_constraint_name,
                       LISTAGG(cc.column_name, ',')
                           WITHIN GROUP (ORDER BY cc.position) AS columns
                FROM all_constraints c
                LEFT JOIN all_cons_columns cc
                  ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
                WHERE c.owner = :owner AND c.table_name = :tbl
                  AND c.constraint_type IN ('P','U','C','R')
                GROUP BY c.constraint_name, c.constraint_type, c.status,
                         c.validated, c.r_owner, c.r_constraint_name
                ORDER BY c.constraint_type, c.constraint_name
                """,
                {"owner": owner, "tbl": table_name},
            )
            cols = [d[0].lower() for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

            # Fetch search_condition (LONG) in a plain SELECT — no GROUP BY / ORDER BY.
            cur.execute(
                """
                SELECT constraint_name, search_condition
                FROM all_constraints
                WHERE owner = :owner AND table_name = :tbl
                  AND constraint_type IN ('P','U','C','R')
                """,
                {"owner": owner, "tbl": table_name},
            )
            search_map: dict[str, str] = {}
            for cname, sc in cur.fetchall():
                search_map[cname] = str(sc) if sc is not None else None

        for row in rows:
            row["search_condition"] = search_map.get(row["constraint_name"])
        return rows

    @staticmethod
    def _fetch_triggers(conn: Any, owner: str, table_name: str) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            # ORDER BY is not allowed when selecting a LONG column (trigger_body),
            # so sort in Python after fetching.
            cur.execute(
                """
                SELECT trigger_name, trigger_type, triggering_event, status, action_type,
                       trigger_body
                FROM all_triggers
                WHERE owner = :owner AND table_name = :tbl
                """,
                {"owner": owner, "tbl": table_name},
            )
            cols = [d[0].lower() for d in cur.description]
            rows = []
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                if hasattr(d.get("trigger_body"), "read"):
                    d["trigger_body"] = d["trigger_body"].read()
                rows.append(d)
        rows.sort(key=lambda r: r.get("trigger_name") or "")
        return rows

    # ------------------------------------------------------------------
    # Comparison helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compare_category(
        src_items: list[dict[str, Any]],
        tgt_items: list[dict[str, Any]],
        key_field: str,
        ignore_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        ignore = set(ignore_fields or [])
        src_map = {str(item[key_field]): item for item in src_items}
        tgt_map = {str(item[key_field]): item for item in tgt_items}

        only_src = [src_map[k] for k in src_map if k not in tgt_map]
        only_tgt = [tgt_map[k] for k in tgt_map if k not in src_map]
        different = []
        matching = []

        for key in src_map:
            if key not in tgt_map:
                continue
            src_cmp = {k: (str(v).strip() if v is not None else None)
                       for k, v in src_map[key].items() if k not in ignore}
            tgt_cmp = {k: (str(v).strip() if v is not None else None)
                       for k, v in tgt_map[key].items() if k not in ignore}
            if src_cmp != tgt_cmp:
                diffs = {
                    f: {"src": src_cmp.get(f), "tgt": tgt_cmp.get(f)}
                    for f in set(src_cmp) | set(tgt_cmp)
                    if src_cmp.get(f) != tgt_cmp.get(f)
                }
                different.append({
                    "key": key,
                    "src": src_map[key],
                    "tgt": tgt_map[key],
                    "diffs": diffs,
                })
            else:
                matching.append(src_map[key])

        return {
            "match": not only_src and not only_tgt and not different,
            "only_in_source": only_src,
            "only_in_target": only_tgt,
            "different": different,
            "matching": matching,
        }

    # ------------------------------------------------------------------
    # Main comparison
    # ------------------------------------------------------------------

    def _compare_schema(
        self,
        task_id: str,
        source_table: str,
        target_table: str,
    ) -> dict[str, Any]:
        src_dsn, src_user, src_pwd = _source_cfg(self._config)
        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)

        src_owner, src_tbl = self._resolve(source_table, self._config.oracle_source_schema)
        tgt_owner, tgt_tbl = self._resolve(target_table, self._config.oracle_target_schema)

        self._progress(task_id, "Подключение к базам данных...")

        with _oracle_connect(src_dsn, src_user, src_pwd) as src_conn, \
             _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:

            self._progress(task_id, "Сравнение колонок...")
            columns = self._compare_category(
                self._fetch_columns(src_conn, src_owner, src_tbl),
                self._fetch_columns(tgt_conn, tgt_owner, tgt_tbl),
                "column_name", ignore_fields=["column_id"],
            )

            self._progress(task_id, "Сравнение индексов...")
            indexes = self._compare_category(
                self._fetch_indexes(src_conn, src_owner, src_tbl),
                self._fetch_indexes(tgt_conn, tgt_owner, tgt_tbl),
                "index_name",
            )

            self._progress(task_id, "Сравнение ограничений (constraints)...")
            constraints = self._compare_category(
                self._fetch_constraints(src_conn, src_owner, src_tbl),
                self._fetch_constraints(tgt_conn, tgt_owner, tgt_tbl),
                "constraint_name", ignore_fields=["r_owner", "r_constraint_name"],
            )

            self._progress(task_id, "Сравнение триггеров...")
            triggers = self._compare_category(
                self._fetch_triggers(src_conn, src_owner, src_tbl),
                self._fetch_triggers(tgt_conn, tgt_owner, tgt_tbl),
                "trigger_name",
            )

        overall_match = all(
            cat["match"] for cat in [columns, indexes, constraints, triggers]
        )
        self._progress(task_id, "Готово.")

        return {
            "overall_match": overall_match,
            "source_table": f"{src_owner}.{src_tbl}",
            "target_table": f"{tgt_owner}.{tgt_tbl}",
            "columns": columns,
            "indexes": indexes,
            "constraints": constraints,
            "triggers": triggers,
        }
