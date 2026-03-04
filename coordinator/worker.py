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


def _resolve_oracledb_lob_type() -> type[Any] | None:
    try:
        import oracledb
    except ImportError:
        return None
    return getattr(oracledb, "LOB", None)


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


def _materialize_lob_batch(
    rows: list[Any],
    lob_indexes: tuple[int, ...],
    lob_python_type: type[Any] | None,
) -> list[Any]:
    """Convert source-session LOB locators into plain str/bytes values via read()."""
    if not rows or lob_python_type is None:
        return rows

    materialized_rows: list[Any] = []
    changed_any = False
    for row in rows:
        values = list(row)
        row_changed = False
        indexes = lob_indexes if lob_indexes else tuple(range(len(values)))
        for idx in indexes:
            if idx >= len(values):
                continue
            value = values[idx]
            if value is None:
                continue
            if isinstance(value, lob_python_type):
                values[idx] = value.read()
                row_changed = True
        if row_changed:
            changed_any = True
            materialized_rows.append(tuple(values) if isinstance(row, tuple) else values)
        else:
            materialized_rows.append(row)

    return materialized_rows if changed_any else rows


# ---------------------------------------------------------------------------
# Oracle index management helpers
# ---------------------------------------------------------------------------


def _disable_target_nonunique_indexes(conn: Any, schema: str, table: str) -> None:
    """Mark all non-unique indexes on target table as UNUSABLE (idempotent, fast)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT index_name FROM all_indexes
            WHERE table_owner = :owner AND table_name = :tbl
              AND uniqueness = 'NONUNIQUE' AND status != 'UNUSABLE'
            """,
            owner=schema,
            tbl=table,
        )
        names = [row[0] for row in cur.fetchall()]

    if not names:
        return
    for name in names:
        try:
            with conn.cursor() as cur:
                cur.execute(f'ALTER INDEX "{schema}"."{name}" UNUSABLE')
        except Exception as exc:
            logger.warning("Cannot disable index %s.%s: %s", schema, name, exc)
    logger.info("Disabled non-unique indexes on %s.%s: %s", schema, table, names)


def _rebuild_target_unusable_indexes(conn: Any, schema: str, table: str) -> None:
    """Rebuild all UNUSABLE indexes on target table after bulk load."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT index_name FROM all_indexes
            WHERE table_owner = :owner AND table_name = :tbl AND status = 'UNUSABLE'
            """,
            owner=schema,
            tbl=table,
        )
        names = [row[0] for row in cur.fetchall()]

    for name in names:
        try:
            t0 = time.monotonic()
            with conn.cursor() as cur:
                cur.execute(f'ALTER INDEX "{schema}"."{name}" REBUILD NOLOGGING')
            logger.info("Rebuilt index %s.%s in %ds", schema, name, round(time.monotonic() - t0))
        except Exception as exc:
            logger.warning("Cannot rebuild index %s.%s: %s", schema, name, exc)


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

        # Child static jobs (hybrid background phase) run with CDC already active.
        # Skip index management to avoid disrupting concurrent CDC writes.
        is_child_static = job.parent_job_id is not None

        select_sql, select_params = _build_source_select(
            source_table, insertable_cols, scn_cutoff, start_rowid, end_rowid
        )
        insert_sql = _build_bulk_insert_sql(target_table, insertable_cols)

        src_dsn, src_user, src_pwd = _source_cfg(self._config)
        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)
        rows_processed = 0
        batch_size = self._config.worker_bulk_batch_size
        lob_batch_size = self._config.worker_bulk_lob_batch_size
        tgt_schema, tgt_table = target_table.split(".", 1)

        with _oracle_connect(src_dsn, src_user, src_pwd) as src_conn:
            with _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:
                with tgt_conn.cursor() as _cur:
                    _cur.execute("ALTER SESSION SET SKIP_UNUSABLE_INDEXES = TRUE")
                if not is_child_static:
                    _disable_target_nonunique_indexes(tgt_conn, tgt_schema, tgt_table)
                chunk_id_str = str(chunk["chunk_id"])
                with src_conn.cursor() as src_cur:
                    src_cur.arraysize = batch_size
                    src_cur.prefetchrows = batch_size
                    t0 = time.monotonic()
                    src_cur.execute(select_sql, select_params)
                    lob_type_codes = _detect_lob_column_type_codes(src_cur.description)
                    lob_indexes = tuple(lob_type_codes.keys())
                    lob_python_type = _resolve_oracledb_lob_type()
                    effective_batch_size = lob_batch_size if lob_indexes else batch_size
                    logger.info(
                        "Bulk SELECT executed: elapsed_ms=%d lob_cols=%d effective_batch=%d",
                        round((time.monotonic() - t0) * 1000),
                        len(lob_indexes),
                        effective_batch_size,
                        extra={"chunk_id": chunk_id_str},
                    )
                    with tgt_conn.cursor() as tgt_cur:
                        _apply_lob_input_sizes(tgt_cur, len(insertable_cols), lob_type_codes)
                        batch_no = 0
                        while True:
                            t_fetch = time.monotonic()
                            rows = src_cur.fetchmany(effective_batch_size)
                            fetch_ms = round((time.monotonic() - t_fetch) * 1000)
                            if not rows:
                                break
                            rows_to_insert = _materialize_lob_batch(rows, lob_indexes, lob_python_type)
                            batch_no += 1
                            t_insert = time.monotonic()
                            tgt_cur.executemany(insert_sql, rows_to_insert, batcherrors=True)
                            insert_ms = round((time.monotonic() - t_insert) * 1000)
                            t_commit = time.monotonic()
                            tgt_conn.commit()
                            commit_ms = round((time.monotonic() - t_commit) * 1000)
                            rows_processed += len(rows_to_insert)
                            logger.info(
                                "Bulk batch applied: batch=%d rows=%d total=%d fetch_ms=%d insert_ms=%d commit_ms=%d",
                                batch_no, len(rows_to_insert), rows_processed, fetch_ms, insert_ms, commit_ms,
                                extra={"chunk_id": chunk_id_str},
                            )

        return rows_processed

    def rebuild_indexes_if_done(self, chunk: dict[str, Any]) -> None:
        """Called after complete_chunk. Rebuilds indexes if this was the last chunk for the job.

        For child static jobs: rebuilds when all its own chunks are done (CDC continues unaffected
        since REBUILD NOLOGGING is fast on historical data and indexes were never disabled for child).
        For parent/standalone jobs: rebuilds when all chunks are done.
        """
        job_id = str(chunk["job_id"])
        if not self._repository.all_chunks_completed(job_id):
            return

        job = self._repository.get_job(job_id)
        target_table = job.target_table_name or job.table_name
        tgt_schema, tgt_table = target_table.split(".", 1)
        tgt_dsn, tgt_user, tgt_pwd = _target_cfg(self._config)

        logger.info(
            "All chunks done — rebuilding indexes on %s.%s (job_id=%s)",
            tgt_schema, tgt_table, job_id,
        )
        with _oracle_connect(tgt_dsn, tgt_user, tgt_pwd) as tgt_conn:
            _rebuild_target_unusable_indexes(tgt_conn, tgt_schema, tgt_table)


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
                    key_row = {k: before[k] for k in state.pk_cols if k in before}
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
        chunk = self._repository.claim_bulk_chunk(self._worker_id)
        if not chunk:
            return False

        chunk_id = str(chunk["chunk_id"])
        logger.info("Bulk chunk claimed", extra={"chunk_id": chunk_id, "worker_id": self._worker_id})
        try:
            rows = self._bulk.process(chunk)
            self._repository.complete_chunk(chunk_id, rows)
            # Check AFTER marking complete — all_chunks_completed may now be True
            self._bulk.rebuild_indexes_if_done(chunk)
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
        chunk = self._repository.claim_bulk_chunk(self._worker_id)
        if not chunk:
            return False

        chunk_id = str(chunk["chunk_id"])
        logger.info("Bulk chunk claimed", extra={"chunk_id": chunk_id, "worker_id": self._worker_id})
        try:
            rows = self._bulk.process(chunk)
            self._repository.complete_chunk(chunk_id, rows)
            self._bulk.rebuild_indexes_if_done(chunk)
            logger.info("Bulk chunk completed", extra={"chunk_id": chunk_id, "rows": rows})
        except Exception as exc:
            logger.exception("Bulk chunk failed", extra={"chunk_id": chunk_id})
            self._repository.fail_chunk(chunk_id, str(exc))
        return True
