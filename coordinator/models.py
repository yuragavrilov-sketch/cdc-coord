from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class JobRecord:
    job_id: str
    table_name: str
    target_table_name: str
    migration_mode: str
    message_key_columns: list[str] | None
    scn_cutoff: int | None
    catchup_target: int | None
    debezium_connector_name: str | None
    kafka_topic_name: str | None
    consumer_group_name: str | None
    debezium_config: dict[str, Any] | None
    cdc_owner_worker_id: str | None
    cdc_started_at: datetime | None
    cdc_heartbeat_at: datetime | None
    status: str
    config: dict[str, Any]
    idempotency_key: str | None
    created_at: datetime | None
    completed_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SqlTemplateRecord:
    job_id: str
    table_name: str
    pk_columns: list[str]
    all_columns: list[str]
    insertable_columns: list[str]
    bulk_merge_sql: str
    bulk_insert_sql: str | None
    cdc_merge_sql: str | None
    cdc_delete_sql: str | None
    has_lobs: bool
    has_timestamps: bool


@dataclass(slots=True)
class RowIdChunk:
    start_rowid: str | None
    end_rowid: str | None


@dataclass(slots=True)
class OracleTableMetadata:
    table_name: str
    all_columns: list[str]
    insertable_columns: list[str]
    pk_columns: list[str]
    nullable_columns: set[str]
    has_lobs: bool
    has_timestamps: bool

