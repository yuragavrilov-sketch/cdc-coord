from __future__ import annotations

import os
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


_ORACLE_IDENTIFIER_RE = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def normalize_oracle_identifier(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _strip_quotes(value.strip()).upper()
    if not normalized:
        return None
    if not _ORACLE_IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"Invalid Oracle identifier: {value!r}")
    return normalized


def normalize_oracle_schema(value: str | None, fallback_user: str | None = None) -> str | None:
    schema = normalize_oracle_identifier(value)
    if schema:
        return schema
    return normalize_oracle_identifier(fallback_user)


def load_env_file(path: str | Path = ".env") -> None:
    env_path = Path(path)
    if not env_path.is_absolute():
        env_path = Path.cwd() / env_path

    if not env_path.exists() or not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = _strip_quotes(value.strip())
        os.environ.setdefault(key, value)


@dataclass(slots=True)
class AppConfig:
    postgres_dsn: str
    debezium_connect_url: str
    debezium_connector_template: dict[str, str] = field(default_factory=dict)
    debezium_schema_history_topic_prefix: str = "migration.schema-history"
    topic_prefix: str = "migration"
    log_level: str = "INFO"
    monitor_heartbeat_timeout_seconds: int = 120
    monitor_connector_start_timeout_seconds: int = 60
    monitor_connector_poll_interval_seconds: int = 2
    default_chunk_size: int = 100_000
    oracle_source_dsn: str | None = None
    oracle_source_user: str | None = None
    oracle_source_password: str | None = None
    oracle_source_schema: str | None = None
    oracle_target_dsn: str | None = None
    oracle_target_user: str | None = None
    oracle_target_password: str | None = None
    oracle_target_schema: str | None = None
    kafka_bootstrap_servers: str = "localhost:9092"
    worker_bulk_batch_size: int = 1000
    worker_bulk_lob_batch_size: int = 100
    worker_cdc_batch_size: int = 500
    worker_poll_interval_seconds: int = 5
    worker_heartbeat_interval_seconds: int = 30
    worker_chunk_timeout_seconds: int = 600
    # Hybrid mode
    hybrid_recent_rows: int = 2_000_000
    # CDC worker (multi-table)
    cdc_worker_job_limit: int = 20
    cdc_worker_refresh_interval_seconds: int = 10

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_env_file()

        source_user = normalize_oracle_identifier(os.getenv("ORACLE_SOURCE_USER"))
        target_user = normalize_oracle_identifier(os.getenv("ORACLE_TARGET_USER"))
        source_schema = normalize_oracle_schema(os.getenv("ORACLE_SOURCE_SCHEMA"), fallback_user=source_user)
        target_schema = normalize_oracle_schema(os.getenv("ORACLE_TARGET_SCHEMA"), fallback_user=target_user)

        topic_prefix = os.getenv("TOPIC_PREFIX", "migration")
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        schema_history_prefix = os.getenv("DEBEZIUM_SCHEMA_HISTORY_TOPIC_PREFIX", "migration.schema-history")
        lob_enabled = os.getenv("DEBEZIUM_LOB_ENABLED", "false").lower() in {"true", "1", "yes"}

        template = {
            "connector.class": "io.debezium.connector.oracle.OracleConnector",
            "tasks.max": "1",
            "snapshot.mode": "no_data",
            "snapshot.locking.mode": "none",
            "log.mining.strategy": "online_catalog",
            "database.connection.adapter": "logminer",
            "log.mining.continuous.mine": "true",
            "heartbeat.interval.ms": "30000",
            "topic.prefix": topic_prefix,
            "database.hostname": os.getenv("ORACLE_SOURCE_HOST", ""),
            "database.port": os.getenv("ORACLE_SOURCE_PORT", "1521"),
            "database.user": source_user or "",
            "database.password": os.getenv("ORACLE_SOURCE_PASSWORD", ""),
            "database.dbname": os.getenv("ORACLE_SOURCE_SERVICE", ""),
            # Topic auto-creation
            "topic.creation.default.replication.factor": os.getenv("DEBEZIUM_TOPIC_REPLICATION_FACTOR", "1"),
            "topic.creation.default.partitions": os.getenv("DEBEZIUM_TOPIC_PARTITIONS", "1"),
            "topic.creation.default.cleanup.policy": os.getenv("DEBEZIUM_TOPIC_CLEANUP_POLICY", "delete"),
            "topic.creation.default.retention.ms": os.getenv("DEBEZIUM_TOPIC_RETENTION_MS", "604800000"),
            "topic.creation.default.compression.type": os.getenv("DEBEZIUM_TOPIC_COMPRESSION_TYPE", "snappy"),
            # LOB handling
            "lob.enabled": "true" if lob_enabled else "false",
            "lob.fetch.size": os.getenv("DEBEZIUM_LOB_FETCH_SIZE", "0"),
            "lob.fetch.buffer.size": os.getenv("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", "0"),
            # Schema history (per-connector topic added in _build_debezium_config)
            "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap,
            "include.schema.changes": "false",
            # Message converters
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            # Type handling
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            # Misc
            "provide.transaction.metadata": "false",
            "tombstones.on.delete": "true",
            "skipped.operations": "none",
            # Transforms: unwrap is static; route regex/replacement are per-connector
            "transforms": "unwrap,route",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,table,source.ts_ms",
            "transforms.unwrap.add.fields.prefix": "__",
            "transforms.route.type": "io.debezium.transforms.ByLogicalTableRouter",
        }

        return cls(
            postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"),
            debezium_connect_url=os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083"),
            debezium_connector_template=template,
            debezium_schema_history_topic_prefix=schema_history_prefix,
            topic_prefix=topic_prefix,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            monitor_heartbeat_timeout_seconds=int(os.getenv("CDC_HEARTBEAT_TIMEOUT_SECONDS", "120")),
            monitor_connector_start_timeout_seconds=int(os.getenv("CONNECTOR_START_TIMEOUT_SECONDS", "60")),
            monitor_connector_poll_interval_seconds=int(os.getenv("CONNECTOR_POLL_INTERVAL_SECONDS", "2")),
            default_chunk_size=int(os.getenv("DEFAULT_CHUNK_SIZE", "100000")),
            oracle_source_dsn=os.getenv("ORACLE_SOURCE_DSN"),
            oracle_source_user=source_user,
            oracle_source_password=os.getenv("ORACLE_SOURCE_PASSWORD"),
            oracle_source_schema=source_schema,
            oracle_target_dsn=os.getenv("ORACLE_TARGET_DSN"),
            oracle_target_user=target_user,
            oracle_target_password=os.getenv("ORACLE_TARGET_PASSWORD"),
            oracle_target_schema=target_schema,
            kafka_bootstrap_servers=kafka_bootstrap,
            worker_bulk_batch_size=int(os.getenv("WORKER_BULK_BATCH_SIZE", "1000")),
            worker_bulk_lob_batch_size=int(os.getenv("WORKER_BULK_LOB_BATCH_SIZE", "100")),
            worker_cdc_batch_size=int(os.getenv("WORKER_CDC_BATCH_SIZE", "500")),
            worker_poll_interval_seconds=int(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "5")),
            worker_heartbeat_interval_seconds=int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "30")),
            worker_chunk_timeout_seconds=int(os.getenv("WORKER_CHUNK_TIMEOUT_SECONDS", "600")),
            hybrid_recent_rows=int(os.getenv("HYBRID_RECENT_ROWS", "2000000")),
            cdc_worker_job_limit=int(os.getenv("CDC_WORKER_JOB_LIMIT", "20")),
            cdc_worker_refresh_interval_seconds=int(os.getenv("CDC_WORKER_REFRESH_INTERVAL_SECONDS", "10")),
        )

    def validate(self) -> None:
        required = {
            "postgres_dsn": self.postgres_dsn,
            "debezium_connect_url": self.debezium_connect_url,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required settings: {', '.join(missing)}")

    def merge_connector_template(self, updates: dict[str, Any]) -> dict[str, Any]:
        cfg: dict[str, Any] = dict(self.debezium_connector_template)
        cfg.update(updates)
        return cfg

