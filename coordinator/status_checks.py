from __future__ import annotations

import socket
import urllib.error
import urllib.request
from urllib.parse import urlparse

from .config import AppConfig
from .repositories import CoordinatorRepository


def check_oracle_connection(dsn: str | None, *, name: str, timeout: float = 2.0) -> dict[str, str | None]:
    if not dsn:
        return {
            "name": name,
            "status": "unknown",
            "message": "DSN is not configured",
            "error": "missing_dsn",
        }

    host, port = _extract_oracle_host_port(dsn)
    if not host:
        return {
            "name": name,
            "status": "unknown",
            "message": "Unable to parse host from DSN",
            "error": "invalid_dsn",
        }

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return {
                "name": name,
                "status": "up",
                "message": f"Connected to {host}:{port}",
                "error": None,
            }
    except OSError as exc:
        return {
            "name": name,
            "status": "down",
            "message": f"Connection failed to {host}:{port}",
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "name": name,
            "status": "unknown",
            "message": f"Unexpected error while checking {name}",
            "error": str(exc),
        }


def check_state_db_connection(repository: CoordinatorRepository) -> dict[str, str | None]:
    try:
        is_up = bool(repository.ping())
        return {
            "name": "state-db",
            "status": "up" if is_up else "down",
            "message": "SELECT 1 succeeded" if is_up else "Repository ping returned False",
            "error": None if is_up else "ping_false",
        }
    except Exception as exc:
        return {
            "name": "state-db",
            "status": "down",
            "message": "State DB ping failed",
            "error": str(exc),
        }


def check_kafka_connection(config: AppConfig, timeout: float = 2.0) -> dict[str, str | None]:
    broker = config.debezium_connector_template.get("bootstrap.servers") or ""
    if not broker:
        return {
            "name": "kafka",
            "status": "unknown",
            "message": "bootstrap.servers is not configured",
            "error": "missing_bootstrap_servers",
        }

    first = broker.split(",", 1)[0].strip()
    if not first:
        return {
            "name": "kafka",
            "status": "unknown",
            "message": "bootstrap.servers is empty",
            "error": "invalid_bootstrap_servers",
        }

    host, port = _split_host_port(first, default_port=9092)
    if not host:
        return {
            "name": "kafka",
            "status": "unknown",
            "message": "Unable to parse Kafka host",
            "error": "invalid_bootstrap_servers",
        }

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return {
                "name": "kafka",
                "status": "up",
                "message": f"Connected to {host}:{port}",
                "error": None,
            }
    except OSError as exc:
        return {
            "name": "kafka",
            "status": "down",
            "message": f"Connection failed to {host}:{port}",
            "error": str(exc),
        }


def check_kafka_connect_connection(connect_url: str, timeout: float = 2.0) -> dict[str, str | None]:
    if not connect_url:
        return {
            "name": "kafka-connect",
            "status": "unknown",
            "message": "Debezium Connect URL is not configured",
            "error": "missing_connect_url",
        }

    req = urllib.request.Request(f"{connect_url.rstrip('/')}/connectors", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            response.read()
        return {
            "name": "kafka-connect",
            "status": "up",
            "message": f"HTTP check succeeded: {connect_url.rstrip('/')}/connectors",
            "error": None,
        }
    except urllib.error.URLError as exc:
        return {
            "name": "kafka-connect",
            "status": "down",
            "message": "Debezium Connect is unavailable",
            "error": str(exc.reason or exc),
        }
    except Exception as exc:
        return {
            "name": "kafka-connect",
            "status": "unknown",
            "message": "Unexpected error while checking Debezium Connect",
            "error": str(exc),
        }


def collect_status_payloads(config: AppConfig, repository: CoordinatorRepository) -> dict[str, dict[str, str | None]]:
    return {
        "src_db": check_oracle_connection(config.oracle_source_dsn, name="source-oracle"),
        "dst_db": check_oracle_connection(config.oracle_target_dsn, name="target-oracle"),
        "statedb": check_state_db_connection(repository),
        "kafka": check_kafka_connection(config),
        "kafka_connect": check_kafka_connect_connection(config.debezium_connect_url),
    }


def collect_dashboard_service_statuses(
    config: AppConfig,
    repository: CoordinatorRepository,
) -> list[dict[str, str | None]]:
    payloads = collect_status_payloads(config, repository)
    return [
        _to_dashboard_item("SRC DB", "src_db", payloads["src_db"]),
        _to_dashboard_item("DST DB", "dst_db", payloads["dst_db"]),
        _to_dashboard_item("KAFKA", "kafka", payloads["kafka"]),
        _to_dashboard_item("KAFKACONNECT", "kafkaconnect", payloads["kafka_connect"]),
        _to_dashboard_item("STATEDB", "statedb", payloads["statedb"]),
    ]


def _to_dashboard_item(title: str, key: str, payload: dict[str, str | None]) -> dict[str, str | None]:
    base_message = payload.get("message") or ""
    error = payload.get("error")
    return {
        "name": title,
        "key": key,
        "status": payload.get("status") or "unknown",
        "message": f"{base_message} ({error})" if error else base_message,
    }


def _extract_oracle_host_port(dsn: str) -> tuple[str | None, int]:
    text = dsn.strip()
    if not text:
        return None, 1521

    if "//" in text:
        parsed = urlparse(text if text.startswith(("http://", "https://")) else f"oracle://{text}")
        return parsed.hostname, parsed.port or 1521

    host, port = _split_host_port(text, default_port=1521)
    return host, port


def _split_host_port(value: str, default_port: int) -> tuple[str | None, int]:
    host = value
    port = default_port
    if ":" in value:
        raw_host, raw_port = value.rsplit(":", 1)
        host = raw_host.strip()
        if raw_port.strip().isdigit():
            port = int(raw_port.strip())
    return host or None, port

