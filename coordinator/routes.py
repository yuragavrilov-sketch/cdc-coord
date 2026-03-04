from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

from .config import AppConfig
from .errors import ValidationError
from .monitoring import MonitoringService
from .oracle import OracleClientConfig, OracleIntrospector
from .repositories import CoordinatorRepository
from .services import CoordinatorService, CreateJobRequest
from .status_checks import (
    check_kafka_connect_connection,
    check_kafka_connection,
    check_oracle_connection,
    check_state_db_connection,
    collect_status_payloads,
)

logger = logging.getLogger(__name__)


def build_api_blueprint(
    service: CoordinatorService,
    monitoring: MonitoringService,
    config: AppConfig,
    repository: CoordinatorRepository,
) -> Blueprint:
    bp = Blueprint("coordinator_api", __name__)

    @bp.get("/health")
    def api_health() -> tuple[dict, int]:
        return {"status": "ok", "service": "coordinator-api"}, 200

    @bp.post("/jobs")
    def create_job():
        payload = request.get_json(silent=True) or {}
        if not payload:
            raise ValidationError("Request body must be a JSON object")

        table_name = (payload.get("table_name") or "").strip()
        migration_mode = (payload.get("migration_mode") or "").strip().lower()
        if not table_name:
            raise ValidationError("table_name is required")
        if migration_mode not in {"cdc", "static"}:
            raise ValidationError("migration_mode must be 'cdc' or 'static'")

        raw_key_columns = payload.get("message_key_columns")
        if raw_key_columns is not None and not isinstance(raw_key_columns, list):
            raise ValidationError("message_key_columns must be an array of strings")

        key_columns = [str(value).strip() for value in (raw_key_columns or []) if str(value).strip()]

        create_request = CreateJobRequest(
            table_name=table_name,
            target_table_name=payload.get("target_table_name"),
            migration_mode=migration_mode,
            message_key_columns=key_columns or None,
            idempotency_key=request.headers.get("Idempotency-Key") or payload.get("idempotency_key"),
            chunk_count=payload.get("chunk_count"),
        )
        job = service.create_job(create_request)
        return jsonify(job.to_dict()), 201

    @bp.get("/jobs")
    def list_jobs():
        limit = request.args.get("limit", default=100, type=int)
        limit = min(max(limit, 1), 1000)
        jobs = [job.to_dict() for job in service.list_jobs(limit=limit)]
        return jsonify({"items": jobs, "count": len(jobs)}), 200

    @bp.get("/jobs/<job_id>")
    def get_job(job_id: str):
        return jsonify(service.get_job(job_id).to_dict()), 200

    @bp.delete("/jobs/<job_id>")
    def delete_job(job_id: str):
        result = service.delete_job(job_id)
        return jsonify({
            "deleted": True,
            "debezium_warning": result.get("debezium_warning"),
        }), 200

    @bp.get("/jobs/<job_id>/chunks")
    def get_job_chunks(job_id: str):
        service.get_job(job_id)  # raises NotFoundError if job doesn't exist
        chunks = repository.list_chunks(job_id)

        by_status: dict[str, int] = {}
        for c in chunks:
            by_status[c["status"]] = by_status.get(c["status"], 0) + 1

        completed = [c for c in chunks if c["status"] == "completed" and c.get("elapsed_seconds")]
        total_elapsed = sum(float(c["elapsed_seconds"]) for c in completed)
        total_rows_completed = sum(c["rows_processed"] for c in completed)
        avg_speed = round(total_rows_completed / total_elapsed, 1) if total_elapsed > 0 else None

        return jsonify({
            "chunks": chunks,
            "stats": {
                "total": len(chunks),
                "by_status": by_status,
                "rows_processed": sum(c["rows_processed"] for c in chunks),
                "avg_rows_per_second": avg_speed,
            },
        }), 200

    @bp.get("/tables/<path:table_name>/columns")
    def get_table_columns(table_name: str):
        target_cfg = (
            OracleClientConfig(
                dsn=config.oracle_target_dsn,
                user=config.oracle_target_user,
                password=config.oracle_target_password,
            )
            if config.oracle_target_dsn and config.oracle_target_user and config.oracle_target_password
            else None
        )
        introspector = OracleIntrospector(
            source=None,
            target=target_cfg,
            source_schema=None,
            target_schema=config.oracle_target_schema,
        )
        metadata = introspector.fetch_table_metadata(table_name)
        not_null_columns = [c for c in metadata.all_columns if c not in metadata.nullable_columns]
        return jsonify({
            "columns": metadata.all_columns,
            "pk_columns": metadata.pk_columns,
            "not_null_columns": not_null_columns,
        }), 200

    @bp.post("/monitor/run-once")
    def run_monitor_once():
        result = monitoring.run_once()
        return (
            jsonify(
                {
                    "bulk_transitions": result.bulk_transitions,
                    "connectors": result.connectors,
                    "released_owners": result.released_owners,
                }
            ),
            200,
        )

    @bp.get("/connections/src-db")
    def api_connection_src_db():
        return jsonify(check_oracle_connection(config.oracle_source_dsn, name="source-oracle")), 200

    @bp.get("/connections/dst-db")
    def api_connection_dst_db():
        return jsonify(check_oracle_connection(config.oracle_target_dsn, name="target-oracle")), 200

    @bp.get("/connections/statedb")
    def api_connection_statedb():
        return jsonify(check_state_db_connection(repository)), 200

    @bp.get("/connections/kafka")
    def api_connection_kafka():
        return jsonify(check_kafka_connection(config)), 200

    @bp.get("/connections/kafka-connect")
    def api_connection_kafka_connect():
        return jsonify(check_kafka_connect_connection(config.debezium_connect_url)), 200

    @bp.get("/connections")
    def api_connections():
        payloads = collect_status_payloads(config, repository)
        return jsonify({"items": list(payloads.values()), "count": len(payloads)}), 200

    @bp.get("/system-status")
    def api_system_status():
        payloads = collect_status_payloads(config, repository)
        statuses = {item.get("status", "unknown") for item in payloads.values()}
        overall = "down" if "down" in statuses else "up" if statuses == {"up"} else "degraded"
        return jsonify({"status": overall, "services": payloads}), 200

    return bp


def build_legacy_api_blueprint(
    service: CoordinatorService,
    config: AppConfig,
    repository: CoordinatorRepository,
) -> Blueprint:
    bp = Blueprint("coordinator_api_legacy", __name__)

    @bp.get("/jobs")
    def legacy_list_jobs():
        limit = request.args.get("limit", default=100, type=int)
        limit = min(max(limit, 1), 1000)
        try:
            jobs = [job.to_dict() for job in service.list_jobs(limit=limit)]
        except Exception:
            logger.exception("Failed to list jobs for legacy API")
            jobs = []
        return jsonify({"items": jobs, "count": len(jobs)}), 200

    @bp.get("/workers")
    def legacy_workers():
        # Worker registry is not implemented in this service yet.
        return jsonify({"items": [], "count": 0, "status": "unknown"}), 200

    @bp.get("/connections/db")
    @bp.get("/connections/statedb")
    def legacy_connection_db():
        return jsonify(check_state_db_connection(repository)), 200

    @bp.get("/connections/src-db")
    def legacy_connection_src_db():
        return jsonify(check_oracle_connection(config.oracle_source_dsn, name="source-oracle")), 200

    @bp.get("/connections/dst-db")
    def legacy_connection_dst_db():
        return jsonify(check_oracle_connection(config.oracle_target_dsn, name="target-oracle")), 200

    @bp.get("/connections/kafka")
    def legacy_connection_kafka():
        return jsonify(check_kafka_connection(config)), 200

    @bp.get("/connections/kafka-connect")
    def legacy_connection_kafka_connect():
        return jsonify(check_kafka_connect_connection(config.debezium_connect_url)), 200

    @bp.get("/connections")
    def legacy_connections():
        src_response, _ = legacy_connection_src_db()
        dst_response, _ = legacy_connection_dst_db()
        state_response, _ = legacy_connection_db()
        kafka_response, _ = legacy_connection_kafka()
        connect_response, _ = legacy_connection_kafka_connect()
        return (
            jsonify(
                {
                    "items": [
                        src_response.get_json(),
                        dst_response.get_json(),
                        state_response.get_json(),
                        kafka_response.get_json(),
                        connect_response.get_json(),
                    ],
                    "count": 5,
                }
            ),
            200,
        )

    @bp.get("/stats")
    def legacy_stats():
        jobs_response, _ = legacy_list_jobs()
        jobs_payload = jobs_response.get_json(silent=True) or {}
        count = int(jobs_payload.get("count") or 0)
        return jsonify({"jobs_total": count, "jobs_active": count, "workers": 0}), 200

    @bp.get("/system-status")
    def legacy_system_status():
        src_response, _ = legacy_connection_src_db()
        dst_response, _ = legacy_connection_dst_db()
        state_response, _ = legacy_connection_db()
        kafka_response, _ = legacy_connection_kafka()
        connect_response, _ = legacy_connection_kafka_connect()

        services = {
            "src_db": src_response.get_json(silent=True) or {"status": "unknown"},
            "dst_db": dst_response.get_json(silent=True) or {"status": "unknown"},
            "statedb": state_response.get_json(silent=True) or {"status": "unknown"},
            "kafka": kafka_response.get_json(silent=True) or {"status": "unknown"},
            "kafka_connect": connect_response.get_json(silent=True) or {"status": "unknown"},
        }
        statuses = {item.get("status", "unknown") for item in services.values()}
        overall = "down" if "down" in statuses else "up" if statuses == {"up"} else "degraded"
        return jsonify({"status": overall, "services": services}), 200

    return bp

