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
        if migration_mode not in {"cdc", "static", "hybrid"}:
            raise ValidationError("migration_mode must be 'cdc', 'static', or 'hybrid'")

        raw_key_columns = payload.get("message_key_columns")
        if raw_key_columns is not None and not isinstance(raw_key_columns, list):
            raise ValidationError("message_key_columns must be an array of strings")

        key_columns = [str(value).strip() for value in (raw_key_columns or []) if str(value).strip()]

        raw_recent_rows = payload.get("recent_rows")
        if raw_recent_rows is not None:
            try:
                raw_recent_rows = int(raw_recent_rows)
                if raw_recent_rows <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                raise ValidationError("recent_rows must be a positive integer")

        create_request = CreateJobRequest(
            table_name=table_name,
            target_table_name=payload.get("target_table_name"),
            migration_mode=migration_mode,
            message_key_columns=key_columns or None,
            idempotency_key=request.headers.get("Idempotency-Key") or payload.get("idempotency_key"),
            chunk_size=payload.get("chunk_size"),
            recent_rows=raw_recent_rows,
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
        stats = repository.get_chunk_stats(job_id)

        per_page = request.args.get("per_page", default=0, type=int)
        if per_page > 0:
            per_page = min(per_page, 500)
            search  = request.args.get("search",  "").strip()
            sort_by = request.args.get("sort",    "assigned_at")
            order   = request.args.get("order",   "asc")
            page    = max(request.args.get("page", default=1, type=int), 1)
            chunks, total = repository.list_chunks_paginated(
                job_id, search=search, sort_by=sort_by, order=order, page=page, per_page=per_page,
            )
            pages = max(1, (total + per_page - 1) // per_page)
            return jsonify({
                "chunks": chunks, "stats": stats,
                "total": total, "page": page, "per_page": per_page, "pages": pages,
            }), 200

        return jsonify({"chunks": [], "stats": stats}), 200

    @bp.get("/tables")
    def list_tables():
        search  = request.args.get("search",   "").strip()
        sort_by = request.args.get("sort",      "table_name")
        order   = request.args.get("order",     "asc")
        page    = request.args.get("page",      default=1,  type=int)
        per_page = request.args.get("per_page", default=20, type=int)
        per_page = min(max(per_page, 1), 200)
        items, total = repository.list_tables_paginated(
            search=search, sort_by=sort_by, order=order, page=page, per_page=per_page,
        )
        pages = max(1, (total + per_page - 1) // per_page)
        return jsonify({"items": items, "total": total, "page": page, "per_page": per_page, "pages": pages}), 200

    @bp.delete("/tables/<path:table_name>")
    def delete_table(table_name: str):
        result = service.delete_jobs_for_table(table_name)
        return jsonify(result), 200

    @bp.get("/tables/<path:table_name>/jobs")
    def get_jobs_for_table(table_name: str):
        jobs = repository.list_jobs_for_table(table_name)
        items = []
        for job in jobs:
            d = job.to_dict()
            d["_chunk_stats"] = repository.get_chunk_stats(job.job_id)
            items.append(d)
        return jsonify({"items": items, "count": len(items)}), 200

    @bp.get("/source-tables")
    def list_source_tables():
        source_cfg = (
            OracleClientConfig(
                dsn=config.oracle_source_dsn,
                user=config.oracle_source_user,
                password=config.oracle_source_password,
            )
            if config.oracle_source_dsn and config.oracle_source_user and config.oracle_source_password
            else None
        )
        introspector = OracleIntrospector(
            source=source_cfg,
            target=None,
            source_schema=config.oracle_source_schema,
            target_schema=None,
        )
        tables = introspector.list_source_tables()
        return jsonify({"tables": tables, "schema": config.oracle_source_schema}), 200

    @bp.get("/target-tables")
    def list_target_tables():
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
        tables = introspector.list_target_tables()
        return jsonify({"tables": tables, "schema": config.oracle_target_schema}), 200

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
                    "stale_chunks_reclaimed": result.stale_chunks_reclaimed,
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

