from flask import Flask

from .config import AppConfig
from .db import Database
from .errors import register_error_handlers
from .logging_config import configure_logging
from .monitoring import MonitoringService
from .repositories import CoordinatorRepository
from .routes import build_api_blueprint, build_legacy_api_blueprint
from .services import CoordinatorService
from .ui import build_ui_blueprint


def create_app(config: AppConfig | None = None) -> Flask:
    app_config = config or AppConfig.from_env()
    configure_logging(app_config.log_level)

    app = Flask(__name__)
    app.config["COORDINATOR_CONFIG"] = app_config

    database = Database(app_config.postgres_dsn)
    repository = CoordinatorRepository(database)
    service = CoordinatorService(app_config, repository)
    monitoring = MonitoringService(app_config, repository)

    try:
        repository.ensure_schema()
    except Exception:
        app.logger.exception("Failed to ensure schema on startup; continuing in degraded mode")

    monitoring.start_background_loop(interval_seconds=30)

    app.register_blueprint(build_ui_blueprint(app_config, repository))
    app.register_blueprint(build_api_blueprint(service, monitoring, app_config, repository), url_prefix="/api/v1")
    app.register_blueprint(build_legacy_api_blueprint(service, app_config, repository), url_prefix="/api")
    register_error_handlers(app)

    @app.get("/health")
    def health() -> tuple[dict, int]:
        try:
            database_ok = repository.ping()
        except Exception:
            database_ok = False
        return {
            "status": "ok" if database_ok else "degraded",
            "database": "up" if database_ok else "down",
            "service": "coordinator",
        }, 200 if database_ok else 503

    return app

