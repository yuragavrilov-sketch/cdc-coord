from __future__ import annotations

import logging

from flask import Blueprint, render_template

from .config import AppConfig
from .repositories import CoordinatorRepository
from .status_checks import collect_dashboard_service_statuses

logger = logging.getLogger(__name__)


def build_ui_blueprint(config: AppConfig, repository: CoordinatorRepository) -> Blueprint:
    bp = Blueprint("coordinator_ui", __name__, template_folder="templates")

    @bp.get("/")
    def dashboard():
        service_statuses = collect_dashboard_service_statuses(config, repository)
        return render_template("dashboard.html", service_statuses=service_statuses)

    return bp
