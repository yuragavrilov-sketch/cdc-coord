from __future__ import annotations

import logging
from http import HTTPStatus

from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException

logger = logging.getLogger(__name__)


class CoordinatorError(Exception):
    status_code = HTTPStatus.BAD_REQUEST
    error_code = "coordinator_error"

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_response(self) -> tuple[dict, int]:
        payload = {
            "error": self.error_code,
            "message": self.message,
            "details": self.details,
        }
        return payload, int(self.status_code)


class ValidationError(CoordinatorError):
    error_code = "validation_error"


class ConflictError(CoordinatorError):
    status_code = HTTPStatus.CONFLICT
    error_code = "conflict"


class NotFoundError(CoordinatorError):
    status_code = HTTPStatus.NOT_FOUND
    error_code = "not_found"


class ExternalServiceError(CoordinatorError):
    status_code = HTTPStatus.BAD_GATEWAY
    error_code = "external_service_error"


def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(CoordinatorError)
    def handle_coordinator_error(exc: CoordinatorError):
        payload, code = exc.to_response()
        return jsonify(payload), code

    @app.errorhandler(Exception)
    def handle_unexpected_error(exc: Exception):
        if isinstance(exc, HTTPException):
            return exc
        logger.exception("Unhandled exception in coordinator")
        return jsonify(
            {
                "error": "internal_error",
                "message": "Internal server error",
                "details": {},
            }
        ), HTTPStatus.INTERNAL_SERVER_ERROR

