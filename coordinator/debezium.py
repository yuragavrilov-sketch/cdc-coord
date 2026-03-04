from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any

from .errors import ExternalServiceError

logger = logging.getLogger(__name__)


class DebeziumClient:
    def __init__(self, connect_url: str, timeout_seconds: int = 10) -> None:
        self._base_url = connect_url.rstrip("/")
        self._timeout = timeout_seconds

    def create_connector(self, name: str, config: dict[str, Any]) -> None:
        payload = {"name": name, "config": config}
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self._base_url}/connectors",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as response:
                response.read()
            logger.info("Debezium connector created", extra={"connector": name})
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="ignore")
            if exc.code == 409:
                logger.info("Debezium connector already exists", extra={"connector": name})
                return
            raise ExternalServiceError(
                f"Failed to create Debezium connector '{name}'",
                details={"status": exc.code, "response": response_body},
            ) from exc
        except urllib.error.URLError as exc:
            raise ExternalServiceError(
                "Debezium connect is unavailable",
                details={"reason": str(exc.reason)},
            ) from exc

    def get_connector_status(self, name: str) -> str:
        req = urllib.request.Request(f"{self._base_url}/connectors/{name}/status", method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return "MISSING"
            body = exc.read().decode("utf-8", errors="ignore")
            raise ExternalServiceError(
                f"Failed to read Debezium connector status for '{name}'",
                details={"status": exc.code, "response": body},
            ) from exc
        except urllib.error.URLError as exc:
            raise ExternalServiceError(
                "Debezium connect is unavailable",
                details={"reason": str(exc.reason)},
            ) from exc

        connector_state = payload.get("connector", {}).get("state", "UNKNOWN")
        task_states = [task.get("state", "UNKNOWN") for task in payload.get("tasks", [])]
        if connector_state == "RUNNING" and all(state == "RUNNING" for state in task_states):
            return "RUNNING"
        if connector_state == "FAILED" or any(state == "FAILED" for state in task_states):
            return "FAILED"
        return connector_state

    def delete_connector(self, name: str) -> str | None:
        """Delete a connector. Returns None on success, or an error message string."""
        req = urllib.request.Request(
            f"{self._base_url}/connectors/{name}",
            method="DELETE",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as response:
                response.read()
            logger.info("Debezium connector deleted", extra={"connector": name})
            return None
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                logger.info("Debezium connector not found (already deleted)", extra={"connector": name})
                return None
            body = exc.read().decode("utf-8", errors="ignore")
            return f"HTTP {exc.code}: {body}"
        except urllib.error.URLError as exc:
            return f"Debezium недоступен: {exc.reason}"

    def wait_for_running(self, name: str, timeout_seconds: int, poll_interval_seconds: int) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_status = "UNKNOWN"
        while time.monotonic() < deadline:
            last_status = self.get_connector_status(name)
            if last_status == "RUNNING":
                return
            if last_status == "FAILED":
                raise ExternalServiceError(
                    f"Debezium connector '{name}' entered FAILED state",
                    details={"status": last_status},
                )
            time.sleep(poll_interval_seconds)

        raise ExternalServiceError(
            f"Timeout waiting Debezium connector '{name}' to RUNNING",
            details={"status": last_status, "timeout_seconds": timeout_seconds},
        )

