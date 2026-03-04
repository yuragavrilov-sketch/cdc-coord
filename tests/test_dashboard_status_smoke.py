from __future__ import annotations

import unittest
from unittest.mock import patch

from coordinator.app import create_app
from coordinator.config import AppConfig


class DashboardStatusSmokeTests(unittest.TestCase):
    def _build_app(self):
        config = AppConfig(
            postgres_dsn="postgresql://invalid:invalid@127.0.0.1:1/invalid",
            debezium_connect_url="http://127.0.0.1:1",
            oracle_source_dsn="127.0.0.1:1/source",
            oracle_target_dsn="127.0.0.1:1/target",
        )
        return create_app(config)

    def test_root_returns_200(self):
        app = self._build_app()
        client = app.test_client()

        response = client.get("/")

        self.assertEqual(response.status_code, 200)

    def test_legacy_db_connection_has_status_and_message(self):
        app = self._build_app()
        client = app.test_client()

        response = client.get("/api/connections/db")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(payload, dict)
        self.assertIn("status", payload)
        self.assertIn("message", payload)

    def test_src_card_shows_down_and_reason_when_source_check_fails(self):
        app = self._build_app()
        client = app.test_client()

        mocked_statuses = [
            {
                "name": "SRC DB",
                "key": "src_db",
                "status": "down",
                "message": "Connection failed to source Oracle (ConnectionRefusedError)",
            },
            {"name": "DST DB", "key": "dst_db", "status": "up", "message": "ok"},
            {"name": "KAFKA", "key": "kafka", "status": "unknown", "message": "n/a"},
            {"name": "KAFKACONNECT", "key": "kafkaconnect", "status": "unknown", "message": "n/a"},
            {"name": "STATEDB", "key": "statedb", "status": "unknown", "message": "n/a"},
        ]

        with patch("coordinator.ui.collect_dashboard_service_statuses", return_value=mocked_statuses):
            response = client.get("/")

        html = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('data-service-key="src_db"', html)
        self.assertIn("DOWN", html)
        self.assertIn("Connection failed to source Oracle (ConnectionRefusedError)", html)


if __name__ == "__main__":
    unittest.main()

