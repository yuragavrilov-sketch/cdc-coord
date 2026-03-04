from __future__ import annotations

import unittest

from coordinator.config import AppConfig
from coordinator.models import OracleTableMetadata
from coordinator.services import CoordinatorService


class DebeziumRegexNormalizationTests(unittest.TestCase):
    def _build_fake_service(self):
        config = AppConfig(
            postgres_dsn="postgresql://invalid:invalid@127.0.0.1:1/invalid",
            debezium_connect_url="http://127.0.0.1:1",
        )

        class _FakeService:
            def __init__(self, cfg: AppConfig):
                self._config = cfg

        return _FakeService(config)

    @staticmethod
    def _metadata(table_name: str = "ISS.ORDERS") -> OracleTableMetadata:
        return OracleTableMetadata(
            table_name=table_name,
            all_columns=["ID"],
            insertable_columns=["ID"],
            pk_columns=["ID"],
            nullable_columns=set(),
            has_lobs=False,
            has_timestamps=False,
        )

    def test_topic_regex_uses_normalized_hash_table_name(self):
        service = self._build_fake_service()

        cfg = CoordinatorService._build_debezium_config(
            service,
            connector_name="migration-iss_orders.job-1",
            source_table="ISS.ORD#ERS",
            source_schema="ISS",
            source_table_only="ORD#ERS",
            topic_name="migration.ISS_ORDERS.job-1",
            scn_cutoff=123,
            key_columns=["ID"],
            metadata=self._metadata("ISS.ORD#ERS"),
        )

        self.assertEqual(cfg["table.include.list"], "ISS.ORD#ERS")
        self.assertEqual(cfg["transforms.route.topic.regex"], r".*\.ISS\.ORD_ERS$")

    def test_topic_regex_still_escapes_other_special_symbols(self):
        service = self._build_fake_service()

        cfg = CoordinatorService._build_debezium_config(
            service,
            connector_name="migration-iss_orders.job-2",
            source_table="ISS.ORD$ERS",
            source_schema="ISS",
            source_table_only="ORD$ERS",
            topic_name="migration.ISS_ORD$ERS.job-2",
            scn_cutoff=123,
            key_columns=["ID"],
            metadata=self._metadata("ISS.ORD$ERS"),
        )

        self.assertEqual(cfg["transforms.route.topic.regex"], r".*\.ISS\.ORD\$ERS$")


if __name__ == "__main__":
    unittest.main()
