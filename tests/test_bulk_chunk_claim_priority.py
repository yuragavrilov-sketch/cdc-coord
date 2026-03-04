from __future__ import annotations

import unittest

from coordinator.repositories import CoordinatorRepository


class _FakeCursor:
    def __init__(self, row: dict | None):
        self._row = row
        self.executed_sql: str | None = None
        self.executed_params: tuple | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = params

    def fetchone(self):
        return self._row


class _FakeConnection:
    def __init__(self, row: dict | None):
        self._cursor = _FakeCursor(row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


class _FakeDb:
    def __init__(self, row: dict | None):
        self._conn = _FakeConnection(row)

    def connection(self):
        return self._conn


class BulkChunkClaimPriorityTests(unittest.TestCase):
    def test_claim_bulk_chunk_sql_prioritizes_cdc_mode_before_others(self):
        sql = CoordinatorRepository._claim_bulk_chunk_sql()

        self.assertIn("FOR UPDATE OF c SKIP LOCKED", sql)
        self.assertIn("j.status IN ('bulk_running', 'bulk_loading')", sql)
        self.assertIn("CASE WHEN j.migration_mode = 'cdc' THEN 0 ELSE 1 END", sql)

    def test_claim_bulk_chunk_uses_prioritized_sql_and_returns_claimed_chunk(self):
        fake_row = {
            "chunk_id": "chunk-1",
            "job_id": "job-cdc",
            "table_name": "ISS.ORDERS",
            "start_rowid": "AA",
            "end_rowid": "AB",
        }
        repo = CoordinatorRepository(_FakeDb(fake_row))

        claimed = repo.claim_bulk_chunk("worker-1")

        self.assertEqual(claimed, fake_row)
        executed_sql = repo._db._conn._cursor.executed_sql
        self.assertIsNotNone(executed_sql)
        self.assertIn("CASE WHEN j.migration_mode = 'cdc' THEN 0 ELSE 1 END", executed_sql)
        self.assertEqual(repo._db._conn._cursor.executed_params, ("worker-1",))


if __name__ == "__main__":
    unittest.main()
