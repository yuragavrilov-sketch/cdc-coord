from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

from .config import normalize_oracle_identifier
from .errors import ExternalServiceError, ValidationError
from .models import OracleTableMetadata, RowIdChunk

logger = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    normalized = normalize_oracle_identifier(identifier)
    if not normalized:
        raise ValidationError("Oracle identifier is empty", details={"identifier": identifier})
    return f'"{normalized}"'


def _resolve_table_name(table_name: str, default_schema: str | None) -> tuple[str, str]:
    raw_name = str(table_name or "").strip()
    if not raw_name:
        raise ValidationError("table_name is required")

    schema: str | None = None
    table = raw_name
    if "." in raw_name:
        schema, table = raw_name.split(".", 1)
    else:
        schema = default_schema

    normalized_schema = normalize_oracle_identifier(schema)
    normalized_table = normalize_oracle_identifier(table)
    if not normalized_schema:
        raise ValidationError(
            "table schema is required",
            details={"table_name": table_name, "expected": "SCHEMA.TABLE or configured default schema"},
        )
    if not normalized_table:
        raise ValidationError("table name is invalid", details={"table_name": table_name})
    return normalized_schema, normalized_table


def _qualified_table_name(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


@dataclass(slots=True)
class OracleClientConfig:
    dsn: str
    user: str
    password: str


class OracleIntrospector:
    def __init__(
        self,
        source: OracleClientConfig | None,
        target: OracleClientConfig | None,
        source_schema: str | None,
        target_schema: str | None,
    ) -> None:
        self._source = source
        self._target = target
        self._source_schema = normalize_oracle_identifier(source_schema)
        self._target_schema = normalize_oracle_identifier(target_schema)

    def _connect(self, cfg: OracleClientConfig):
        try:
            import oracledb
        except ImportError as exc:
            raise ExternalServiceError(
                "Python package oracledb is required for coordinator Oracle integration"
            ) from exc

        try:
            return oracledb.connect(user=cfg.user, password=cfg.password, dsn=cfg.dsn)
        except Exception as exc:
            raise ExternalServiceError("Unable to connect to Oracle", details={"dsn": cfg.dsn}) from exc

    def read_current_scn(self) -> int:
        if not self._source:
            raise ValidationError("Oracle source connection is not configured")

        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                row = cur.fetchone()
        if not row:
            raise ExternalServiceError("Failed to fetch CURRENT_SCN")
        return int(row[0])

    def list_source_tables(self) -> list[str]:
        if not self._source:
            raise ValidationError("Oracle source connection is not configured")
        if not self._source_schema:
            raise ValidationError("Oracle source schema is not configured")

        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM all_tables
                    WHERE owner = :owner
                    ORDER BY table_name
                    """,
                    owner=self._source_schema,
                )
                rows = cur.fetchall()

        return [
            name
            for row in rows
            if row and row[0]
            if (name := normalize_oracle_identifier(str(row[0]))) is not None
        ]

    def fetch_table_metadata(self, table_name: str) -> OracleTableMetadata:
        if not self._target:
            raise ValidationError("Oracle target connection is not configured")

        schema, table = _resolve_table_name(table_name, self._target_schema)
        with self._connect(self._target) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, nullable, data_type, virtual_column
                    FROM all_tab_cols
                    WHERE owner = :owner
                      AND table_name = :table_name
                    ORDER BY column_id
                    """,
                    owner=schema,
                    table_name=table,
                )
                rows = cur.fetchall()

                if not rows:
                    raise ValidationError(
                        "Target table not found",
                        details={"table_name": table_name},
                    )

                cur.execute(
                    """
                    SELECT acc.column_name
                    FROM all_constraints ac
                    JOIN all_cons_columns acc
                      ON ac.owner = acc.owner
                     AND ac.constraint_name = acc.constraint_name
                     AND ac.table_name = acc.table_name
                    WHERE ac.owner = :owner
                      AND ac.table_name = :table_name
                      AND ac.constraint_type = 'P'
                    ORDER BY acc.position
                    """,
                    owner=schema,
                    table_name=table,
                )
                pk_rows = cur.fetchall()

                # Find columns with disabled NOT NULL check constraints
                # (NOT NULL DISABLE: all_tab_cols.nullable = 'Y' but constraint exists)
                # search_condition is LONG type — oracledb returns it as str
                cur.execute(
                    """
                    SELECT acc.column_name, ac.search_condition
                    FROM all_constraints ac
                    JOIN all_cons_columns acc
                      ON ac.owner = acc.owner
                     AND ac.constraint_name = acc.constraint_name
                     AND ac.table_name = acc.table_name
                    WHERE ac.owner = :owner
                      AND ac.table_name = :table_name
                      AND ac.constraint_type = 'C'
                      AND ac.status = 'DISABLED'
                    """,
                    owner=schema,
                    table_name=table,
                )
                disabled_check_rows = cur.fetchall()

        # Columns that appear nullable in all_tab_cols but have a disabled NOT NULL constraint
        disabled_not_null: set[str] = set()
        for col_name, search_cond in disabled_check_rows:
            if search_cond and "IS NOT NULL" in str(search_cond).upper():
                disabled_not_null.add(str(col_name))

        all_columns: list[str] = []
        insertable_columns: list[str] = []
        nullable_columns: set[str] = set()
        has_lobs = False
        has_timestamps = False

        for column_name, nullable, data_type, virtual_column in rows:
            col = str(column_name)
            all_columns.append(col)
            if str(nullable) == "Y" and col not in disabled_not_null:
                nullable_columns.add(col)

            dt = str(data_type).upper()
            if dt in {"CLOB", "BLOB", "NCLOB"}:
                has_lobs = True
            if "TIMESTAMP" in dt or dt == "DATE":
                has_timestamps = True

            if str(virtual_column) != "YES":
                insertable_columns.append(col)

        pk_columns = [str(item[0]) for item in pk_rows]
        return OracleTableMetadata(
            table_name=f"{schema}.{table}",
            all_columns=all_columns,
            insertable_columns=insertable_columns,
            pk_columns=pk_columns,
            nullable_columns=nullable_columns,
            has_lobs=has_lobs,
            has_timestamps=has_timestamps,
        )

    def list_source_table_columns(self, table_name: str) -> dict:
        if not self._source:
            raise ValidationError("Oracle source connection is not configured")

        schema, table = _resolve_table_name(table_name, self._source_schema)
        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, nullable, virtual_column
                    FROM all_tab_cols
                    WHERE owner = :owner
                      AND table_name = :table_name
                    ORDER BY column_id
                    """,
                    owner=schema,
                    table_name=table,
                )
                col_rows = cur.fetchall()

                if not col_rows:
                    raise ValidationError(
                        "Source table not found",
                        details={"table_name": table_name},
                    )

                cur.execute(
                    """
                    SELECT acc.column_name
                    FROM all_constraints ac
                    JOIN all_cons_columns acc
                      ON ac.owner = acc.owner
                     AND ac.constraint_name = acc.constraint_name
                     AND ac.table_name = acc.table_name
                    WHERE ac.owner = :owner
                      AND ac.table_name = :table_name
                      AND ac.constraint_type = 'P'
                    ORDER BY acc.position
                    """,
                    owner=schema,
                    table_name=table,
                )
                pk_rows = cur.fetchall()

        columns: list[str] = []
        not_null_columns: list[str] = []
        for col_name, nullable, virtual_col in col_rows:
            col = str(col_name)
            if str(virtual_col) != "YES":
                columns.append(col)
                if str(nullable) != "Y":
                    not_null_columns.append(col)

        pk_columns = [str(r[0]) for r in pk_rows]
        return {
            "columns": columns,
            "pk_columns": pk_columns,
            "not_null_columns": not_null_columns,
        }

    def validate_key_columns(self, metadata: OracleTableMetadata, key_columns: list[str]) -> None:
        normalized = {c.upper() for c in metadata.all_columns}
        missing = [c for c in key_columns if c.upper() not in normalized]
        if missing:
            raise ValidationError(
                "message_key_columns contain unknown columns",
                details={"missing_columns": missing},
            )

        nullable = {c.upper() for c in metadata.nullable_columns}
        invalid_nullable = [c for c in key_columns if c.upper() in nullable]
        if invalid_nullable:
            raise ValidationError(
                "message_key_columns must be NOT NULL",
                details={"nullable_columns": invalid_nullable},
            )

    def build_rowid_chunks(self, table_name: str, chunk_size: int) -> list[RowIdChunk]:
        if not self._source:
            raise ValidationError("Oracle source connection is not configured")

        schema, table = _resolve_table_name(table_name, self._source_schema)

        approx_rows = self._get_approx_row_count(schema, table)
        chunk_count = max(1, math.ceil(approx_rows / chunk_size)) if approx_rows > 0 else 1
        if approx_rows == 0:
            logger.warning(
                "No row count statistics for %s.%s; creating 1 chunk. "
                "Run DBMS_STATS.GATHER_TABLE_STATS to populate statistics.",
                schema, table,
            )

        # Build ROWID ranges from Oracle extents — no full table scan
        extent_query = """
            SELECT
                DBMS_ROWID.ROWID_CREATE(1, o.data_object_id, e.relative_fno, e.block_id, 0),
                DBMS_ROWID.ROWID_CREATE(1, o.data_object_id, e.relative_fno,
                                        e.block_id + e.blocks - 1, 32767),
                e.blocks
            FROM all_extents e
            JOIN all_objects o
                ON o.owner = e.owner
               AND o.object_name = e.segment_name
               AND o.subobject_name IS NULL
               AND o.object_type = 'TABLE'
            WHERE e.owner = UPPER(:owner)
              AND e.segment_name = UPPER(:table_name)
            ORDER BY e.relative_fno, e.block_id
        """

        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(extent_query, owner=schema, table_name=table)
                extents = cur.fetchall()  # [(start_rowid, end_rowid, blocks), ...]

        if not extents:
            return [RowIdChunk(start_rowid=None, end_rowid=None)]

        total_blocks = sum(e[2] for e in extents)
        target_blocks = max(1, total_blocks // chunk_count)

        logger.info(
            "Chunking %s.%s: ~%d rows, %d extents, %d blocks → ~%d chunks",
            schema, table, approx_rows, len(extents), total_blocks, chunk_count,
        )

        # Group consecutive extents until we reach target block count per chunk
        chunks: list[RowIdChunk] = []
        group_start: str | None = None
        group_end: str | None = None
        group_blocks = 0

        for start_rowid, end_rowid, blocks in extents:
            if group_blocks == 0:
                group_start = start_rowid
            group_end = end_rowid
            group_blocks += blocks

            if group_blocks >= target_blocks:
                chunks.append(RowIdChunk(start_rowid=group_start, end_rowid=group_end))
                group_blocks = 0

        if group_blocks > 0:
            chunks.append(RowIdChunk(start_rowid=group_start, end_rowid=group_end))

        return chunks

    def _get_approx_row_count(self, schema: str, table: str) -> int:
        """Return num_rows from Oracle CBO statistics. Returns 0 if stats are absent."""
        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT num_rows FROM all_tables"
                    " WHERE owner = :owner AND table_name = :table_name",
                    owner=schema,
                    table_name=table,
                )
                row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

