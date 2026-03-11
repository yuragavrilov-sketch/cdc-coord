from __future__ import annotations

import logging
import math
import uuid
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


def _build_pk_range_chunks(
    pk_col: str, pk_from: int, pk_to: int, chunk_size: int, use_scn: bool
) -> list[RowIdChunk]:
    """Divide [pk_from, pk_to] into RowIdChunk intervals of chunk_size rows (by PK range)."""
    if pk_from > pk_to:
        return []
    chunks: list[RowIdChunk] = []
    current = pk_from
    while current <= pk_to:
        end = current + chunk_size - 1
        chunks.append(RowIdChunk(
            start_rowid=None,
            end_rowid=None,
            chunk_meta={"pk_col": pk_col, "pk_from": current, "pk_to": end, "use_scn": use_scn},
        ))
        current = end + 1
    return chunks


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

    def list_target_tables(self) -> list[str]:
        if not self._target:
            raise ValidationError("Oracle target connection is not configured")
        if not self._target_schema:
            raise ValidationError("Oracle target schema is not configured")

        with self._connect(self._target) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM all_tables
                    WHERE owner = :owner
                    ORDER BY table_name
                    """,
                    owner=self._target_schema,
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

                # Find columns with disabled NOT NULL check constraints.
                # search_condition is LONG in all_constraints — cannot be used in JOIN,
                # GROUP BY, ORDER BY, or subqueries. Fetch it in a separate plain SELECT
                # and merge with column names in Python.
                cur.execute(
                    """
                    SELECT acc.column_name, ac.constraint_name
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
                col_to_cname = {col: cname for col, cname in cur.fetchall()}

                cur.execute(
                    """
                    SELECT constraint_name, search_condition
                    FROM all_constraints
                    WHERE owner = :owner
                      AND table_name = :table_name
                      AND constraint_type = 'C'
                      AND status = 'DISABLED'
                    """,
                    owner=schema,
                    table_name=table,
                )
                search_by_cname = {cname: sc for cname, sc in cur.fetchall()}

                disabled_check_rows = [
                    (col, search_by_cname.get(cname))
                    for col, cname in col_to_cname.items()
                ]

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

    def build_hybrid_split(
        self, table_name: str, chunk_size: int, recent_rows: int
    ) -> tuple[list[RowIdChunk], list[RowIdChunk]]:
        """Split table chunks into (recent, historical).

        Recent chunks cover the last ~recent_rows rows by ROWID order (processed first
        with AS OF SCN + CDC). Historical chunks cover older rows (background static load).
        Returns (recent_chunks, historical_chunks). historical_chunks may be empty.
        """
        all_chunks = self.build_rowid_chunks(table_name, chunk_size)

        # Single empty chunk or no ROWID splits — everything goes to recent
        if len(all_chunks) <= 1:
            return all_chunks, []

        scn_count = max(1, math.ceil(recent_rows / chunk_size))
        if scn_count >= len(all_chunks):
            return all_chunks, []

        split_idx = len(all_chunks) - scn_count
        historical = all_chunks[:split_idx]
        recent = all_chunks[split_idx:]
        logger.info(
            "Hybrid split for %s: %d recent chunks, %d historical chunks (recent_rows=%d, chunk_size=%d)",
            table_name, len(recent), len(historical), recent_rows, chunk_size,
        )
        return recent, historical

    def read_max_pk(self, table_name: str, pk_col: str) -> int | None:
        """Read MAX(pk_col) from the source table. Returns None if the table is empty."""
        if not self._source:
            raise ValidationError("Oracle source connection is not configured")
        schema, table = _resolve_table_name(table_name, self._source_schema)
        qualified = _qualified_table_name(schema, table)
        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT MAX("{pk_col}") FROM {qualified}')
                row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def build_pk_split_chunks(
        self,
        table_name: str,
        chunk_size: int,
        split_pk: int,
        pk_col: str,
    ) -> tuple[list[RowIdChunk], list[RowIdChunk]]:
        """Build (tail_chunks, history_chunks) split at split_pk.

        tail_chunks:    WHERE pk_col >= split_pk — loaded AS OF SCN (use_scn=True).
        history_chunks: WHERE pk_col <  split_pk — loaded without SCN (use_scn=False),
                        started after the tail job reaches bulk_done.
        Chunking is by equal PK-range intervals of chunk_size.
        """
        if not self._source:
            raise ValidationError("Oracle source connection is not configured")
        schema, table = _resolve_table_name(table_name, self._source_schema)
        qualified = _qualified_table_name(schema, table)
        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT MIN("{pk_col}"), MAX("{pk_col}") FROM {qualified}')
                row = cur.fetchone()

        if not row or row[0] is None:
            # Empty table: one tail sentinel chunk
            return [RowIdChunk(
                start_rowid=None,
                end_rowid=None,
                chunk_meta={"pk_col": pk_col, "pk_from": split_pk, "pk_to": None, "use_scn": True},
            )], []

        min_pk = int(row[0])
        max_pk = int(row[1])

        tail_chunks = _build_pk_range_chunks(pk_col, split_pk, max_pk, chunk_size, use_scn=True)
        if not tail_chunks:
            # split_pk > max_pk: no tail rows at snapshot moment; create one empty sentinel
            tail_chunks = [RowIdChunk(
                start_rowid=None,
                end_rowid=None,
                chunk_meta={"pk_col": pk_col, "pk_from": split_pk, "pk_to": split_pk, "use_scn": True},
            )]

        history_chunks = (
            _build_pk_range_chunks(pk_col, min_pk, split_pk - 1, chunk_size, use_scn=False)
            if min_pk < split_pk else []
        )

        logger.info(
            "PK split for %s: split_pk=%d, %d tail chunks (pk>=%d, AS OF SCN),"
            " %d history chunks (pk<%d, no SCN), chunk_size=%d",
            table_name, split_pk,
            len(tail_chunks), split_pk,
            len(history_chunks), split_pk,
            chunk_size,
        )
        return tail_chunks, history_chunks

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

        chunks = self._chunks_by_parallel_execute(schema, table, chunk_size)
        if chunks is None:
            chunks = self._chunks_by_sample(schema, table, chunk_count)
        return chunks or [RowIdChunk(start_rowid=None, end_rowid=None)]

    def _chunks_by_parallel_execute(
        self, schema: str, table: str, chunk_size: int
    ) -> list[RowIdChunk] | None:
        """Build ROWID chunks via DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID.
        Fast, no full table scan, uses Oracle extent map internally.
        Returns None if the user lacks EXECUTE ON DBMS_PARALLEL_EXECUTE."""
        import oracledb

        task_name = f"coord_{uuid.uuid4().hex[:20]}"
        with self._connect(self._source) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        BEGIN
                            DBMS_PARALLEL_EXECUTE.CREATE_TASK(:task_name);
                            DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(
                                task_name   => :task_name,
                                table_owner => :owner,
                                table_name  => :table_name,
                                by_row      => TRUE,
                                chunk_size  => :chunk_size
                            );
                        END;
                        """,
                        task_name=task_name,
                        owner=schema,
                        table_name=table,
                        chunk_size=chunk_size,
                    )
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT start_rowid, end_rowid
                        FROM user_parallel_execute_chunks
                        WHERE task_name = :task_name
                        ORDER BY chunk_id
                        """,
                        task_name=task_name,
                    )
                    rows = cur.fetchall()
            except oracledb.DatabaseError as exc:
                err = str(exc)
                if "ORA-00942" in err or "ORA-06550" in err or "ORA-04042" in err:
                    logger.warning(
                        "DBMS_PARALLEL_EXECUTE unavailable for %s.%s (%s); "
                        "falling back to SAMPLE BLOCK chunking",
                        schema, table, err.split("\n")[0],
                    )
                    return None
                raise
            finally:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:task_name); END;",
                            task_name=task_name,
                        )
                except Exception:
                    pass  # best-effort cleanup

        chunks = [RowIdChunk(start_rowid=r[0], end_rowid=r[1]) for r in rows]
        logger.info(
            "Chunking %s.%s: DBMS_PARALLEL_EXECUTE → %d chunks (chunk_size=%d rows)",
            schema, table, len(chunks), chunk_size,
        )
        return chunks or None

    def _chunks_by_sample(
        self, schema: str, table: str, chunk_count: int
    ) -> list[RowIdChunk]:
        """Build ROWID chunks via SAMPLE BLOCK — fast, no data dictionary access needed."""
        qualified_table = _qualified_table_name(schema, table)
        query = """
            SELECT MIN(rid), MAX(rid)
            FROM (
                SELECT ROWID rid,
                       NTILE(:chunk_count) OVER (ORDER BY ROWID) AS chunk_no
                FROM {qualified_table} SAMPLE BLOCK(1)
            ) t
            GROUP BY chunk_no
            ORDER BY chunk_no
        """.format(qualified_table=qualified_table)

        logger.info("Chunking %s.%s: SAMPLE BLOCK(1) → ~%d chunks", schema, table, chunk_count)
        chunks: list[RowIdChunk] = []
        with self._connect(self._source) as conn:
            with conn.cursor() as cur:
                cur.execute(query, chunk_count=chunk_count)
                for start_rowid, end_rowid in cur.fetchall():
                    chunks.append(RowIdChunk(start_rowid=start_rowid, end_rowid=end_rowid))
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

