from __future__ import annotations

from .config import normalize_oracle_identifier
from .models import OracleTableMetadata, SqlTemplateRecord


def _quote_col(column: str) -> str:
    return f'"{column}"'


def _quote_table_name(table_name: str) -> str:
    raw = str(table_name or "").strip()
    if "." not in raw:
        raise ValueError("table_name must be schema-qualified as SCHEMA.TABLE")
    schema, table = raw.split(".", 1)
    normalized_schema = normalize_oracle_identifier(schema)
    normalized_table = normalize_oracle_identifier(table)
    if not normalized_schema or not normalized_table:
        raise ValueError(f"Invalid table_name: {table_name!r}")
    return f'"{normalized_schema}"."{normalized_table}"'


def _build_merge_on(alias_t: str, alias_s: str, key_columns: list[str]) -> str:
    return " AND ".join(f"{alias_t}.{_quote_col(col)} = {alias_s}.{_quote_col(col)}" for col in key_columns)


def build_sql_templates(
    *,
    job_id: str,
    table_name: str,
    metadata: OracleTableMetadata,
    key_columns: list[str],
    migration_mode: str,
) -> SqlTemplateRecord:
    qualified_table = _quote_table_name(table_name)
    insert_columns = metadata.insertable_columns
    non_key_columns = [col for col in insert_columns if col not in key_columns]

    merge_on = _build_merge_on("t", "s", key_columns)
    insert_cols = ", ".join(_quote_col(col) for col in insert_columns)
    insert_vals = ", ".join(f"s.{_quote_col(col)}" for col in insert_columns)

    update_set = ", ".join(f"t.{_quote_col(col)} = s.{_quote_col(col)}" for col in non_key_columns)

    bulk_merge_sql = (
        f"MERGE INTO {qualified_table} t "
        f"USING :source_subquery s ON ({merge_on}) "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )

    bulk_insert_sql = (
        f"INSERT /*+ APPEND */ INTO {qualified_table} ({insert_cols}) "
        f"SELECT {', '.join(f's.{_quote_col(c)}' for c in insert_columns)} FROM :source_subquery s"
    )

    cdc_merge_sql = None
    cdc_delete_sql = None
    if migration_mode == "cdc":
        cdc_merge_sql = (
            f"MERGE INTO {qualified_table} t "
            f"USING :payload s ON ({merge_on}) "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )

        cdc_delete_sql = (
            f"DELETE FROM {qualified_table} t "
            f"WHERE "
            + " AND ".join(f"t.{_quote_col(col)} = :{col}" for col in key_columns)
        )

    return SqlTemplateRecord(
        job_id=job_id,
        table_name=table_name,
        pk_columns=key_columns,
        all_columns=metadata.all_columns,
        insertable_columns=metadata.insertable_columns,
        bulk_merge_sql=bulk_merge_sql,
        bulk_insert_sql=bulk_insert_sql,
        cdc_merge_sql=cdc_merge_sql,
        cdc_delete_sql=cdc_delete_sql,
        has_lobs=metadata.has_lobs,
        has_timestamps=metadata.has_timestamps,
    )

