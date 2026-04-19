"""DuckDB schema initialization for MALF ledgers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


MALF_TABLES = (
    "malf_run",
    "malf_work_queue",
    "malf_checkpoint",
    "malf_pivot_ledger",
    "malf_wave_ledger",
    "malf_state_snapshot",
    "malf_wave_scale_snapshot",
    "malf_wave_scale_profile",
)


@dataclass(frozen=True)
class MalfRunBackfillColumn:
    name: str
    type_sql: str
    default_sql: str | None
    backfill_sql: str | None
    not_null: bool


MALF_RUN_BACKFILL_COLUMNS = (
    MalfRunBackfillColumn(
        name="symbols_total",
        type_sql="BIGINT",
        default_sql="0",
        backfill_sql="0",
        not_null=True,
    ),
    MalfRunBackfillColumn(
        name="symbols_completed",
        type_sql="BIGINT",
        default_sql="0",
        backfill_sql="0",
        not_null=True,
    ),
    MalfRunBackfillColumn(
        name="current_symbol",
        type_sql="TEXT",
        default_sql=None,
        backfill_sql=None,
        not_null=False,
    ),
    MalfRunBackfillColumn(
        name="elapsed_seconds",
        type_sql="DOUBLE",
        default_sql="0",
        backfill_sql="0",
        not_null=True,
    ),
    MalfRunBackfillColumn(
        name="estimated_remaining_symbols",
        type_sql="BIGINT",
        default_sql="0",
        backfill_sql="0",
        not_null=True,
    ),
)


def initialize_malf_schema(database_path: Path, *, actions: list[str] | None = None) -> None:
    """Create the formal MALF schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_run (
                run_id TEXT PRIMARY KEY,
                timeframe TEXT NOT NULL,
                status TEXT NOT NULL,
                source_path TEXT,
                input_rows BIGINT NOT NULL DEFAULT 0,
                symbols_total BIGINT NOT NULL DEFAULT 0,
                symbols_seen BIGINT NOT NULL DEFAULT 0,
                symbols_completed BIGINT NOT NULL DEFAULT 0,
                symbols_updated BIGINT NOT NULL DEFAULT 0,
                inserted_pivots BIGINT NOT NULL DEFAULT 0,
                inserted_waves BIGINT NOT NULL DEFAULT 0,
                inserted_state_snapshots BIGINT NOT NULL DEFAULT 0,
                inserted_wave_scale_snapshots BIGINT NOT NULL DEFAULT 0,
                inserted_wave_scale_profiles BIGINT NOT NULL DEFAULT 0,
                current_symbol TEXT,
                elapsed_seconds DOUBLE NOT NULL DEFAULT 0,
                estimated_remaining_symbols BIGINT NOT NULL DEFAULT 0,
                latest_bar_dt TIMESTAMP,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        ensure_malf_run_backfill_columns(connection=connection, actions=actions)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_work_queue (
                queue_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                status TEXT NOT NULL,
                source_bar_count BIGINT NOT NULL DEFAULT 0,
                requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                claimed_at TIMESTAMP,
                finished_at TIMESTAMP,
                last_bar_dt TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_checkpoint (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                last_bar_dt TIMESTAMP,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, timeframe)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_pivot_ledger (
                pivot_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                wave_id TEXT NOT NULL,
                bar_dt TIMESTAMP NOT NULL,
                pivot_type TEXT NOT NULL,
                price DOUBLE NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_wave_ledger (
                wave_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                direction TEXT NOT NULL,
                start_bar_dt TIMESTAMP NOT NULL,
                end_bar_dt TIMESTAMP NOT NULL,
                guard_bar_dt TIMESTAMP NOT NULL,
                guard_price DOUBLE NOT NULL,
                extreme_price DOUBLE NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                life_state TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_state_snapshot (
                snapshot_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                bar_dt TIMESTAMP NOT NULL,
                wave_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                guard_price DOUBLE NOT NULL,
                extreme_price DOUBLE NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                life_state TEXT NOT NULL,
                update_rank DOUBLE NOT NULL,
                stagnation_rank DOUBLE NOT NULL,
                wave_position_zone TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_wave_scale_snapshot (
                snapshot_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                bar_dt TIMESTAMP NOT NULL,
                direction TEXT NOT NULL,
                wave_id TEXT NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                life_state TEXT NOT NULL,
                update_rank DOUBLE NOT NULL,
                stagnation_rank DOUBLE NOT NULL,
                wave_position_zone TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_wave_scale_profile (
                profile_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                direction TEXT NOT NULL,
                wave_id TEXT NOT NULL,
                sample_size BIGINT NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                update_rank DOUBLE NOT NULL,
                stagnation_rank DOUBLE NOT NULL,
                wave_position_zone TEXT NOT NULL
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_malf_wave_symbol_tf ON malf_wave_ledger(symbol, timeframe)")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_malf_snapshot_symbol_tf ON malf_wave_scale_snapshot(symbol, timeframe, bar_dt)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_malf_checkpoint_tf ON malf_checkpoint(timeframe, symbol)"
        )


def ensure_malf_run_backfill_columns(
    *,
    connection: duckdb.DuckDBPyConnection,
    actions: list[str] | None = None,
) -> None:
    """Backfill stable progress columns using DuckDB-compatible ALTER steps."""

    for column in MALF_RUN_BACKFILL_COLUMNS:
        _ensure_backfill_column(connection=connection, table_name="malf_run", column=column, actions=actions)


def inspect_table_columns(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, dict[str, object]]:
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {
        row[1]: {
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": bool(row[3]),
            "default": row[4],
            "pk": bool(row[5]),
        }
        for row in rows
    }


def _ensure_column(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    columns = {row[1] for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
    if column_name in columns:
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def _ensure_backfill_column(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    column: MalfRunBackfillColumn,
    actions: list[str] | None,
) -> None:
    columns = inspect_table_columns(connection=connection, table_name=table_name)
    if column.name not in columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column.name} {column.type_sql}")
        _append_action(actions, f"add_column:{table_name}.{column.name}:{column.type_sql}")
        columns = inspect_table_columns(connection=connection, table_name=table_name)

    if column.default_sql is not None and _normalize_default(columns[column.name]["default"]) != column.default_sql:
        connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN {column.name} SET DEFAULT {column.default_sql}")
        _append_action(actions, f"set_default:{table_name}.{column.name}:{column.default_sql}")

    if column.backfill_sql is not None:
        null_count = connection.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column.name} IS NULL"
        ).fetchone()[0]
        if null_count:
            connection.execute(f"UPDATE {table_name} SET {column.name} = {column.backfill_sql} WHERE {column.name} IS NULL")
            _append_action(actions, f"backfill_nulls:{table_name}.{column.name}:{null_count}")

    columns = inspect_table_columns(connection=connection, table_name=table_name)
    if column.not_null and not columns[column.name]["notnull"]:
        connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN {column.name} SET NOT NULL")
        _append_action(actions, f"set_not_null:{table_name}.{column.name}")


def _append_action(actions: list[str] | None, action: str) -> None:
    if actions is not None:
        actions.append(action)


def _normalize_default(default_value: object) -> str | None:
    if default_value is None:
        return None
    return str(default_value).strip().strip("'").strip('"')
