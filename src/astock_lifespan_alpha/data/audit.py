"""Read-only source fact audit for the current data freeze gate."""

from __future__ import annotations

from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.data.contracts import DataSourceFactAuditSummary, SourceFactTableAudit

SOURCE_TABLES = {
    "market_base": "stock_daily_adjusted",
    "market_base_week": "stock_weekly_adjusted",
    "market_base_month": "stock_monthly_adjusted",
    "raw_market": "stock_daily_bar",
    "raw_market_week": "stock_weekly_bar",
    "raw_market_month": "stock_monthly_bar",
}


def audit_data_source_fact_freeze(*, settings: WorkspaceRoots | None = None) -> DataSourceFactAuditSummary:
    """Inspect registered source fact ledgers using read-only DuckDB connections."""

    workspace = settings or default_settings()
    source_paths = workspace.source_databases.as_dict()
    table_audits = {
        source_key: _audit_one_table(source_key=source_key, source_path=source_paths[source_key], table_name=table_name)
        for source_key, table_name in SOURCE_TABLES.items()
    }
    raw_base_code_delta = _audit_raw_base_code_delta(
        raw_path=source_paths["raw_market"],
        base_path=source_paths["market_base"],
    )
    return DataSourceFactAuditSummary(
        runner_name="audit_data_source_fact_freeze",
        status="completed",
        tables=table_audits,
        raw_base_code_delta=raw_base_code_delta,
        message="data source fact audit completed.",
    )


def _audit_one_table(*, source_key: str, source_path: Path, table_name: str) -> SourceFactTableAudit:
    if not source_path.exists():
        return SourceFactTableAudit(
            source_key=source_key,
            source_path=str(source_path),
            table_name=table_name,
            exists=False,
            row_count=0,
            symbol_count=0,
            min_trade_date=None,
            max_trade_date=None,
        )
    with duckdb.connect(str(source_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if table_name not in available_tables:
            return SourceFactTableAudit(
                source_key=source_key,
                source_path=str(source_path),
                table_name=None,
                exists=True,
                row_count=0,
                symbol_count=0,
                min_trade_date=None,
                max_trade_date=None,
            )
        columns = {row[1] for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        code_column = "code" if "code" in columns else "symbol"
        date_column = "trade_date" if "trade_date" in columns else "bar_dt"
        row_count, symbol_count, min_date, max_date = connection.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {code_column}), MIN({date_column}), MAX({date_column}) FROM {table_name}"
        ).fetchone()
        adjust_counts: dict[str, int] = {}
        duplicate_groups = 0
        if "adjust_method" in columns:
            adjust_counts = {
                str(adjust_method): int(count)
                for adjust_method, count in connection.execute(
                    f"SELECT adjust_method, COUNT(*) FROM {table_name} GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            duplicate_groups = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT {code_column}, {date_column}, COUNT(*) AS row_count
                        FROM {table_name}
                        WHERE adjust_method = 'backward'
                        GROUP BY 1, 2
                        HAVING row_count > 1
                    )
                    """
                ).fetchone()[0]
            )
    return SourceFactTableAudit(
        source_key=source_key,
        source_path=str(source_path),
        table_name=table_name,
        exists=True,
        row_count=int(row_count),
        symbol_count=int(symbol_count),
        min_trade_date=str(min_date) if min_date is not None else None,
        max_trade_date=str(max_date) if max_date is not None else None,
        adjust_method_counts=adjust_counts,
        backward_duplicate_groups=duplicate_groups,
    )


def _audit_raw_base_code_delta(*, raw_path: Path, base_path: Path) -> dict[str, tuple[str, ...]]:
    if not raw_path.exists() or not base_path.exists():
        return {"raw_only_codes": (), "base_only_codes": ()}
    with duckdb.connect(str(raw_path), read_only=True) as raw_connection:
        raw_tables = {row[0] for row in raw_connection.execute("SHOW TABLES").fetchall()}
        if "stock_daily_bar" not in raw_tables:
            raw_codes: set[str] = set()
        else:
            raw_codes = {str(row[0]) for row in raw_connection.execute("SELECT DISTINCT code FROM stock_daily_bar").fetchall()}
    with duckdb.connect(str(base_path), read_only=True) as base_connection:
        base_tables = {row[0] for row in base_connection.execute("SHOW TABLES").fetchall()}
        if "stock_daily_adjusted" not in base_tables:
            base_codes: set[str] = set()
        else:
            base_codes = {str(row[0]) for row in base_connection.execute("SELECT DISTINCT code FROM stock_daily_adjusted").fetchall()}
    return {
        "raw_only_codes": tuple(sorted(raw_codes - base_codes)),
        "base_only_codes": tuple(sorted(base_codes - raw_codes)),
    }
