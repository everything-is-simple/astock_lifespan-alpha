"""Read-only audit for isolated stock producer target ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.data.contracts import DataStockProducerTargetAuditSummary, SourceFactTableAudit
from astock_lifespan_alpha.data.ledger_timeframe import market_base_timeframe_ledger_path, raw_market_timeframe_ledger_path
from astock_lifespan_alpha.data.schema import MARKET_BASE_STOCK_TABLE_BY_TIMEFRAME, RAW_STOCK_TABLE_BY_TIMEFRAME


def audit_stock_producer_target(
    *,
    settings: WorkspaceRoots | None = None,
    target_data_root: Path | str,
) -> DataStockProducerTargetAuditSummary:
    """Audit isolated producer raw/base ledgers without mutating them."""

    del settings
    target_root = Path(target_data_root).resolve()
    table_audits: dict[str, SourceFactTableAudit] = {}
    for timeframe in ("day", "week", "month"):
        raw_key = "raw_market" if timeframe == "day" else f"raw_market_{timeframe}"
        base_key = "market_base" if timeframe == "day" else f"market_base_{timeframe}"
        table_audits[raw_key] = _audit_one_table(
            source_key=raw_key,
            source_path=raw_market_timeframe_ledger_path(target_root, timeframe=timeframe),
            table_name=RAW_STOCK_TABLE_BY_TIMEFRAME[timeframe],
        )
        table_audits[base_key] = _audit_one_table(
            source_key=base_key,
            source_path=market_base_timeframe_ledger_path(target_root, timeframe=timeframe),
            table_name=MARKET_BASE_STOCK_TABLE_BY_TIMEFRAME[timeframe],
        )

    raw_base_code_delta = _audit_raw_base_code_delta(
        raw_path=raw_market_timeframe_ledger_path(target_root, timeframe="day"),
        base_path=market_base_timeframe_ledger_path(target_root, timeframe="day"),
    )
    gate_failures = _resolve_gate_failures(table_audits=table_audits, raw_base_code_delta=raw_base_code_delta)
    status = "failed" if gate_failures else "completed"
    return DataStockProducerTargetAuditSummary(
        runner_name="audit_stock_producer_target",
        status=status,
        target_data_root=str(target_root),
        tables=table_audits,
        raw_base_code_delta=raw_base_code_delta,
        excluded_non_stock_codes=_read_excluded_non_stock_codes(
            raw_market_timeframe_ledger_path(target_root, timeframe="day")
        ),
        gate_failures=gate_failures,
        message="stock producer target audit completed." if status == "completed" else "stock producer target audit failed.",
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
    raw_codes = _read_codes(database_path=raw_path, table_name="stock_daily_bar")
    base_codes = _read_codes(database_path=base_path, table_name="stock_daily_adjusted")
    return {
        "raw_only_codes": tuple(sorted(raw_codes - base_codes)),
        "base_only_codes": tuple(sorted(base_codes - raw_codes)),
    }


def _read_codes(*, database_path: Path, table_name: str) -> set[str]:
    if not database_path.exists():
        return set()
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if table_name not in available_tables:
            return set()
        return {str(row[0]) for row in connection.execute(f"SELECT DISTINCT code FROM {table_name}").fetchall()}


def _read_excluded_non_stock_codes(raw_path: Path) -> tuple[str, ...]:
    if not raw_path.exists():
        return ()
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "raw_ingest_file" not in available_tables:
            return ()
        rows = connection.execute(
            """
            SELECT DISTINCT code
            FROM raw_ingest_file
            WHERE action = 'excluded_non_stock'
            ORDER BY code
            """
        ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _resolve_gate_failures(
    *,
    table_audits: dict[str, SourceFactTableAudit],
    raw_base_code_delta: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    failures: list[str] = []
    if raw_base_code_delta["raw_only_codes"] or raw_base_code_delta["base_only_codes"]:
        failures.append("day raw/base code delta is not empty.")
    duplicate_tables = [
        key for key, audit in table_audits.items() if audit.backward_duplicate_groups > 0
    ]
    if duplicate_tables:
        failures.append("backward duplicate groups found: " + ", ".join(sorted(duplicate_tables)))
    return tuple(failures)
