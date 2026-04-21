"""Source adapters for stage-five trade runners."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.trade.contracts import ExecutionPriceRow, PortfolioPlanTradeInputRow


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")


@dataclass(frozen=True)
class TradeSourceDataset:
    """portfolio_plan rows plus execution-price-line rows."""

    portfolio_plan_source_path: Path | None
    execution_price_source_path: Path | None
    rows_by_work_unit: dict[tuple[str, str], list[PortfolioPlanTradeInputRow]]
    execution_prices_by_symbol: dict[str, list[ExecutionPriceRow]]

    @property
    def row_count(self) -> int:
        return sum(len(rows) for rows in self.rows_by_work_unit.values())


def load_trade_source_rows(*, settings: WorkspaceRoots, portfolio_id: str) -> TradeSourceDataset:
    """Load portfolio_plan_snapshot rows and execution_price_line rows."""

    portfolio_plan_path = settings.databases.portfolio_plan if settings.databases.portfolio_plan.exists() else None
    execution_price_path = settings.source_databases.market_base if settings.source_databases.market_base.exists() else None

    rows_by_work_unit: dict[tuple[str, str], list[PortfolioPlanTradeInputRow]] = {}
    if portfolio_plan_path is not None:
        rows_by_work_unit = _load_portfolio_plan_rows(database_path=portfolio_plan_path, portfolio_id=portfolio_id)

    execution_prices_by_symbol: dict[str, list[ExecutionPriceRow]] = {}
    if execution_price_path is not None:
        execution_prices_by_symbol = _load_execution_price_rows(execution_price_path)

    return TradeSourceDataset(
        portfolio_plan_source_path=portfolio_plan_path,
        execution_price_source_path=execution_price_path,
        rows_by_work_unit=rows_by_work_unit,
        execution_prices_by_symbol=execution_prices_by_symbol,
    )


def _load_portfolio_plan_rows(
    *,
    database_path: Path,
    portfolio_id: str,
) -> dict[tuple[str, str], list[PortfolioPlanTradeInputRow]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "portfolio_plan_snapshot" not in available_tables:
            return {}
        rows = connection.execute(
            """
            SELECT
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                symbol,
                CAST(reference_trade_date AS DATE) AS reference_trade_date,
                CAST(planned_entry_trade_date AS DATE) AS planned_entry_trade_date,
                CAST(scheduled_exit_trade_date AS DATE) AS scheduled_exit_trade_date,
                position_action_decision,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status,
                blocking_reason_code,
                planned_exit_reason_code
            FROM portfolio_plan_snapshot
            WHERE portfolio_id = ?
            ORDER BY portfolio_id, symbol, reference_trade_date, candidate_nk
            """,
            [portfolio_id],
        ).fetchall()

    grouped: dict[tuple[str, str], list[PortfolioPlanTradeInputRow]] = defaultdict(list)
    for (
        plan_snapshot_nk,
        candidate_nk,
        row_portfolio_id,
        symbol,
        reference_trade_date,
        planned_entry_trade_date,
        scheduled_exit_trade_date,
        position_action_decision,
        requested_weight,
        admitted_weight,
        trimmed_weight,
        plan_status,
        blocking_reason_code,
        planned_exit_reason_code,
    ) in rows:
        row = PortfolioPlanTradeInputRow(
            plan_snapshot_nk=str(plan_snapshot_nk),
            candidate_nk=str(candidate_nk),
            portfolio_id=str(row_portfolio_id),
            symbol=str(symbol),
            reference_trade_date=_as_date(reference_trade_date) if reference_trade_date is not None else None,
            planned_entry_trade_date=_as_date(planned_entry_trade_date) if planned_entry_trade_date is not None else None,
            scheduled_exit_trade_date=_as_date(scheduled_exit_trade_date) if scheduled_exit_trade_date is not None else None,
            position_action_decision=str(position_action_decision),
            requested_weight=float(requested_weight),
            admitted_weight=float(admitted_weight),
            trimmed_weight=float(trimmed_weight),
            plan_status=str(plan_status),
            blocking_reason_code=str(blocking_reason_code) if blocking_reason_code is not None else None,
            planned_exit_reason_code=str(planned_exit_reason_code) if planned_exit_reason_code is not None else None,
        )
        grouped[(row.portfolio_id, row.symbol)].append(row)
    return dict(grouped)


def _load_execution_price_rows(database_path: Path) -> dict[str, list[ExecutionPriceRow]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        for table_name in DAY_TABLE_CANDIDATES:
            if table_name not in available_tables:
                continue
            column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_names = {row[1] for row in column_info}
            date_column = _pick_required_column(column_names, ("bar_dt", "trade_date", "date"))
            symbol_column = _pick_required_column(column_names, ("symbol", "code"))
            if "open" not in column_names:
                return {}
            rows = connection.execute(
                f"""
                SELECT
                    {symbol_column} AS symbol,
                    CAST({date_column} AS DATE) AS trade_date,
                    CAST(open AS DOUBLE) AS open_price
                FROM {table_name}
                ORDER BY symbol, trade_date
                """
            ).fetchall()
            grouped: dict[str, list[ExecutionPriceRow]] = defaultdict(list)
            for symbol, trade_date, open_price in rows:
                grouped[str(symbol)].append(
                    ExecutionPriceRow(
                        symbol=str(symbol),
                        trade_date=_as_date(trade_date),
                        open_price=float(open_price) if open_price is not None else None,
                    )
                )
            return dict(grouped)
    return {}


def pick_next_execution_price(
    *,
    rows: list[ExecutionPriceRow],
    reference_trade_date: date | None,
) -> ExecutionPriceRow | None:
    """Return the first execution-price-line row after reference_trade_date."""

    if reference_trade_date is None:
        return None
    for row in rows:
        if row.trade_date > reference_trade_date:
            return row
    return None


def _pick_required_column(column_names: set[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in column_names:
            return candidate
    raise ValueError(f"Could not resolve required source columns from candidates: {candidates}")


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()
