"""Fact-layer adapters for MALF runners."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.malf.contracts import OhlcBar, Timeframe


DAY_TABLE_CANDIDATES = ("market_base_day", "bars_day", "price_bar_day", "market_day")
WEEK_TABLE_CANDIDATES = ("market_base_week", "bars_week", "price_bar_week", "market_week")
MONTH_TABLE_CANDIDATES = ("market_base_month", "bars_month", "price_bar_month", "market_month")


@dataclass(frozen=True)
class SourceBars:
    """Loaded fact-layer bars grouped by symbol."""

    source_path: Path | None
    bars_by_symbol: dict[str, list[OhlcBar]]

    @property
    def row_count(self) -> int:
        return sum(len(bars) for bars in self.bars_by_symbol.values())


def load_source_bars(settings: WorkspaceRoots, timeframe: Timeframe) -> SourceBars:
    """Load fact-layer bars for the requested timeframe."""

    direct_candidates = {
        Timeframe.DAY: DAY_TABLE_CANDIDATES,
        Timeframe.WEEK: WEEK_TABLE_CANDIDATES,
        Timeframe.MONTH: MONTH_TABLE_CANDIDATES,
    }[timeframe]
    for database_path in (settings.source_databases.market_base, settings.source_databases.raw_market):
        if not database_path.exists():
            continue
        direct_rows = _load_rows_from_database(database_path, direct_candidates)
        if direct_rows:
            return SourceBars(source_path=database_path, bars_by_symbol=_group_rows_by_symbol(direct_rows))
        if timeframe is Timeframe.DAY:
            continue
        day_rows = _load_rows_from_database(database_path, DAY_TABLE_CANDIDATES)
        if day_rows:
            aggregated_rows = _aggregate_rows(day_rows, timeframe)
            return SourceBars(source_path=database_path, bars_by_symbol=_group_rows_by_symbol(aggregated_rows))
    return SourceBars(source_path=None, bars_by_symbol={})


def _load_rows_from_database(database_path: Path, table_candidates: tuple[str, ...]) -> list[OhlcBar]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        for table_name in table_candidates:
            if table_name not in available_tables:
                continue
            return _query_ohlc_rows(connection, table_name)
    return []


def _query_ohlc_rows(connection: duckdb.DuckDBPyConnection, table_name: str) -> list[OhlcBar]:
    column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    column_names = {row[1] for row in column_info}
    date_column = _pick_required_column(column_names, ("bar_dt", "trade_date", "date"))
    symbol_column = _pick_required_column(column_names, ("symbol",))
    open_column = _pick_required_column(column_names, ("open",))
    high_column = _pick_required_column(column_names, ("high",))
    low_column = _pick_required_column(column_names, ("low",))
    close_column = _pick_required_column(column_names, ("close",))
    rows = connection.execute(
        f"""
        SELECT
            {symbol_column} AS symbol,
            CAST({date_column} AS TIMESTAMP) AS bar_dt,
            CAST({open_column} AS DOUBLE) AS open,
            CAST({high_column} AS DOUBLE) AS high,
            CAST({low_column} AS DOUBLE) AS low,
            CAST({close_column} AS DOUBLE) AS close
        FROM {table_name}
        ORDER BY symbol, bar_dt
        """
    ).fetchall()
    return [
        OhlcBar(
            symbol=str(symbol),
            bar_dt=_as_datetime(bar_dt),
            open=float(open_price),
            high=float(high_price),
            low=float(low_price),
            close=float(close_price),
        )
        for symbol, bar_dt, open_price, high_price, low_price, close_price in rows
    ]


def _pick_required_column(column_names: set[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in column_names:
            return candidate
    raise ValueError(f"Could not resolve required source columns from candidates: {candidates}")


def _group_rows_by_symbol(rows: Iterable[OhlcBar]) -> dict[str, list[OhlcBar]]:
    grouped: dict[str, list[OhlcBar]] = defaultdict(list)
    for row in rows:
        grouped[row.symbol].append(row)
    return dict(grouped)


def _aggregate_rows(rows: list[OhlcBar], timeframe: Timeframe) -> list[OhlcBar]:
    grouped: dict[tuple[str, tuple[int, ...]], list[OhlcBar]] = defaultdict(list)
    for row in rows:
        if timeframe is Timeframe.WEEK:
            period_key = (row.bar_dt.isocalendar().year, row.bar_dt.isocalendar().week)
        elif timeframe is Timeframe.MONTH:
            period_key = (row.bar_dt.year, row.bar_dt.month)
        else:
            raise ValueError(f"Unsupported aggregation timeframe: {timeframe}")
        grouped[(row.symbol, period_key)].append(row)

    aggregated_rows: list[OhlcBar] = []
    for (symbol, _period_key), period_rows in sorted(grouped.items(), key=lambda item: (item[0][0], item[1][0].bar_dt)):
        ordered_rows = sorted(period_rows, key=lambda bar: bar.bar_dt)
        aggregated_rows.append(
            OhlcBar(
                symbol=symbol,
                bar_dt=ordered_rows[-1].bar_dt,
                open=ordered_rows[0].open,
                high=max(bar.high for bar in ordered_rows),
                low=min(bar.low for bar in ordered_rows),
                close=ordered_rows[-1].close,
            )
        )
    return aggregated_rows


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
