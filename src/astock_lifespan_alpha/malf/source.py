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


DAY_ADJUST_METHOD = "backward"
DAY_TABLE_CANDIDATES = (
    "stock_daily_adjusted",
    "market_base_day",
    "bars_day",
    "price_bar_day",
    "market_day",
    "stock_daily_bar",
)
WEEK_TABLE_CANDIDATES = (
    "stock_weekly_adjusted",
    "market_base_week",
    "bars_week",
    "price_bar_week",
    "market_week",
    "stock_weekly_bar",
)
MONTH_TABLE_CANDIDATES = (
    "stock_monthly_adjusted",
    "market_base_month",
    "bars_month",
    "price_bar_month",
    "market_month",
    "stock_monthly_bar",
)


@dataclass(frozen=True)
class SourceBars:
    """Loaded fact-layer bars grouped by symbol."""

    source_path: Path | None
    bars_by_symbol: dict[str, list[OhlcBar]]
    selected_adjust_method: str | None = None
    duplicate_symbol_trade_date_groups: int = 0
    duplicate_symbol_trade_date_examples: tuple[str, ...] = ()

    @property
    def row_count(self) -> int:
        return sum(len(bars) for bars in self.bars_by_symbol.values())

    def validate_for_timeframe(self, timeframe: Timeframe) -> None:
        if timeframe is not Timeframe.DAY or self.duplicate_symbol_trade_date_groups == 0:
            return
        examples = ", ".join(self.duplicate_symbol_trade_date_examples) if self.duplicate_symbol_trade_date_examples else "-"
        adjust_method = self.selected_adjust_method or "unfiltered"
        raise SourceContractViolationError(
            "MALF day source contract violated after formalization: "
            f"adjust_method={adjust_method}, duplicate_symbol_trade_date_groups={self.duplicate_symbol_trade_date_groups}, "
            f"examples={examples}"
        )


@dataclass(frozen=True)
class ResolvedSourceTable:
    source_path: Path
    table_name: str


@dataclass(frozen=True)
class LoadedRowsResult:
    rows: list[OhlcBar]
    selected_adjust_method: str | None = None


@dataclass(frozen=True)
class StreamedRowsResult:
    source_path: Path | None
    rows_by_symbol: Iterable[tuple[str, list[OhlcBar]]]
    selected_symbols: tuple[str, ...] = ()
    selected_adjust_method: str | None = None


class SourceContractViolationError(ValueError):
    """Raised when the loaded source bars violate the formal MALF source contract."""


def load_source_bars(
    settings: WorkspaceRoots,
    timeframe: Timeframe,
    *,
    start_symbol: str | None = None,
    end_symbol: str | None = None,
    symbol_limit: int | None = None,
) -> SourceBars:
    """Load fact-layer bars for the requested timeframe."""
    return load_source_bars_limited(
        settings,
        timeframe,
        start_symbol=start_symbol,
        end_symbol=end_symbol,
        symbol_limit=symbol_limit,
    )


def stream_source_bars(
    settings: WorkspaceRoots,
    timeframe: Timeframe,
    *,
    start_symbol: str | None = None,
    end_symbol: str | None = None,
    symbol_limit: int | None = None,
) -> StreamedRowsResult:
    """Stream fact-layer bars grouped by symbol for memory-bounded runner execution."""

    resolved_source = resolve_source_table(settings, timeframe)
    if resolved_source is not None:
        return _stream_rows_from_table(
            resolved_source.source_path,
            resolved_source.table_name,
            timeframe=timeframe,
            start_symbol=start_symbol,
            end_symbol=end_symbol,
            symbol_limit=symbol_limit,
        )
    loaded_source = load_source_bars(
        settings,
        timeframe,
        start_symbol=start_symbol,
        end_symbol=end_symbol,
        symbol_limit=symbol_limit,
    )
    return StreamedRowsResult(
        source_path=loaded_source.source_path,
        rows_by_symbol=loaded_source.bars_by_symbol.items(),
        selected_symbols=tuple(sorted(loaded_source.bars_by_symbol)),
        selected_adjust_method=loaded_source.selected_adjust_method,
    )


def load_source_bars_limited(
    settings: WorkspaceRoots,
    timeframe: Timeframe,
    *,
    start_symbol: str | None = None,
    end_symbol: str | None = None,
    symbol_limit: int | None,
) -> SourceBars:
    """Load fact-layer bars with an optional symbol limit for diagnostics."""

    resolved_source = resolve_source_table(settings, timeframe)
    if resolved_source is not None:
        direct_rows = _load_rows_from_table(
            resolved_source.source_path,
            resolved_source.table_name,
            timeframe=timeframe,
            start_symbol=start_symbol,
            end_symbol=end_symbol,
            symbol_limit=symbol_limit,
        )
        if direct_rows.rows:
            duplicate_group_count = 0
            duplicate_examples: tuple[str, ...] = ()
            if timeframe is Timeframe.DAY:
                duplicate_group_count, duplicate_examples = _summarize_loaded_duplicates(direct_rows.rows)
            return SourceBars(
                source_path=resolved_source.source_path,
                bars_by_symbol=_group_rows_by_symbol(direct_rows.rows),
                selected_adjust_method=direct_rows.selected_adjust_method,
                duplicate_symbol_trade_date_groups=duplicate_group_count,
                duplicate_symbol_trade_date_examples=duplicate_examples,
            )
    if timeframe is not Timeframe.DAY:
        for database_path in (settings.source_databases.market_base, settings.source_databases.raw_market):
            if not database_path.exists():
                continue
            day_rows = _load_rows_from_database(database_path, DAY_TABLE_CANDIDATES)
            if day_rows:
                aggregated_rows = _aggregate_rows(day_rows, timeframe)
                return SourceBars(source_path=database_path, bars_by_symbol=_group_rows_by_symbol(aggregated_rows))
    return SourceBars(source_path=None, bars_by_symbol={})


def resolve_source_table(settings: WorkspaceRoots, timeframe: Timeframe) -> ResolvedSourceTable | None:
    table_candidates = {
        Timeframe.DAY: DAY_TABLE_CANDIDATES,
        Timeframe.WEEK: WEEK_TABLE_CANDIDATES,
        Timeframe.MONTH: MONTH_TABLE_CANDIDATES,
    }[timeframe]
    database_paths = {
        Timeframe.DAY: (settings.source_databases.market_base, settings.source_databases.raw_market),
        Timeframe.WEEK: (settings.source_databases.market_base_week, settings.source_databases.raw_market_week),
        Timeframe.MONTH: (settings.source_databases.market_base_month, settings.source_databases.raw_market_month),
    }[timeframe]
    for database_path in database_paths:
        table_name = _resolve_table_name(database_path, table_candidates)
        if table_name is not None:
            return ResolvedSourceTable(source_path=database_path, table_name=table_name)
    return None


def _resolve_table_name(database_path: Path, table_candidates: tuple[str, ...]) -> str | None:
    if not database_path.exists():
        return None
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    for table_name in table_candidates:
        if table_name in available_tables:
            return table_name
    return None


def _load_rows_from_database(database_path: Path, table_candidates: tuple[str, ...]) -> list[OhlcBar]:
    table_name = _resolve_table_name(database_path, table_candidates)
    if table_name is None:
        return []
    return _load_rows_from_table(
        database_path,
        table_name,
        timeframe=Timeframe.DAY,
        start_symbol=None,
        end_symbol=None,
        symbol_limit=None,
    ).rows


def _load_rows_from_table(
    database_path: Path,
    table_name: str,
    *,
    timeframe: Timeframe,
    start_symbol: str | None,
    end_symbol: str | None,
    symbol_limit: int | None,
) -> LoadedRowsResult:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return _query_ohlc_rows(
            connection,
            table_name,
            timeframe=timeframe,
            start_symbol=start_symbol,
            end_symbol=end_symbol,
            symbol_limit=symbol_limit,
        )


def _stream_rows_from_table(
    database_path: Path,
    table_name: str,
    *,
    timeframe: Timeframe,
    start_symbol: str | None,
    end_symbol: str | None,
    symbol_limit: int | None,
) -> StreamedRowsResult:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        column_names = {row[1] for row in column_info}
        symbol_column = _pick_required_column(column_names, ("symbol", "code"))
        base_where_clause = ""
        base_params: list[object] = []
        selected_adjust_method = DAY_ADJUST_METHOD if timeframe is Timeframe.DAY and "adjust_method" in column_names else None
        if timeframe is Timeframe.DAY and "adjust_method" in column_names:
            base_where_clause = "WHERE adjust_method = ?"
            base_params = [DAY_ADJUST_METHOD]
        symbols = tuple(
            _select_filtered_symbols(
                connection=connection,
                table_name=table_name,
                symbol_column=symbol_column,
                base_where_clause=base_where_clause,
                base_params=base_params,
                start_symbol=start_symbol,
                end_symbol=end_symbol,
                symbol_limit=symbol_limit,
            )
        )

    def _generator() -> Iterable[tuple[str, list[OhlcBar]]]:
        with duckdb.connect(str(database_path), read_only=True) as connection:
            column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_names = {row[1] for row in column_info}
            date_column = _pick_required_column(column_names, ("bar_dt", "trade_date", "date"))
            symbol_column = _pick_required_column(column_names, ("symbol", "code"))
            open_column = _pick_required_column(column_names, ("open",))
            high_column = _pick_required_column(column_names, ("high",))
            low_column = _pick_required_column(column_names, ("low",))
            close_column = _pick_required_column(column_names, ("close",))
            where_clause = ""
            params: list[object] = []
            if timeframe is Timeframe.DAY and "adjust_method" in column_names:
                where_clause = "WHERE adjust_method = ?"
                params = [DAY_ADJUST_METHOD]
            for symbol in symbols:
                symbol_where_clause = f"{where_clause} {'AND' if where_clause else 'WHERE'} {symbol_column} = ?"
                symbol_rows = connection.execute(
                    f"""
                    SELECT
                        CAST({date_column} AS TIMESTAMP) AS bar_dt,
                        CAST({open_column} AS DOUBLE) AS open,
                        CAST({high_column} AS DOUBLE) AS high,
                        CAST({low_column} AS DOUBLE) AS low,
                        CAST({close_column} AS DOUBLE) AS close
                    FROM {table_name}
                    {symbol_where_clause}
                    ORDER BY bar_dt
                    """,
                    [*params, symbol],
                ).fetchall()
                current_rows = [
                    OhlcBar(
                        symbol=symbol,
                        bar_dt=_as_datetime(bar_dt),
                        open=float(open_price),
                        high=float(high_price),
                        low=float(low_price),
                        close=float(close_price),
                    )
                    for bar_dt, open_price, high_price, low_price, close_price in symbol_rows
                ]
                _validate_streamed_symbol_rows(
                    symbol=symbol,
                    rows=current_rows,
                    timeframe=timeframe,
                    selected_adjust_method=selected_adjust_method,
                )
                yield symbol, current_rows

    return StreamedRowsResult(
        source_path=database_path,
        rows_by_symbol=_generator(),
        selected_symbols=symbols,
        selected_adjust_method=selected_adjust_method,
    )


def _query_ohlc_rows(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    timeframe: Timeframe,
    start_symbol: str | None,
    end_symbol: str | None,
    symbol_limit: int | None,
) -> LoadedRowsResult:
    column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    column_names = {row[1] for row in column_info}
    date_column = _pick_required_column(column_names, ("bar_dt", "trade_date", "date"))
    symbol_column = _pick_required_column(column_names, ("symbol", "code"))
    open_column = _pick_required_column(column_names, ("open",))
    high_column = _pick_required_column(column_names, ("high",))
    low_column = _pick_required_column(column_names, ("low",))
    close_column = _pick_required_column(column_names, ("close",))
    where_parts: list[str] = []
    params: list[object] = []
    selected_adjust_method: str | None = None
    if timeframe is Timeframe.DAY and "adjust_method" in column_names:
        where_parts.append("adjust_method = ?")
        params.append(DAY_ADJUST_METHOD)
        selected_adjust_method = DAY_ADJUST_METHOD
    selected_symbols: list[str] | None = None
    if start_symbol is not None or end_symbol is not None or symbol_limit is not None:
        selected_symbols = _select_filtered_symbols(
            connection=connection,
            table_name=table_name,
            symbol_column=symbol_column,
            base_where_clause=f"WHERE {' AND '.join(where_parts)}" if where_parts else "",
            base_params=params,
            start_symbol=start_symbol,
            end_symbol=end_symbol,
            symbol_limit=symbol_limit,
        )
        if not selected_symbols:
            return LoadedRowsResult(rows=[], selected_adjust_method=selected_adjust_method)
        placeholders = ", ".join(["?"] * len(selected_symbols))
        where_parts.append(f"{symbol_column} IN ({placeholders})")
        params.extend(selected_symbols)
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
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
        {where_clause}
        ORDER BY symbol, bar_dt
        """,
        params,
    ).fetchall()
    return LoadedRowsResult(
        rows=[
            OhlcBar(
                symbol=str(symbol),
                bar_dt=_as_datetime(bar_dt),
                open=float(open_price),
                high=float(high_price),
                low=float(low_price),
                close=float(close_price),
            )
            for symbol, bar_dt, open_price, high_price, low_price, close_price in rows
        ],
        selected_adjust_method=selected_adjust_method,
    )

def _select_filtered_symbols(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    symbol_column: str,
    base_where_clause: str,
    base_params: list[object],
    start_symbol: str | None,
    end_symbol: str | None,
    symbol_limit: int | None,
) -> list[str]:
    where_parts: list[str] = []
    params = list(base_params)
    if start_symbol is not None:
        where_parts.append(f"{symbol_column} >= ?")
        params.append(start_symbol)
    if end_symbol is not None:
        where_parts.append(f"{symbol_column} <= ?")
        params.append(end_symbol)
    joined_where = ""
    if base_where_clause and where_parts:
        joined_where = f"{base_where_clause} AND {' AND '.join(where_parts)}"
    elif base_where_clause:
        joined_where = base_where_clause
    elif where_parts:
        joined_where = f"WHERE {' AND '.join(where_parts)}"
    limit_clause = ""
    if symbol_limit is not None:
        limit_clause = "LIMIT ?"
        params.append(symbol_limit)
    rows = connection.execute(
        f"""
        SELECT DISTINCT {symbol_column}
        FROM {table_name}
        {joined_where}
        ORDER BY 1
        {limit_clause}
        """,
        params,
    ).fetchall()
    return [str(row[0]) for row in rows]


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


def _summarize_loaded_duplicates(rows: list[OhlcBar], *, example_limit: int = 5) -> tuple[int, tuple[str, ...]]:
    duplicate_counts: dict[tuple[str, datetime], int] = defaultdict(int)
    for row in rows:
        duplicate_counts[(row.symbol, row.bar_dt)] += 1
    duplicate_keys = [
        (symbol, bar_dt, count)
        for (symbol, bar_dt), count in duplicate_counts.items()
        if count > 1
    ]
    examples = tuple(
        f"{symbol}@{bar_dt.date().isoformat()}x{count}"
        for symbol, bar_dt, count in duplicate_keys[:example_limit]
    )
    return len(duplicate_keys), examples


def _validate_streamed_symbol_rows(
    *,
    symbol: str,
    rows: list[OhlcBar],
    timeframe: Timeframe,
    selected_adjust_method: str | None,
) -> None:
    if timeframe is not Timeframe.DAY:
        return
    duplicate_group_count, duplicate_examples = _summarize_loaded_duplicates(rows)
    if duplicate_group_count == 0:
        return
    examples = ", ".join(duplicate_examples) if duplicate_examples else f"{symbol}@duplicate"
    raise SourceContractViolationError(
        "MALF day source contract violated after formalization: "
        f"adjust_method={selected_adjust_method or 'unfiltered'}, duplicate_symbol_trade_date_groups={duplicate_group_count}, "
        f"examples={examples}"
    )


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
