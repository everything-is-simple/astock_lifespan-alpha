"""Source adapters for stage-four position runners."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.position.contracts import PositionInputRow


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")


@dataclass(frozen=True)
class PositionSourceDataset:
    """Joined alpha_signal and market reference rows grouped by symbol."""

    alpha_source_path: Path | None
    market_source_path: Path | None
    rows_by_symbol: dict[str, list[PositionInputRow]]

    @property
    def row_count(self) -> int:
        return sum(len(rows) for rows in self.rows_by_symbol.values())


def load_position_source_rows(settings: WorkspaceRoots) -> PositionSourceDataset:
    """Load alpha_signal rows and enrich them with market reference prices."""

    alpha_source_path = settings.databases.alpha_signal if settings.databases.alpha_signal.exists() else None
    market_source_path = settings.source_databases.market_base if settings.source_databases.market_base.exists() else None
    if alpha_source_path is None or market_source_path is None:
        return PositionSourceDataset(alpha_source_path=alpha_source_path, market_source_path=market_source_path, rows_by_symbol={})

    alpha_rows = _load_alpha_signal_rows(alpha_source_path)
    market_rows = _load_market_rows(market_source_path)
    if not alpha_rows or not market_rows:
        return PositionSourceDataset(alpha_source_path=alpha_source_path, market_source_path=market_source_path, rows_by_symbol={})

    market_by_symbol: dict[str, list[dict[str, object]]] = defaultdict(list)
    for market_row in market_rows:
        market_by_symbol[market_row["symbol"]].append(market_row)

    rows_by_symbol: dict[str, list[PositionInputRow]] = defaultdict(list)
    for alpha_row in alpha_rows:
        reference = _pick_reference_row(market_by_symbol.get(alpha_row["symbol"], []), alpha_row["signal_date"])
        rows_by_symbol[alpha_row["symbol"]].append(
            PositionInputRow(
                signal_nk=alpha_row["signal_nk"],
                symbol=alpha_row["symbol"],
                signal_date=alpha_row["signal_date"],
                trigger_type=alpha_row["trigger_type"],
                formal_signal_status=alpha_row["formal_signal_status"],
                source_trigger_event_nk=alpha_row["source_trigger_event_nk"],
                wave_id=alpha_row["wave_id"],
                direction=alpha_row["direction"],
                new_count=alpha_row["new_count"],
                no_new_span=alpha_row["no_new_span"],
                life_state=alpha_row["life_state"],
                update_rank=alpha_row["update_rank"],
                stagnation_rank=alpha_row["stagnation_rank"],
                wave_position_zone=alpha_row["wave_position_zone"],
                reference_trade_date=reference["trade_date"] if reference is not None else None,
                reference_price=reference["close"] if reference is not None else None,
            )
        )

    ordered_rows = {
        symbol: sorted(rows, key=lambda row: (row.signal_date, row.signal_nk))
        for symbol, rows in rows_by_symbol.items()
    }
    return PositionSourceDataset(
        alpha_source_path=alpha_source_path,
        market_source_path=market_source_path,
        rows_by_symbol=ordered_rows,
    )


def _load_alpha_signal_rows(database_path: Path) -> list[dict[str, object]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "alpha_signal" not in available_tables:
            return []
        rows = connection.execute(
            """
            SELECT
                signal_nk,
                symbol,
                CAST(signal_date AS DATE) AS signal_date,
                trigger_type,
                formal_signal_status,
                source_trigger_event_nk,
                wave_id,
                direction,
                new_count,
                no_new_span,
                life_state,
                update_rank,
                stagnation_rank,
                wave_position_zone
            FROM alpha_signal
            ORDER BY symbol, signal_date, signal_nk
            """
        ).fetchall()
    return [
        {
            "signal_nk": str(signal_nk),
            "symbol": str(symbol),
            "signal_date": _as_date(signal_date),
            "trigger_type": str(trigger_type),
            "formal_signal_status": str(formal_signal_status),
            "source_trigger_event_nk": str(source_trigger_event_nk),
            "wave_id": str(wave_id),
            "direction": str(direction),
            "new_count": int(new_count),
            "no_new_span": int(no_new_span),
            "life_state": str(life_state),
            "update_rank": float(update_rank),
            "stagnation_rank": float(stagnation_rank),
            "wave_position_zone": str(wave_position_zone),
        }
        for signal_nk, symbol, signal_date, trigger_type, formal_signal_status, source_trigger_event_nk, wave_id, direction, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone in rows
    ]


def _load_market_rows(database_path: Path) -> list[dict[str, object]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        for table_name in DAY_TABLE_CANDIDATES:
            if table_name not in available_tables:
                continue
            column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_names = {row[1] for row in column_info}
            date_column = _pick_required_column(column_names, ("bar_dt", "trade_date", "date"))
            symbol_column = _pick_required_column(column_names, ("symbol", "code"))
            rows = connection.execute(
                f"""
                SELECT
                    {symbol_column} AS symbol,
                    CAST({date_column} AS DATE) AS trade_date,
                    CAST(close AS DOUBLE) AS close
                FROM {table_name}
                ORDER BY symbol, trade_date
                """
            ).fetchall()
            return [
                {
                    "symbol": str(symbol),
                    "trade_date": _as_date(trade_date),
                    "close": float(close_price),
                }
                for symbol, trade_date, close_price in rows
            ]
    return []


def _pick_required_column(column_names: set[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in column_names:
            return candidate
    raise ValueError(f"Could not resolve required source columns from candidates: {candidates}")


def _pick_reference_row(rows: list[dict[str, object]], signal_date: date) -> dict[str, object] | None:
    for row in rows:
        if row["trade_date"] >= signal_date:
            return row
    return None


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()
