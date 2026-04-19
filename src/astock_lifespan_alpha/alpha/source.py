"""Shared source adapters for stage-three alpha runners."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.alpha.contracts import AlphaInputRow


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")


@dataclass(frozen=True)
class AlphaSourceDataset:
    """Joined alpha source rows grouped by symbol."""

    market_source_path: Path | None
    malf_source_path: Path | None
    rows_by_symbol: dict[str, list[AlphaInputRow]]

    @property
    def row_count(self) -> int:
        return sum(len(rows) for rows in self.rows_by_symbol.values())


def load_alpha_source_rows(settings: WorkspaceRoots) -> AlphaSourceDataset:
    """Load joined market-base day bars and MALF day snapshots."""

    market_source_path = settings.source_databases.market_base if settings.source_databases.market_base.exists() else None
    malf_source_path = settings.databases.malf_day if settings.databases.malf_day.exists() else None
    if market_source_path is None or malf_source_path is None:
        return AlphaSourceDataset(market_source_path=market_source_path, malf_source_path=malf_source_path, rows_by_symbol={})

    market_rows = _load_market_rows(market_source_path)
    malf_rows = _load_malf_rows(malf_source_path)
    if not market_rows or not malf_rows:
        return AlphaSourceDataset(market_source_path=market_source_path, malf_source_path=malf_source_path, rows_by_symbol={})

    malf_by_key = {(row["symbol"], row["signal_date"]): row for row in malf_rows}
    rows_by_symbol: dict[str, list[AlphaInputRow]] = defaultdict(list)
    for market_row in market_rows:
        key = (market_row["symbol"], market_row["signal_date"])
        malf_row = malf_by_key.get(key)
        if malf_row is None:
            continue
        rows_by_symbol[market_row["symbol"]].append(
            AlphaInputRow(
                symbol=market_row["symbol"],
                signal_date=market_row["signal_date"],
                open=market_row["open"],
                high=market_row["high"],
                low=market_row["low"],
                close=market_row["close"],
                wave_id=malf_row["wave_id"],
                direction=malf_row["direction"],
                new_count=malf_row["new_count"],
                no_new_span=malf_row["no_new_span"],
                life_state=malf_row["life_state"],
                update_rank=malf_row["update_rank"],
                stagnation_rank=malf_row["stagnation_rank"],
                wave_position_zone=malf_row["wave_position_zone"],
            )
        )
    ordered_rows = {
        symbol: sorted(rows, key=lambda row: row.signal_date)
        for symbol, rows in rows_by_symbol.items()
    }
    return AlphaSourceDataset(
        market_source_path=market_source_path,
        malf_source_path=malf_source_path,
        rows_by_symbol=ordered_rows,
    )


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
                    CAST({date_column} AS DATE) AS signal_date,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close
                FROM {table_name}
                ORDER BY symbol, signal_date
                """
            ).fetchall()
            return [
                {
                    "symbol": str(symbol),
                    "signal_date": _as_date(signal_date),
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                }
                for symbol, signal_date, open_price, high_price, low_price, close_price in rows
            ]
    return []


def _load_malf_rows(database_path: Path) -> list[dict[str, object]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "malf_wave_scale_snapshot" not in available_tables:
            return []
        rows = connection.execute(
            """
            SELECT
                symbol,
                CAST(bar_dt AS DATE) AS signal_date,
                wave_id,
                direction,
                new_count,
                no_new_span,
                life_state,
                update_rank,
                stagnation_rank,
                wave_position_zone
            FROM malf_wave_scale_snapshot
            ORDER BY symbol, signal_date
            """
        ).fetchall()
        return [
            {
                "symbol": str(symbol),
                "signal_date": _as_date(signal_date),
                "wave_id": str(wave_id),
                "direction": str(direction),
                "new_count": int(new_count),
                "no_new_span": int(no_new_span),
                "life_state": str(life_state),
                "update_rank": float(update_rank),
                "stagnation_rank": float(stagnation_rank),
                "wave_position_zone": str(wave_position_zone),
            }
            for symbol, signal_date, wave_id, direction, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone in rows
        ]


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
