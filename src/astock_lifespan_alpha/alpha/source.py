"""Shared source adapters for stage-three alpha runners."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.alpha.contracts import AlphaInputRow


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")
ALPHA_SOURCE_VIEW_NAME = "alpha_source_input"


@dataclass(frozen=True)
class AlphaSourceDataset:
    """Joined alpha source rows grouped by symbol."""

    market_source_path: Path | None
    malf_source_path: Path | None
    rows_by_symbol: dict[str, list[AlphaInputRow]]

    @property
    def row_count(self) -> int:
        return sum(len(rows) for rows in self.rows_by_symbol.values())


@dataclass(frozen=True)
class AlphaSourceMetadata:
    """Alpha source paths and cardinality without materializing source rows."""

    market_source_path: Path | None
    malf_source_path: Path | None
    row_count: int
    symbol_count: int


def load_alpha_source_rows(settings: WorkspaceRoots) -> AlphaSourceDataset:
    """Load joined market-base day bars and MALF day snapshots."""

    metadata = load_alpha_source_metadata(settings)
    return AlphaSourceDataset(
        market_source_path=metadata.market_source_path,
        malf_source_path=metadata.malf_source_path,
        rows_by_symbol=dict(iter_alpha_source_symbol_rows(settings)),
    )


def load_alpha_source_metadata(settings: WorkspaceRoots) -> AlphaSourceMetadata:
    """Return joined source cardinality without loading all rows into memory."""

    market_source_path, malf_source_path = _resolve_source_paths(settings)
    if market_source_path is None or malf_source_path is None:
        return AlphaSourceMetadata(
            market_source_path=market_source_path,
            malf_source_path=malf_source_path,
            row_count=0,
            symbol_count=0,
        )

    with duckdb.connect(str(market_source_path), read_only=True) as connection:
        market_source = _resolve_market_source(connection)
        if market_source is None or not _has_malf_snapshot(connection=connection, malf_source_path=malf_source_path):
            return AlphaSourceMetadata(
                market_source_path=market_source_path,
                malf_source_path=malf_source_path,
                row_count=0,
                symbol_count=0,
            )
        market_select_sql = _market_select_sql(market_source)
        row_count, symbol_count = connection.execute(
            f"""
            SELECT COUNT(*) AS row_count, COUNT(DISTINCT market.symbol) AS symbol_count
            FROM ({market_select_sql}) market
            INNER JOIN malf_source.malf_wave_scale_snapshot snapshot
                ON snapshot.symbol = market.symbol
                AND CAST(snapshot.bar_dt AS DATE) = market.signal_date
            """
        ).fetchone()
    return AlphaSourceMetadata(
        market_source_path=market_source_path,
        malf_source_path=malf_source_path,
        row_count=int(row_count),
        symbol_count=int(symbol_count),
    )


def iter_alpha_source_symbol_rows(
    settings: WorkspaceRoots, *, batch_size: int = 10000
) -> Iterator[tuple[str, list[AlphaInputRow]]]:
    """Yield joined alpha source rows grouped by symbol without full materialization."""

    market_source_path, malf_source_path = _resolve_source_paths(settings)
    if market_source_path is None or malf_source_path is None:
        return

    with duckdb.connect(str(market_source_path), read_only=True) as connection:
        market_source = _resolve_market_source(connection)
        if market_source is None or not _has_malf_snapshot(connection=connection, malf_source_path=malf_source_path):
            return
        market_select_sql = _market_select_sql(market_source)
        cursor = connection.execute(
            f"""
            SELECT
                market.symbol,
                market.signal_date,
                market.open,
                market.high,
                market.low,
                market.close,
                snapshot.wave_id,
                snapshot.direction,
                snapshot.new_count,
                snapshot.no_new_span,
                snapshot.life_state,
                snapshot.update_rank,
                snapshot.stagnation_rank,
                snapshot.wave_position_zone
            FROM ({market_select_sql}) market
            INNER JOIN malf_source.malf_wave_scale_snapshot snapshot
                ON snapshot.symbol = market.symbol
                AND CAST(snapshot.bar_dt AS DATE) = market.signal_date
            ORDER BY market.symbol, market.signal_date
            """
        )

        current_symbol: str | None = None
        current_rows: list[AlphaInputRow] = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for (
                symbol,
                signal_date,
                open_price,
                high_price,
                low_price,
                close_price,
                wave_id,
                direction,
                new_count,
                no_new_span,
                life_state,
                update_rank,
                stagnation_rank,
                wave_position_zone,
            ) in rows:
                symbol_value = str(symbol)
                if current_symbol is not None and symbol_value != current_symbol:
                    yield current_symbol, current_rows
                    current_rows = []
                current_symbol = symbol_value
                current_rows.append(
                    AlphaInputRow(
                        symbol=symbol_value,
                        signal_date=_as_date(signal_date),
                        open=float(open_price),
                        high=float(high_price),
                        low=float(low_price),
                        close=float(close_price),
                        wave_id=str(wave_id),
                        direction=str(direction),
                        new_count=int(new_count),
                        no_new_span=int(no_new_span),
                        life_state=str(life_state),
                        update_rank=float(update_rank),
                        stagnation_rank=float(stagnation_rank),
                        wave_position_zone=str(wave_position_zone),
                    )
                )
        if current_symbol is not None:
            yield current_symbol, current_rows


def attach_alpha_source_view(connection: duckdb.DuckDBPyConnection, settings: WorkspaceRoots) -> AlphaSourceMetadata:
    """Attach source databases and create the joined alpha source temp view."""

    metadata = load_alpha_source_metadata(settings)
    if metadata.row_count == 0 or metadata.market_source_path is None or metadata.malf_source_path is None:
        return metadata

    connection.execute(f"ATTACH {_duckdb_string_literal(metadata.market_source_path)} AS alpha_market_source (READ_ONLY)")
    connection.execute(f"ATTACH {_duckdb_string_literal(metadata.malf_source_path)} AS alpha_malf_source (READ_ONLY)")
    market_source = _resolve_market_source(connection, catalog="alpha_market_source")
    if market_source is None:
        return AlphaSourceMetadata(
            market_source_path=metadata.market_source_path,
            malf_source_path=metadata.malf_source_path,
            row_count=0,
            symbol_count=0,
        )
    market_select_sql = _market_select_sql(market_source, catalog="alpha_market_source")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {ALPHA_SOURCE_VIEW_NAME} AS
        SELECT
            market.symbol,
            market.signal_date,
            market.open,
            market.high,
            market.low,
            market.close,
            snapshot.wave_id,
            snapshot.direction,
            snapshot.new_count,
            snapshot.no_new_span,
            snapshot.life_state,
            snapshot.update_rank,
            snapshot.stagnation_rank,
            snapshot.wave_position_zone
        FROM ({market_select_sql}) market
        INNER JOIN alpha_malf_source.malf_wave_scale_snapshot snapshot
            ON snapshot.symbol = market.symbol
            AND CAST(snapshot.bar_dt AS DATE) = market.signal_date
        """
    )
    return metadata


def _resolve_source_paths(settings: WorkspaceRoots) -> tuple[Path | None, Path | None]:
    market_source_path = settings.source_databases.market_base if settings.source_databases.market_base.exists() else None
    malf_source_path = settings.databases.malf_day if settings.databases.malf_day.exists() else None
    return market_source_path, malf_source_path


@dataclass(frozen=True)
class _MarketSource:
    table_name: str
    symbol_column: str
    date_column: str
    has_adjust_method: bool


def _resolve_market_source(connection: duckdb.DuckDBPyConnection, *, catalog: str | None = None) -> _MarketSource | None:
    if catalog is None:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    else:
        available_tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = ?",
                [catalog],
            ).fetchall()
        }
    for table_name in DAY_TABLE_CANDIDATES:
        if table_name not in available_tables:
            continue
        if catalog is None:
            column_info = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_names = {row[1] for row in column_info}
        else:
            column_names = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_catalog = ? AND table_name = ?
                    """,
                    [catalog, table_name],
                ).fetchall()
            }
        date_column = _pick_required_column(column_names, ("bar_dt", "trade_date", "date"))
        symbol_column = _pick_required_column(column_names, ("symbol", "code"))
        return _MarketSource(
            table_name=table_name,
            symbol_column=symbol_column,
            date_column=date_column,
            has_adjust_method="adjust_method" in column_names,
        )
    return None


def _has_malf_snapshot(*, connection: duckdb.DuckDBPyConnection, malf_source_path: Path) -> bool:
    connection.execute(f"ATTACH {_duckdb_string_literal(malf_source_path)} AS malf_source (READ_ONLY)")
    available_tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'malf_source'"
        ).fetchall()
    }
    return "malf_wave_scale_snapshot" in available_tables


def _market_select_sql(source: _MarketSource, *, catalog: str | None = None) -> str:
    adjust_filter = "WHERE adjust_method = 'backward'" if source.has_adjust_method else ""
    table_reference = f"{catalog}.{source.table_name}" if catalog is not None else source.table_name
    return f"""
        SELECT
            {source.symbol_column} AS symbol,
            CAST({source.date_column} AS DATE) AS signal_date,
            CAST(open AS DOUBLE) AS open,
            CAST(high AS DOUBLE) AS high,
            CAST(low AS DOUBLE) AS low,
            CAST(close AS DOUBLE) AS close
        FROM {table_reference}
        {adjust_filter}
    """


def _duckdb_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


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
