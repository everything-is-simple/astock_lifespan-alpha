"""Stock-only raw_market to market_base materialization runner."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.data.contracts import MarketBaseBuildSummary
from astock_lifespan_alpha.data.ledger_timeframe import (
    market_base_timeframe_ledger_path,
    normalize_timeframe,
    raw_market_timeframe_ledger_path,
)
from astock_lifespan_alpha.data.safety import ensure_safe_target_data_root, resolve_target_data_root
from astock_lifespan_alpha.data.schema import (
    MARKET_BASE_STOCK_TABLE_BY_TIMEFRAME,
    RAW_STOCK_TABLE_BY_TIMEFRAME,
    initialize_market_base_schema,
    initialize_raw_market_schema,
)


def run_market_base_build(
    *,
    settings: WorkspaceRoots | None = None,
    target_data_root: Path | str | None = None,
    timeframe: str = "day",
    adjust_method: str = "backward",
    instruments: list[str] | tuple[str, ...] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = 1000,
    build_mode: str = "full",
    consume_dirty_only: bool | None = None,
    mark_clean_on_success: bool = True,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> MarketBaseBuildSummary:
    """Materialize isolated stock market_base rows from isolated raw rows."""

    workspace = settings or default_settings()
    target_root = ensure_safe_target_data_root(
        settings=workspace,
        target_data_root=resolve_target_data_root(settings=workspace, target_data_root=target_data_root),
    )
    normalized_timeframe = normalize_timeframe(timeframe)
    normalized_adjust = str(adjust_method).strip().lower()
    normalized_build_mode = _normalize_build_mode(build_mode)
    should_consume_dirty_only = normalized_build_mode == "incremental" if consume_dirty_only is None else consume_dirty_only
    materialization_run_id = run_id or f"market-base-stock-{normalized_timeframe}-{uuid4().hex[:12]}"

    raw_path = initialize_raw_market_schema(target_root, timeframe=normalized_timeframe)
    base_path = initialize_market_base_schema(target_root, timeframe=normalized_timeframe)
    raw_table = RAW_STOCK_TABLE_BY_TIMEFRAME[normalized_timeframe]
    market_table = MARKET_BASE_STOCK_TABLE_BY_TIMEFRAME[normalized_timeframe]
    selected_instruments = _resolve_scope(
        raw_path=raw_path,
        raw_table=raw_table,
        timeframe=normalized_timeframe,
        adjust_method=normalized_adjust,
        instruments=_normalize_instruments(instruments),
        consume_dirty_only=should_consume_dirty_only,
    )

    source_rows = _load_raw_rows(
        raw_path=raw_path,
        raw_table=raw_table,
        adjust_method=normalized_adjust,
        instruments=selected_instruments,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    consumed_dirty_count = 0
    with duckdb.connect(str(base_path)) as connection:
        connection.execute(
            """
            INSERT INTO base_build_run (
                run_id, status, timeframe, adjust_method, source_row_count, inserted_count,
                reused_count, rematerialized_count, consumed_dirty_count, message
            )
            VALUES (?, 'running', ?, ?, ?, 0, 0, 0, 0, 'market_base build started.')
            """,
            [materialization_run_id, normalized_timeframe, normalized_adjust, len(source_rows)],
        )
        for row in source_rows:
            existing = connection.execute(
                f"SELECT open, high, low, close FROM {market_table} WHERE daily_bar_nk = ?",
                [row["daily_bar_nk"]],
            ).fetchone()
            if existing is None:
                inserted_count += 1
            elif (existing[0], existing[1], existing[2], existing[3]) == (
                row["open"],
                row["high"],
                row["low"],
                row["close"],
            ):
                reused_count += 1
            else:
                rematerialized_count += 1
            connection.execute(f"DELETE FROM {market_table} WHERE daily_bar_nk = ?", [row["daily_bar_nk"]])
            connection.execute(
                f"""
                INSERT INTO {market_table} (
                    daily_bar_nk, code, name, timeframe, trade_date, adjust_method, open, high,
                    low, close, volume, amount, source_bar_nk, first_seen_run_id,
                    last_materialized_run_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    row["daily_bar_nk"],
                    row["code"],
                    row["name"],
                    normalized_timeframe,
                    row["trade_date"],
                    normalized_adjust,
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["amount"],
                    row["source_bar_nk"],
                    materialization_run_id,
                    materialization_run_id,
                ],
            )
        if mark_clean_on_success:
            consumed_dirty_count = _mark_dirty_consumed(
                base_connection=connection,
                raw_path=raw_path,
                codes=tuple(sorted({str(row["code"]) for row in source_rows})),
                timeframe=normalized_timeframe,
                adjust_method=normalized_adjust,
                run_id=materialization_run_id,
            )
        connection.execute(
            """
            UPDATE base_build_run
            SET status = 'completed', inserted_count = ?, reused_count = ?,
                rematerialized_count = ?, consumed_dirty_count = ?,
                message = 'market_base build completed.', finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [inserted_count, reused_count, rematerialized_count, consumed_dirty_count, materialization_run_id],
        )

    summary = MarketBaseBuildSummary(
        runner_name="run_market_base_build",
        run_id=materialization_run_id,
        status="completed",
        source_raw_path=str(raw_market_timeframe_ledger_path(target_root, timeframe=normalized_timeframe)),
        target_base_path=str(market_base_timeframe_ledger_path(target_root, timeframe=normalized_timeframe)),
        timeframe=normalized_timeframe,
        adjust_method=normalized_adjust,
        source_row_count=len(source_rows),
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        consumed_dirty_count=consumed_dirty_count,
        message="market_base build completed.",
    )
    if summary_path is not None:
        _write_summary(summary.as_dict(), summary_path)
    return summary


def _resolve_scope(
    *,
    raw_path: Path,
    raw_table: str,
    timeframe: str,
    adjust_method: str,
    instruments: tuple[str, ...],
    consume_dirty_only: bool,
) -> tuple[str, ...]:
    if instruments:
        return instruments
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        if consume_dirty_only:
            rows = connection.execute(
                """
                SELECT code
                FROM base_dirty_instrument
                WHERE timeframe = ? AND adjust_method = ? AND dirty_status = 'pending'
                ORDER BY code
                """,
                [timeframe, adjust_method],
            ).fetchall()
        else:
            rows = connection.execute(
                f"""
                SELECT DISTINCT code
                FROM {raw_table}
                WHERE timeframe = ? AND adjust_method = ?
                ORDER BY code
                """,
                [timeframe, adjust_method],
            ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _load_raw_rows(
    *,
    raw_path: Path,
    raw_table: str,
    adjust_method: str,
    instruments: tuple[str, ...],
    start_date: str | None,
    end_date: str | None,
    limit: int | None,
) -> list[dict[str, object]]:
    where_parts = ["adjust_method = ?"]
    params: list[object] = [adjust_method]
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_parts.append(f"code IN ({placeholders})")
        params.extend(instruments)
    if start_date is not None:
        where_parts.append("trade_date >= ?")
        params.append(start_date)
    if end_date is not None:
        where_parts.append("trade_date <= ?")
        params.append(end_date)
    limit_clause = ""
    if limit is not None and limit > 0:
        limit_clause = "LIMIT ?"
        params.append(limit)
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            SELECT
                bar_nk,
                code,
                name,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                amount
            FROM {raw_table}
            WHERE {' AND '.join(where_parts)}
            ORDER BY code, trade_date
            {limit_clause}
            """,
            params,
        ).fetchall()
    return [
        {
            "daily_bar_nk": f"{code}|{trade_date}|{adjust_method}",
            "source_bar_nk": bar_nk,
            "code": str(code),
            "name": str(name),
            "trade_date": trade_date,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "amount": amount,
        }
        for bar_nk, code, name, trade_date, open_price, high_price, low_price, close_price, volume, amount in rows
    ]


def _mark_dirty_consumed(
    *,
    base_connection: duckdb.DuckDBPyConnection,
    raw_path: Path,
    codes: tuple[str, ...],
    timeframe: str,
    adjust_method: str,
    run_id: str,
) -> int:
    if not codes:
        return 0
    placeholders = ", ".join("?" for _ in codes)
    dirty_rows = []
    with duckdb.connect(str(raw_path)) as raw_connection:
        dirty_rows = raw_connection.execute(
            f"""
            SELECT dirty_nk, asset_type, timeframe, code, adjust_method, dirty_reason, source_run_id, source_file_nk
            FROM base_dirty_instrument
            WHERE timeframe = ? AND adjust_method = ? AND dirty_status = 'pending' AND code IN ({placeholders})
            ORDER BY code
            """,
            [timeframe, adjust_method, *codes],
        ).fetchall()
        raw_connection.execute(
            f"""
            UPDATE base_dirty_instrument
            SET dirty_status = 'consumed', last_consumed_run_id = ?, consumed_at = CURRENT_TIMESTAMP
            WHERE timeframe = ? AND adjust_method = ? AND dirty_status = 'pending' AND code IN ({placeholders})
            """,
            [run_id, timeframe, adjust_method, *codes],
        )
    for dirty_nk, asset_type, row_timeframe, code, row_adjust_method, dirty_reason, source_run_id, source_file_nk in dirty_rows:
        base_connection.execute(
            """
            INSERT INTO base_dirty_instrument (
                dirty_nk, asset_type, timeframe, code, adjust_method, dirty_reason, source_run_id,
                source_file_nk, dirty_status, last_consumed_run_id, consumed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'consumed', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(dirty_nk) DO UPDATE SET
                dirty_status = 'consumed',
                last_consumed_run_id = excluded.last_consumed_run_id,
                consumed_at = excluded.consumed_at
            """,
            [dirty_nk, asset_type, row_timeframe, code, row_adjust_method, dirty_reason, source_run_id, source_file_nk, run_id],
        )
    return len(dirty_rows)


def _normalize_build_mode(build_mode: str) -> str:
    normalized = str(build_mode).strip().lower()
    if normalized not in {"full", "incremental"}:
        raise ValueError(f"Unsupported market_base build_mode: {build_mode}")
    return normalized


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    return tuple(sorted({str(item).strip().upper() for item in instruments or () if str(item).strip()}))


def _write_summary(payload: dict[str, object], summary_path: Path) -> None:
    import json

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
