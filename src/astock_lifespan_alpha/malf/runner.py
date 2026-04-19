"""Stage-two MALF runners."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.contracts import CheckpointSummary, MalfRunSummary, RunStatus, Timeframe
from astock_lifespan_alpha.malf.engine import EngineResult, run_malf_engine
from astock_lifespan_alpha.malf.schema import initialize_malf_schema
from astock_lifespan_alpha.malf.source import load_source_bars


def run_malf_day_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_day_build", timeframe=Timeframe.DAY, settings=settings)


def run_malf_week_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_week_build", timeframe=Timeframe.WEEK, settings=settings)


def run_malf_month_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_month_build", timeframe=Timeframe.MONTH, settings=settings)


def _run_malf_build(*, runner_name: str, timeframe: Timeframe, settings: WorkspaceRoots | None) -> MalfRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = {
        Timeframe.DAY: workspace.databases.malf_day,
        Timeframe.WEEK: workspace.databases.malf_week,
        Timeframe.MONTH: workspace.databases.malf_month,
    }[timeframe]
    initialize_malf_schema(target_path)

    run_id = f"{timeframe.value}-{uuid4().hex[:12]}"
    source = load_source_bars(workspace, timeframe)
    message = "MALF run completed."
    counts = {
        "pivot_rows": 0,
        "wave_rows": 0,
        "state_snapshot_rows": 0,
        "wave_scale_snapshot_rows": 0,
        "wave_scale_profile_rows": 0,
    }
    symbols_updated = 0
    latest_bar_dt: datetime | None = None

    with duckdb.connect(str(target_path)) as connection:
        _insert_run_stub(
            connection=connection,
            run_id=run_id,
            timeframe=timeframe,
            source_path=str(source.source_path) if source.source_path is not None else None,
            input_rows=source.row_count,
            symbols_seen=len(source.bars_by_symbol),
        )
        connection.execute("DELETE FROM malf_work_queue WHERE timeframe = ?", [timeframe.value])
        for symbol, bars in source.bars_by_symbol.items():
            queue_id = f"{run_id}:{symbol}"
            latest_symbol_bar_dt = bars[-1].bar_dt
            latest_bar_dt = (
                latest_symbol_bar_dt if latest_bar_dt is None or latest_symbol_bar_dt > latest_bar_dt else latest_bar_dt
            )
            connection.execute(
                """
                INSERT INTO malf_work_queue (
                    queue_id, symbol, timeframe, status, source_bar_count, claimed_at, last_bar_dt
                )
                VALUES (?, ?, ?, 'running', ?, CURRENT_TIMESTAMP, ?)
                """,
                [queue_id, symbol, timeframe.value, len(bars), latest_symbol_bar_dt],
            )
            checkpoint_bar_dt = _load_checkpoint_bar_dt(connection=connection, timeframe=timeframe, symbol=symbol)
            if checkpoint_bar_dt is not None and checkpoint_bar_dt >= latest_symbol_bar_dt:
                connection.execute(
                    """
                    UPDATE malf_work_queue
                    SET status = 'skipped', finished_at = CURRENT_TIMESTAMP
                    WHERE queue_id = ?
                    """,
                    [queue_id],
                )
                continue

            result = run_malf_engine(symbol=symbol, timeframe=timeframe, bars=bars)
            _replace_symbol_rows(connection=connection, timeframe=timeframe, symbol=symbol)
            _insert_result_rows(connection=connection, run_id=run_id, result=result)
            _upsert_checkpoint(
                connection=connection,
                timeframe=timeframe,
                symbol=symbol,
                run_id=run_id,
                last_bar_dt=latest_symbol_bar_dt,
            )
            connection.execute(
                """
                UPDATE malf_work_queue
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE queue_id = ?
                """,
                [queue_id],
            )
            counts["pivot_rows"] += len(result.pivots)
            counts["wave_rows"] += len(result.waves)
            counts["state_snapshot_rows"] += len(result.state_snapshots)
            counts["wave_scale_snapshot_rows"] += len(result.wave_scale_snapshots)
            counts["wave_scale_profile_rows"] += len(result.wave_scale_profiles)
            symbols_updated += 1

        if not source.bars_by_symbol:
            message = "MALF schema initialized without source bars."

        connection.execute(
            """
            UPDATE malf_run
            SET
                status = ?,
                symbols_updated = ?,
                inserted_pivots = ?,
                inserted_waves = ?,
                inserted_state_snapshots = ?,
                inserted_wave_scale_snapshots = ?,
                inserted_wave_scale_profiles = ?,
                latest_bar_dt = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                RunStatus.COMPLETED.value,
                symbols_updated,
                counts["pivot_rows"],
                counts["wave_rows"],
                counts["state_snapshot_rows"],
                counts["wave_scale_snapshot_rows"],
                counts["wave_scale_profile_rows"],
                latest_bar_dt,
                message,
                run_id,
            ],
        )

    return MalfRunSummary(
        runner_name=runner_name,
        timeframe=timeframe.value,
        run_id=run_id,
        status=RunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_path=str(source.source_path) if source.source_path is not None else None,
        message=message,
        materialization_counts=counts,
        checkpoint_summary=CheckpointSummary(
            symbols_seen=len(source.bars_by_symbol),
            symbols_updated=symbols_updated,
            latest_bar_dt=latest_bar_dt.isoformat() if latest_bar_dt is not None else None,
        ),
    )


def _insert_run_stub(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    timeframe: Timeframe,
    source_path: str | None,
    input_rows: int,
    symbols_seen: int,
) -> None:
    connection.execute(
        """
        INSERT INTO malf_run (run_id, timeframe, status, source_path, input_rows, symbols_seen, message)
        VALUES (?, ?, 'running', ?, ?, ?, 'MALF run started.')
        """,
        [run_id, timeframe.value, source_path, input_rows, symbols_seen],
    )


def _load_checkpoint_bar_dt(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    symbol: str,
) -> datetime | None:
    row = connection.execute(
        "SELECT last_bar_dt FROM malf_checkpoint WHERE timeframe = ? AND symbol = ?",
        [timeframe.value, symbol],
    ).fetchone()
    return row[0] if row else None


def _replace_symbol_rows(*, connection: duckdb.DuckDBPyConnection, timeframe: Timeframe, symbol: str) -> None:
    for table_name in (
        "malf_pivot_ledger",
        "malf_wave_ledger",
        "malf_state_snapshot",
        "malf_wave_scale_snapshot",
        "malf_wave_scale_profile",
    ):
        connection.execute(f"DELETE FROM {table_name} WHERE timeframe = ? AND symbol = ?", [timeframe.value, symbol])


def _insert_result_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    result: EngineResult,
) -> None:
    if result.pivots:
        connection.executemany(
            """
            INSERT INTO malf_pivot_ledger (
                pivot_nk, run_id, symbol, timeframe, wave_id, bar_dt, pivot_type, price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.pivot_nk,
                    run_id,
                    row.symbol,
                    row.timeframe,
                    row.wave_id,
                    row.bar_dt,
                    row.pivot_type,
                    row.price,
                )
                for row in result.pivots
            ],
        )
    if result.waves:
        connection.executemany(
            """
            INSERT INTO malf_wave_ledger (
                wave_id, run_id, symbol, timeframe, direction, start_bar_dt, end_bar_dt,
                guard_bar_dt, guard_price, extreme_price, new_count, no_new_span, life_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.wave_id,
                    run_id,
                    row.symbol,
                    row.timeframe,
                    row.direction,
                    row.start_bar_dt,
                    row.end_bar_dt,
                    row.guard_bar_dt,
                    row.guard_price,
                    row.extreme_price,
                    row.new_count,
                    row.no_new_span,
                    row.life_state,
                )
                for row in result.waves
            ],
        )
    if result.state_snapshots:
        connection.executemany(
            """
            INSERT INTO malf_state_snapshot (
                snapshot_nk, run_id, symbol, timeframe, bar_dt, wave_id, direction,
                guard_price, extreme_price, new_count, no_new_span, life_state,
                update_rank, stagnation_rank, wave_position_zone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.snapshot_nk,
                    run_id,
                    row.symbol,
                    row.timeframe,
                    row.bar_dt,
                    row.wave_id,
                    row.direction,
                    row.guard_price,
                    row.extreme_price,
                    row.new_count,
                    row.no_new_span,
                    row.life_state,
                    row.update_rank,
                    row.stagnation_rank,
                    row.wave_position_zone,
                )
                for row in result.state_snapshots
            ],
        )
    if result.wave_scale_snapshots:
        connection.executemany(
            """
            INSERT INTO malf_wave_scale_snapshot (
                snapshot_nk, run_id, symbol, timeframe, bar_dt, direction, wave_id,
                new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.snapshot_nk,
                    run_id,
                    row.symbol,
                    row.timeframe,
                    row.bar_dt,
                    row.direction,
                    row.wave_id,
                    row.new_count,
                    row.no_new_span,
                    row.life_state,
                    row.update_rank,
                    row.stagnation_rank,
                    row.wave_position_zone,
                )
                for row in result.wave_scale_snapshots
            ],
        )
    if result.wave_scale_profiles:
        connection.executemany(
            """
            INSERT INTO malf_wave_scale_profile (
                profile_nk, run_id, symbol, timeframe, direction, wave_id, sample_size,
                new_count, no_new_span, update_rank, stagnation_rank, wave_position_zone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.profile_nk,
                    run_id,
                    row.symbol,
                    row.timeframe,
                    row.direction,
                    row.wave_id,
                    row.sample_size,
                    row.new_count,
                    row.no_new_span,
                    row.update_rank,
                    row.stagnation_rank,
                    row.wave_position_zone,
                )
                for row in result.wave_scale_profiles
            ],
        )


def _upsert_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    symbol: str,
    run_id: str,
    last_bar_dt: datetime,
) -> None:
    updated_at = datetime.utcnow()
    connection.execute(
        """
        INSERT INTO malf_checkpoint (symbol, timeframe, last_bar_dt, last_run_id, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(symbol, timeframe) DO UPDATE
        SET
            last_bar_dt = excluded.last_bar_dt,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [symbol, timeframe.value, last_bar_dt, run_id, updated_at],
    )
