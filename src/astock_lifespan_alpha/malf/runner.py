"""Stage-two MALF runners."""

from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.contracts import (
    CheckpointSummary,
    MalfRunSummary,
    RunStatus,
    Timeframe,
    WriteTimingSummary,
)
from astock_lifespan_alpha.malf.engine import EngineResult, run_malf_engine
from astock_lifespan_alpha.malf.schema import initialize_malf_schema
from astock_lifespan_alpha.malf.source import load_source_bars, stream_source_bars

try:  # pragma: no cover - exercised only when optional Arrow runtime is installed.
    import pyarrow as pa
except ImportError:  # pragma: no cover - fallback covered through pandas/executemany paths.
    pa = None

try:
    import pandas as pd
except ImportError:  # pragma: no cover - minimal runtime fallback.
    pd = None


_WRITE_BATCH_SYMBOL_LIMIT = 25


@dataclass(frozen=True)
class _SymbolBuildResult:
    symbol: str
    queue_id: str
    latest_bar_dt: datetime
    queue_status: str
    pivot_rows: int
    wave_rows: int
    state_snapshot_rows: int
    wave_scale_snapshot_rows: int
    wave_scale_profile_rows: int
    symbol_updated: bool
    result: EngineResult | None = None


@dataclass
class _WriteTimingAccumulator:
    delete_old_rows_seconds: float = 0.0
    insert_ledgers_seconds: float = 0.0
    checkpoint_seconds: float = 0.0
    queue_update_seconds: float = 0.0

    def add(self, other: "_WriteTimingAccumulator") -> None:
        self.delete_old_rows_seconds += other.delete_old_rows_seconds
        self.insert_ledgers_seconds += other.insert_ledgers_seconds
        self.checkpoint_seconds += other.checkpoint_seconds
        self.queue_update_seconds += other.queue_update_seconds

    def as_summary(self) -> WriteTimingSummary:
        delete_old_rows_seconds = round(self.delete_old_rows_seconds, 6)
        insert_ledgers_seconds = round(self.insert_ledgers_seconds, 6)
        checkpoint_seconds = round(self.checkpoint_seconds, 6)
        queue_update_seconds = round(self.queue_update_seconds, 6)
        return WriteTimingSummary(
            delete_old_rows_seconds=delete_old_rows_seconds,
            insert_ledgers_seconds=insert_ledgers_seconds,
            checkpoint_seconds=checkpoint_seconds,
            queue_update_seconds=queue_update_seconds,
            write_seconds=round(
                delete_old_rows_seconds
                + insert_ledgers_seconds
                + checkpoint_seconds
                + queue_update_seconds,
                6,
            ),
        )


def _time_write_phase(accumulator: _WriteTimingAccumulator, phase_name: str, operation):
    started_at = perf_counter()
    try:
        return operation()
    finally:
        elapsed = perf_counter() - started_at
        setattr(accumulator, phase_name, getattr(accumulator, phase_name) + elapsed)


def run_malf_day_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_day_build", timeframe=Timeframe.DAY, settings=settings)


def run_malf_week_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_week_build", timeframe=Timeframe.WEEK, settings=settings)


def run_malf_month_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_month_build", timeframe=Timeframe.MONTH, settings=settings)


def _resolve_active_target_path(*, target_path: Path, timeframe: Timeframe, run_id: str) -> Path:
    if timeframe is not Timeframe.DAY or not target_path.exists():
        return target_path
    if not _target_has_incomplete_work(target_path):
        return target_path
    return target_path.with_name(f"{target_path.stem}.{run_id}.building{target_path.suffix}")


def _target_has_incomplete_work(target_path: Path) -> bool:
    try:
        with duckdb.connect(str(target_path), read_only=True) as connection:
            table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            if "malf_work_queue" in table_names:
                running_queue_count = connection.execute(
                    "SELECT COUNT(*) FROM malf_work_queue WHERE status = 'running'"
                ).fetchone()[0]
                if running_queue_count:
                    return True
            if "malf_run" in table_names:
                running_run_count = connection.execute(
                    "SELECT COUNT(*) FROM malf_run WHERE status = 'running'"
                ).fetchone()[0]
                if running_run_count:
                    return True
    except duckdb.Error:
        return True
    return False


def _promote_rebuilt_database(*, build_path: Path, target_path: Path, run_id: str) -> None:
    backup_path = target_path.with_name(f"{target_path.stem}.backup-{run_id}{target_path.suffix}")
    if target_path.exists():
        target_path.replace(backup_path)
    build_path.replace(target_path)


def _run_malf_build(*, runner_name: str, timeframe: Timeframe, settings: WorkspaceRoots | None) -> MalfRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = {
        Timeframe.DAY: workspace.databases.malf_day,
        Timeframe.WEEK: workspace.databases.malf_week,
        Timeframe.MONTH: workspace.databases.malf_month,
    }[timeframe]
    run_id = f"{timeframe.value}-{uuid4().hex[:12]}"
    active_target_path = _resolve_active_target_path(target_path=target_path, timeframe=timeframe, run_id=run_id)
    initialize_malf_schema(active_target_path)
    source = load_source_bars(workspace, timeframe) if timeframe is not Timeframe.DAY else None
    if source is not None:
        source.validate_for_timeframe(timeframe)
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
    source_path = str(source.source_path) if source is not None and source.source_path is not None else None
    symbols_seen = 0
    input_rows = 0
    source_items = source.bars_by_symbol.items() if source is not None else ()
    write_timing = _WriteTimingAccumulator()
    pending_writes: list[_SymbolBuildResult] = []
    if timeframe is Timeframe.DAY:
        streamed_source = stream_source_bars(workspace, timeframe)
        source_path = str(streamed_source.source_path) if streamed_source.source_path is not None else None
        source_items = streamed_source.rows_by_symbol

    with duckdb.connect(str(active_target_path)) as connection:
        _time_write_phase(
            write_timing,
            "queue_update_seconds",
            lambda: _insert_run_stub(
                connection=connection,
                run_id=run_id,
                timeframe=timeframe,
                source_path=source_path,
                input_rows=input_rows,
                symbols_seen=symbols_seen,
            ),
        )
        _time_write_phase(
            write_timing,
            "queue_update_seconds",
            lambda: connection.execute("DELETE FROM malf_work_queue WHERE timeframe = ?", [timeframe.value]),
        )
        for symbol, bars in source_items:
            input_rows += len(bars)
            symbols_seen += 1
            symbol_result = _execute_symbol_build(
                connection=connection,
                timeframe=timeframe,
                run_id=run_id,
                symbol=symbol,
                bars=bars,
                write_timing=write_timing,
            )
            latest_bar_dt = (
                symbol_result.latest_bar_dt
                if latest_bar_dt is None or symbol_result.latest_bar_dt > latest_bar_dt
                else latest_bar_dt
            )
            counts["pivot_rows"] += symbol_result.pivot_rows
            counts["wave_rows"] += symbol_result.wave_rows
            counts["state_snapshot_rows"] += symbol_result.state_snapshot_rows
            counts["wave_scale_snapshot_rows"] += symbol_result.wave_scale_snapshot_rows
            counts["wave_scale_profile_rows"] += symbol_result.wave_scale_profile_rows
            symbols_updated += 1 if symbol_result.symbol_updated else 0
            if symbol_result.symbol_updated:
                pending_writes.append(symbol_result)
            if len(pending_writes) >= _WRITE_BATCH_SYMBOL_LIMIT:
                _flush_symbol_writes(
                    connection=connection,
                    timeframe=timeframe,
                    run_id=run_id,
                    symbol_results=pending_writes,
                    write_timing=write_timing,
                )
                pending_writes.clear()

        _flush_symbol_writes(
            connection=connection,
            timeframe=timeframe,
            run_id=run_id,
            symbol_results=pending_writes,
            write_timing=write_timing,
        )

        if symbols_seen == 0:
            message = "MALF schema initialized without source bars."
        elif active_target_path != target_path:
            message = "MALF run completed. Existing target was backed up before rebuild."

        _time_write_phase(
            write_timing,
            "queue_update_seconds",
            lambda: connection.execute(
                """
                UPDATE malf_run
                SET
                    input_rows = ?,
                    symbols_seen = ?,
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
                    input_rows,
                    symbols_seen,
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
            ),
        )

    if active_target_path != target_path:
        _promote_rebuilt_database(build_path=active_target_path, target_path=target_path, run_id=run_id)

    return MalfRunSummary(
        runner_name=runner_name,
        timeframe=timeframe.value,
        run_id=run_id,
        status=RunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_path=source_path,
        message=message,
        materialization_counts=counts,
        checkpoint_summary=CheckpointSummary(
            symbols_seen=symbols_seen,
            symbols_updated=symbols_updated,
            latest_bar_dt=latest_bar_dt.isoformat() if latest_bar_dt is not None else None,
        ),
        write_timing_summary=write_timing.as_summary(),
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


def _execute_symbol_build(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    run_id: str,
    symbol: str,
    bars: list,
    write_timing: _WriteTimingAccumulator,
) -> _SymbolBuildResult:
    queue_id = f"{run_id}:{symbol}"
    latest_symbol_bar_dt = bars[-1].bar_dt
    _time_write_phase(
        write_timing,
        "queue_update_seconds",
        lambda: _insert_work_queue_running(
            connection=connection,
            queue_id=queue_id,
            symbol=symbol,
            timeframe=timeframe,
            source_bar_count=len(bars),
            last_bar_dt=latest_symbol_bar_dt,
        ),
    )
    checkpoint_bar_dt = _time_write_phase(
        write_timing,
        "checkpoint_seconds",
        lambda: _load_checkpoint_bar_dt(connection=connection, timeframe=timeframe, symbol=symbol),
    )
    if checkpoint_bar_dt is not None and checkpoint_bar_dt >= latest_symbol_bar_dt:
        _time_write_phase(
            write_timing,
            "queue_update_seconds",
            lambda: _update_work_queue_status_batch(connection=connection, queue_ids=[queue_id], status="skipped"),
        )
        return _SymbolBuildResult(
            symbol=symbol,
            queue_id=queue_id,
            latest_bar_dt=latest_symbol_bar_dt,
            queue_status="skipped",
            pivot_rows=0,
            wave_rows=0,
            state_snapshot_rows=0,
            wave_scale_snapshot_rows=0,
            wave_scale_profile_rows=0,
            symbol_updated=False,
        )

    result = run_malf_engine(symbol=symbol, timeframe=timeframe, bars=bars)
    return _SymbolBuildResult(
        symbol=symbol,
        queue_id=queue_id,
        latest_bar_dt=latest_symbol_bar_dt,
        queue_status="pending_write",
        pivot_rows=len(result.pivots),
        wave_rows=len(result.waves),
        state_snapshot_rows=len(result.state_snapshots),
        wave_scale_snapshot_rows=len(result.wave_scale_snapshots),
        wave_scale_profile_rows=len(result.wave_scale_profiles),
        symbol_updated=True,
        result=result,
    )


def _insert_work_queue_running(
    *,
    connection: duckdb.DuckDBPyConnection,
    queue_id: str,
    symbol: str,
    timeframe: Timeframe,
    source_bar_count: int,
    last_bar_dt: datetime,
) -> None:
    connection.execute(
        """
        INSERT INTO malf_work_queue (
            queue_id, symbol, timeframe, status, source_bar_count, claimed_at, last_bar_dt
        )
        VALUES (?, ?, ?, 'running', ?, CURRENT_TIMESTAMP, ?)
        """,
        [queue_id, symbol, timeframe.value, source_bar_count, last_bar_dt],
    )


def _flush_symbol_writes(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    run_id: str,
    symbol_results: list[_SymbolBuildResult],
    write_timing: _WriteTimingAccumulator,
) -> None:
    if not symbol_results:
        return
    updated_results = [item for item in symbol_results if item.symbol_updated and item.result is not None]
    if not updated_results:
        return
    _time_write_phase(
        write_timing,
        "insert_ledgers_seconds",
        lambda: _insert_result_rows_batch(
            connection=connection,
            run_id=run_id,
            results=[item.result for item in updated_results if item.result is not None],
            replace_existing=True,
        ),
    )
    _time_write_phase(
        write_timing,
        "checkpoint_seconds",
        lambda: _upsert_checkpoints_batch(
            connection=connection,
            timeframe=timeframe,
            run_id=run_id,
            symbol_results=updated_results,
        ),
    )
    _time_write_phase(
        write_timing,
        "queue_update_seconds",
        lambda: _update_work_queue_status_batch(
            connection=connection,
            queue_ids=[item.queue_id for item in updated_results],
            status="completed",
        ),
    )


def _replace_symbol_rows(*, connection: duckdb.DuckDBPyConnection, timeframe: Timeframe, symbol: str) -> None:
    _replace_symbol_rows_batch(connection=connection, timeframe=timeframe, symbols=[symbol])


def _replace_symbol_rows_batch(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    symbols: list[str],
) -> None:
    if not symbols:
        return
    placeholders = ", ".join(["?"] * len(symbols))
    params: list[object] = [timeframe.value, *symbols]
    for table_name in (
        "malf_pivot_ledger",
        "malf_wave_ledger",
        "malf_state_snapshot",
        "malf_wave_scale_snapshot",
        "malf_wave_scale_profile",
    ):
        connection.execute(f"DELETE FROM {table_name} WHERE timeframe = ? AND symbol IN ({placeholders})", params)


def _insert_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: list[str],
    rows: list[tuple],
    replace_existing: bool = False,
) -> None:
    if not rows:
        return
    column_sql = ", ".join(columns)
    insert_keyword = "INSERT OR REPLACE" if replace_existing else "INSERT"
    registered_name = f"_{table_name}_batch"
    if pa is not None:
        payload = pa.Table.from_pylist([dict(zip(columns, row)) for row in rows])
        connection.register(registered_name, payload)
        try:
            connection.execute(
                f"{insert_keyword} INTO {table_name} ({column_sql}) SELECT {column_sql} FROM {registered_name}"
            )
        finally:
            connection.unregister(registered_name)
        return
    if pd is not None:
        payload = pd.DataFrame.from_records(rows, columns=columns)
        connection.register(registered_name, payload)
        try:
            connection.execute(
                f"{insert_keyword} INTO {table_name} ({column_sql}) SELECT {column_sql} FROM {registered_name}"
            )
        finally:
            connection.unregister(registered_name)
        return
    placeholders = ", ".join(["?"] * len(columns))
    connection.executemany(
        f"{insert_keyword} INTO {table_name} ({column_sql}) VALUES ({placeholders})",
        rows,
    )


def _insert_result_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    result: EngineResult,
) -> None:
    _insert_result_rows_batch(connection=connection, run_id=run_id, results=[result])


def _insert_result_rows_batch(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    results: list[EngineResult],
    replace_existing: bool = False,
) -> None:
    pivot_rows = [
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
        for result in results
        for row in result.pivots
    ]
    _insert_rows(
        connection=connection,
        table_name="malf_pivot_ledger",
        columns=["pivot_nk", "run_id", "symbol", "timeframe", "wave_id", "bar_dt", "pivot_type", "price"],
        rows=pivot_rows,
        replace_existing=replace_existing,
    )
    wave_rows = [
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
        for result in results
        for row in result.waves
    ]
    _insert_rows(
        connection=connection,
        table_name="malf_wave_ledger",
        columns=[
            "wave_id",
            "run_id",
            "symbol",
            "timeframe",
            "direction",
            "start_bar_dt",
            "end_bar_dt",
            "guard_bar_dt",
            "guard_price",
            "extreme_price",
            "new_count",
            "no_new_span",
            "life_state",
        ],
        rows=wave_rows,
        replace_existing=replace_existing,
    )
    state_snapshot_rows = [
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
        for result in results
        for row in result.state_snapshots
    ]
    _insert_rows(
        connection=connection,
        table_name="malf_state_snapshot",
        columns=[
            "snapshot_nk",
            "run_id",
            "symbol",
            "timeframe",
            "bar_dt",
            "wave_id",
            "direction",
            "guard_price",
            "extreme_price",
            "new_count",
            "no_new_span",
            "life_state",
            "update_rank",
            "stagnation_rank",
            "wave_position_zone",
        ],
        rows=state_snapshot_rows,
        replace_existing=replace_existing,
    )
    wave_scale_snapshot_rows = [
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
        for result in results
        for row in result.wave_scale_snapshots
    ]
    _insert_rows(
        connection=connection,
        table_name="malf_wave_scale_snapshot",
        columns=[
            "snapshot_nk",
            "run_id",
            "symbol",
            "timeframe",
            "bar_dt",
            "direction",
            "wave_id",
            "new_count",
            "no_new_span",
            "life_state",
            "update_rank",
            "stagnation_rank",
            "wave_position_zone",
        ],
        rows=wave_scale_snapshot_rows,
        replace_existing=replace_existing,
    )
    wave_scale_profile_rows = [
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
        for result in results
        for row in result.wave_scale_profiles
    ]
    _insert_rows(
        connection=connection,
        table_name="malf_wave_scale_profile",
        columns=[
            "profile_nk",
            "run_id",
            "symbol",
            "timeframe",
            "direction",
            "wave_id",
            "sample_size",
            "new_count",
            "no_new_span",
            "update_rank",
            "stagnation_rank",
            "wave_position_zone",
        ],
        rows=wave_scale_profile_rows,
        replace_existing=replace_existing,
    )


def _upsert_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    symbol: str,
    run_id: str,
    last_bar_dt: datetime,
) -> None:
    _upsert_checkpoints_batch(
        connection=connection,
        timeframe=timeframe,
        run_id=run_id,
        symbol_results=[
            _SymbolBuildResult(
                symbol=symbol,
                queue_id="",
                latest_bar_dt=last_bar_dt,
                queue_status="completed",
                pivot_rows=0,
                wave_rows=0,
                state_snapshot_rows=0,
                wave_scale_snapshot_rows=0,
                wave_scale_profile_rows=0,
                symbol_updated=True,
            )
        ],
    )


def _upsert_checkpoints_batch(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    run_id: str,
    symbol_results: list[_SymbolBuildResult],
) -> None:
    if not symbol_results:
        return
    updated_at = datetime.utcnow()
    connection.executemany(
        """
        INSERT INTO malf_checkpoint (symbol, timeframe, last_bar_dt, last_run_id, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(symbol, timeframe) DO UPDATE
        SET
            last_bar_dt = excluded.last_bar_dt,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [(item.symbol, timeframe.value, item.latest_bar_dt, run_id, updated_at) for item in symbol_results],
    )


def _update_work_queue_status_batch(
    *,
    connection: duckdb.DuckDBPyConnection,
    queue_ids: list[str],
    status: str,
) -> None:
    if not queue_ids:
        return
    placeholders = ", ".join(["?"] * len(queue_ids))
    connection.execute(
        f"""
        UPDATE malf_work_queue
        SET status = ?, finished_at = CURRENT_TIMESTAMP
        WHERE queue_id IN ({placeholders})
        """,
        [status, *queue_ids],
    )
