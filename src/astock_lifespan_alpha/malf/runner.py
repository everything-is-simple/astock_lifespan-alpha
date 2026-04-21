"""Stage-two MALF runners."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.contracts import (
    ArtifactSummary,
    CheckpointSummary,
    MalfRunSummary,
    ProgressSummary,
    RunStatus,
    SegmentSummary,
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


@dataclass(frozen=True)
class _SegmentSelection:
    start_symbol: str | None = None
    end_symbol: str | None = None
    symbol_limit: int | None = None
    resume: bool = True
    full_universe: bool = True

    def as_summary(self) -> SegmentSummary:
        return SegmentSummary(
            start_symbol=self.start_symbol,
            end_symbol=self.end_symbol,
            symbol_limit=self.symbol_limit,
            resume=self.resume,
            full_universe=self.full_universe,
        )


@dataclass(frozen=True)
class _DayBuildArtifacts:
    active_target_path: Path
    active_build_path: Path | None
    abandoned_build_artifacts: tuple[Path, ...]


@dataclass
class _RunProgressState:
    progress_path: Path | None
    symbols_total: int
    materialization_counts: dict[str, int]
    started_at: float = field(default_factory=perf_counter)
    symbols_seen: int = 0
    symbols_completed: int = 0
    current_symbol: str | None = None

    def elapsed_seconds(self) -> float:
        return round(perf_counter() - self.started_at, 6)

    def estimated_remaining_symbols(self) -> int:
        return max(self.symbols_total - self.symbols_completed, 0)

    def as_summary(self) -> ProgressSummary:
        return ProgressSummary(
            symbols_total=self.symbols_total,
            symbols_seen=self.symbols_seen,
            symbols_completed=self.symbols_completed,
            current_symbol=self.current_symbol,
            elapsed_seconds=self.elapsed_seconds(),
            estimated_remaining_symbols=self.estimated_remaining_symbols(),
            ledger_rows_written=dict(self.materialization_counts),
            progress_path=str(self.progress_path) if self.progress_path is not None else None,
        )


def _time_write_phase(accumulator: _WriteTimingAccumulator, phase_name: str, operation):
    started_at = perf_counter()
    try:
        return operation()
    finally:
        elapsed = perf_counter() - started_at
        setattr(accumulator, phase_name, getattr(accumulator, phase_name) + elapsed)


def run_malf_day_build(
    *,
    start_symbol: str | None = None,
    end_symbol: str | None = None,
    symbol_limit: int | None = None,
    resume: bool = True,
    progress_path: Path | None = None,
    settings: WorkspaceRoots | None = None,
) -> MalfRunSummary:
    return _run_malf_build(
        runner_name="run_malf_day_build",
        timeframe=Timeframe.DAY,
        start_symbol=start_symbol,
        end_symbol=end_symbol,
        symbol_limit=symbol_limit,
        resume=resume,
        progress_path=progress_path,
        settings=settings,
    )


def run_malf_week_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_week_build", timeframe=Timeframe.WEEK, settings=settings)


def run_malf_month_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary:
    return _run_malf_build(runner_name="run_malf_month_build", timeframe=Timeframe.MONTH, settings=settings)


def _target_has_incomplete_work(target_path: Path) -> bool:
    try:
        with duckdb.connect(str(target_path), read_only=True) as connection:
            table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            if "malf_work_queue" in table_names:
                if "malf_checkpoint" in table_names:
                    running_queue_count = connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM malf_work_queue queue
                        WHERE queue.status = 'running'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM malf_checkpoint checkpoint
                              WHERE checkpoint.symbol = queue.symbol
                                AND checkpoint.timeframe = queue.timeframe
                                AND (
                                    queue.last_bar_dt IS NULL
                                    OR checkpoint.last_bar_dt >= queue.last_bar_dt
                                )
                          )
                        """
                    ).fetchone()[0]
                else:
                    running_queue_count = connection.execute(
                        "SELECT COUNT(*) FROM malf_work_queue WHERE status = 'running'"
                    ).fetchone()[0]
                if running_queue_count:
                    return True
            if "malf_run" in table_names and "malf_work_queue" not in table_names:
                running_run_count = connection.execute(
                    "SELECT COUNT(*) FROM malf_run WHERE status = 'running'"
                ).fetchone()[0]
                if running_run_count:
                    return True
    except duckdb.Error:
        return True
    return False


def _resolve_day_build_artifacts(
    *,
    target_path: Path,
    run_id: str,
    selection: _SegmentSelection,
) -> _DayBuildArtifacts:
    pattern = f"{target_path.stem}.*.building{target_path.suffix}"
    existing_builds = tuple(
        sorted(
            target_path.parent.glob(pattern),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    )
    if selection.full_universe and target_path.exists() and not _target_has_incomplete_work(target_path):
        return _DayBuildArtifacts(
            active_target_path=target_path,
            active_build_path=None,
            abandoned_build_artifacts=existing_builds,
        )
    if selection.resume and existing_builds:
        return _DayBuildArtifacts(
            active_target_path=existing_builds[0],
            active_build_path=existing_builds[0],
            abandoned_build_artifacts=existing_builds[1:],
        )
    if not selection.full_universe or (target_path.exists() and _target_has_incomplete_work(target_path)):
        build_path = target_path.with_name(f"{target_path.stem}.{run_id}.building{target_path.suffix}")
        return _DayBuildArtifacts(
            active_target_path=build_path,
            active_build_path=build_path,
            abandoned_build_artifacts=existing_builds,
        )
    return _DayBuildArtifacts(
        active_target_path=target_path,
        active_build_path=None,
        abandoned_build_artifacts=existing_builds,
    )


def _promote_rebuilt_database(*, build_path: Path, target_path: Path, run_id: str) -> None:
    backup_path = target_path.with_name(f"{target_path.stem}.backup-{run_id}{target_path.suffix}")
    if target_path.exists():
        target_path.replace(backup_path)
    build_path.replace(target_path)


def _prepare_database_path(database_path: Path) -> None:
    if not database_path.exists():
        return
    try:
        with duckdb.connect(str(database_path)):
            return
    except duckdb.Error:
        database_path.unlink()


def _run_malf_build(
    *,
    runner_name: str,
    timeframe: Timeframe,
    start_symbol: str | None = None,
    end_symbol: str | None = None,
    symbol_limit: int | None = None,
    resume: bool = True,
    progress_path: Path | None = None,
    settings: WorkspaceRoots | None,
) -> MalfRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = {
        Timeframe.DAY: workspace.databases.malf_day,
        Timeframe.WEEK: workspace.databases.malf_week,
        Timeframe.MONTH: workspace.databases.malf_month,
    }[timeframe]
    run_id = f"{timeframe.value}-{uuid4().hex[:12]}"
    selection = _SegmentSelection(
        start_symbol=start_symbol if timeframe is Timeframe.DAY else None,
        end_symbol=end_symbol if timeframe is Timeframe.DAY else None,
        symbol_limit=symbol_limit if timeframe is Timeframe.DAY else None,
        resume=resume if timeframe is Timeframe.DAY else True,
        full_universe=(
            timeframe is not Timeframe.DAY
            or (start_symbol is None and end_symbol is None and symbol_limit is None)
        ),
    )

    artifacts = _resolve_day_build_artifacts(target_path=target_path, run_id=run_id, selection=selection) if timeframe is Timeframe.DAY else _DayBuildArtifacts(
        active_target_path=target_path,
        active_build_path=None,
        abandoned_build_artifacts=(),
    )
    active_target_path = artifacts.active_target_path
    _prepare_database_path(active_target_path)
    initialize_malf_schema(active_target_path)

    source = None
    source_path: str | None = None
    source_items = ()
    symbols_total = 0
    if timeframe is Timeframe.DAY:
        streamed_source = stream_source_bars(
            workspace,
            timeframe,
            start_symbol=selection.start_symbol,
            end_symbol=selection.end_symbol,
            symbol_limit=selection.symbol_limit,
        )
        source_path = str(streamed_source.source_path) if streamed_source.source_path is not None else None
        source_items = streamed_source.rows_by_symbol
        symbols_total = len(streamed_source.selected_symbols)
    else:
        source = load_source_bars(workspace, timeframe)
        source.validate_for_timeframe(timeframe)
        source_path = str(source.source_path) if source.source_path is not None else None
        source_items = source.bars_by_symbol.items()
        symbols_total = len(source.bars_by_symbol)

    message = "MALF run completed."
    counts = {
        "pivot_rows": 0,
        "wave_rows": 0,
        "state_snapshot_rows": 0,
        "wave_scale_snapshot_rows": 0,
        "wave_scale_profile_rows": 0,
    }
    progress_state = _RunProgressState(
        progress_path=_resolve_progress_path(
            workspace=workspace,
            runner_name=runner_name,
            run_id=run_id,
            timeframe=timeframe,
            requested_path=progress_path,
        ),
        symbols_total=symbols_total,
        materialization_counts=counts,
    )
    write_timing = _WriteTimingAccumulator()
    latest_bar_dt: datetime | None = None
    input_rows = 0
    symbols_updated = 0
    pending_writes: list[_SymbolBuildResult] = []
    selected_symbol_latest_bar_dts: dict[str, datetime] = {}

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
                symbols_total=symbols_total,
            ),
        )
        _persist_progress(
            connection=connection,
            run_id=run_id,
            input_rows=input_rows,
            latest_bar_dt=latest_bar_dt,
            message="MALF run started.",
            status="running",
            symbols_updated=symbols_updated,
            counts=counts,
            progress_state=progress_state,
        )

        for symbol, bars in source_items:
            input_rows += len(bars)
            progress_state.symbols_seen += 1
            progress_state.current_symbol = symbol
            selected_symbol_latest_bar_dts[symbol] = bars[-1].bar_dt
            symbol_result = _execute_symbol_build(
                connection=connection,
                timeframe=timeframe,
                run_id=run_id,
                symbol=symbol,
                bars=bars,
                write_timing=write_timing,
                resume=selection.resume,
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
            if symbol_result.queue_status == "skipped":
                progress_state.symbols_completed += 1
            elif symbol_result.symbol_updated:
                pending_writes.append(symbol_result)
            if len(pending_writes) >= _WRITE_BATCH_SYMBOL_LIMIT:
                progress_state.symbols_completed += _flush_symbol_writes(
                    connection=connection,
                    timeframe=timeframe,
                    run_id=run_id,
                    symbol_results=pending_writes,
                    write_timing=write_timing,
                )
                pending_writes.clear()
            _persist_progress(
                connection=connection,
                run_id=run_id,
                input_rows=input_rows,
                latest_bar_dt=latest_bar_dt,
                message="MALF run in progress.",
                status="running",
                symbols_updated=symbols_updated,
                counts=counts,
                progress_state=progress_state,
            )

        progress_state.symbols_completed += _flush_symbol_writes(
            connection=connection,
            timeframe=timeframe,
            run_id=run_id,
            symbol_results=pending_writes,
            write_timing=write_timing,
        )
        pending_writes.clear()
        progress_state.current_symbol = None

        promoted_to_target = False
        if symbols_total == 0:
            message = "MALF schema initialized without source bars."
        elif timeframe is Timeframe.DAY and not selection.full_universe:
            message = "MALF day segmented build completed without target promotion."
        elif (
            timeframe is Timeframe.DAY
            and artifacts.active_build_path is not None
            and selection.full_universe
            and progress_state.symbols_completed == symbols_total
            and _selected_symbols_checkpointed(
                connection=connection,
                timeframe=timeframe,
                symbol_latest_bar_dts=selected_symbol_latest_bar_dts,
            )
        ):
            promoted_to_target = True
            message = "MALF day full-universe build completed and building database is ready for promotion."
        elif timeframe is Timeframe.DAY and artifacts.active_build_path is not None:
            message = "MALF day build progress was written to a building database; target promotion remains pending."

        _persist_progress(
            connection=connection,
            run_id=run_id,
            input_rows=input_rows,
            latest_bar_dt=latest_bar_dt,
            message=message,
            status=RunStatus.COMPLETED.value,
            symbols_updated=symbols_updated,
            counts=counts,
            progress_state=progress_state,
            finished=True,
        )

    if timeframe is Timeframe.DAY and promoted_to_target and artifacts.active_build_path is not None:
        _promote_rebuilt_database(build_path=artifacts.active_build_path, target_path=target_path, run_id=run_id)

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
            symbols_seen=progress_state.symbols_seen,
            symbols_updated=symbols_updated,
            latest_bar_dt=latest_bar_dt.isoformat() if latest_bar_dt is not None else None,
        ),
        write_timing_summary=write_timing.as_summary(),
        segment_summary=selection.as_summary(),
        progress_summary=progress_state.as_summary(),
        artifact_summary=ArtifactSummary(
            active_build_path=str(artifacts.active_build_path) if artifacts.active_build_path is not None else None,
            abandoned_build_artifacts=tuple(str(path) for path in artifacts.abandoned_build_artifacts),
            promoted_to_target=promoted_to_target,
        ),
    )


def _resolve_progress_path(
    *,
    workspace: WorkspaceRoots,
    runner_name: str,
    run_id: str,
    timeframe: Timeframe,
    requested_path: Path | None,
) -> Path | None:
    if requested_path is not None:
        requested_path.parent.mkdir(parents=True, exist_ok=True)
        return requested_path
    if timeframe is not Timeframe.DAY:
        return None
    progress_file = workspace.module_report_root("malf") / f"{runner_name}-{run_id}-progress.json"
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    return progress_file


def _persist_progress(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    input_rows: int,
    latest_bar_dt: datetime | None,
    message: str,
    status: str,
    symbols_updated: int,
    counts: dict[str, int],
    progress_state: _RunProgressState,
    finished: bool = False,
) -> None:
    progress_summary = progress_state.as_summary()
    finished_sql = ", finished_at = CURRENT_TIMESTAMP" if finished else ""
    connection.execute(
        f"""
        UPDATE malf_run
        SET
            input_rows = ?,
            symbols_total = ?,
            symbols_seen = ?,
            symbols_completed = ?,
            status = ?,
            symbols_updated = ?,
            inserted_pivots = ?,
            inserted_waves = ?,
            inserted_state_snapshots = ?,
            inserted_wave_scale_snapshots = ?,
            inserted_wave_scale_profiles = ?,
            current_symbol = ?,
            elapsed_seconds = ?,
            estimated_remaining_symbols = ?,
            latest_bar_dt = ?,
            message = ?
            {finished_sql}
        WHERE run_id = ?
        """,
        [
            input_rows,
            progress_summary.symbols_total,
            progress_summary.symbols_seen,
            progress_summary.symbols_completed,
            status,
            symbols_updated,
            counts["pivot_rows"],
            counts["wave_rows"],
            counts["state_snapshot_rows"],
            counts["wave_scale_snapshot_rows"],
            counts["wave_scale_profile_rows"],
            progress_summary.current_symbol,
            progress_summary.elapsed_seconds,
            progress_summary.estimated_remaining_symbols,
            latest_bar_dt,
            message,
            run_id,
        ],
    )
    _write_progress_sidecar(run_id=run_id, message=message, status=status, summary=progress_summary)


def _write_progress_sidecar(
    *,
    run_id: str,
    message: str,
    status: str,
    summary: ProgressSummary,
) -> None:
    if summary.progress_path is None:
        return
    payload = {
        "run_id": run_id,
        "status": status,
        "message": message,
        **summary.as_dict(),
    }
    progress_path = Path(summary.progress_path)
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _insert_run_stub(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    timeframe: Timeframe,
    source_path: str | None,
    input_rows: int,
    symbols_total: int,
) -> None:
    connection.execute(
        """
        INSERT INTO malf_run (
            run_id,
            timeframe,
            status,
            source_path,
            input_rows,
            symbols_total,
            symbols_seen,
            symbols_completed,
            estimated_remaining_symbols,
            message
        )
        VALUES (?, ?, 'running', ?, ?, ?, 0, 0, ?, 'MALF run started.')
        """,
        [run_id, timeframe.value, source_path, input_rows, symbols_total, symbols_total],
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
    resume: bool,
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
    checkpoint_bar_dt = None
    if resume:
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
) -> int:
    if not symbol_results:
        return 0
    updated_results = [item for item in symbol_results if item.symbol_updated and item.result is not None]
    if not updated_results:
        return 0
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
    return len(updated_results)


def _selected_symbols_checkpointed(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: Timeframe,
    symbol_latest_bar_dts: dict[str, datetime],
) -> bool:
    if not symbol_latest_bar_dts:
        return False
    return all(
        (checkpoint_bar_dt := _load_checkpoint_bar_dt(connection=connection, timeframe=timeframe, symbol=symbol))
        is not None
        and checkpoint_bar_dt >= latest_bar_dt
        for symbol, latest_bar_dt in symbol_latest_bar_dts.items()
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
