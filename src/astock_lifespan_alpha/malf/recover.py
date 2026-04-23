"""Formal MALF day target recovery entrypoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.schema import initialize_malf_schema


@dataclass(frozen=True)
class MaterializedMalfRunSummary:
    """Resolved baseline run used to rebuild the formal MALF target."""

    run_id: str
    source_path: str | None
    started_at: str | None
    finished_at: str | None
    symbols_total: int
    symbols_completed: int
    queue_rows: int
    pivot_rows: int
    wave_rows: int
    state_snapshot_rows: int
    wave_scale_snapshot_rows: int
    wave_scale_profile_rows: int

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "source_path": self.source_path,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "symbols_total": self.symbols_total,
            "symbols_completed": self.symbols_completed,
            "queue_rows": self.queue_rows,
            "pivot_rows": self.pivot_rows,
            "wave_rows": self.wave_rows,
            "state_snapshot_rows": self.state_snapshot_rows,
            "wave_scale_snapshot_rows": self.wave_scale_snapshot_rows,
            "wave_scale_profile_rows": self.wave_scale_profile_rows,
        }


@dataclass(frozen=True)
class MalfDayFormalTargetRecoverySummary:
    """Serialized summary for formal MALF day target recovery."""

    runner_name: str
    status: str
    target_path: str
    requested_baseline_run_id: str | None
    resolved_baseline: MaterializedMalfRunSummary
    recovery_path: str
    quarantine_path: str
    recovered_checkpoint_count: int
    recovered_running_run_count: int
    recovered_running_queue_count: int
    message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "status": self.status,
            "target_path": self.target_path,
            "requested_baseline_run_id": self.requested_baseline_run_id,
            "resolved_baseline": self.resolved_baseline.as_dict(),
            "recovery_path": self.recovery_path,
            "quarantine_path": self.quarantine_path,
            "recovered_checkpoint_count": self.recovered_checkpoint_count,
            "recovered_running_run_count": self.recovered_running_run_count,
            "recovered_running_queue_count": self.recovered_running_queue_count,
            "message": self.message,
        }


def recover_malf_day_formal_target(
    *,
    baseline_run_id: str | None = None,
    settings: WorkspaceRoots | None = None,
) -> MalfDayFormalTargetRecoverySummary:
    """Rebuild the canonical MALF day target from the latest materialized completed baseline."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.malf_day
    if not target_path.exists():
        raise FileNotFoundError(f"MALF day formal target does not exist: {target_path}")

    baseline = _resolve_materialized_baseline(target_path=target_path, requested_run_id=baseline_run_id)
    artifact_token = uuid4().hex[:12]
    recovery_path = target_path.with_name(f"{target_path.stem}.recover-{artifact_token}{target_path.suffix}")
    quarantine_path = target_path.with_name(f"{target_path.stem}.quarantine-{artifact_token}{target_path.suffix}")
    _rebuild_recovery_database(target_path=target_path, recovery_path=recovery_path, baseline=baseline)
    checkpoint_count, running_run_count, running_queue_count = _validate_recovered_database(
        recovery_path=recovery_path,
        baseline_run_id=baseline.run_id,
    )
    target_path.replace(quarantine_path)
    recovery_path.replace(target_path)
    message = (
        f"Recovered MALF day formal target from materialized baseline {baseline.run_id}; "
        f"canonical target replaced and polluted target quarantined."
    )
    return MalfDayFormalTargetRecoverySummary(
        runner_name="recover_malf_day_formal_target",
        status="completed",
        target_path=str(target_path),
        requested_baseline_run_id=baseline_run_id,
        resolved_baseline=baseline,
        recovery_path=str(recovery_path),
        quarantine_path=str(quarantine_path),
        recovered_checkpoint_count=checkpoint_count,
        recovered_running_run_count=running_run_count,
        recovered_running_queue_count=running_queue_count,
        message=message,
    )


def _resolve_materialized_baseline(*, target_path: Path, requested_run_id: str | None) -> MaterializedMalfRunSummary:
    with duckdb.connect(str(target_path), read_only=True) as connection:
        params: list[object] = []
        run_filter = ""
        if requested_run_id is not None:
            run_filter = "AND run.run_id = ?"
            params.append(requested_run_id)
        row = connection.execute(
            f"""
            WITH materialized_counts AS (
                SELECT
                    run.run_id,
                    run.source_path,
                    run.started_at,
                    run.finished_at,
                    run.symbols_total,
                    run.symbols_completed,
                    (
                        SELECT COUNT(*)
                        FROM malf_work_queue AS queue
                        WHERE queue.timeframe = 'day'
                          AND split_part(queue.queue_id, ':', 1) = run.run_id
                          AND queue.status <> 'running'
                    ) AS queue_rows,
                        (
                            SELECT COUNT(*)
                            FROM malf_pivot_ledger AS pivot_row
                            WHERE pivot_row.timeframe = 'day' AND pivot_row.run_id = run.run_id
                        ) AS pivot_rows,
                        (
                            SELECT COUNT(*)
                            FROM malf_wave_ledger AS wave_row
                            WHERE wave_row.timeframe = 'day' AND wave_row.run_id = run.run_id
                        ) AS wave_rows,
                        (
                            SELECT COUNT(*)
                            FROM malf_state_snapshot AS snapshot_row
                            WHERE snapshot_row.timeframe = 'day' AND snapshot_row.run_id = run.run_id
                        ) AS state_snapshot_rows,
                        (
                            SELECT COUNT(*)
                            FROM malf_wave_scale_snapshot AS snapshot_row
                            WHERE snapshot_row.timeframe = 'day' AND snapshot_row.run_id = run.run_id
                        ) AS wave_scale_snapshot_rows,
                        (
                            SELECT COUNT(*)
                            FROM malf_wave_scale_profile AS profile_row
                            WHERE profile_row.timeframe = 'day' AND profile_row.run_id = run.run_id
                        ) AS wave_scale_profile_rows
                FROM malf_run AS run
                WHERE run.timeframe = 'day'
                  AND run.status = 'completed'
                  {run_filter}
            )
            SELECT
                run_id,
                source_path,
                started_at,
                finished_at,
                symbols_total,
                symbols_completed,
                queue_rows,
                pivot_rows,
                wave_rows,
                state_snapshot_rows,
                wave_scale_snapshot_rows,
                wave_scale_profile_rows
            FROM materialized_counts
            WHERE state_snapshot_rows > 0
            ORDER BY finished_at DESC NULLS LAST, started_at DESC NULLS LAST
            LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            if requested_run_id is None:
                raise ValueError("Could not resolve a materialized completed MALF day baseline run.")
            raise ValueError(
                f"Requested MALF day baseline run {requested_run_id} is not a materialized completed run."
            )
    return MaterializedMalfRunSummary(
        run_id=str(row[0]),
        source_path=row[1],
        started_at=_format_datetime(row[2]),
        finished_at=_format_datetime(row[3]),
        symbols_total=int(row[4] or 0),
        symbols_completed=int(row[5] or 0),
        queue_rows=int(row[6] or 0),
        pivot_rows=int(row[7] or 0),
        wave_rows=int(row[8] or 0),
        state_snapshot_rows=int(row[9] or 0),
        wave_scale_snapshot_rows=int(row[10] or 0),
        wave_scale_profile_rows=int(row[11] or 0),
    )


def _rebuild_recovery_database(
    *,
    target_path: Path,
    recovery_path: Path,
    baseline: MaterializedMalfRunSummary,
) -> None:
    if recovery_path.exists():
        recovery_path.unlink()
    initialize_malf_schema(recovery_path)
    with duckdb.connect(str(recovery_path)) as connection:
        connection.execute(f"ATTACH '{target_path}' AS live (READ_ONLY)")
        try:
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
                    symbols_updated,
                    inserted_pivots,
                    inserted_waves,
                    inserted_state_snapshots,
                    inserted_wave_scale_snapshots,
                    inserted_wave_scale_profiles,
                    current_symbol,
                    elapsed_seconds,
                    estimated_remaining_symbols,
                    latest_bar_dt,
                    message,
                    started_at,
                    finished_at
                )
                SELECT
                    run_id,
                    timeframe,
                    status,
                    source_path,
                    input_rows,
                    symbols_total,
                    symbols_seen,
                    symbols_completed,
                    symbols_updated,
                    inserted_pivots,
                    inserted_waves,
                    inserted_state_snapshots,
                    inserted_wave_scale_snapshots,
                    inserted_wave_scale_profiles,
                    current_symbol,
                    elapsed_seconds,
                    estimated_remaining_symbols,
                    latest_bar_dt,
                    message,
                    started_at,
                    finished_at
                FROM live.malf_run
                WHERE run_id = ?
                """,
                [baseline.run_id],
            )
            connection.execute(
                """
                INSERT INTO malf_work_queue (
                    queue_id,
                    symbol,
                    timeframe,
                    status,
                    source_bar_count,
                    requested_at,
                    claimed_at,
                    finished_at,
                    last_bar_dt
                )
                SELECT
                    queue_id,
                    symbol,
                    timeframe,
                    status,
                    source_bar_count,
                    requested_at,
                    claimed_at,
                    finished_at,
                    last_bar_dt
                FROM live.malf_work_queue
                WHERE timeframe = 'day'
                  AND split_part(queue_id, ':', 1) = ?
                  AND status <> 'running'
                """,
                [baseline.run_id],
            )
            for table_name, columns in (
                (
                    "malf_pivot_ledger",
                    "pivot_nk, run_id, symbol, timeframe, wave_id, bar_dt, pivot_type, price",
                ),
                (
                    "malf_wave_ledger",
                    "wave_id, run_id, symbol, timeframe, direction, start_bar_dt, end_bar_dt, guard_bar_dt, "
                    "guard_price, extreme_price, new_count, no_new_span, life_state",
                ),
                (
                    "malf_state_snapshot",
                    "snapshot_nk, run_id, symbol, timeframe, bar_dt, wave_id, direction, guard_price, "
                    "extreme_price, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone",
                ),
                (
                    "malf_wave_scale_snapshot",
                    "snapshot_nk, run_id, symbol, timeframe, bar_dt, direction, wave_id, new_count, no_new_span, "
                    "life_state, update_rank, stagnation_rank, wave_position_zone",
                ),
                (
                    "malf_wave_scale_profile",
                    "profile_nk, run_id, symbol, timeframe, direction, wave_id, sample_size, new_count, no_new_span, "
                    "update_rank, stagnation_rank, wave_position_zone",
                ),
            ):
                connection.execute(
                    f"""
                    INSERT INTO {table_name} ({columns})
                    SELECT {columns}
                    FROM live.{table_name}
                    WHERE timeframe = 'day' AND run_id = ?
                    """,
                    [baseline.run_id],
                )
            connection.execute(
                """
                INSERT INTO malf_checkpoint (
                    symbol,
                    timeframe,
                    last_bar_dt,
                    last_run_id,
                    updated_at
                )
                SELECT
                    symbol,
                    timeframe,
                    MAX(bar_dt) AS last_bar_dt,
                    ? AS last_run_id,
                    CURRENT_TIMESTAMP AS updated_at
                FROM malf_state_snapshot
                WHERE timeframe = 'day' AND run_id = ?
                GROUP BY symbol, timeframe
                """,
                [baseline.run_id, baseline.run_id],
            )
        finally:
            connection.execute("DETACH live")


def _validate_recovered_database(*, recovery_path: Path, baseline_run_id: str) -> tuple[int, int, int]:
    with duckdb.connect(str(recovery_path), read_only=True) as connection:
        checkpoint_count = connection.execute(
            "SELECT COUNT(*) FROM malf_checkpoint WHERE timeframe = 'day' AND last_run_id = ?",
            [baseline_run_id],
        ).fetchone()[0]
        running_run_count = connection.execute(
            "SELECT COUNT(*) FROM malf_run WHERE timeframe = 'day' AND status = 'running'"
        ).fetchone()[0]
        running_queue_count = connection.execute(
            "SELECT COUNT(*) FROM malf_work_queue WHERE timeframe = 'day' AND status = 'running'"
        ).fetchone()[0]
        if running_run_count or running_queue_count:
            raise ValueError(
                f"Recovered MALF day formal target still contains running state: "
                f"runs={running_run_count}, queues={running_queue_count}"
            )
    return int(checkpoint_count), int(running_run_count), int(running_queue_count)


def _format_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
