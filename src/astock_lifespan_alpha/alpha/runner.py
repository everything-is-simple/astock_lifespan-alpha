"""Stage-three alpha runners."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.alpha.contracts import (
    AlphaCheckpointSummary,
    AlphaRunStatus,
    AlphaRunSummary,
    TriggerType,
)
from astock_lifespan_alpha.alpha.engine import (
    AlphaSignalRow,
    TriggerEvaluationResult,
    TriggerEventRow,
    build_alpha_signal_rows,
    evaluate_trigger_rows,
)
from astock_lifespan_alpha.alpha.schema import (
    SIGNAL_TABLES,
    TRIGGER_TABLES,
    initialize_alpha_signal_schema,
    initialize_alpha_trigger_schema,
)
from astock_lifespan_alpha.alpha.source import load_alpha_source_rows
from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings


TRIGGER_TARGET_PATHS = {
    TriggerType.BOF: "alpha_bof",
    TriggerType.TST: "alpha_tst",
    TriggerType.PB: "alpha_pb",
    TriggerType.CPB: "alpha_cpb",
    TriggerType.BPB: "alpha_bpb",
}


def run_alpha_bof_build(*, settings: WorkspaceRoots | None = None) -> AlphaRunSummary:
    return _run_trigger_build(runner_name="run_alpha_bof_build", trigger_type=TriggerType.BOF, settings=settings)


def run_alpha_tst_build(*, settings: WorkspaceRoots | None = None) -> AlphaRunSummary:
    return _run_trigger_build(runner_name="run_alpha_tst_build", trigger_type=TriggerType.TST, settings=settings)


def run_alpha_pb_build(*, settings: WorkspaceRoots | None = None) -> AlphaRunSummary:
    return _run_trigger_build(runner_name="run_alpha_pb_build", trigger_type=TriggerType.PB, settings=settings)


def run_alpha_cpb_build(*, settings: WorkspaceRoots | None = None) -> AlphaRunSummary:
    return _run_trigger_build(runner_name="run_alpha_cpb_build", trigger_type=TriggerType.CPB, settings=settings)


def run_alpha_bpb_build(*, settings: WorkspaceRoots | None = None) -> AlphaRunSummary:
    return _run_trigger_build(runner_name="run_alpha_bpb_build", trigger_type=TriggerType.BPB, settings=settings)


def run_alpha_signal_build(*, settings: WorkspaceRoots | None = None) -> AlphaRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.alpha_signal
    initialize_alpha_signal_schema(target_path)

    run_id = f"alpha-signal-{uuid4().hex[:12]}"
    message = "alpha_signal run completed."
    counts = {"signal_rows": 0}
    sources_updated = 0
    latest_signal_date: date | None = None
    source_events = _load_events_from_trigger_databases(workspace)

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO alpha_signal_run (run_id, status, source_trigger_count, message)
            VALUES (?, 'running', ?, 'alpha_signal run started.')
            """,
            [run_id, len(source_events)],
        )
        connection.execute("DELETE FROM alpha_signal_work_queue")
        for source_trigger_db, events in source_events.items():
            last_source_date = max((event.signal_date for event in events), default=None)
            if latest_signal_date is None or (last_source_date is not None and last_source_date > latest_signal_date):
                latest_signal_date = last_source_date
            queue_id = f"{run_id}:{source_trigger_db}"
            connection.execute(
                """
                INSERT INTO alpha_signal_work_queue (
                    queue_id, source_trigger_db, status, source_row_count, claimed_at, last_signal_date
                ) VALUES (?, ?, 'running', ?, CURRENT_TIMESTAMP, ?)
                """,
                [queue_id, source_trigger_db, len(events), last_source_date],
            )
            checkpoint_date = _load_signal_checkpoint_date(connection=connection, source_trigger_db=source_trigger_db)
            if checkpoint_date is not None and last_source_date is not None and checkpoint_date >= last_source_date:
                connection.execute(
                    """
                    UPDATE alpha_signal_work_queue
                    SET status = 'skipped', finished_at = CURRENT_TIMESTAMP
                    WHERE queue_id = ?
                    """,
                    [queue_id],
                )
                continue
            connection.execute("DELETE FROM alpha_signal WHERE source_trigger_db = ?", [source_trigger_db])
            signal_rows = build_alpha_signal_rows(trigger_events={source_trigger_db: events})
            _insert_signal_rows(connection=connection, run_id=run_id, rows=signal_rows)
            _upsert_signal_checkpoint(
                connection=connection,
                source_trigger_db=source_trigger_db,
                run_id=run_id,
                last_signal_date=last_source_date,
            )
            connection.execute(
                """
                UPDATE alpha_signal_work_queue
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE queue_id = ?
                """,
                [queue_id],
            )
            counts["signal_rows"] += len(signal_rows)
            sources_updated += 1

        if not source_events:
            message = "alpha_signal schema initialized without trigger events."

        connection.execute(
            """
            UPDATE alpha_signal_run
            SET
                status = ?,
                sources_updated = ?,
                inserted_signals = ?,
                latest_signal_date = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                AlphaRunStatus.COMPLETED.value,
                sources_updated,
                counts["signal_rows"],
                latest_signal_date,
                message,
                run_id,
            ],
        )

    return AlphaRunSummary(
        runner_name="run_alpha_signal_build",
        scope=TriggerType.SIGNAL.value,
        run_id=run_id,
        status=AlphaRunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={
            trigger_db: str(getattr(workspace.databases, trigger_db))
            for trigger_db in source_events.keys()
        },
        message=message,
        materialization_counts=counts,
        checkpoint_summary=AlphaCheckpointSummary(
            work_units_seen=len(source_events),
            work_units_updated=sources_updated,
            latest_signal_date=latest_signal_date.isoformat() if latest_signal_date is not None else None,
        ),
    )


def _run_trigger_build(
    *,
    runner_name: str,
    trigger_type: TriggerType,
    settings: WorkspaceRoots | None,
) -> AlphaRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = getattr(workspace.databases, TRIGGER_TARGET_PATHS[trigger_type])
    initialize_alpha_trigger_schema(target_path)

    run_id = f"{trigger_type.value}-{uuid4().hex[:12]}"
    source = load_alpha_source_rows(workspace)
    message = f"{trigger_type.value} run completed."
    counts = {"event_rows": 0, "profile_rows": 0}
    symbols_updated = 0
    latest_signal_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO alpha_run (
                run_id, trigger_type, status, market_source_path, malf_source_path, input_rows, symbols_seen, message
            ) VALUES (?, ?, 'running', ?, ?, ?, ?, 'alpha run started.')
            """,
            [
                run_id,
                trigger_type.value,
                str(source.market_source_path) if source.market_source_path is not None else None,
                str(source.malf_source_path) if source.malf_source_path is not None else None,
                source.row_count,
                len(source.rows_by_symbol),
            ],
        )
        connection.execute("DELETE FROM alpha_work_queue WHERE trigger_type = ?", [trigger_type.value])
        for symbol, rows in source.rows_by_symbol.items():
            last_signal_date = rows[-1].signal_date
            if latest_signal_date is None or last_signal_date > latest_signal_date:
                latest_signal_date = last_signal_date
            queue_id = f"{run_id}:{symbol}"
            connection.execute(
                """
                INSERT INTO alpha_work_queue (
                    queue_id, symbol, trigger_type, status, source_row_count, claimed_at, last_signal_date
                ) VALUES (?, ?, ?, 'running', ?, CURRENT_TIMESTAMP, ?)
                """,
                [queue_id, symbol, trigger_type.value, len(rows), last_signal_date],
            )
            checkpoint_date = _load_trigger_checkpoint_date(
                connection=connection,
                trigger_type=trigger_type,
                symbol=symbol,
            )
            if checkpoint_date is not None and checkpoint_date >= last_signal_date:
                connection.execute(
                    """
                    UPDATE alpha_work_queue
                    SET status = 'skipped', finished_at = CURRENT_TIMESTAMP
                    WHERE queue_id = ?
                    """,
                    [queue_id],
                )
                continue

            result = evaluate_trigger_rows(trigger_type=trigger_type, rows=rows)
            _replace_trigger_symbol_rows(connection=connection, trigger_type=trigger_type, symbol=symbol)
            _insert_trigger_result_rows(connection=connection, run_id=run_id, result=result)
            _upsert_trigger_checkpoint(
                connection=connection,
                trigger_type=trigger_type,
                symbol=symbol,
                run_id=run_id,
                last_signal_date=last_signal_date,
            )
            connection.execute(
                """
                UPDATE alpha_work_queue
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE queue_id = ?
                """,
                [queue_id],
            )
            counts["event_rows"] += len(result.events)
            counts["profile_rows"] += len(result.profiles)
            symbols_updated += 1

        if not source.rows_by_symbol:
            message = f"{trigger_type.value} schema initialized without source rows."

        connection.execute(
            """
            UPDATE alpha_run
            SET
                status = ?,
                symbols_updated = ?,
                inserted_trigger_events = ?,
                inserted_trigger_profiles = ?,
                latest_signal_date = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                AlphaRunStatus.COMPLETED.value,
                symbols_updated,
                counts["event_rows"],
                counts["profile_rows"],
                latest_signal_date,
                message,
                run_id,
            ],
        )

    return AlphaRunSummary(
        runner_name=runner_name,
        scope=trigger_type.value,
        run_id=run_id,
        status=AlphaRunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={
            "market_base_day": str(source.market_source_path) if source.market_source_path is not None else None,
            "malf_day_snapshot": str(source.malf_source_path) if source.malf_source_path is not None else None,
        },
        message=message,
        materialization_counts=counts,
        checkpoint_summary=AlphaCheckpointSummary(
            work_units_seen=len(source.rows_by_symbol),
            work_units_updated=symbols_updated,
            latest_signal_date=latest_signal_date.isoformat() if latest_signal_date is not None else None,
        ),
    )


def _replace_trigger_symbol_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    trigger_type: TriggerType,
    symbol: str,
) -> None:
    connection.execute(
        "DELETE FROM alpha_trigger_event WHERE trigger_type = ? AND symbol = ?",
        [trigger_type.value, symbol],
    )
    connection.execute(
        "DELETE FROM alpha_trigger_profile WHERE trigger_type = ? AND symbol = ?",
        [trigger_type.value, symbol],
    )


def _insert_trigger_result_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    result: TriggerEvaluationResult,
) -> None:
    if result.events:
        connection.executemany(
            """
            INSERT INTO alpha_trigger_event (
                event_nk, run_id, symbol, signal_date, trigger_type, formal_signal_status, source_bar_dt,
                wave_id, direction, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.event_nk,
                    run_id,
                    row.symbol,
                    row.signal_date,
                    row.trigger_type,
                    row.formal_signal_status,
                    row.source_bar_dt,
                    row.wave_id,
                    row.direction,
                    row.new_count,
                    row.no_new_span,
                    row.life_state,
                    row.update_rank,
                    row.stagnation_rank,
                    row.wave_position_zone,
                )
                for row in result.events
            ],
        )
    if result.profiles:
        connection.executemany(
            """
            INSERT INTO alpha_trigger_profile (
                profile_nk, run_id, symbol, trigger_type, formal_signal_status, event_count,
                latest_signal_date, avg_update_rank, avg_stagnation_rank
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.profile_nk,
                    run_id,
                    row.symbol,
                    row.trigger_type,
                    row.formal_signal_status,
                    row.event_count,
                    row.latest_signal_date,
                    row.avg_update_rank,
                    row.avg_stagnation_rank,
                )
                for row in result.profiles
            ],
        )


def _load_trigger_checkpoint_date(
    *,
    connection: duckdb.DuckDBPyConnection,
    trigger_type: TriggerType,
    symbol: str,
) -> date | None:
    row = connection.execute(
        "SELECT last_signal_date FROM alpha_checkpoint WHERE trigger_type = ? AND symbol = ?",
        [trigger_type.value, symbol],
    ).fetchone()
    return row[0] if row else None


def _upsert_trigger_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    trigger_type: TriggerType,
    symbol: str,
    run_id: str,
    last_signal_date: date,
) -> None:
    updated_at = datetime.utcnow()
    connection.execute(
        """
        INSERT INTO alpha_checkpoint (symbol, trigger_type, last_signal_date, last_run_id, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(symbol, trigger_type) DO UPDATE
        SET
            last_signal_date = excluded.last_signal_date,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [symbol, trigger_type.value, last_signal_date, run_id, updated_at],
    )


def _load_events_from_trigger_databases(workspace: WorkspaceRoots) -> dict[str, list[TriggerEventRow]]:
    source_events: dict[str, list[TriggerEventRow]] = {}
    for trigger_type, attribute_name in TRIGGER_TARGET_PATHS.items():
        database_path: Path = getattr(workspace.databases, attribute_name)
        if not database_path.exists():
            continue
        with duckdb.connect(str(database_path), read_only=True) as connection:
            available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            if "alpha_trigger_event" not in available_tables:
                continue
            rows = connection.execute(
                """
                SELECT
                    event_nk, symbol, signal_date, trigger_type, formal_signal_status, source_bar_dt,
                    wave_id, direction, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone
                FROM alpha_trigger_event
                ORDER BY symbol, signal_date
                """
            ).fetchall()
            source_events[attribute_name] = [
                TriggerEventRow(
                    event_nk=str(event_nk),
                    symbol=str(symbol),
                    signal_date=signal_date,
                    trigger_type=str(trigger_name),
                    formal_signal_status=str(formal_signal_status),
                    source_bar_dt=source_bar_dt,
                    wave_id=str(wave_id),
                    direction=str(direction),
                    new_count=int(new_count),
                    no_new_span=int(no_new_span),
                    life_state=str(life_state),
                    update_rank=float(update_rank),
                    stagnation_rank=float(stagnation_rank),
                    wave_position_zone=str(wave_position_zone),
                )
                for event_nk, symbol, signal_date, trigger_name, formal_signal_status, source_bar_dt, wave_id, direction, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone in rows
            ]
    return source_events


def _load_signal_checkpoint_date(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_trigger_db: str,
) -> date | None:
    row = connection.execute(
        "SELECT last_signal_date FROM alpha_signal_checkpoint WHERE source_trigger_db = ?",
        [source_trigger_db],
    ).fetchone()
    return row[0] if row else None


def _upsert_signal_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_trigger_db: str,
    run_id: str,
    last_signal_date: date | None,
) -> None:
    updated_at = datetime.utcnow()
    connection.execute(
        """
        INSERT INTO alpha_signal_checkpoint (source_trigger_db, last_signal_date, last_run_id, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(source_trigger_db) DO UPDATE
        SET
            last_signal_date = excluded.last_signal_date,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [source_trigger_db, last_signal_date, run_id, updated_at],
    )


def _insert_signal_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    rows: list[AlphaSignalRow],
) -> None:
    if not rows:
        return
    connection.executemany(
        """
        INSERT INTO alpha_signal (
            signal_nk, run_id, symbol, signal_date, trigger_type, formal_signal_status, source_trigger_db,
            source_trigger_event_nk, wave_id, direction, new_count, no_new_span, life_state,
            update_rank, stagnation_rank, wave_position_zone
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.signal_nk,
                run_id,
                row.symbol,
                row.signal_date,
                row.trigger_type,
                row.formal_signal_status,
                row.source_trigger_db,
                row.source_trigger_event_nk,
                row.wave_id,
                row.direction,
                row.new_count,
                row.no_new_span,
                row.life_state,
                row.update_rank,
                row.stagnation_rank,
                row.wave_position_zone,
            )
            for row in rows
        ],
    )
