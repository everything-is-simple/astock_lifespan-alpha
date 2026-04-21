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
from astock_lifespan_alpha.alpha.schema import (
    SIGNAL_TABLES,
    TRIGGER_TABLES,
    initialize_alpha_signal_schema,
    initialize_alpha_trigger_schema,
)
from astock_lifespan_alpha.alpha.source import ALPHA_SOURCE_VIEW_NAME, attach_alpha_source_view
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
    source_trigger_paths = _discover_trigger_event_sources(workspace)

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO alpha_signal_run (run_id, status, source_trigger_count, message)
            VALUES (?, 'running', ?, 'alpha_signal run started.')
            """,
            [run_id, len(source_trigger_paths)],
        )
        connection.execute("DELETE FROM alpha_signal_work_queue")
        for source_trigger_db, database_path in source_trigger_paths.items():
            alias = f"source_{source_trigger_db}"
            connection.execute(f"ATTACH {_duckdb_string_literal(database_path)} AS {alias} (READ_ONLY)")
            source_row_count, last_source_date = connection.execute(
                f"SELECT COUNT(*), MAX(signal_date) FROM {alias}.alpha_trigger_event"
            ).fetchone()
            if latest_signal_date is None or (last_source_date is not None and last_source_date > latest_signal_date):
                latest_signal_date = last_source_date
            queue_id = f"{run_id}:{source_trigger_db}"
            connection.execute(
                """
                INSERT INTO alpha_signal_work_queue (
                    queue_id, source_trigger_db, status, source_row_count, claimed_at, last_signal_date
                ) VALUES (?, ?, 'running', ?, CURRENT_TIMESTAMP, ?)
                """,
                [queue_id, source_trigger_db, source_row_count, last_source_date],
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
            connection.execute(
                f"""
                INSERT INTO alpha_signal (
                    signal_nk, run_id, symbol, signal_date, trigger_type, formal_signal_status, source_trigger_db,
                    source_trigger_event_nk, wave_id, direction, new_count, no_new_span, life_state,
                    update_rank, stagnation_rank, wave_position_zone
                )
                SELECT
                    CONCAT(trigger_type, ':', event_nk),
                    ?,
                    symbol,
                    signal_date,
                    trigger_type,
                    formal_signal_status,
                    ?,
                    event_nk,
                    wave_id,
                    direction,
                    new_count,
                    no_new_span,
                    life_state,
                    update_rank,
                    stagnation_rank,
                    wave_position_zone
                FROM {alias}.alpha_trigger_event
                """,
                [run_id, source_trigger_db],
            )
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
            counts["signal_rows"] += int(source_row_count)
            sources_updated += 1

        if not source_trigger_paths:
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
            trigger_db: str(database_path)
            for trigger_db, database_path in source_trigger_paths.items()
        },
        message=message,
        materialization_counts=counts,
        checkpoint_summary=AlphaCheckpointSummary(
            work_units_seen=len(source_trigger_paths),
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
    message = f"{trigger_type.value} run completed."
    counts = {"event_rows": 0, "profile_rows": 0}
    symbols_updated = 0
    latest_signal_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        source = attach_alpha_source_view(connection, workspace)
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
                source.symbol_count,
            ],
        )
        connection.execute("DELETE FROM alpha_work_queue WHERE trigger_type = ?", [trigger_type.value])
        if source.symbol_count == 0:
            message = f"{trigger_type.value} schema initialized without source rows."
        else:
            counts, symbols_updated, latest_signal_date = _materialize_trigger_build_sql(
                connection=connection,
                run_id=run_id,
                trigger_type=trigger_type,
            )

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
            work_units_seen=source.symbol_count,
            work_units_updated=symbols_updated,
            latest_signal_date=latest_signal_date.isoformat() if latest_signal_date is not None else None,
        ),
    )


def _materialize_trigger_build_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    trigger_type: TriggerType,
) -> tuple[dict[str, int], int, date | None]:
    trigger_value = trigger_type.value
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE alpha_source_symbol_summary AS
        SELECT
            symbol,
            COUNT(*) AS source_row_count,
            MAX(signal_date) AS last_signal_date
        FROM {ALPHA_SOURCE_VIEW_NAME}
        GROUP BY symbol
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE alpha_symbols_to_update AS
        SELECT summary.symbol, summary.source_row_count, summary.last_signal_date
        FROM alpha_source_symbol_summary summary
        LEFT JOIN alpha_checkpoint checkpoint
            ON checkpoint.symbol = summary.symbol
            AND checkpoint.trigger_type = ?
        WHERE checkpoint.last_signal_date IS NULL
            OR checkpoint.last_signal_date < summary.last_signal_date
        """,
        [trigger_value],
    )
    latest_signal_date = connection.execute(
        "SELECT MAX(last_signal_date) FROM alpha_source_symbol_summary"
    ).fetchone()[0]
    connection.execute(
        """
        INSERT INTO alpha_work_queue (
            queue_id, symbol, trigger_type, status, source_row_count, claimed_at, finished_at, last_signal_date
        )
        SELECT
            CONCAT(?, ':', summary.symbol),
            summary.symbol,
            ?,
            CASE WHEN updates.symbol IS NULL THEN 'skipped' ELSE 'completed' END,
            summary.source_row_count,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            summary.last_signal_date
        FROM alpha_source_symbol_summary summary
        LEFT JOIN alpha_symbols_to_update updates
            ON updates.symbol = summary.symbol
        """,
        [run_id, trigger_value],
    )
    connection.execute(
        """
        DELETE FROM alpha_trigger_event
        WHERE trigger_type = ?
            AND symbol IN (SELECT symbol FROM alpha_symbols_to_update)
        """,
        [trigger_value],
    )
    connection.execute(
        """
        DELETE FROM alpha_trigger_profile
        WHERE trigger_type = ?
            AND symbol IN (SELECT symbol FROM alpha_symbols_to_update)
        """,
        [trigger_value],
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE alpha_trigger_candidate_rows AS
        SELECT
            source.*,
            LAG(source.high) OVER (PARTITION BY source.symbol ORDER BY source.signal_date) AS previous_high,
            LAG(source.low) OVER (PARTITION BY source.symbol ORDER BY source.signal_date) AS previous_low,
            LAG(source.open) OVER (PARTITION BY source.symbol ORDER BY source.signal_date) AS previous_open,
            LAG(source.close) OVER (PARTITION BY source.symbol ORDER BY source.signal_date) AS previous_close
        FROM {ALPHA_SOURCE_VIEW_NAME} source
        INNER JOIN alpha_symbols_to_update updates
            ON updates.symbol = source.symbol
        """
    )
    status_sql = _trigger_status_sql(trigger_type)
    where_sql = _trigger_where_sql(trigger_type)
    connection.execute(
        f"""
        INSERT INTO alpha_trigger_event (
            event_nk, run_id, symbol, signal_date, trigger_type, formal_signal_status, source_bar_dt,
            wave_id, direction, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone
        )
        SELECT
            CONCAT(symbol, ':', ?, ':', CAST(signal_date AS VARCHAR), ':', wave_id),
            ?,
            symbol,
            signal_date,
            ?,
            {status_sql},
            signal_date,
            wave_id,
            direction,
            new_count,
            no_new_span,
            life_state,
            update_rank,
            stagnation_rank,
            wave_position_zone
        FROM alpha_trigger_candidate_rows
        WHERE {where_sql}
        """,
        [trigger_value, run_id, trigger_value],
    )
    connection.execute(
        """
        INSERT INTO alpha_trigger_profile (
            profile_nk, run_id, symbol, trigger_type, formal_signal_status, event_count,
            latest_signal_date, avg_update_rank, avg_stagnation_rank
        )
        SELECT
            CONCAT(symbol, ':', ?, ':', formal_signal_status),
            ?,
            symbol,
            ?,
            formal_signal_status,
            COUNT(*),
            MAX(signal_date),
            ROUND(AVG(update_rank), 2),
            ROUND(AVG(stagnation_rank), 2)
        FROM alpha_trigger_event
        WHERE run_id = ?
        GROUP BY symbol, formal_signal_status
        """,
        [trigger_value, run_id, trigger_value, run_id],
    )
    connection.execute(
        """
        INSERT INTO alpha_checkpoint (symbol, trigger_type, last_signal_date, last_run_id, updated_at)
        SELECT symbol, ?, last_signal_date, ?, CURRENT_TIMESTAMP
        FROM alpha_symbols_to_update
        ON CONFLICT(symbol, trigger_type) DO UPDATE
        SET
            last_signal_date = excluded.last_signal_date,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [trigger_value, run_id],
    )
    event_rows = connection.execute(
        "SELECT COUNT(*) FROM alpha_trigger_event WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    profile_rows = connection.execute(
        "SELECT COUNT(*) FROM alpha_trigger_profile WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    symbols_updated = connection.execute("SELECT COUNT(*) FROM alpha_symbols_to_update").fetchone()[0]
    return {"event_rows": int(event_rows), "profile_rows": int(profile_rows)}, int(symbols_updated), latest_signal_date


def _trigger_status_sql(trigger_type: TriggerType) -> str:
    if trigger_type is TriggerType.BOF:
        return "CASE WHEN close >= previous_high THEN 'confirmed' ELSE 'candidate' END"
    if trigger_type is TriggerType.TST:
        return "CASE WHEN close >= previous_high AND close >= open THEN 'confirmed' ELSE 'candidate' END"
    if trigger_type is TriggerType.PB:
        return "CASE WHEN low >= previous_low THEN 'confirmed' ELSE 'candidate' END"
    if trigger_type is TriggerType.CPB:
        return "CASE WHEN close > previous_close THEN 'confirmed' ELSE 'candidate' END"
    if trigger_type is TriggerType.BPB:
        return "CASE WHEN low < previous_low THEN 'confirmed' ELSE 'candidate' END"
    raise ValueError(f"Unsupported trigger type: {trigger_type}")


def _trigger_where_sql(trigger_type: TriggerType) -> str:
    if trigger_type is TriggerType.BOF:
        return """
            direction = 'up'
            AND life_state IN ('alive', 'reborn')
            AND wave_position_zone IN ('early_progress', 'mature_progress')
            AND high > previous_high
        """
    if trigger_type is TriggerType.TST:
        return """
            direction = 'up'
            AND no_new_span >= 1
            AND wave_position_zone != 'weak_stagnation'
            AND low <= previous_high
        """
    if trigger_type is TriggerType.PB:
        return """
            direction = 'up'
            AND life_state = 'alive'
            AND no_new_span >= 1
            AND wave_position_zone IN ('mature_progress', 'mature_stagnation')
            AND close < previous_close
        """
    if trigger_type is TriggerType.CPB:
        return """
            direction = 'up'
            AND life_state = 'alive'
            AND no_new_span >= 1
            AND wave_position_zone IN ('mature_progress', 'mature_stagnation')
            AND previous_close < previous_open
            AND close >= open
        """
    if trigger_type is TriggerType.BPB:
        return """
            direction = 'down'
            AND life_state IN ('alive', 'reborn')
            AND no_new_span >= 1
            AND wave_position_zone IN ('mature_progress', 'mature_stagnation', 'weak_stagnation')
            AND close < previous_close
        """
    raise ValueError(f"Unsupported trigger type: {trigger_type}")


def _discover_trigger_event_sources(workspace: WorkspaceRoots) -> dict[str, Path]:
    source_paths: dict[str, Path] = {}
    for _trigger_type, attribute_name in TRIGGER_TARGET_PATHS.items():
        database_path: Path = getattr(workspace.databases, attribute_name)
        if not database_path.exists():
            continue
        with duckdb.connect(str(database_path), read_only=True) as connection:
            available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            if "alpha_trigger_event" not in available_tables:
                continue
        source_paths[attribute_name] = database_path
    return source_paths


def _duckdb_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


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
