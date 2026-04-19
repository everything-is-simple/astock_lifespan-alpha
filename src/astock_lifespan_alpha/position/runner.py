"""Stage-four position runner."""

from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.position.contracts import (
    PositionCheckpointSummary,
    PositionRunStatus,
    PositionRunSummary,
)
from astock_lifespan_alpha.position.engine import evaluate_position_rows
from astock_lifespan_alpha.position.schema import initialize_position_schema
from astock_lifespan_alpha.position.source import load_position_source_rows


def run_position_from_alpha_signal(*, settings: WorkspaceRoots | None = None) -> PositionRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.position
    initialize_position_schema(target_path)

    run_id = f"position-{uuid4().hex[:12]}"
    source = load_position_source_rows(workspace)
    message = "position run completed."
    counts = {"candidate_rows": 0, "capacity_rows": 0, "sizing_rows": 0}
    symbols_updated = 0
    latest_signal_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO position_run (
                run_id, status, alpha_source_path, market_source_path, input_rows, symbols_seen, message
            ) VALUES (?, 'running', ?, ?, ?, ?, 'position run started.')
            """,
            [
                run_id,
                str(source.alpha_source_path) if source.alpha_source_path is not None else None,
                str(source.market_source_path) if source.market_source_path is not None else None,
                source.row_count,
                len(source.rows_by_symbol),
            ],
        )
        connection.execute("DELETE FROM position_work_queue")
        for symbol, rows in source.rows_by_symbol.items():
            last_signal_date = rows[-1].signal_date
            if latest_signal_date is None or last_signal_date > latest_signal_date:
                latest_signal_date = last_signal_date
            queue_id = f"{run_id}:{symbol}"
            connection.execute(
                """
                INSERT INTO position_work_queue (
                    queue_id, symbol, status, source_row_count, claimed_at, last_signal_date
                ) VALUES (?, ?, 'running', ?, CURRENT_TIMESTAMP, ?)
                """,
                [queue_id, symbol, len(rows), last_signal_date],
            )
            checkpoint_date = _load_position_checkpoint_date(connection=connection, symbol=symbol)
            if checkpoint_date is not None and checkpoint_date >= last_signal_date:
                connection.execute(
                    """
                    UPDATE position_work_queue
                    SET status = 'skipped', finished_at = CURRENT_TIMESTAMP
                    WHERE queue_id = ?
                    """,
                    [queue_id],
                )
                continue

            result = evaluate_position_rows(rows)
            _replace_position_symbol_rows(connection=connection, symbol=symbol)
            _insert_position_rows(connection=connection, run_id=run_id, result=result)
            _upsert_position_checkpoint(
                connection=connection,
                symbol=symbol,
                run_id=run_id,
                last_signal_date=last_signal_date,
            )
            connection.execute(
                """
                UPDATE position_work_queue
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE queue_id = ?
                """,
                [queue_id],
            )
            counts["candidate_rows"] += len(result.candidates)
            counts["capacity_rows"] += len(result.capacities)
            counts["sizing_rows"] += len(result.sizings)
            symbols_updated += 1

        if not source.rows_by_symbol:
            message = "position schema initialized without source rows."

        connection.execute(
            """
            UPDATE position_run
            SET
                status = ?,
                symbols_updated = ?,
                inserted_candidates = ?,
                inserted_capacity_rows = ?,
                inserted_sizing_rows = ?,
                latest_signal_date = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                PositionRunStatus.COMPLETED.value,
                symbols_updated,
                counts["candidate_rows"],
                counts["capacity_rows"],
                counts["sizing_rows"],
                latest_signal_date,
                message,
                run_id,
            ],
        )

    return PositionRunSummary(
        runner_name="run_position_from_alpha_signal",
        run_id=run_id,
        status=PositionRunStatus.COMPLETED.value,
        target_path=str(workspace.databases.position),
        source_paths={
            "alpha_signal": str(source.alpha_source_path) if source.alpha_source_path is not None else None,
            "market_base_day": str(source.market_source_path) if source.market_source_path is not None else None,
        },
        message=message,
        materialization_counts=counts,
        checkpoint_summary=PositionCheckpointSummary(
            work_units_seen=len(source.rows_by_symbol),
            work_units_updated=symbols_updated,
            latest_signal_date=latest_signal_date.isoformat() if latest_signal_date is not None else None,
        ),
    )


def _replace_position_symbol_rows(*, connection: duckdb.DuckDBPyConnection, symbol: str) -> None:
    connection.execute("DELETE FROM position_candidate_audit WHERE symbol = ?", [symbol])
    connection.execute("DELETE FROM position_capacity_snapshot WHERE symbol = ?", [symbol])
    connection.execute("DELETE FROM position_sizing_snapshot WHERE symbol = ?", [symbol])


def _insert_position_rows(*, connection: duckdb.DuckDBPyConnection, run_id: str, result) -> None:
    if result.candidates:
        connection.executemany(
            """
            INSERT INTO position_candidate_audit (
                candidate_nk, run_id, signal_nk, symbol, signal_date, trigger_type, formal_signal_status,
                candidate_status, blocked_reason_code, source_trigger_event_nk, wave_id, direction, new_count,
                no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone,
                reference_trade_date, reference_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.candidate_nk,
                    run_id,
                    row.signal_nk,
                    row.symbol,
                    row.signal_date,
                    row.trigger_type,
                    row.formal_signal_status,
                    row.candidate_status,
                    row.blocked_reason_code,
                    row.source_trigger_event_nk,
                    row.wave_id,
                    row.direction,
                    row.new_count,
                    row.no_new_span,
                    row.life_state,
                    row.update_rank,
                    row.stagnation_rank,
                    row.wave_position_zone,
                    row.reference_trade_date,
                    row.reference_price,
                )
                for row in result.candidates
            ],
        )
    if result.capacities:
        connection.executemany(
            """
            INSERT INTO position_capacity_snapshot (
                capacity_nk, run_id, candidate_nk, symbol, signal_date, policy_id, capacity_status,
                capacity_ceiling_weight, reference_trade_date, reference_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.capacity_nk,
                    run_id,
                    row.candidate_nk,
                    row.symbol,
                    row.signal_date,
                    row.policy_id,
                    row.capacity_status,
                    row.capacity_ceiling_weight,
                    row.reference_trade_date,
                    row.reference_price,
                )
                for row in result.capacities
            ],
        )
    if result.sizings:
        connection.executemany(
            """
            INSERT INTO position_sizing_snapshot (
                sizing_nk, run_id, candidate_nk, symbol, signal_date, policy_id, position_action_decision,
                requested_weight, final_allowed_position_weight, required_reduction_weight, candidate_status,
                reference_trade_date, reference_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.sizing_nk,
                    run_id,
                    row.candidate_nk,
                    row.symbol,
                    row.signal_date,
                    row.policy_id,
                    row.position_action_decision,
                    row.requested_weight,
                    row.final_allowed_position_weight,
                    row.required_reduction_weight,
                    row.candidate_status,
                    row.reference_trade_date,
                    row.reference_price,
                )
                for row in result.sizings
            ],
        )


def _load_position_checkpoint_date(*, connection: duckdb.DuckDBPyConnection, symbol: str) -> date | None:
    row = connection.execute(
        "SELECT last_signal_date FROM position_checkpoint WHERE symbol = ?",
        [symbol],
    ).fetchone()
    return row[0] if row else None


def _upsert_position_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    symbol: str,
    run_id: str,
    last_signal_date: date,
) -> None:
    updated_at = datetime.utcnow()
    connection.execute(
        """
        INSERT INTO position_checkpoint (symbol, last_signal_date, last_run_id, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE
        SET
            last_signal_date = excluded.last_signal_date,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [symbol, last_signal_date, run_id, updated_at],
    )
