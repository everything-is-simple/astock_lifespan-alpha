"""Stage-four position runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.position.contracts import (
    PositionCheckpointSummary,
    PositionRunStatus,
    PositionRunSummary,
)
from astock_lifespan_alpha.position.engine import STAGE_FOUR_POLICY_ID
from astock_lifespan_alpha.position.schema import initialize_position_schema


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")
POSITION_SOURCE_VIEW_NAME = "position_source_input"


@dataclass(frozen=True)
class _PositionSourceMetadata:
    alpha_source_path: Path | None
    market_source_path: Path | None
    row_count: int
    symbol_count: int


def run_position_from_alpha_signal(*, settings: WorkspaceRoots | None = None) -> PositionRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.position
    initialize_position_schema(target_path)

    run_id = f"position-{uuid4().hex[:12]}"
    message = "position run completed."
    counts = {
        "candidate_rows": 0,
        "capacity_rows": 0,
        "sizing_rows": 0,
        "exit_plan_rows": 0,
        "exit_leg_rows": 0,
    }
    symbols_updated = 0
    latest_signal_date: date | None = None
    full_refresh_required = False

    with duckdb.connect(str(target_path)) as connection:
        source = _attach_position_source_view(connection=connection, workspace=workspace)
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
                source.symbol_count,
            ],
        )
        connection.execute("DELETE FROM position_work_queue")
        if source.symbol_count == 0:
            message = "position schema initialized without source rows."
        else:
            full_refresh_required = _position_contract_drift_requires_full_refresh(connection=connection)
            counts, symbols_updated, latest_signal_date = _materialize_position_sql(
                connection=connection,
                run_id=run_id,
                full_refresh_required=full_refresh_required,
            )
            if full_refresh_required:
                message = "position run completed with contract-drift full refresh."

        connection.execute(
            """
            UPDATE position_run
            SET
                status = ?,
                symbols_updated = ?,
                inserted_candidates = ?,
                inserted_capacity_rows = ?,
                inserted_sizing_rows = ?,
                inserted_exit_plan_rows = ?,
                inserted_exit_leg_rows = ?,
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
                counts["exit_plan_rows"],
                counts["exit_leg_rows"],
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
            work_units_seen=source.symbol_count,
            work_units_updated=symbols_updated,
            latest_signal_date=latest_signal_date.isoformat() if latest_signal_date is not None else None,
        ),
    )


def _attach_position_source_view(
    *,
    connection: duckdb.DuckDBPyConnection,
    workspace: WorkspaceRoots,
) -> _PositionSourceMetadata:
    alpha_source_path = workspace.databases.alpha_signal if workspace.databases.alpha_signal.exists() else None
    market_source_path = workspace.source_databases.market_base if workspace.source_databases.market_base.exists() else None
    if alpha_source_path is None or market_source_path is None:
        return _PositionSourceMetadata(
            alpha_source_path=alpha_source_path,
            market_source_path=market_source_path,
            row_count=0,
            symbol_count=0,
        )

    connection.execute(f"ATTACH {_duckdb_string_literal(alpha_source_path)} AS position_alpha_source (READ_ONLY)")
    connection.execute(f"ATTACH {_duckdb_string_literal(market_source_path)} AS position_market_source (READ_ONLY)")
    if not _attached_table_exists(connection=connection, catalog="position_alpha_source", table_name="alpha_signal"):
        return _PositionSourceMetadata(
            alpha_source_path=alpha_source_path,
            market_source_path=market_source_path,
            row_count=0,
            symbol_count=0,
        )
    market_source = _resolve_market_source(connection=connection, catalog="position_market_source")
    if market_source is None:
        return _PositionSourceMetadata(
            alpha_source_path=alpha_source_path,
            market_source_path=market_source_path,
            row_count=0,
            symbol_count=0,
        )
    market_select_sql = _market_select_sql(market_source, catalog="position_market_source")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW position_market_reference AS
        {market_select_sql}
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {POSITION_SOURCE_VIEW_NAME} AS
        SELECT
            signal.signal_nk,
            signal.symbol,
            signal.signal_date,
            signal.trigger_type,
            signal.formal_signal_status,
            signal.source_trigger_event_nk,
            signal.wave_id,
            signal.direction,
            signal.new_count,
            signal.no_new_span,
            signal.life_state,
            signal.update_rank,
            signal.stagnation_rank,
            signal.wave_position_zone,
            reference.trade_date AS reference_trade_date,
            reference.close AS reference_price
        FROM position_alpha_source.alpha_signal signal
        ASOF LEFT JOIN position_market_reference reference
            ON reference.symbol = signal.symbol
            AND signal.signal_date <= reference.trade_date
        """
    )
    row_count, symbol_count = connection.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT symbol) FROM {POSITION_SOURCE_VIEW_NAME}"
    ).fetchone()
    return _PositionSourceMetadata(
        alpha_source_path=alpha_source_path,
        market_source_path=market_source_path,
        row_count=int(row_count),
        symbol_count=int(symbol_count),
    )


@dataclass(frozen=True)
class _MarketSource:
    table_name: str
    symbol_column: str
    date_column: str
    has_adjust_method: bool


def _resolve_market_source(*, connection: duckdb.DuckDBPyConnection, catalog: str) -> _MarketSource | None:
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
        return _MarketSource(
            table_name=table_name,
            symbol_column=_pick_required_column(column_names, ("symbol", "code")),
            date_column=_pick_required_column(column_names, ("bar_dt", "trade_date", "date")),
            has_adjust_method="adjust_method" in column_names,
        )
    return None


def _market_select_sql(source: _MarketSource, *, catalog: str) -> str:
    adjust_filter = "WHERE adjust_method = 'backward'" if source.has_adjust_method else ""
    return f"""
        SELECT
            {source.symbol_column} AS symbol,
            CAST({source.date_column} AS DATE) AS trade_date,
            CAST(close AS DOUBLE) AS close
        FROM {catalog}.{source.table_name}
        {adjust_filter}
    """


def _attached_table_exists(*, connection: duckdb.DuckDBPyConnection, catalog: str, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = ? AND table_name = ?
        """,
        [catalog, table_name],
    ).fetchone()
    return bool(row[0])


def _materialize_position_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    full_refresh_required: bool,
) -> tuple[dict[str, int], int, date | None]:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE position_source_symbol_summary AS
        SELECT
            symbol,
            COUNT(*) AS source_row_count,
            MAX(signal_date) AS last_signal_date
        FROM {POSITION_SOURCE_VIEW_NAME}
        GROUP BY symbol
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE position_symbols_to_update AS
        SELECT summary.symbol, summary.source_row_count, summary.last_signal_date
        FROM position_source_symbol_summary summary
        LEFT JOIN position_checkpoint checkpoint
            ON checkpoint.symbol = summary.symbol
        WHERE ? = TRUE
            OR checkpoint.last_signal_date IS NULL
            OR checkpoint.last_signal_date < summary.last_signal_date
        """
        ,
        [full_refresh_required],
    )
    latest_signal_date = connection.execute(
        "SELECT MAX(last_signal_date) FROM position_source_symbol_summary"
    ).fetchone()[0]
    connection.execute(
        """
        INSERT INTO position_work_queue (
            queue_id, symbol, status, source_row_count, claimed_at, finished_at, last_signal_date
        )
        SELECT
            CONCAT(?, ':', summary.symbol),
            summary.symbol,
            CASE WHEN updates.symbol IS NULL THEN 'skipped' ELSE 'completed' END,
            summary.source_row_count,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            summary.last_signal_date
        FROM position_source_symbol_summary summary
        LEFT JOIN position_symbols_to_update updates
            ON updates.symbol = summary.symbol
        """,
        [run_id],
    )
    for table_name in (
        "position_candidate_audit",
        "position_capacity_snapshot",
        "position_sizing_snapshot",
        "position_exit_plan",
        "position_exit_leg",
    ):
        connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE symbol IN (SELECT symbol FROM position_symbols_to_update)
            """
        )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE position_evaluation_rows AS
        SELECT
            *,
            entry_reference.trade_date AS planned_entry_trade_date,
            CASE
                WHEN candidate_status = 'admitted' THEN ROUND(LEAST(requested_weight, 0.15), 4)
                ELSE 0.0
            END AS final_allowed_position_weight
        FROM (
            SELECT
                *,
                CASE
                    WHEN reference_trade_date IS NULL OR reference_price IS NULL THEN 'blocked'
                    WHEN direction != 'up' THEN 'blocked'
                    WHEN formal_signal_status != 'confirmed' THEN 'blocked'
                    WHEN wave_position_zone = 'weak_stagnation' THEN 'blocked'
                    ELSE 'admitted'
                END AS candidate_status,
                CASE
                    WHEN reference_trade_date IS NULL OR reference_price IS NULL THEN 'missing_reference_price'
                    WHEN direction != 'up' THEN 'direction_not_long'
                    WHEN formal_signal_status != 'confirmed' THEN 'signal_not_confirmed'
                    WHEN wave_position_zone = 'weak_stagnation' THEN 'weak_wave_position'
                    ELSE NULL
                END AS blocked_reason_code,
                CASE
                    WHEN update_rank < stagnation_rank THEN ROUND(base_weight * 0.8, 4)
                    ELSE ROUND(base_weight, 4)
                END AS requested_weight
            FROM (
                SELECT
                    source.*,
                    CASE wave_position_zone
                        WHEN 'early_progress' THEN 0.12
                        WHEN 'mature_progress' THEN 0.10
                        WHEN 'mature_stagnation' THEN 0.06
                        WHEN 'weak_stagnation' THEN 0.0
                        ELSE 0.04
                    END AS base_weight
                FROM {POSITION_SOURCE_VIEW_NAME} source
                INNER JOIN position_symbols_to_update updates
                    ON updates.symbol = source.symbol
            )
        ) AS evaluated
        ASOF LEFT JOIN position_market_reference AS entry_reference
            ON entry_reference.symbol = evaluated.symbol
            AND evaluated.reference_trade_date < entry_reference.trade_date
        """
    )
    connection.execute(
        """
        INSERT INTO position_candidate_audit (
            candidate_nk, run_id, signal_nk, symbol, signal_date, trigger_type, formal_signal_status,
            candidate_status, blocked_reason_code, source_trigger_event_nk, wave_id, direction, new_count,
            no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone,
            reference_trade_date, reference_price
        )
        SELECT
            signal_nk,
            ?,
            signal_nk,
            symbol,
            signal_date,
            trigger_type,
            formal_signal_status,
            candidate_status,
            blocked_reason_code,
            source_trigger_event_nk,
            wave_id,
            direction,
            new_count,
            no_new_span,
            life_state,
            update_rank,
            stagnation_rank,
            wave_position_zone,
            reference_trade_date,
            reference_price
        FROM position_evaluation_rows
        """,
        [run_id],
    )
    connection.execute(
        """
        INSERT INTO position_capacity_snapshot (
            capacity_nk, run_id, candidate_nk, symbol, signal_date, policy_id, capacity_status,
            capacity_ceiling_weight, reference_trade_date, reference_price
        )
        SELECT
            CONCAT(signal_nk, ':capacity'),
            ?,
            signal_nk,
            symbol,
            signal_date,
            ?,
            CASE WHEN final_allowed_position_weight > 0 THEN 'enabled' ELSE 'blocked' END,
            requested_weight,
            reference_trade_date,
            reference_price
        FROM position_evaluation_rows
        """,
        [run_id, STAGE_FOUR_POLICY_ID],
    )
    connection.execute(
        """
        INSERT INTO position_sizing_snapshot (
            sizing_nk, run_id, candidate_nk, symbol, signal_date, policy_id, position_action_decision,
            requested_weight, final_allowed_position_weight, required_reduction_weight, candidate_status,
            reference_trade_date, reference_price, planned_entry_trade_date
        )
        SELECT
            CONCAT(signal_nk, ':sizing'),
            ?,
            signal_nk,
            symbol,
            signal_date,
            ?,
            CASE WHEN final_allowed_position_weight > 0 THEN 'open' ELSE 'blocked' END,
            requested_weight,
            final_allowed_position_weight,
            0.0,
            candidate_status,
            reference_trade_date,
            reference_price,
            planned_entry_trade_date
        FROM position_evaluation_rows
        """,
        [run_id, STAGE_FOUR_POLICY_ID],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE position_exit_trigger_candidates AS
        WITH exit_trigger_rows AS (
            SELECT
                symbol,
                signal_date,
                signal_nk AS exit_trigger_signal_nk,
                CASE
                    WHEN direction != 'up' THEN 'direction_not_long_exit'
                    WHEN formal_signal_status != 'confirmed' THEN 'signal_not_confirmed_exit'
                    WHEN wave_position_zone = 'weak_stagnation' THEN 'weak_wave_position_exit'
                    WHEN no_new_span >= 2 THEN 'time_stop_no_new_span_exit'
                    ELSE NULL
                END AS exit_reason_code
            FROM position_evaluation_rows
            WHERE
                direction != 'up'
                OR formal_signal_status != 'confirmed'
                OR wave_position_zone = 'weak_stagnation'
                OR no_new_span >= 2
        ),
        first_exit_trigger_per_day AS (
            SELECT
                symbol,
                signal_date AS exit_signal_date,
                exit_trigger_signal_nk,
                exit_reason_code,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, signal_date
                    ORDER BY exit_trigger_signal_nk
                ) AS signal_rank
            FROM exit_trigger_rows
        ),
        next_exit_dates AS (
            SELECT
                signal_nk AS candidate_nk,
                symbol,
                signal_date AS entry_signal_date,
                MIN(
                    CASE
                        WHEN direction != 'up'
                            OR formal_signal_status != 'confirmed'
                            OR wave_position_zone = 'weak_stagnation'
                            OR no_new_span >= 2
                        THEN signal_date
                        ELSE NULL
                    END
                ) OVER (
                    PARTITION BY symbol
                    ORDER BY signal_date
                    RANGE BETWEEN INTERVAL 1 DAY FOLLOWING AND UNBOUNDED FOLLOWING
                ) AS trigger_rank
            FROM position_evaluation_rows
        )
        SELECT
            next_exit_dates.candidate_nk,
            next_exit_dates.symbol,
            next_exit_dates.entry_signal_date,
            trigger.exit_trigger_signal_nk,
            trigger.exit_signal_date,
            trigger.exit_reason_code,
            exit_reference.trade_date AS planned_exit_trade_date,
            exit_reference.open_price AS planned_exit_reference_price
        FROM (
            SELECT *
            FROM next_exit_dates
            WHERE trigger_rank IS NOT NULL
        ) AS next_exit_dates
        INNER JOIN (
            SELECT
                symbol,
                exit_signal_date,
                exit_trigger_signal_nk,
                exit_reason_code
            FROM first_exit_trigger_per_day
            WHERE signal_rank = 1
        ) AS trigger
            ON trigger.symbol = next_exit_dates.symbol
            AND trigger.exit_signal_date = next_exit_dates.trigger_rank
        ASOF LEFT JOIN (
            SELECT symbol, trade_date, CAST(close AS DOUBLE) AS open_price
            FROM position_market_reference
        ) AS exit_reference
            ON exit_reference.symbol = next_exit_dates.symbol
            AND trigger.exit_signal_date < exit_reference.trade_date
        WHERE EXISTS (
            SELECT 1
            FROM position_evaluation_rows AS entry
            WHERE entry.signal_nk = next_exit_dates.candidate_nk
                AND entry.final_allowed_position_weight > 0
        )
        """
    )
    connection.execute(
        """
        INSERT INTO position_exit_plan (
            exit_plan_nk, run_id, candidate_nk, symbol, entry_signal_date, exit_trigger_signal_nk,
            exit_signal_date, exit_reason_code, exit_plan_status, planned_exit_trade_date,
            planned_exit_reference_price
        )
        SELECT
            CONCAT(candidate_nk, ':exit-plan'),
            ?,
            candidate_nk,
            symbol,
            entry_signal_date,
            exit_trigger_signal_nk,
            exit_signal_date,
            exit_reason_code,
            'planned',
            planned_exit_trade_date,
            planned_exit_reference_price
        FROM position_exit_trigger_candidates
        """,
        [run_id],
    )
    connection.execute(
        """
        INSERT INTO position_exit_leg (
            exit_leg_nk, run_id, exit_plan_nk, candidate_nk, symbol, planned_exit_trade_date,
            exit_action_decision, exit_plan_status, exit_reason_code, planned_exit_reference_price
        )
        SELECT
            CONCAT(candidate_nk, ':exit-leg'),
            ?,
            CONCAT(candidate_nk, ':exit-plan'),
            candidate_nk,
            symbol,
            planned_exit_trade_date,
            'full_exit',
            'planned',
            exit_reason_code,
            planned_exit_reference_price
        FROM position_exit_trigger_candidates
        """,
        [run_id],
    )
    connection.execute(
        """
        INSERT INTO position_checkpoint (symbol, last_signal_date, last_run_id, updated_at)
        SELECT symbol, last_signal_date, ?, CURRENT_TIMESTAMP
        FROM position_symbols_to_update
        ON CONFLICT(symbol) DO UPDATE
        SET
            last_signal_date = excluded.last_signal_date,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [run_id],
    )
    candidate_rows = connection.execute(
        "SELECT COUNT(*) FROM position_candidate_audit WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    capacity_rows = connection.execute(
        "SELECT COUNT(*) FROM position_capacity_snapshot WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    sizing_rows = connection.execute(
        "SELECT COUNT(*) FROM position_sizing_snapshot WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    exit_plan_rows = connection.execute(
        "SELECT COUNT(*) FROM position_exit_plan WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    exit_leg_rows = connection.execute(
        "SELECT COUNT(*) FROM position_exit_leg WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    symbols_updated = connection.execute("SELECT COUNT(*) FROM position_symbols_to_update").fetchone()[0]
    return (
        {
            "candidate_rows": int(candidate_rows),
            "capacity_rows": int(capacity_rows),
            "sizing_rows": int(sizing_rows),
            "exit_plan_rows": int(exit_plan_rows),
            "exit_leg_rows": int(exit_leg_rows),
        },
        int(symbols_updated),
        latest_signal_date,
    )


def _pick_required_column(column_names: set[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in column_names:
            return candidate
    raise ValueError(f"Could not resolve required source columns from candidates: {candidates}")


def _duckdb_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _position_contract_drift_requires_full_refresh(
    *,
    connection: duckdb.DuckDBPyConnection,
) -> bool:
    available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    required_tables = {
        "position_candidate_audit",
        "position_capacity_snapshot",
        "position_sizing_snapshot",
        "position_exit_plan",
        "position_exit_leg",
    }
    if not required_tables.issubset(available_tables):
        return True
    sizing_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info('position_sizing_snapshot')").fetchall()
    }
    if "planned_entry_trade_date" not in sizing_columns:
        return True
    candidate_rows = int(connection.execute("SELECT COUNT(*) FROM position_candidate_audit").fetchone()[0])
    if candidate_rows == 0:
        return False
    planned_entry_rows = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM position_sizing_snapshot
            WHERE planned_entry_trade_date IS NOT NULL
            """
        ).fetchone()[0]
    )
    exit_plan_rows = int(connection.execute("SELECT COUNT(*) FROM position_exit_plan").fetchone()[0])
    exit_leg_rows = int(connection.execute("SELECT COUNT(*) FROM position_exit_leg").fetchone()[0])
    return planned_entry_rows == 0 or exit_plan_rows == 0 or exit_leg_rows == 0
