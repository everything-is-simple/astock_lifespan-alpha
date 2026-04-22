"""Stage-four portfolio plan runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from heapq import heappop, heappush
from pathlib import Path
import sys
from time import perf_counter
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.portfolio_plan.contracts import (
    PortfolioPlanCheckpointSummary,
    PortfolioPlanRunSummary,
    PortfolioPlanStatus,
)
from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema


PORTFOLIO_PLAN_CONTRACT_VERSION = "stage4_portfolio_plan_v2"
PORTFOLIO_PLAN_PROGRESS_UPDATE_INTERVAL = 100


@dataclass(frozen=True)
class _PortfolioPlanSourceMetadata:
    position_source_path: Path | None
    row_count: int
    source_available: bool


def run_portfolio_plan_build(
    *,
    portfolio_id: str = "core",
    portfolio_gross_cap_weight: float = 0.50,
    settings: WorkspaceRoots | None = None,
) -> PortfolioPlanRunSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.portfolio_plan
    initialize_portfolio_plan_schema(target_path)

    run_id = f"portfolio-plan-{uuid4().hex[:12]}"
    source_path = workspace.databases.position
    counts = {"snapshot_rows": 0, "admitted_count": 0, "blocked_count": 0, "trimmed_count": 0}
    message = "portfolio_plan run completed."
    work_units_updated = 0
    latest_reference_trade_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        source = _attach_position_source_view(connection=connection, source_path=source_path)
        connection.execute(
            """
            INSERT INTO portfolio_plan_run (
                run_id, status, portfolio_id, source_position_path, bounded_candidate_count,
                portfolio_gross_cap_weight, message
            ) VALUES (?, 'running', ?, ?, ?, ?, 'portfolio_plan run started.')
            """,
            [run_id, portfolio_id, str(source_path), source.row_count, portfolio_gross_cap_weight],
        )

        try:
            connection.execute("DELETE FROM portfolio_plan_work_queue")
            if source.row_count == 0:
                message = "portfolio_plan schema initialized without position rows."
            else:
                _create_portfolio_plan_source_work_unit_summary(
                    connection=connection,
                    portfolio_id=portfolio_id,
                    portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                )
                if _portfolio_plan_checkpoint_fast_path_available(connection=connection, portfolio_id=portfolio_id):
                    counts, work_units_updated, latest_reference_trade_date = _record_reused_portfolio_plan_sql(
                        connection=connection,
                        run_id=run_id,
                        portfolio_id=portfolio_id,
                    )
                else:
                    counts, work_units_updated, latest_reference_trade_date, message = _materialize_portfolio_plan_sql(
                        connection=connection,
                        run_id=run_id,
                        portfolio_id=portfolio_id,
                        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                    )
            connection.execute(
                """
                UPDATE portfolio_plan_run
                SET
                    status = ?,
                    admitted_count = ?,
                    blocked_count = ?,
                    trimmed_count = ?,
                    message = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
                """,
                [
                    PortfolioPlanStatus.COMPLETED.value,
                    counts["admitted_count"],
                    counts["blocked_count"],
                    counts["trimmed_count"],
                    message,
                    run_id,
                ],
            )
        except Exception as exc:
            connection.execute(
                """
                UPDATE portfolio_plan_run
                SET status = 'interrupted', message = ?, finished_at = CURRENT_TIMESTAMP
                WHERE run_id = ? AND finished_at IS NULL
                """,
                [f"portfolio_plan run interrupted: {exc}", run_id],
            )
            raise

    return PortfolioPlanRunSummary(
        runner_name="run_portfolio_plan_build",
        run_id=run_id,
        status=PortfolioPlanStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={"position": str(source_path) if source_path.exists() else None},
        message=message,
        materialization_counts=counts,
        checkpoint_summary=PortfolioPlanCheckpointSummary(
            work_units_seen=1 if source.row_count else 0,
            work_units_updated=work_units_updated,
            latest_reference_trade_date=latest_reference_trade_date.isoformat()
            if latest_reference_trade_date is not None
            else None,
        ),
    )


def _attach_position_source_view(*, connection: duckdb.DuckDBPyConnection, source_path: Path) -> _PortfolioPlanSourceMetadata:
    if not source_path.exists():
        return _PortfolioPlanSourceMetadata(position_source_path=None, row_count=0, source_available=False)
    connection.execute(f"ATTACH {_duckdb_string_literal(source_path)} AS portfolio_position_source (READ_ONLY)")
    available_tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'portfolio_position_source'"
        ).fetchall()
    }
    required_tables = {
        "position_candidate_audit",
        "position_capacity_snapshot",
        "position_sizing_snapshot",
        "position_exit_plan",
    }
    if not required_tables.issubset(available_tables):
        return _PortfolioPlanSourceMetadata(position_source_path=source_path, row_count=0, source_available=False)
    connection.execute(
        """
        CREATE OR REPLACE TEMP VIEW portfolio_position_source_rows AS
        SELECT
            audit.candidate_nk,
            audit.symbol,
            audit.reference_trade_date,
            audit.signal_date,
            audit.candidate_status,
            audit.blocked_reason_code,
            sizing.position_action_decision,
            sizing.final_allowed_position_weight,
            sizing.planned_entry_trade_date,
            exit_plan.planned_exit_trade_date AS scheduled_exit_trade_date,
            exit_plan.exit_reason_code AS planned_exit_reason_code
        FROM portfolio_position_source.position_candidate_audit AS audit
        INNER JOIN portfolio_position_source.position_capacity_snapshot AS capacity
            ON capacity.candidate_nk = audit.candidate_nk
        INNER JOIN portfolio_position_source.position_sizing_snapshot AS sizing
            ON sizing.candidate_nk = audit.candidate_nk
        LEFT JOIN portfolio_position_source.position_exit_plan AS exit_plan
            ON exit_plan.candidate_nk = audit.candidate_nk
        """
    )
    return _PortfolioPlanSourceMetadata(
        position_source_path=source_path,
        row_count=int(connection.execute("SELECT COUNT(*) FROM portfolio_position_source_rows").fetchone()[0]),
        source_available=True,
    )


def _create_portfolio_plan_source_work_unit_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    portfolio_gross_cap_weight: float,
) -> None:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_source_work_unit_summary AS
        SELECT
            ? AS portfolio_id,
            COUNT(*) AS source_row_count,
            MAX(reference_trade_date) AS last_reference_trade_date,
            md5(
                string_agg(
                    CONCAT(
                        candidate_nk,
                        '|',
                        symbol,
                        '|',
                        COALESCE(CAST(reference_trade_date AS VARCHAR), ''),
                        '|',
                        COALESCE(CAST(signal_date AS VARCHAR), ''),
                        '|',
                        candidate_status,
                        '|',
                        COALESCE(blocked_reason_code, ''),
                        '|',
                        position_action_decision,
                        '|',
                        CAST(final_allowed_position_weight AS VARCHAR),
                        '|',
                        COALESCE(CAST(planned_entry_trade_date AS VARCHAR), ''),
                        '|',
                        COALESCE(CAST(scheduled_exit_trade_date AS VARCHAR), ''),
                        '|',
                        COALESCE(planned_exit_reason_code, ''),
                        '|',
                        CAST(? AS VARCHAR)
                    )
                    ORDER BY planned_entry_trade_date, reference_trade_date, signal_date, candidate_nk
                )
            ) AS source_fingerprint
        FROM portfolio_position_source_rows
        """,
        [portfolio_id, portfolio_gross_cap_weight],
    )


def _portfolio_plan_checkpoint_fast_path_available(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
) -> bool:
    row = connection.execute(
        """
        SELECT
            source.source_row_count,
            source.last_reference_trade_date,
            source.source_fingerprint,
            checkpoint.last_reference_trade_date,
            checkpoint.last_source_fingerprint
        FROM portfolio_plan_source_work_unit_summary AS source
        LEFT JOIN portfolio_plan_checkpoint AS checkpoint
            ON checkpoint.portfolio_id = source.portfolio_id
        WHERE source.portfolio_id = ?
        """,
        [portfolio_id],
    ).fetchone()
    if row is None:
        return False
    source_row_count, source_last_date, source_fingerprint, checkpoint_last_date, checkpoint_fingerprint = row
    if int(source_row_count or 0) == 0:
        return False
    if checkpoint_fingerprint != source_fingerprint or checkpoint_last_date != source_last_date:
        return False
    existing_snapshot_count = connection.execute(
        "SELECT COUNT(*) FROM portfolio_plan_snapshot WHERE portfolio_id = ?",
        [portfolio_id],
    ).fetchone()[0]
    return int(existing_snapshot_count) == int(source_row_count)


def _record_reused_portfolio_plan_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    portfolio_id: str,
) -> tuple[dict[str, int], int, date | None]:
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            INSERT INTO portfolio_plan_run_snapshot (
                run_id, plan_snapshot_nk, candidate_nk, plan_status, materialization_action
            )
            SELECT ?, plan_snapshot_nk, candidate_nk, plan_status, 'reused'
            FROM portfolio_plan_snapshot
            WHERE portfolio_id = ?
            """,
            [run_id, portfolio_id],
        )
        _insert_portfolio_plan_work_queue_sql(connection=connection, run_id=run_id, status="reused")
        _upsert_portfolio_plan_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    status_counts = dict(
        connection.execute(
            """
            SELECT plan_status, COUNT(*)
            FROM portfolio_plan_snapshot
            WHERE portfolio_id = ?
            GROUP BY plan_status
            """,
            [portfolio_id],
        ).fetchall()
    )
    snapshot_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM portfolio_plan_snapshot WHERE portfolio_id = ?",
            [portfolio_id],
        ).fetchone()[0]
    )
    latest_reference_trade_date = connection.execute(
        """
        SELECT last_reference_trade_date
        FROM portfolio_plan_source_work_unit_summary
        WHERE portfolio_id = ?
        """,
        [portfolio_id],
    ).fetchone()[0]
    return (
        {
            "snapshot_rows": snapshot_rows,
            "admitted_count": int(status_counts.get("admitted", 0)),
            "blocked_count": int(status_counts.get("blocked", 0)),
            "trimmed_count": int(status_counts.get("trimmed", 0)),
        },
        0,
        latest_reference_trade_date,
    )


def _materialize_portfolio_plan_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    portfolio_id: str,
    portfolio_gross_cap_weight: float,
) -> tuple[dict[str, int], int, date | None, str]:
    phase_seconds: dict[str, float] = {}
    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_existing_snapshot AS
        SELECT *
        FROM portfolio_plan_snapshot
        WHERE portfolio_id = ?
        """,
        [portfolio_id],
    )
    if int(connection.execute("SELECT COUNT(*) FROM portfolio_plan_existing_snapshot").fetchone()[0]) > 0:
        connection.execute(
            """
            CREATE INDEX portfolio_plan_existing_snapshot_nk_idx
            ON portfolio_plan_existing_snapshot (plan_snapshot_nk)
            """
        )
    phase_seconds["existing_snapshot_seconds"] = perf_counter() - phase_started

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_ordered_source AS
        SELECT *
        FROM (
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY planned_entry_trade_date NULLS LAST, reference_trade_date, signal_date, candidate_nk
                ) AS global_row_number,
                ROW_NUMBER() OVER (
                    PARTITION BY planned_entry_trade_date
                    ORDER BY reference_trade_date, signal_date, candidate_nk
                ) AS entry_row_number,
                candidate_nk,
                symbol,
                reference_trade_date,
                planned_entry_trade_date,
                scheduled_exit_trade_date,
                signal_date,
                candidate_status,
                blocked_reason_code,
                planned_exit_reason_code,
                position_action_decision,
                ROUND(LEAST(final_allowed_position_weight, 0.15), 4) AS requested_weight
            FROM portfolio_position_source_rows
        )
        ORDER BY global_row_number
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_entry_date_ranges AS
        SELECT
            planned_entry_trade_date,
            MIN(global_row_number) AS start_row_number,
            MAX(global_row_number) AS end_row_number
        FROM portfolio_plan_ordered_source
        WHERE planned_entry_trade_date IS NOT NULL
        GROUP BY planned_entry_trade_date
        ORDER BY planned_entry_trade_date
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_materialized AS
        SELECT
            CAST(NULL AS VARCHAR) AS plan_snapshot_nk,
            CAST(NULL AS VARCHAR) AS candidate_nk,
            CAST(NULL AS VARCHAR) AS portfolio_id,
            CAST(NULL AS VARCHAR) AS symbol,
            CAST(NULL AS DATE) AS reference_trade_date,
            CAST(NULL AS DATE) AS planned_entry_trade_date,
            CAST(NULL AS DATE) AS scheduled_exit_trade_date,
            CAST(NULL AS VARCHAR) AS position_action_decision,
            CAST(NULL AS DOUBLE) AS requested_weight,
            CAST(NULL AS DOUBLE) AS admitted_weight,
            CAST(NULL AS DOUBLE) AS trimmed_weight,
            CAST(NULL AS VARCHAR) AS plan_status,
            CAST(NULL AS VARCHAR) AS blocking_reason_code,
            CAST(NULL AS VARCHAR) AS planned_exit_reason_code,
            CAST(NULL AS DOUBLE) AS portfolio_gross_cap_weight,
            CAST(NULL AS DOUBLE) AS current_portfolio_gross_weight,
            CAST(NULL AS DOUBLE) AS remaining_portfolio_capacity_weight,
            CAST(NULL AS DOUBLE) AS portfolio_gross_used_weight,
            CAST(NULL AS DOUBLE) AS portfolio_gross_remaining_weight,
            CAST(NULL AS VARCHAR) AS portfolio_plan_contract_version
        WHERE 1 = 0
        """
    )
    entry_dates = [
        (row[0], int(row[1]), int(row[2]))
        for row in connection.execute(
            """
            SELECT planned_entry_trade_date, start_row_number, end_row_number
            FROM portfolio_plan_entry_date_ranges
            ORDER BY planned_entry_trade_date
            """
        ).fetchall()
    ]
    phase_seconds["prepare_source_seconds"] = perf_counter() - phase_started

    _update_portfolio_plan_run_message_sql(
        connection=connection,
        run_id=run_id,
        message=(
            "portfolio_plan slow path prepared: "
            f"source_rows={int(connection.execute('SELECT COUNT(*) FROM portfolio_plan_ordered_source').fetchone()[0])}, "
            f"entry_dates={len(entry_dates)}"
        ),
    )

    phase_started = perf_counter()
    null_entry_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM portfolio_plan_ordered_source WHERE planned_entry_trade_date IS NULL"
        ).fetchone()[0]
    )
    if null_entry_rows:
        connection.execute(
            f"""
            INSERT INTO portfolio_plan_materialized
            SELECT
                CONCAT(
                    ?,
                    ':',
                    candidate_nk,
                    ':',
                    COALESCE(CAST(reference_trade_date AS VARCHAR), 'None'),
                    ':',
                    ?
                ) AS plan_snapshot_nk,
                candidate_nk,
                ? AS portfolio_id,
                symbol,
                reference_trade_date,
                planned_entry_trade_date,
                scheduled_exit_trade_date,
                position_action_decision,
                requested_weight,
                0.0 AS admitted_weight,
                0.0 AS trimmed_weight,
                'blocked' AS plan_status,
                CASE
                    WHEN candidate_status != 'admitted' THEN COALESCE(blocked_reason_code, 'candidate_blocked')
                    WHEN requested_weight <= 0 THEN 'no_position_capacity'
                    ELSE 'missing_next_execution_trade_date'
                END AS blocking_reason_code,
                planned_exit_reason_code,
                {portfolio_gross_cap_weight} AS portfolio_gross_cap_weight,
                0.0 AS current_portfolio_gross_weight,
                {portfolio_gross_cap_weight} AS remaining_portfolio_capacity_weight,
                0.0 AS portfolio_gross_used_weight,
                {portfolio_gross_cap_weight} AS portfolio_gross_remaining_weight,
                ? AS portfolio_plan_contract_version
            FROM portfolio_plan_ordered_source
            WHERE planned_entry_trade_date IS NULL
            """,
            [portfolio_id, PORTFOLIO_PLAN_CONTRACT_VERSION, portfolio_id, PORTFOLIO_PLAN_CONTRACT_VERSION],
        )

    exit_weight_by_date: dict[date, float] = {}
    exit_dates_heap: list[date] = []
    active_gross_weight = 0.0
    materialized_rows = null_entry_rows
    processed_dates = 0
    for entry_trade_date, start_row_number, end_row_number in entry_dates:
        while exit_dates_heap and exit_dates_heap[0] <= entry_trade_date:
            expired_trade_date = heappop(exit_dates_heap)
            expired_weight = exit_weight_by_date.pop(expired_trade_date, 0.0)
            active_gross_weight = _round_weight(max(active_gross_weight - expired_weight, 0.0))

        carry_in_weight = _round_weight(active_gross_weight)
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE portfolio_plan_current_batch AS
            WITH batch_rows AS (
                SELECT
                    *,
                    CASE
                        WHEN candidate_status = 'admitted' AND requested_weight > 0 THEN requested_weight
                        ELSE 0.0
                    END AS eligible_requested_weight
                FROM portfolio_plan_ordered_source
                WHERE global_row_number BETWEEN ? AND ?
            ),
            batch_with_prefix AS (
                SELECT
                    *,
                    COALESCE(
                        SUM(eligible_requested_weight) OVER (
                            ORDER BY entry_row_number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ),
                        0.0
                    ) AS eligible_requested_before_weight
                FROM batch_rows
            ),
            weighted_rows AS (
                SELECT
                    candidate_nk,
                    symbol,
                    reference_trade_date,
                    planned_entry_trade_date,
                    scheduled_exit_trade_date,
                    position_action_decision,
                    requested_weight,
                    planned_exit_reason_code,
                    candidate_status,
                    blocked_reason_code,
                    ROUND(
                        {carry_in_weight}
                        + LEAST(
                            eligible_requested_before_weight,
                            GREATEST({portfolio_gross_cap_weight} - {carry_in_weight}, 0.0)
                        ),
                        4
                    ) AS current_portfolio_gross_weight,
                    ROUND(
                        GREATEST(
                            GREATEST({portfolio_gross_cap_weight} - {carry_in_weight}, 0.0)
                            - eligible_requested_before_weight,
                            0.0
                        ),
                        4
                    ) AS available_before_weight
                FROM batch_with_prefix
            ),
            materialized_rows AS (
                SELECT
                    candidate_nk,
                    symbol,
                    reference_trade_date,
                    planned_entry_trade_date,
                    scheduled_exit_trade_date,
                    position_action_decision,
                    requested_weight,
                    planned_exit_reason_code,
                    current_portfolio_gross_weight,
                    CASE
                        WHEN candidate_status != 'admitted' THEN 'blocked'
                        WHEN requested_weight <= 0 THEN 'blocked'
                        WHEN available_before_weight >= requested_weight THEN 'admitted'
                        WHEN available_before_weight > 0 THEN 'trimmed'
                        ELSE 'blocked'
                    END AS plan_status,
                    CASE
                        WHEN candidate_status != 'admitted' THEN COALESCE(blocked_reason_code, 'candidate_blocked')
                        WHEN requested_weight <= 0 THEN 'no_position_capacity'
                        WHEN available_before_weight >= requested_weight THEN NULL
                        WHEN available_before_weight > 0 THEN 'portfolio_capacity_trimmed'
                        ELSE 'portfolio_capacity_exhausted'
                    END AS blocking_reason_code,
                    CASE
                        WHEN candidate_status = 'admitted' AND requested_weight > 0 AND available_before_weight >= requested_weight
                            THEN ROUND(requested_weight, 4)
                        WHEN candidate_status = 'admitted' AND requested_weight > 0 AND available_before_weight > 0
                            THEN ROUND(available_before_weight, 4)
                        ELSE 0.0
                    END AS admitted_weight
                FROM weighted_rows
            )
            SELECT
                CONCAT(
                    ?,
                    ':',
                    candidate_nk,
                    ':',
                    COALESCE(CAST(reference_trade_date AS VARCHAR), 'None'),
                    ':',
                    ?
                ) AS plan_snapshot_nk,
                candidate_nk,
                ? AS portfolio_id,
                symbol,
                reference_trade_date,
                planned_entry_trade_date,
                scheduled_exit_trade_date,
                position_action_decision,
                requested_weight,
                admitted_weight,
                CASE
                    WHEN plan_status = 'trimmed' THEN ROUND(requested_weight - admitted_weight, 4)
                    ELSE 0.0
                END AS trimmed_weight,
                plan_status,
                blocking_reason_code,
                planned_exit_reason_code,
                {portfolio_gross_cap_weight} AS portfolio_gross_cap_weight,
                current_portfolio_gross_weight,
                ROUND(
                    GREATEST({portfolio_gross_cap_weight} - (current_portfolio_gross_weight + admitted_weight), 0.0),
                    4
                ) AS remaining_portfolio_capacity_weight,
                ROUND(current_portfolio_gross_weight + admitted_weight, 4) AS portfolio_gross_used_weight,
                ROUND(
                    GREATEST({portfolio_gross_cap_weight} - (current_portfolio_gross_weight + admitted_weight), 0.0),
                    4
                ) AS portfolio_gross_remaining_weight,
                ? AS portfolio_plan_contract_version
            FROM materialized_rows
            ORDER BY planned_entry_trade_date, reference_trade_date, candidate_nk
            """,
            [
                start_row_number,
                end_row_number,
                portfolio_id,
                PORTFOLIO_PLAN_CONTRACT_VERSION,
                portfolio_id,
                PORTFOLIO_PLAN_CONTRACT_VERSION,
            ],
        )
        connection.execute(
            """
            INSERT INTO portfolio_plan_materialized
            SELECT *
            FROM portfolio_plan_current_batch
            """
        )

        batch_rows, admitted_today = connection.execute(
            """
            SELECT COUNT(*), COALESCE(ROUND(SUM(admitted_weight), 4), 0.0)
            FROM portfolio_plan_current_batch
            """
        ).fetchone()
        materialized_rows += int(batch_rows)
        active_gross_weight = _round_weight(active_gross_weight + float(admitted_today))
        processed_dates += 1

        for scheduled_exit_trade_date, exit_weight in connection.execute(
            """
            SELECT scheduled_exit_trade_date, ROUND(SUM(admitted_weight), 4)
            FROM portfolio_plan_current_batch
            WHERE admitted_weight > 0 AND scheduled_exit_trade_date IS NOT NULL
            GROUP BY scheduled_exit_trade_date
            """
        ).fetchall():
            updated_weight = _round_weight(exit_weight_by_date.get(scheduled_exit_trade_date, 0.0) + float(exit_weight))
            if scheduled_exit_trade_date not in exit_weight_by_date:
                heappush(exit_dates_heap, scheduled_exit_trade_date)
            exit_weight_by_date[scheduled_exit_trade_date] = updated_weight

        if (
            processed_dates % PORTFOLIO_PLAN_PROGRESS_UPDATE_INTERVAL == 0
            or processed_dates == len(entry_dates)
        ):
            _update_portfolio_plan_run_message_sql(
                connection=connection,
                run_id=run_id,
                message=(
                    "portfolio_plan slow path running: "
                    f"dates={processed_dates}/{len(entry_dates)}, "
                    f"latest_entry_trade_date={entry_trade_date.isoformat()}, "
                    f"materialized_rows={materialized_rows}, "
                    f"active_gross_weight={active_gross_weight:.4f}"
                ),
            )
    phase_seconds["materialize_batches_seconds"] = perf_counter() - phase_started

    _update_portfolio_plan_run_message_sql(
        connection=connection,
        run_id=run_id,
        message=(
            "portfolio_plan slow path finished date batches: "
            f"dates={len(entry_dates)}/{len(entry_dates)}, "
            f"materialized_rows={materialized_rows}. "
            "Building materialized_with_action join."
        ),
    )
    phase_started = perf_counter()
    connection.execute(
        """
        CREATE INDEX portfolio_plan_materialized_nk_idx
        ON portfolio_plan_materialized (plan_snapshot_nk)
        """
    )
    phase_seconds["materialized_index_seconds"] = perf_counter() - phase_started

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_materialized_with_action AS
        SELECT
            materialized.*,
            existing.first_seen_run_id AS existing_first_seen_run_id,
            CASE
                WHEN existing.plan_snapshot_nk IS NULL THEN 'inserted'
                WHEN existing.plan_status = materialized.plan_status
                    AND existing.planned_entry_trade_date IS NOT DISTINCT FROM materialized.planned_entry_trade_date
                    AND existing.scheduled_exit_trade_date IS NOT DISTINCT FROM materialized.scheduled_exit_trade_date
                    AND existing.position_action_decision = materialized.position_action_decision
                    AND existing.requested_weight = materialized.requested_weight
                    AND existing.admitted_weight = materialized.admitted_weight
                    AND existing.trimmed_weight = materialized.trimmed_weight
                    AND existing.blocking_reason_code IS NOT DISTINCT FROM materialized.blocking_reason_code
                    AND existing.planned_exit_reason_code IS NOT DISTINCT FROM materialized.planned_exit_reason_code
                    AND existing.current_portfolio_gross_weight = materialized.current_portfolio_gross_weight
                    AND existing.remaining_portfolio_capacity_weight = materialized.remaining_portfolio_capacity_weight
                    AND existing.portfolio_gross_used_weight = materialized.portfolio_gross_used_weight
                    AND existing.portfolio_gross_remaining_weight = materialized.portfolio_gross_remaining_weight
                    THEN 'reused'
                ELSE 'rematerialized'
            END AS materialization_action
        FROM portfolio_plan_materialized AS materialized
        LEFT JOIN portfolio_plan_existing_snapshot AS existing
            ON existing.plan_snapshot_nk = materialized.plan_snapshot_nk
        """
    )
    phase_seconds["materialized_with_action_seconds"] = perf_counter() - phase_started
    _update_portfolio_plan_run_message_sql(
        connection=connection,
        run_id=run_id,
        message="portfolio_plan slow path built materialized_with_action. Starting committed snapshot replace.",
    )
    phase_started = perf_counter()
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute("DELETE FROM portfolio_plan_snapshot WHERE portfolio_id = ?", [portfolio_id])
        _update_portfolio_plan_run_message_sql(
            connection=connection,
            run_id=run_id,
            message="portfolio_plan committed replace: old snapshot deleted; inserting new snapshot rows.",
        )
        connection.execute(
            """
            INSERT INTO portfolio_plan_snapshot (
                plan_snapshot_nk, candidate_nk, portfolio_id, symbol, reference_trade_date,
                planned_entry_trade_date, scheduled_exit_trade_date, position_action_decision,
                requested_weight, admitted_weight, trimmed_weight,
                plan_status, blocking_reason_code, planned_exit_reason_code,
                portfolio_gross_cap_weight, current_portfolio_gross_weight,
                remaining_portfolio_capacity_weight, portfolio_gross_used_weight, portfolio_gross_remaining_weight,
                portfolio_plan_contract_version, first_seen_run_id, last_materialized_run_id
            )
            SELECT
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                symbol,
                reference_trade_date,
                planned_entry_trade_date,
                scheduled_exit_trade_date,
                position_action_decision,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status,
                blocking_reason_code,
                planned_exit_reason_code,
                portfolio_gross_cap_weight,
                current_portfolio_gross_weight,
                remaining_portfolio_capacity_weight,
                portfolio_gross_used_weight,
                portfolio_gross_remaining_weight,
                portfolio_plan_contract_version,
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM portfolio_plan_materialized_with_action
            """,
            [run_id, run_id],
        )
        _update_portfolio_plan_run_message_sql(
            connection=connection,
            run_id=run_id,
            message="portfolio_plan committed replace: snapshot inserted; writing run_snapshot rows.",
        )
        connection.execute(
            """
            INSERT INTO portfolio_plan_run_snapshot (
                run_id, plan_snapshot_nk, candidate_nk, plan_status, materialization_action
            )
            SELECT ?, plan_snapshot_nk, candidate_nk, plan_status, materialization_action
            FROM portfolio_plan_materialized_with_action
            """,
            [run_id],
        )
        _update_portfolio_plan_run_message_sql(
            connection=connection,
            run_id=run_id,
            message="portfolio_plan committed replace: run_snapshot inserted; updating work_queue and checkpoint.",
        )
        _insert_portfolio_plan_work_queue_sql(connection=connection, run_id=run_id, status="completed")
        _upsert_portfolio_plan_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    phase_seconds["commit_seconds"] = perf_counter() - phase_started
    status_counts = dict(
        connection.execute(
            """
            SELECT plan_status, COUNT(*)
            FROM portfolio_plan_materialized_with_action
            GROUP BY plan_status
            """
        ).fetchall()
    )
    snapshot_rows = int(connection.execute("SELECT COUNT(*) FROM portfolio_plan_materialized_with_action").fetchone()[0])
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM portfolio_plan_source_work_unit_summary"
    ).fetchone()[0]
    return (
        {
            "snapshot_rows": snapshot_rows,
            "admitted_count": int(status_counts.get("admitted", 0)),
            "blocked_count": int(status_counts.get("blocked", 0)),
            "trimmed_count": int(status_counts.get("trimmed", 0)),
        },
        1,
        latest_reference_trade_date,
        (
            "portfolio_plan run completed. "
            "slow_path=date_batched "
            f"entry_dates={len(entry_dates)} "
            f"materialized_rows={snapshot_rows} "
            f"null_entry_rows={null_entry_rows} "
            f"timings={{existing_snapshot_seconds={phase_seconds['existing_snapshot_seconds']:.3f}, "
            f"prepare_source_seconds={phase_seconds['prepare_source_seconds']:.3f}, "
            f"materialize_batches_seconds={phase_seconds['materialize_batches_seconds']:.3f}, "
            f"materialized_index_seconds={phase_seconds['materialized_index_seconds']:.3f}, "
            f"materialized_with_action_seconds={phase_seconds['materialized_with_action_seconds']:.3f}, "
            f"commit_seconds={phase_seconds['commit_seconds']:.3f}}}"
        ),
    )


def _insert_portfolio_plan_work_queue_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
) -> None:
    connection.execute(
        """
        INSERT INTO portfolio_plan_work_queue (
            queue_id, portfolio_id, status, source_row_count, last_reference_trade_date,
            source_fingerprint, claimed_at, finished_at
        )
        SELECT
            CONCAT(?, ':', portfolio_id),
            portfolio_id,
            ?,
            source_row_count,
            last_reference_trade_date,
            source_fingerprint,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM portfolio_plan_source_work_unit_summary
        """,
        [run_id, status],
    )


def _upsert_portfolio_plan_checkpoint_sql(*, connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    connection.execute(
        """
        INSERT INTO portfolio_plan_checkpoint (
            portfolio_id, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        )
        SELECT
            portfolio_id,
            last_reference_trade_date,
            source_fingerprint,
            ?,
            CURRENT_TIMESTAMP
        FROM portfolio_plan_source_work_unit_summary
        ON CONFLICT(portfolio_id) DO UPDATE
        SET
            last_reference_trade_date = excluded.last_reference_trade_date,
            last_source_fingerprint = excluded.last_source_fingerprint,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [run_id],
    )


def _update_portfolio_plan_run_message_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    message: str,
) -> None:
    try:
        print(message, file=sys.stderr, flush=True)
    except OSError:
        pass
    connection.execute(
        """
        UPDATE portfolio_plan_run
        SET message = ?
        WHERE run_id = ? AND finished_at IS NULL
        """,
        [message, run_id],
    )


def _round_weight(value: float) -> float:
    return round(float(value), 4)


def _duckdb_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
