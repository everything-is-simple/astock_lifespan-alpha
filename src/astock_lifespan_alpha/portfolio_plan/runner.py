"""Stage-four portfolio plan runner."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.portfolio_plan.contracts import PortfolioPlanRunSummary, PortfolioPlanStatus
from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema


PORTFOLIO_PLAN_CONTRACT_VERSION = "stage4_portfolio_plan_v1"


def run_portfolio_plan_build(
    *,
    portfolio_id: str = "core",
    portfolio_gross_cap_weight: float = 0.15,
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

    with duckdb.connect(str(target_path)) as connection:
        source_row_count = _attach_position_source_view(connection=connection, source_path=source_path)
        connection.execute(
            """
            INSERT INTO portfolio_plan_run (
                run_id, status, portfolio_id, source_position_path, bounded_candidate_count,
                portfolio_gross_cap_weight, message
            ) VALUES (?, 'running', ?, ?, ?, ?, 'portfolio_plan run started.')
            """,
            [run_id, portfolio_id, str(source_path), source_row_count, portfolio_gross_cap_weight],
        )

        if source_row_count == 0:
            message = "portfolio_plan schema initialized without position rows."
        else:
            counts = _materialize_portfolio_plan_sql(
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

    return PortfolioPlanRunSummary(
        runner_name="run_portfolio_plan_build",
        run_id=run_id,
        status=PortfolioPlanStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={"position": str(source_path) if source_path.exists() else None},
        message=message,
        materialization_counts=counts,
    )


def _attach_position_source_view(*, connection: duckdb.DuckDBPyConnection, source_path: Path) -> int:
    if not source_path.exists():
        return 0
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
    }
    if not required_tables.issubset(available_tables):
        return 0
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
            sizing.final_allowed_position_weight
        FROM portfolio_position_source.position_candidate_audit AS audit
        INNER JOIN portfolio_position_source.position_capacity_snapshot AS capacity
            ON capacity.candidate_nk = audit.candidate_nk
        INNER JOIN portfolio_position_source.position_sizing_snapshot AS sizing
            ON sizing.candidate_nk = audit.candidate_nk
        """
    )
    return int(connection.execute("SELECT COUNT(*) FROM portfolio_position_source_rows").fetchone()[0])


def _materialize_portfolio_plan_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    portfolio_id: str,
    portfolio_gross_cap_weight: float,
) -> dict[str, int]:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE portfolio_plan_materialized AS
        WITH ordered_rows AS (
            SELECT
                *,
                final_allowed_position_weight AS requested_weight,
                candidate_status = 'admitted' AND final_allowed_position_weight > 0 AS consumes_capacity
            FROM portfolio_position_source_rows
        ),
        capacity_rows AS (
            SELECT
                *,
                SUM(CASE WHEN consumes_capacity THEN requested_weight ELSE 0.0 END) OVER (
                    ORDER BY reference_trade_date, signal_date, candidate_nk
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_requested_weight
            FROM ordered_rows
        ),
        planned_rows AS (
            SELECT
                *,
                CASE
                    WHEN consumes_capacity THEN cumulative_requested_weight - requested_weight
                    ELSE cumulative_requested_weight
                END AS prior_requested_weight
            FROM capacity_rows
        ),
        status_rows AS (
            SELECT
                *,
                CASE
                    WHEN candidate_status != 'admitted' THEN 'blocked'
                    WHEN requested_weight <= 0 THEN 'blocked'
                    WHEN {portfolio_gross_cap_weight} - prior_requested_weight >= requested_weight THEN 'admitted'
                    WHEN {portfolio_gross_cap_weight} - prior_requested_weight > 0 THEN 'trimmed'
                    ELSE 'blocked'
                END AS plan_status,
                CASE
                    WHEN candidate_status != 'admitted' THEN COALESCE(blocked_reason_code, 'candidate_blocked')
                    WHEN requested_weight <= 0 THEN 'no_position_capacity'
                    WHEN {portfolio_gross_cap_weight} - prior_requested_weight >= requested_weight THEN NULL
                    WHEN {portfolio_gross_cap_weight} - prior_requested_weight > 0 THEN 'portfolio_capacity_trimmed'
                    ELSE 'portfolio_capacity_exhausted'
                END AS plan_blocking_reason_code,
                CASE
                    WHEN candidate_status = 'admitted'
                        AND requested_weight > 0
                        AND {portfolio_gross_cap_weight} - prior_requested_weight >= requested_weight
                        THEN requested_weight
                    WHEN candidate_status = 'admitted'
                        AND requested_weight > 0
                        AND {portfolio_gross_cap_weight} - prior_requested_weight > 0
                        THEN ROUND({portfolio_gross_cap_weight} - prior_requested_weight, 4)
                    ELSE 0.0
                END AS admitted_weight
            FROM planned_rows
        ),
        weighted_rows AS (
            SELECT
                *,
                CASE WHEN plan_status = 'trimmed' THEN ROUND(requested_weight - admitted_weight, 4) ELSE 0.0 END AS trimmed_weight,
                ROUND(LEAST({portfolio_gross_cap_weight}, cumulative_requested_weight), 4) AS portfolio_gross_used_weight,
                ROUND(GREATEST({portfolio_gross_cap_weight} - cumulative_requested_weight, 0.0), 4) AS portfolio_gross_remaining_weight
            FROM status_rows
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
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            plan_blocking_reason_code AS blocking_reason_code,
            {portfolio_gross_cap_weight} AS portfolio_gross_cap_weight,
            portfolio_gross_used_weight,
            portfolio_gross_remaining_weight,
            ? AS portfolio_plan_contract_version
        FROM weighted_rows
        """
        ,
        [portfolio_id, PORTFOLIO_PLAN_CONTRACT_VERSION, portfolio_id, PORTFOLIO_PLAN_CONTRACT_VERSION],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_materialized_with_action AS
        SELECT
            materialized.*,
            CASE
                WHEN existing.plan_snapshot_nk IS NULL THEN 'inserted'
                WHEN existing.plan_status = materialized.plan_status
                    AND existing.admitted_weight = materialized.admitted_weight
                    AND existing.trimmed_weight = materialized.trimmed_weight
                    AND existing.blocking_reason_code IS NOT DISTINCT FROM materialized.blocking_reason_code
                    AND existing.portfolio_gross_used_weight = materialized.portfolio_gross_used_weight
                    AND existing.portfolio_gross_remaining_weight = materialized.portfolio_gross_remaining_weight
                    THEN 'reused'
                ELSE 'rematerialized'
            END AS materialization_action
        FROM portfolio_plan_materialized materialized
        LEFT JOIN portfolio_plan_snapshot existing
            ON existing.plan_snapshot_nk = materialized.plan_snapshot_nk
        """
    )
    connection.execute(
        """
        INSERT INTO portfolio_plan_snapshot (
            plan_snapshot_nk, candidate_nk, portfolio_id, symbol, reference_trade_date,
            position_action_decision, requested_weight, admitted_weight, trimmed_weight,
            plan_status, blocking_reason_code, portfolio_gross_cap_weight,
            portfolio_gross_used_weight, portfolio_gross_remaining_weight,
            portfolio_plan_contract_version, first_seen_run_id, last_materialized_run_id
        )
        SELECT
            plan_snapshot_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            reference_trade_date,
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            blocking_reason_code,
            portfolio_gross_cap_weight,
            portfolio_gross_used_weight,
            portfolio_gross_remaining_weight,
            portfolio_plan_contract_version,
            ?,
            ?
        FROM portfolio_plan_materialized_with_action
        WHERE materialization_action = 'inserted'
        """,
        [run_id, run_id],
    )
    connection.execute(
        """
        UPDATE portfolio_plan_snapshot AS target
        SET
            position_action_decision = source.position_action_decision,
            requested_weight = source.requested_weight,
            admitted_weight = source.admitted_weight,
            trimmed_weight = source.trimmed_weight,
            plan_status = source.plan_status,
            blocking_reason_code = source.blocking_reason_code,
            portfolio_gross_cap_weight = source.portfolio_gross_cap_weight,
            portfolio_gross_used_weight = source.portfolio_gross_used_weight,
            portfolio_gross_remaining_weight = source.portfolio_gross_remaining_weight,
            portfolio_plan_contract_version = source.portfolio_plan_contract_version,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        FROM portfolio_plan_materialized_with_action AS source
        WHERE target.plan_snapshot_nk = source.plan_snapshot_nk
            AND source.materialization_action != 'inserted'
        """,
        [run_id],
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
    return {
        "snapshot_rows": snapshot_rows,
        "admitted_count": int(status_counts.get("admitted", 0)),
        "blocked_count": int(status_counts.get("blocked", 0)),
        "trimmed_count": int(status_counts.get("trimmed", 0)),
    }


def _duckdb_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
