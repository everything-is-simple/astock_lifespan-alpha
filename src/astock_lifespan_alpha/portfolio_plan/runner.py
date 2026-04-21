"""Stage-four portfolio plan runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.portfolio_plan.contracts import (
    PortfolioPlanCheckpointSummary,
    PortfolioPlanRunSummary,
    PortfolioPlanStatus,
)
from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema


PORTFOLIO_PLAN_CONTRACT_VERSION = "stage4_portfolio_plan_v1"


@dataclass(frozen=True)
class _PortfolioPlanSourceMetadata:
    position_source_path: Path | None
    row_count: int
    source_available: bool


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
                    counts, work_units_updated, latest_reference_trade_date = _materialize_portfolio_plan_sql(
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
            sizing.final_allowed_position_weight
        FROM portfolio_position_source.position_candidate_audit AS audit
        INNER JOIN portfolio_position_source.position_capacity_snapshot AS capacity
            ON capacity.candidate_nk = audit.candidate_nk
        INNER JOIN portfolio_position_source.position_sizing_snapshot AS sizing
            ON sizing.candidate_nk = audit.candidate_nk
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
                        CAST(? AS VARCHAR)
                    )
                    ORDER BY reference_trade_date, signal_date, candidate_nk
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
) -> tuple[dict[str, int], int, date | None]:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_existing_snapshot AS
        SELECT *
        FROM portfolio_plan_snapshot
        WHERE portfolio_id = ?
        """,
        [portfolio_id],
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE portfolio_plan_materialized AS
        WITH ordered_rows AS (
            SELECT
                candidate_nk,
                symbol,
                reference_trade_date,
                signal_date,
                candidate_status,
                blocked_reason_code,
                position_action_decision,
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
        """,
        [portfolio_id, PORTFOLIO_PLAN_CONTRACT_VERSION, portfolio_id, PORTFOLIO_PLAN_CONTRACT_VERSION],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE portfolio_plan_materialized_with_action AS
        SELECT
            materialized.*,
            existing.first_seen_run_id AS existing_first_seen_run_id,
            CASE
                WHEN existing.plan_snapshot_nk IS NULL THEN 'inserted'
                WHEN existing.plan_status = materialized.plan_status
                    AND existing.position_action_decision = materialized.position_action_decision
                    AND existing.requested_weight = materialized.requested_weight
                    AND existing.admitted_weight = materialized.admitted_weight
                    AND existing.trimmed_weight = materialized.trimmed_weight
                    AND existing.blocking_reason_code IS NOT DISTINCT FROM materialized.blocking_reason_code
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
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute("DELETE FROM portfolio_plan_snapshot WHERE portfolio_id = ?", [portfolio_id])
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
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM portfolio_plan_materialized_with_action
            """,
            [run_id, run_id],
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
        _insert_portfolio_plan_work_queue_sql(connection=connection, run_id=run_id, status="completed")
        _upsert_portfolio_plan_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
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


def _duckdb_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
