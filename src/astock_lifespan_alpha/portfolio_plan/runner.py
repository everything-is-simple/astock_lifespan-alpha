"""Stage-four portfolio plan runner."""

from __future__ import annotations

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
    rows = _load_position_rows(source_path)
    counts = {"snapshot_rows": 0, "admitted_count": 0, "blocked_count": 0, "trimmed_count": 0}
    message = "portfolio_plan run completed."
    remaining_capacity = portfolio_gross_cap_weight
    used_capacity = 0.0

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO portfolio_plan_run (
                run_id, status, portfolio_id, source_position_path, bounded_candidate_count,
                portfolio_gross_cap_weight, message
            ) VALUES (?, 'running', ?, ?, ?, ?, 'portfolio_plan run started.')
            """,
            [run_id, portfolio_id, str(source_path), len(rows), portfolio_gross_cap_weight],
        )

        for row in rows:
            plan_status, admitted_weight, trimmed_weight, blocking_reason_code, remaining_capacity = _plan_row(
                row=row,
                remaining_capacity=remaining_capacity,
            )
            if plan_status == "admitted":
                counts["admitted_count"] += 1
            elif plan_status == "trimmed":
                counts["trimmed_count"] += 1
            else:
                counts["blocked_count"] += 1
            used_capacity = round(portfolio_gross_cap_weight - remaining_capacity, 4)
            plan_snapshot_nk = (
                f"{portfolio_id}:{row['candidate_nk']}:{row['reference_trade_date']}:{PORTFOLIO_PLAN_CONTRACT_VERSION}"
            )
            materialization_action = _upsert_plan_snapshot(
                connection=connection,
                run_id=run_id,
                plan_snapshot_nk=plan_snapshot_nk,
                candidate_nk=row["candidate_nk"],
                portfolio_id=portfolio_id,
                symbol=row["symbol"],
                reference_trade_date=row["reference_trade_date"],
                position_action_decision=row["position_action_decision"],
                requested_weight=row["final_allowed_position_weight"],
                admitted_weight=admitted_weight,
                trimmed_weight=trimmed_weight,
                plan_status=plan_status,
                blocking_reason_code=blocking_reason_code,
                portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                portfolio_gross_used_weight=used_capacity,
                portfolio_gross_remaining_weight=remaining_capacity,
            )
            connection.execute(
                """
                INSERT INTO portfolio_plan_run_snapshot (
                    run_id, plan_snapshot_nk, candidate_nk, plan_status, materialization_action
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [run_id, plan_snapshot_nk, row["candidate_nk"], plan_status, materialization_action],
            )
            counts["snapshot_rows"] += 1

        if not rows:
            message = "portfolio_plan schema initialized without position rows."

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


def _load_position_rows(database_path) -> list[dict[str, object]]:
    if not database_path.exists():
        return []
    with duckdb.connect(str(database_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        required_tables = {
            "position_candidate_audit",
            "position_capacity_snapshot",
            "position_sizing_snapshot",
        }
        if not required_tables.issubset(available_tables):
            return []
        rows = connection.execute(
            """
            SELECT
                audit.candidate_nk,
                audit.symbol,
                audit.reference_trade_date,
                audit.candidate_status,
                audit.blocked_reason_code,
                sizing.position_action_decision,
                sizing.final_allowed_position_weight
            FROM position_candidate_audit AS audit
            INNER JOIN position_capacity_snapshot AS capacity
                ON capacity.candidate_nk = audit.candidate_nk
            INNER JOIN position_sizing_snapshot AS sizing
                ON sizing.candidate_nk = audit.candidate_nk
            ORDER BY audit.reference_trade_date, audit.signal_date, audit.candidate_nk
            """
        ).fetchall()
    return [
        {
            "candidate_nk": str(candidate_nk),
            "symbol": str(symbol),
            "reference_trade_date": reference_trade_date,
            "candidate_status": str(candidate_status),
            "blocked_reason_code": str(blocked_reason_code) if blocked_reason_code is not None else None,
            "position_action_decision": str(position_action_decision),
            "final_allowed_position_weight": float(final_allowed_position_weight),
        }
        for candidate_nk, symbol, reference_trade_date, candidate_status, blocked_reason_code, position_action_decision, final_allowed_position_weight in rows
    ]


def _plan_row(*, row: dict[str, object], remaining_capacity: float) -> tuple[str, float, float, str | None, float]:
    requested_weight = float(row["final_allowed_position_weight"])
    if row["candidate_status"] != "admitted":
        return "blocked", 0.0, 0.0, row["blocked_reason_code"] or "candidate_blocked", remaining_capacity
    if requested_weight <= 0:
        return "blocked", 0.0, 0.0, "no_position_capacity", remaining_capacity
    if remaining_capacity >= requested_weight:
        return "admitted", requested_weight, 0.0, None, round(remaining_capacity - requested_weight, 4)
    if remaining_capacity > 0:
        admitted_weight = round(remaining_capacity, 4)
        trimmed_weight = round(requested_weight - admitted_weight, 4)
        return "trimmed", admitted_weight, trimmed_weight, "portfolio_capacity_trimmed", 0.0
    return "blocked", 0.0, 0.0, "portfolio_capacity_exhausted", remaining_capacity


def _upsert_plan_snapshot(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    plan_snapshot_nk: str,
    candidate_nk: str,
    portfolio_id: str,
    symbol: str,
    reference_trade_date,
    position_action_decision: str,
    requested_weight: float,
    admitted_weight: float,
    trimmed_weight: float,
    plan_status: str,
    blocking_reason_code: str | None,
    portfolio_gross_cap_weight: float,
    portfolio_gross_used_weight: float,
    portfolio_gross_remaining_weight: float,
) -> str:
    existing = connection.execute(
        """
        SELECT
            plan_status,
            admitted_weight,
            trimmed_weight,
            blocking_reason_code,
            portfolio_gross_used_weight,
            portfolio_gross_remaining_weight
        FROM portfolio_plan_snapshot
        WHERE plan_snapshot_nk = ?
        """,
        [plan_snapshot_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            """
            INSERT INTO portfolio_plan_snapshot (
                plan_snapshot_nk, candidate_nk, portfolio_id, symbol, reference_trade_date,
                position_action_decision, requested_weight, admitted_weight, trimmed_weight,
                plan_status, blocking_reason_code, portfolio_gross_cap_weight,
                portfolio_gross_used_weight, portfolio_gross_remaining_weight,
                portfolio_plan_contract_version, first_seen_run_id, last_materialized_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
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
                PORTFOLIO_PLAN_CONTRACT_VERSION,
                run_id,
                run_id,
            ],
        )
        return "inserted"

    if existing == (
        plan_status,
        admitted_weight,
        trimmed_weight,
        blocking_reason_code,
        portfolio_gross_used_weight,
        portfolio_gross_remaining_weight,
    ):
        connection.execute(
            """
            UPDATE portfolio_plan_snapshot
            SET last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE plan_snapshot_nk = ?
            """,
            [run_id, plan_snapshot_nk],
        )
        return "reused"

    connection.execute(
        """
        UPDATE portfolio_plan_snapshot
        SET
            position_action_decision = ?,
            requested_weight = ?,
            admitted_weight = ?,
            trimmed_weight = ?,
            plan_status = ?,
            blocking_reason_code = ?,
            portfolio_gross_cap_weight = ?,
            portfolio_gross_used_weight = ?,
            portfolio_gross_remaining_weight = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE plan_snapshot_nk = ?
        """,
        [
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            blocking_reason_code,
            portfolio_gross_cap_weight,
            portfolio_gross_used_weight,
            portfolio_gross_remaining_weight,
            run_id,
            plan_snapshot_nk,
        ],
    )
    return "rematerialized"
