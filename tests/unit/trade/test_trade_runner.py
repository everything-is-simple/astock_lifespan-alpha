from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema
from astock_lifespan_alpha.trade import TradeRunSummary, run_trade_from_portfolio_plan
from astock_lifespan_alpha.trade.schema import TRADE_TABLES, initialize_trade_schema


def test_trade_schema_initializes_formal_tables(tmp_path):
    database_path = tmp_path / "trade.duckdb"

    initialize_trade_schema(database_path)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}

    assert set(TRADE_TABLES).issubset(table_names)


def test_trade_runner_materializes_admitted_trimmed_and_blocked_rows(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", 11.1),
            ("AAA", "2026-01-05T00:00:00", 12.2),
        ],
    )
    _write_portfolio_plan_snapshot(
        workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb",
        [
            _plan_row(
                plan_snapshot_nk="plan:admitted",
                candidate_nk="candidate:admitted",
                reference_trade_date=date(2026, 1, 3),
                requested_weight=0.10,
                admitted_weight=0.10,
                trimmed_weight=0.0,
                plan_status="admitted",
            ),
            _plan_row(
                plan_snapshot_nk="plan:trimmed",
                candidate_nk="candidate:trimmed",
                reference_trade_date=date(2026, 1, 3),
                requested_weight=0.10,
                admitted_weight=0.03,
                trimmed_weight=0.07,
                plan_status="trimmed",
                blocking_reason_code="portfolio_capacity_trimmed",
            ),
            _plan_row(
                plan_snapshot_nk="plan:blocked",
                candidate_nk="candidate:blocked",
                reference_trade_date=date(2026, 1, 3),
                requested_weight=0.10,
                admitted_weight=0.0,
                trimmed_weight=0.0,
                plan_status="blocked",
                blocking_reason_code="portfolio_capacity_exhausted",
            ),
        ],
    )

    summary = run_trade_from_portfolio_plan()

    assert isinstance(summary, TradeRunSummary)
    assert summary.status == "completed"
    assert summary.materialization_counts["intents_inserted"] == 3
    assert summary.materialization_counts["executions_inserted"] == 3
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        intents = connection.execute(
            """
            SELECT candidate_nk, intent_status, planned_trade_date, execution_weight
            FROM trade_order_intent
            ORDER BY candidate_nk
            """
        ).fetchall()
        executions = connection.execute(
            """
            SELECT candidate_nk, execution_status, execution_trade_date, execution_price,
                   executed_weight, source_price_line, execution.blocking_reason_code
            FROM trade_order_execution AS execution
            INNER JOIN trade_order_intent AS intent
                ON intent.order_intent_nk = execution.order_intent_nk
            ORDER BY candidate_nk
            """
        ).fetchall()

    assert intents == [
        ("candidate:admitted", "planned", date(2026, 1, 4), 0.10),
        ("candidate:blocked", "blocked", date(2026, 1, 4), 0.0),
        ("candidate:trimmed", "planned", date(2026, 1, 4), 0.03),
    ]
    assert executions == [
        ("candidate:admitted", "filled", date(2026, 1, 4), 11.1, 0.10, "execution_price_line", None),
        (
            "candidate:blocked",
            "rejected",
            date(2026, 1, 4),
            11.1,
            0.0,
            "execution_price_line",
            "portfolio_capacity_exhausted",
        ),
        ("candidate:trimmed", "filled", date(2026, 1, 4), 11.1, 0.03, "execution_price_line", None),
    ]


def test_trade_runner_rejects_non_open_missing_next_day_and_missing_open(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", None),
            ("BBB", "2026-01-03T00:00:00", 20.0),
            ("CCC", "2026-01-03T00:00:00", 30.0),
            ("CCC", "2026-01-04T00:00:00", 31.0),
        ],
    )
    _write_portfolio_plan_snapshot(
        workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb",
        [
            _plan_row(
                plan_snapshot_nk="plan:missing-open",
                candidate_nk="candidate:missing-open",
                symbol="AAA",
                reference_trade_date=date(2026, 1, 3),
                plan_status="admitted",
                admitted_weight=0.10,
            ),
            _plan_row(
                plan_snapshot_nk="plan:missing-next",
                candidate_nk="candidate:missing-next",
                symbol="BBB",
                reference_trade_date=date(2026, 1, 3),
                plan_status="admitted",
                admitted_weight=0.10,
            ),
            _plan_row(
                plan_snapshot_nk="plan:not-open",
                candidate_nk="candidate:not-open",
                symbol="CCC",
                reference_trade_date=date(2026, 1, 3),
                position_action_decision="blocked",
                plan_status="admitted",
                admitted_weight=0.10,
            ),
        ],
    )

    run_trade_from_portfolio_plan()

    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        rows = connection.execute(
            """
            SELECT intent.candidate_nk, execution.execution_status, execution.blocking_reason_code
            FROM trade_order_execution AS execution
            INNER JOIN trade_order_intent AS intent
                ON intent.order_intent_nk = execution.order_intent_nk
            ORDER BY intent.candidate_nk
            """
        ).fetchall()

    assert rows == [
        ("candidate:missing-next", "rejected", "missing_next_execution_trade_date"),
        ("candidate:missing-open", "rejected", "missing_execution_open_price"),
        ("candidate:not-open", "rejected", "unsupported_position_action"),
    ]


def test_trade_runner_reuses_same_input_and_rematerializes_changed_work_unit(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    market_path = workspace / "data" / "base" / "market_base.duckdb"
    portfolio_plan_path = workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"
    _write_market_base_day(
        market_path,
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", 11.1),
        ],
    )
    _write_portfolio_plan_snapshot(
        portfolio_plan_path,
        [
            _plan_row(
                plan_snapshot_nk="plan:admitted",
                candidate_nk="candidate:admitted",
                reference_trade_date=date(2026, 1, 3),
                requested_weight=0.10,
                admitted_weight=0.10,
                trimmed_weight=0.0,
                plan_status="admitted",
            )
        ],
    )

    first_summary = run_trade_from_portfolio_plan()
    second_summary = run_trade_from_portfolio_plan()
    _update_admitted_weight(portfolio_plan_path, plan_snapshot_nk="plan:admitted", admitted_weight=0.05)
    third_summary = run_trade_from_portfolio_plan()

    assert first_summary.materialization_counts["intents_inserted"] == 1
    assert second_summary.materialization_counts["intents_reused"] == 1
    assert second_summary.checkpoint_summary.work_units_updated == 0
    assert third_summary.materialization_counts["intents_rematerialized"] == 1
    assert third_summary.checkpoint_summary.work_units_updated == 1
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        actions = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT materialization_action FROM trade_run_order_intent"
            ).fetchall()
        }
        execution_weight = connection.execute(
            "SELECT execution_weight FROM trade_order_intent WHERE candidate_nk = 'candidate:admitted'"
        ).fetchone()[0]

    assert {"inserted", "reused", "rematerialized"}.issubset(actions)
    assert execution_weight == 0.05


def _configure_workspace(*, monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)
    return workspace


def _write_market_base_day(database_path: Path, rows: list[tuple[str, str, float | None]]) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS market_base_day")
        connection.execute(
            """
            CREATE TABLE market_base_day (
                symbol TEXT,
                bar_dt TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO market_base_day VALUES (?, ?, ?, ?, ?, ?)",
            [
                (symbol, datetime.fromisoformat(bar_dt), open_price, open_price, open_price, open_price)
                for symbol, bar_dt, open_price in rows
            ],
        )


def _write_portfolio_plan_snapshot(database_path: Path, rows: list[dict[str, object]]) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_portfolio_plan_schema(database_path)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DELETE FROM portfolio_plan_snapshot")
        connection.executemany(
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
                (
                    row["plan_snapshot_nk"],
                    row["candidate_nk"],
                    row["portfolio_id"],
                    row["symbol"],
                    row["reference_trade_date"],
                    row["position_action_decision"],
                    row["requested_weight"],
                    row["admitted_weight"],
                    row["trimmed_weight"],
                    row["plan_status"],
                    row["blocking_reason_code"],
                    0.15,
                    row["admitted_weight"],
                    0.15 - float(row["admitted_weight"]),
                    "stage4_portfolio_plan_v1",
                    "portfolio-run-1",
                    "portfolio-run-1",
                )
                for row in rows
            ],
        )


def _plan_row(
    *,
    plan_snapshot_nk: str,
    candidate_nk: str,
    symbol: str = "AAA",
    reference_trade_date: date,
    position_action_decision: str = "open",
    requested_weight: float = 0.10,
    admitted_weight: float,
    trimmed_weight: float = 0.0,
    plan_status: str,
    blocking_reason_code: str | None = None,
) -> dict[str, object]:
    return {
        "plan_snapshot_nk": plan_snapshot_nk,
        "candidate_nk": candidate_nk,
        "portfolio_id": "core",
        "symbol": symbol,
        "reference_trade_date": reference_trade_date,
        "position_action_decision": position_action_decision,
        "requested_weight": requested_weight,
        "admitted_weight": admitted_weight,
        "trimmed_weight": trimmed_weight,
        "plan_status": plan_status,
        "blocking_reason_code": blocking_reason_code,
    }


def _update_admitted_weight(database_path: Path, *, plan_snapshot_nk: str, admitted_weight: float) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            UPDATE portfolio_plan_snapshot
            SET admitted_weight = ?, requested_weight = ?, updated_at = CURRENT_TIMESTAMP
            WHERE plan_snapshot_nk = ?
            """,
            [admitted_weight, admitted_weight, plan_snapshot_nk],
        )
