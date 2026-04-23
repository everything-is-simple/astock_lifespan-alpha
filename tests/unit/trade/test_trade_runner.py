from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import default_settings
from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema
from astock_lifespan_alpha.trade import TradeRunSummary, run_trade_from_portfolio_plan
from astock_lifespan_alpha.trade.runner import profile_trade_live_path
from astock_lifespan_alpha.trade.schema import TRADE_TABLES, initialize_trade_schema
from astock_lifespan_alpha.trade.source import load_trade_source_rows


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
        position_legs = connection.execute(
            """
            SELECT candidate_nk, position_state, active_weight
            FROM trade_position_leg
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
            None,
            0.0,
            "execution_price_line",
            "portfolio_capacity_exhausted",
        ),
        ("candidate:trimmed", "filled", date(2026, 1, 4), 11.1, 0.03, "execution_price_line", None),
    ]
    assert position_legs == [
        ("candidate:admitted", "open", 0.10),
        ("candidate:trimmed", "open", 0.03),
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


def test_trade_runner_materializes_known_blocked_rows_without_execution_prices(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_portfolio_plan_snapshot(
        workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb",
        [
            _plan_row(
                plan_snapshot_nk="plan:blocked-no-market",
                candidate_nk="candidate:blocked-no-market",
                reference_trade_date=date(2026, 1, 3),
                admitted_weight=0.0,
                plan_status="blocked",
                blocking_reason_code="portfolio_capacity_exhausted",
            ),
        ],
    )

    summary = run_trade_from_portfolio_plan()

    assert summary.status == "completed"
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        rows = connection.execute(
            """
            SELECT intent.candidate_nk, intent.intent_status, execution.execution_status, execution.blocking_reason_code
            FROM trade_order_intent AS intent
            INNER JOIN trade_order_execution AS execution
                ON execution.order_intent_nk = intent.order_intent_nk
            """
        ).fetchall()

    assert rows == [
        ("candidate:blocked-no-market", "blocked", "rejected", "portfolio_capacity_exhausted"),
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


def test_trade_runner_materializes_exit_and_carry_rows(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", 11.1),
            ("AAA", "2026-01-05T00:00:00", 10.7),
        ],
    )
    _write_portfolio_plan_snapshot(
        workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb",
        [
            _plan_row(
                plan_snapshot_nk="plan:exit",
                candidate_nk="candidate:exit",
                reference_trade_date=date(2026, 1, 3),
                planned_entry_trade_date=date(2026, 1, 4),
                scheduled_exit_trade_date=date(2026, 1, 5),
                planned_exit_reason_code="signal_not_confirmed_exit",
                admitted_weight=0.10,
                plan_status="admitted",
            )
        ],
    )

    summary = run_trade_from_portfolio_plan()

    assert summary.materialization_counts["position_legs_inserted"] == 1
    assert summary.materialization_counts["carry_rows_inserted"] == 2
    assert summary.materialization_counts["exit_rows_inserted"] == 1
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        exit_rows = connection.execute(
            """
            SELECT candidate_nk, execution_status, exit_trade_date, exited_weight, exit_reason_code
            FROM trade_exit_execution
            """
        ).fetchall()
        leg_rows = connection.execute(
            """
            SELECT candidate_nk, position_state, exit_trade_date, active_weight
            FROM trade_position_leg
            """
        ).fetchall()
        carry_rows = connection.execute(
            """
            SELECT carry_status, as_of_trade_date, carried_weight
            FROM trade_carry_snapshot
            ORDER BY as_of_trade_date, carry_snapshot_nk
            """
        ).fetchall()

    assert exit_rows == [
        ("candidate:exit", "filled", date(2026, 1, 5), 0.10, "signal_not_confirmed_exit"),
    ]
    assert leg_rows == [("candidate:exit", "closed", date(2026, 1, 5), 0.0)]
    assert carry_rows == [("open", date(2026, 1, 4), 0.10), ("closed", date(2026, 1, 5), 0.0)]


def test_trade_runner_reuses_and_rematerializes_multi_row_work_unit(monkeypatch, tmp_path):
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
                plan_snapshot_nk="plan:multi:1",
                candidate_nk="candidate:multi:1",
                symbol="AAA",
                reference_trade_date=date(2026, 1, 3),
                requested_weight=0.10,
                admitted_weight=0.10,
                trimmed_weight=0.0,
                plan_status="admitted",
            ),
            _plan_row(
                plan_snapshot_nk="plan:multi:2",
                candidate_nk="candidate:multi:2",
                symbol="AAA",
                reference_trade_date=date(2026, 1, 3),
                requested_weight=0.10,
                admitted_weight=0.03,
                trimmed_weight=0.07,
                plan_status="trimmed",
                blocking_reason_code="portfolio_capacity_trimmed",
            ),
        ],
    )

    first_summary = run_trade_from_portfolio_plan()
    second_summary = run_trade_from_portfolio_plan()
    _update_admitted_weight(portfolio_plan_path, plan_snapshot_nk="plan:multi:1", admitted_weight=0.05)
    third_summary = run_trade_from_portfolio_plan()

    assert first_summary.materialization_counts["intents_inserted"] == 2
    assert second_summary.materialization_counts["intents_reused"] == 2
    assert second_summary.checkpoint_summary.work_units_updated == 0
    assert third_summary.checkpoint_summary.work_units_updated == 1
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        latest_actions = connection.execute(
            """
            SELECT intent.candidate_nk, run_intent.materialization_action
            FROM trade_run_order_intent AS run_intent
            INNER JOIN trade_order_intent AS intent
                ON intent.order_intent_nk = run_intent.order_intent_nk
            WHERE run_intent.run_id = ?
            ORDER BY candidate_nk
            """,
            [third_summary.run_id],
        ).fetchall()
        weights = connection.execute(
            """
            SELECT candidate_nk, execution_weight
            FROM trade_order_intent
            WHERE candidate_nk LIKE 'candidate:multi:%'
            ORDER BY candidate_nk
            """
        ).fetchall()

    assert latest_actions == [
        ("candidate:multi:1", "rematerialized"),
        ("candidate:multi:2", "reused"),
    ]
    assert weights == [("candidate:multi:1", 0.05), ("candidate:multi:2", 0.03)]


def test_trade_runner_forces_full_refresh_when_legacy_trade_shape_matches_checkpoint(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    market_path = workspace / "data" / "base" / "market_base.duckdb"
    portfolio_plan_path = workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"
    trade_path = workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"
    _write_market_base_day(
        market_path,
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", 11.1),
            ("AAA", "2026-01-05T00:00:00", 10.7),
        ],
    )
    _write_portfolio_plan_snapshot(
        portfolio_plan_path,
        [
            _plan_row(
                plan_snapshot_nk="plan:legacy",
                candidate_nk="candidate:legacy",
                reference_trade_date=date(2026, 1, 3),
                planned_entry_trade_date=date(2026, 1, 4),
                scheduled_exit_trade_date=date(2026, 1, 5),
                planned_exit_reason_code="signal_not_confirmed_exit",
                admitted_weight=0.10,
                plan_status="admitted",
            )
        ],
    )
    initial_summary = run_trade_from_portfolio_plan()
    _reset_trade_to_legacy_shape(trade_path, last_run_id=initial_summary.run_id)

    summary = run_trade_from_portfolio_plan()

    assert summary.checkpoint_summary.work_units_updated == 1
    assert summary.materialization_counts["intents_reused"] == 1
    assert summary.materialization_counts["executions_reused"] == 1
    assert summary.materialization_counts["position_legs_inserted"] == 1
    assert summary.materialization_counts["carry_rows_inserted"] == 2
    assert summary.materialization_counts["exit_rows_inserted"] == 1
    with duckdb.connect(str(trade_path), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM trade_position_leg").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM trade_carry_snapshot").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM trade_exit_execution").fetchone()[0] == 1


def test_trade_runner_records_phase_progress_message(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", 11.1),
        ],
    )
    _write_portfolio_plan_snapshot(
        workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb",
        [
            _plan_row(
                plan_snapshot_nk="plan:phase",
                candidate_nk="candidate:phase",
                reference_trade_date=date(2026, 1, 3),
                admitted_weight=0.10,
                plan_status="admitted",
            ),
        ],
    )

    summary = run_trade_from_portfolio_plan()

    assert "write_transaction_committed" in summary.message
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"),
        read_only=True,
    ) as connection:
        message = connection.execute(
            "SELECT message FROM trade_run WHERE run_id = ?",
            [summary.run_id],
        ).fetchone()[0]

    assert "write_transaction_committed" in message


def test_profile_trade_live_path_reports_phase_timings(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-03T00:00:00", 10.0),
            ("AAA", "2026-01-04T00:00:00", 11.1),
            ("AAA", "2026-01-05T00:00:00", 10.7),
        ],
    )
    _write_portfolio_plan_snapshot(
        workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb",
        [
            _plan_row(
                plan_snapshot_nk="plan:profile:1",
                candidate_nk="candidate:profile:1",
                reference_trade_date=date(2026, 1, 3),
                admitted_weight=0.10,
                plan_status="admitted",
            ),
            _plan_row(
                plan_snapshot_nk="plan:profile:2",
                candidate_nk="candidate:profile:2",
                reference_trade_date=date(2026, 1, 3),
                admitted_weight=0.0,
                plan_status="blocked",
                blocking_reason_code="portfolio_capacity_exhausted",
            ),
        ],
    )
    run_trade_from_portfolio_plan()

    summary = profile_trade_live_path(settings=default_settings(repo_root=workspace / "repo"))

    assert summary["runner_name"] == "profile_trade_live_path"
    assert summary["source_row_count"] == 2
    assert summary["work_units_seen"] == 1
    phases = {phase["phase"] for phase in summary["phase_timings"]}
    assert {"source_attached", "work_unit_summary_ready", "intent_materialized", "action_tables_ready"}.issubset(phases)
    assert summary["dominant_phase"] in phases


def test_trade_source_reads_stock_daily_adjusted_code_trade_date(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_daily_adjusted(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", date(2026, 1, 3), 10.0),
            ("AAA", date(2026, 1, 4), 11.1),
        ],
    )

    dataset = load_trade_source_rows(settings=default_settings(repo_root=workspace / "repo"), portfolio_id="core")

    assert dataset.execution_price_source_path == workspace / "data" / "base" / "market_base.duckdb"
    assert dataset.execution_prices_by_symbol["AAA"][0].trade_date == date(2026, 1, 3)
    assert dataset.execution_prices_by_symbol["AAA"][1].open_price == 11.1


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


def _write_stock_daily_adjusted(database_path: Path, rows: list[tuple[str, date, float | None]]) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS stock_daily_adjusted")
        connection.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?)",
            [
                (symbol, trade_date, open_price, open_price, open_price, open_price)
                for symbol, trade_date, open_price in rows
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
                planned_entry_trade_date, scheduled_exit_trade_date, position_action_decision,
                requested_weight, admitted_weight, trimmed_weight,
                plan_status, blocking_reason_code, planned_exit_reason_code, portfolio_gross_cap_weight,
                current_portfolio_gross_weight, remaining_portfolio_capacity_weight,
                portfolio_gross_used_weight, portfolio_gross_remaining_weight,
                portfolio_plan_contract_version, first_seen_run_id, last_materialized_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["plan_snapshot_nk"],
                    row["candidate_nk"],
                    row["portfolio_id"],
                    row["symbol"],
                    row["reference_trade_date"],
                    row["planned_entry_trade_date"],
                    row["scheduled_exit_trade_date"],
                    row["position_action_decision"],
                    row["requested_weight"],
                    row["admitted_weight"],
                    row["trimmed_weight"],
                    row["plan_status"],
                    row["blocking_reason_code"],
                    row["planned_exit_reason_code"],
                    row["portfolio_gross_cap_weight"],
                    row["current_portfolio_gross_weight"],
                    row["remaining_portfolio_capacity_weight"],
                    row["portfolio_gross_used_weight"],
                    row["portfolio_gross_remaining_weight"],
                    "stage4_portfolio_plan_v2",
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
    planned_entry_trade_date: date | None = None,
    scheduled_exit_trade_date: date | None = None,
    position_action_decision: str = "open",
    requested_weight: float = 0.10,
    admitted_weight: float,
    trimmed_weight: float = 0.0,
    plan_status: str,
    blocking_reason_code: str | None = None,
    planned_exit_reason_code: str | None = None,
    portfolio_gross_cap_weight: float = 0.50,
    current_portfolio_gross_weight: float = 0.0,
    remaining_portfolio_capacity_weight: float | None = None,
) -> dict[str, object]:
    final_admitted_weight = admitted_weight if plan_status in {"admitted", "trimmed"} else 0.0
    remaining_capacity = (
        remaining_portfolio_capacity_weight
        if remaining_portfolio_capacity_weight is not None
        else round(max(portfolio_gross_cap_weight - (current_portfolio_gross_weight + final_admitted_weight), 0.0), 4)
    )
    return {
        "plan_snapshot_nk": plan_snapshot_nk,
        "candidate_nk": candidate_nk,
        "portfolio_id": "core",
        "symbol": symbol,
        "reference_trade_date": reference_trade_date,
        "planned_entry_trade_date": planned_entry_trade_date or (reference_trade_date + timedelta(days=1)),
        "scheduled_exit_trade_date": scheduled_exit_trade_date,
        "position_action_decision": position_action_decision,
        "requested_weight": requested_weight,
        "admitted_weight": admitted_weight,
        "trimmed_weight": trimmed_weight,
        "plan_status": plan_status,
        "blocking_reason_code": blocking_reason_code,
        "planned_exit_reason_code": planned_exit_reason_code,
        "portfolio_gross_cap_weight": portfolio_gross_cap_weight,
        "current_portfolio_gross_weight": current_portfolio_gross_weight,
        "remaining_portfolio_capacity_weight": remaining_capacity,
        "portfolio_gross_used_weight": round(current_portfolio_gross_weight + final_admitted_weight, 4),
        "portfolio_gross_remaining_weight": remaining_capacity,
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


def _reset_trade_to_legacy_shape(database_path: Path, *, last_run_id: str) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DELETE FROM trade_position_leg")
        connection.execute("DELETE FROM trade_carry_snapshot")
        connection.execute("DELETE FROM trade_exit_execution")
        connection.execute(
            """
            UPDATE trade_checkpoint
            SET last_run_id = ?, updated_at = CURRENT_TIMESTAMP
            """,
            [last_run_id],
        )
