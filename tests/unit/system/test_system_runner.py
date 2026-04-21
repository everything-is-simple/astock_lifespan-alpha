from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from astock_lifespan_alpha.system import SystemRunSummary, repair_system_schema, run_system_from_trade
from astock_lifespan_alpha.system import runner as system_runner
from astock_lifespan_alpha.system.schema import SYSTEM_TABLES, initialize_system_schema
from astock_lifespan_alpha.trade.schema import initialize_trade_schema


def test_system_schema_initializes_formal_tables(tmp_path):
    database_path = tmp_path / "system.duckdb"

    initialize_system_schema(database_path)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}

    assert set(SYSTEM_TABLES).issubset(table_names)


def test_system_runner_materializes_trade_readout_and_summary(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    trade_path = workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"
    _write_trade_rows(
        trade_path,
        [
            _trade_row(
                order_intent_nk="intent:filled",
                order_execution_nk="execution:filled",
                symbol="AAA",
                execution_status="filled",
                executed_weight=0.10,
                blocking_reason_code=None,
            ),
            _trade_row(
                order_intent_nk="intent:rejected",
                order_execution_nk="execution:rejected",
                symbol="BBB",
                execution_status="rejected",
                executed_weight=0.0,
                blocking_reason_code="missing_execution_open_price",
            ),
        ],
    )

    summary = run_system_from_trade()

    assert isinstance(summary, SystemRunSummary)
    assert summary.runner_name == "run_system_from_trade"
    assert summary.status == "completed"
    assert summary.readout_rows == 2
    assert summary.summary_rows == 1
    assert summary.checkpoint_summary.work_units_seen == 2
    assert summary.checkpoint_summary.work_units_updated == 2
    assert summary.source_paths["trade"] == str(trade_path)
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "system" / "system.duckdb"),
        read_only=True,
    ) as connection:
        readout = connection.execute(
            """
            SELECT order_intent_nk, order_execution_nk, portfolio_id, symbol,
                   execution_status, executed_weight, blocking_reason_code,
                   system_contract_version
            FROM system_trade_readout
            ORDER BY order_execution_nk
            """
        ).fetchall()
        summary_row = connection.execute(
            """
            SELECT portfolio_id, execution_count, filled_count, rejected_count,
                   symbol_count, gross_executed_weight, latest_execution_trade_date,
                   system_contract_version
            FROM system_portfolio_trade_summary
            """
        ).fetchone()

    assert readout == [
        ("intent:filled", "execution:filled", "core", "AAA", "filled", 0.10, None, "stage6_system_v1"),
        (
            "intent:rejected",
            "execution:rejected",
            "core",
            "BBB",
            "rejected",
            0.0,
            "missing_execution_open_price",
            "stage6_system_v1",
        ),
    ]
    assert summary_row == ("core", 2, 1, 1, 2, 0.10, date(2026, 1, 4), "stage6_system_v1")


def test_system_runner_handles_missing_trade_database(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)

    summary = run_system_from_trade()

    assert summary.status == "completed"
    assert summary.readout_rows == 0
    assert summary.summary_rows == 0
    assert summary.checkpoint_summary.work_units_seen == 0
    assert summary.source_paths["trade"] is None
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "system" / "system.duckdb"),
        read_only=True,
    ) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        run_rows = connection.execute("SELECT status, readout_rows, summary_rows FROM system_run").fetchall()

    assert set(SYSTEM_TABLES).issubset(table_names)
    assert run_rows == [("completed", 0, 0)]


def test_system_runner_rematerializes_portfolio_without_duplicate_readout(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    trade_path = workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"
    _write_trade_rows(
        trade_path,
        [
            _trade_row(
                order_intent_nk="intent:filled",
                order_execution_nk="execution:filled",
                symbol="AAA",
                execution_status="filled",
                executed_weight=0.10,
                blocking_reason_code=None,
            )
        ],
    )

    first_summary = run_system_from_trade()
    second_summary = run_system_from_trade()

    assert first_summary.readout_rows == 1
    assert second_summary.readout_rows == 1
    assert second_summary.checkpoint_summary.work_units_updated == 0
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "system" / "system.duckdb"),
        read_only=True,
    ) as connection:
        readout_count = connection.execute("SELECT COUNT(*) FROM system_trade_readout").fetchone()[0]
        summary_count = connection.execute("SELECT COUNT(*) FROM system_portfolio_trade_summary").fetchone()[0]
        run_count = connection.execute("SELECT COUNT(*) FROM system_run").fetchone()[0]
        queue_rows = connection.execute(
            "SELECT status, source_row_count FROM system_work_queue ORDER BY requested_at DESC LIMIT 1"
        ).fetchall()

    assert readout_count == 1
    assert summary_count == 1
    assert run_count == 2
    assert queue_rows == [("reused", 1)]


def test_system_runner_rolls_back_failed_partial_rematerialization(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    trade_path = workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"
    _write_trade_rows(
        trade_path,
        [
            _trade_row(
                order_intent_nk="intent:filled",
                order_execution_nk="execution:filled",
                symbol="AAA",
                execution_status="filled",
                executed_weight=0.10,
                blocking_reason_code=None,
            )
        ],
    )

    run_system_from_trade()
    with duckdb.connect(str(trade_path)) as connection:
        connection.execute(
            """
            UPDATE trade_order_execution
            SET executed_weight = 0.05, last_materialized_run_id = 'trade-run-2'
            WHERE order_execution_nk = 'execution:filled'
            """
        )

    system_path = workspace / "data" / "astock_lifespan_alpha" / "system" / "system.duckdb"
    with duckdb.connect(str(system_path), read_only=True) as connection:
        before_rows = connection.execute(
            """
            SELECT order_execution_nk, executed_weight
            FROM system_trade_readout
            ORDER BY order_execution_nk
            """
        ).fetchall()

    def fail_checkpoint(*, connection, run_id):
        raise RuntimeError("checkpoint write failed")

    monkeypatch.setattr(system_runner, "_upsert_system_checkpoint_sql", fail_checkpoint)
    with pytest.raises(RuntimeError, match="checkpoint write failed"):
        run_system_from_trade()

    with duckdb.connect(str(system_path), read_only=True) as connection:
        after_rows = connection.execute(
            """
            SELECT order_execution_nk, executed_weight
            FROM system_trade_readout
            ORDER BY order_execution_nk
            """
        ).fetchall()
        latest_run = connection.execute(
            "SELECT status FROM system_run ORDER BY started_at DESC LIMIT 1"
        ).fetchone()[0]

    assert after_rows == before_rows
    assert latest_run == "interrupted"


def test_repair_system_schema_is_idempotent(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    trade_path = workspace / "data" / "astock_lifespan_alpha" / "trade" / "trade.duckdb"
    _write_trade_rows(
        trade_path,
        [
            _trade_row(
                order_intent_nk="intent:filled",
                order_execution_nk="execution:filled",
                symbol="AAA",
                execution_status="filled",
                executed_weight=0.10,
                blocking_reason_code=None,
            )
        ],
    )

    run_system_from_trade()
    system_path = workspace / "data" / "astock_lifespan_alpha" / "system" / "system.duckdb"
    with duckdb.connect(str(system_path)) as connection:
        connection.execute("DELETE FROM system_checkpoint")

    first_summary = repair_system_schema()
    second_summary = repair_system_schema()

    assert first_summary.checkpoint_rows_backfilled == 1
    assert second_summary.checkpoint_rows_backfilled == 0
    with duckdb.connect(str(system_path), read_only=True) as connection:
        checkpoint_rows = connection.execute(
            "SELECT portfolio_id, symbol FROM system_checkpoint"
        ).fetchall()

    assert checkpoint_rows == [("core", "AAA")]


def _configure_workspace(*, monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)
    return workspace


def _write_trade_rows(database_path: Path, rows: list[dict[str, object]]) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_trade_schema(database_path)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DELETE FROM trade_order_execution")
        connection.execute("DELETE FROM trade_order_intent")
        for row in rows:
            connection.execute(
                """
                INSERT INTO trade_order_intent (
                    order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
                    reference_trade_date, planned_trade_date, position_action_decision,
                    intent_status, requested_weight, admitted_weight, execution_weight,
                    blocking_reason_code, trade_contract_version,
                    first_seen_run_id, last_materialized_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["order_intent_nk"],
                    row["plan_snapshot_nk"],
                    row["candidate_nk"],
                    row["portfolio_id"],
                    row["symbol"],
                    row["reference_trade_date"],
                    row["planned_trade_date"],
                    row["position_action_decision"],
                    row["intent_status"],
                    row["requested_weight"],
                    row["admitted_weight"],
                    row["execution_weight"],
                    row["intent_blocking_reason_code"],
                    "stage5_trade_v1",
                    "trade-run-1",
                    "trade-run-1",
                ],
            )
            connection.execute(
                """
                INSERT INTO trade_order_execution (
                    order_execution_nk, order_intent_nk, portfolio_id, symbol,
                    execution_status, execution_trade_date, execution_price,
                    executed_weight, blocking_reason_code, source_price_line,
                    trade_contract_version, first_seen_run_id, last_materialized_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["order_execution_nk"],
                    row["order_intent_nk"],
                    row["portfolio_id"],
                    row["symbol"],
                    row["execution_status"],
                    row["execution_trade_date"],
                    row["execution_price"],
                    row["executed_weight"],
                    row["execution_blocking_reason_code"],
                    "execution_price_line",
                    "stage5_trade_v1",
                    "trade-run-1",
                    "trade-run-1",
                ],
            )


def _trade_row(
    *,
    order_intent_nk: str,
    order_execution_nk: str,
    symbol: str,
    execution_status: str,
    executed_weight: float,
    blocking_reason_code: str | None,
) -> dict[str, object]:
    return {
        "order_intent_nk": order_intent_nk,
        "order_execution_nk": order_execution_nk,
        "plan_snapshot_nk": f"plan:{symbol}",
        "candidate_nk": f"candidate:{symbol}",
        "portfolio_id": "core",
        "symbol": symbol,
        "reference_trade_date": date(2026, 1, 3),
        "planned_trade_date": date(2026, 1, 4),
        "position_action_decision": "open",
        "intent_status": "planned" if execution_status == "filled" else "blocked",
        "requested_weight": 0.10,
        "admitted_weight": executed_weight,
        "execution_weight": executed_weight,
        "intent_blocking_reason_code": blocking_reason_code,
        "execution_status": execution_status,
        "execution_trade_date": date(2026, 1, 4),
        "execution_price": 11.1 if execution_status == "filled" else None,
        "executed_weight": executed_weight,
        "execution_blocking_reason_code": blocking_reason_code,
    }
