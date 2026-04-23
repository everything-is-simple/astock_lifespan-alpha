"""Stage-five trade runner."""

from __future__ import annotations

import ctypes
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Callable
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.trade.contracts import (
    EXECUTION_PRICE_LINE,
    TRADE_CONTRACT_VERSION,
    TradeCheckpointSummary,
    TradeRunStatus,
    TradeRunSummary,
)
from astock_lifespan_alpha.trade.schema import initialize_trade_schema


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")
TRADE_DELETE_WORK_UNIT_BATCH_SIZE = 250
TRADE_CUTOVER_TARGET_TABLES = (
    "trade_order_intent",
    "trade_order_execution",
    "trade_position_leg",
    "trade_carry_snapshot",
    "trade_exit_execution",
    "trade_work_queue",
    "trade_checkpoint",
    "trade_run_order_intent",
)
TRADE_SECONDARY_INDEXES = (
    ("idx_trade_intent_work_unit", "trade_order_intent", "(portfolio_id, symbol, reference_trade_date)"),
    ("idx_trade_execution_work_unit", "trade_order_execution", "(portfolio_id, symbol, execution_trade_date)"),
    ("idx_trade_position_leg_work_unit", "trade_position_leg", "(portfolio_id, symbol, entry_trade_date)"),
)


@dataclass(frozen=True)
class _TradeSourceMetadata:
    portfolio_plan_source_path: Path | None
    execution_price_source_path: Path | None
    row_count: int
    work_unit_count: int


@dataclass(frozen=True)
class _TradePhaseMetric:
    phase: str
    elapsed_seconds: float
    row_count: int | None = None
    rss_mb: float | None = None
    detail: str | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "phase": self.phase,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
        }
        if self.row_count is not None:
            payload["row_count"] = self.row_count
        if self.rss_mb is not None:
            payload["rss_mb"] = round(self.rss_mb, 2)
        if self.detail is not None:
            payload["detail"] = self.detail
        return payload


class _TradePhaseRecorder:
    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
        run_id: str | None = None,
        emit_stderr: bool = False,
    ) -> None:
        self._connection = connection
        self._run_id = run_id
        self._emit_stderr = emit_stderr
        self.metrics: list[_TradePhaseMetric] = []
        self.latest_message: str | None = None

    def record(
        self,
        phase: str,
        *,
        elapsed_seconds: float,
        row_count: int | None = None,
        detail: str | None = None,
    ) -> None:
        metric = _TradePhaseMetric(
            phase=phase,
            elapsed_seconds=elapsed_seconds,
            row_count=row_count,
            rss_mb=_working_set_mb(),
            detail=detail,
        )
        self.metrics.append(metric)
        message = _format_trade_phase_message(metric)
        self.latest_message = message
        if self._connection is not None and self._run_id is not None:
            self._connection.execute(
                "UPDATE trade_run SET message = ? WHERE run_id = ?",
                [message, self._run_id],
            )
        if self._emit_stderr:
            print(message, file=sys.stderr, flush=True)


class _ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def _working_set_mb() -> float | None:
    if sys.platform != "win32":
        return None
    try:
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(_ProcessMemoryCounters)
        if not ctypes.windll.psapi.GetProcessMemoryInfo(
            ctypes.windll.kernel32.GetCurrentProcess(),
            ctypes.byref(counters),
            counters.cb,
        ):
            return None
        return counters.WorkingSetSize / (1024 * 1024)
    except Exception:
        return None


def _format_trade_phase_message(metric: _TradePhaseMetric) -> str:
    parts = [metric.phase, f"elapsed_seconds={metric.elapsed_seconds:.6f}"]
    if metric.row_count is not None:
        parts.append(f"rows={metric.row_count}")
    if metric.rss_mb is not None:
        parts.append(f"rss_mb={metric.rss_mb:.2f}")
    if metric.detail:
        parts.append(metric.detail)
    return "trade phase " + " ".join(parts)


def _table_row_count(*, connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)


def _record_phase(
    *,
    connection: duckdb.DuckDBPyConnection,
    phase_observer: Callable[..., None] | None,
    phase: str,
    started_at: float,
    table_name: str | None = None,
    detail: str | None = None,
) -> int | None:
    row_count = _table_row_count(connection=connection, table_name=table_name) if table_name is not None else None
    if phase_observer is not None:
        phase_observer(
            phase,
            elapsed_seconds=perf_counter() - started_at,
            row_count=row_count,
            detail=detail,
        )
    return row_count


def run_trade_from_portfolio_plan(
    *,
    portfolio_id: str = "core",
    settings: WorkspaceRoots | None = None,
) -> TradeRunSummary:
    """Build the minimal rolling portfolio_plan -> trade execution ledger."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.trade
    initialize_trade_schema(target_path)

    run_id = f"trade-{uuid4().hex[:12]}"
    message = "trade run completed."
    counts = {
        "intents_inserted": 0,
        "intents_reused": 0,
        "intents_rematerialized": 0,
        "executions_inserted": 0,
        "executions_reused": 0,
        "executions_rematerialized": 0,
        "position_legs_inserted": 0,
        "position_legs_reused": 0,
        "position_legs_rematerialized": 0,
        "carry_rows_inserted": 0,
        "carry_rows_reused": 0,
        "carry_rows_rematerialized": 0,
        "exit_rows_inserted": 0,
        "exit_rows_reused": 0,
        "exit_rows_rematerialized": 0,
    }
    work_units_updated = 0
    latest_reference_trade_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        source = _attach_trade_source_views(connection=connection, workspace=workspace, portfolio_id=portfolio_id)
        connection.execute(
            """
            INSERT INTO trade_run (
                run_id, status, portfolio_id, source_portfolio_plan_path, source_execution_price_path,
                input_rows, work_units_seen, message
            ) VALUES (?, 'running', ?, ?, ?, ?, ?, 'trade run started.')
            """,
            [
                run_id,
                portfolio_id,
                str(source.portfolio_plan_source_path) if source.portfolio_plan_source_path is not None else None,
                str(source.execution_price_source_path) if source.execution_price_source_path is not None else None,
                source.row_count,
                source.work_unit_count,
            ],
        )
        phase_recorder = _TradePhaseRecorder(connection=connection, run_id=run_id, emit_stderr=True)
        phase_recorder.record(
            "source_attached",
            elapsed_seconds=0.0,
            row_count=source.row_count,
            detail=f"work_units={source.work_unit_count}",
        )
        connection.execute("DELETE FROM trade_work_queue")

        if source.row_count == 0:
            message = "trade schema initialized without portfolio_plan rows."
        else:
            counts, work_units_updated, latest_reference_trade_date = _materialize_trade_sql(
                connection=connection,
                run_id=run_id,
                phase_observer=phase_recorder.record,
            )
            message = phase_recorder.latest_message or message

        connection.execute(
            """
            UPDATE trade_run
            SET
                status = ?,
                work_units_updated = ?,
                inserted_order_intents = ?,
                reused_order_intents = ?,
                rematerialized_order_intents = ?,
                inserted_order_executions = ?,
                reused_order_executions = ?,
                rematerialized_order_executions = ?,
                inserted_position_legs = ?,
                reused_position_legs = ?,
                rematerialized_position_legs = ?,
                inserted_carry_rows = ?,
                reused_carry_rows = ?,
                rematerialized_carry_rows = ?,
                inserted_exit_rows = ?,
                reused_exit_rows = ?,
                rematerialized_exit_rows = ?,
                latest_reference_trade_date = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                TradeRunStatus.COMPLETED.value,
                work_units_updated,
                counts["intents_inserted"],
                counts["intents_reused"],
                counts["intents_rematerialized"],
                counts["executions_inserted"],
                counts["executions_reused"],
                counts["executions_rematerialized"],
                counts["position_legs_inserted"],
                counts["position_legs_reused"],
                counts["position_legs_rematerialized"],
                counts["carry_rows_inserted"],
                counts["carry_rows_reused"],
                counts["carry_rows_rematerialized"],
                counts["exit_rows_inserted"],
                counts["exit_rows_reused"],
                counts["exit_rows_rematerialized"],
                latest_reference_trade_date,
                message,
                run_id,
            ],
        )

    return TradeRunSummary(
        runner_name="run_trade_from_portfolio_plan",
        run_id=run_id,
        status=TradeRunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={
            "portfolio_plan": str(source.portfolio_plan_source_path) if source.portfolio_plan_source_path else None,
            "execution_price_line": str(source.execution_price_source_path) if source.execution_price_source_path else None,
        },
        message=message,
        materialization_counts=counts,
        checkpoint_summary=TradeCheckpointSummary(
            work_units_seen=source.work_unit_count,
            work_units_updated=work_units_updated,
            latest_reference_trade_date=latest_reference_trade_date.isoformat()
            if latest_reference_trade_date is not None
            else None,
        ),
    )


def _attach_trade_source_views(
    *,
    connection: duckdb.DuckDBPyConnection,
    workspace: WorkspaceRoots,
    portfolio_id: str,
) -> _TradeSourceMetadata:
    portfolio_plan_path = workspace.databases.portfolio_plan if workspace.databases.portfolio_plan.exists() else None
    execution_price_path = workspace.source_databases.market_base if workspace.source_databases.market_base.exists() else None
    if portfolio_plan_path is None:
        return _TradeSourceMetadata(
            portfolio_plan_source_path=portfolio_plan_path,
            execution_price_source_path=execution_price_path,
            row_count=0,
            work_unit_count=0,
        )

    connection.execute(f"ATTACH {_duckdb_string_literal(portfolio_plan_path)} AS trade_plan_source (READ_ONLY)")
    if not _attached_table_exists(connection=connection, catalog="trade_plan_source", table_name="portfolio_plan_snapshot"):
        return _TradeSourceMetadata(
            portfolio_plan_source_path=portfolio_plan_path,
            execution_price_source_path=execution_price_path,
            row_count=0,
            work_unit_count=0,
        )
    portfolio_id_literal = _duckdb_string_literal(portfolio_id)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW trade_plan_source_rows AS
        SELECT
            plan_snapshot_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            CAST(reference_trade_date AS DATE) AS reference_trade_date,
            CAST(planned_entry_trade_date AS DATE) AS planned_entry_trade_date,
            CAST(scheduled_exit_trade_date AS DATE) AS scheduled_exit_trade_date,
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            blocking_reason_code,
            planned_exit_reason_code
        FROM trade_plan_source.portfolio_plan_snapshot
        WHERE portfolio_id = {portfolio_id_literal}
        """
    )
    if execution_price_path is not None:
        connection.execute(f"ATTACH {_duckdb_string_literal(execution_price_path)} AS trade_price_source (READ_ONLY)")
        market_source = _resolve_market_source(connection=connection, catalog="trade_price_source")
        if market_source is not None:
            connection.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW trade_execution_price_source AS
                {_market_select_sql(market_source, catalog="trade_price_source")}
                """
            )
    if not _temp_view_exists(connection=connection, view_name="trade_execution_price_source"):
        connection.execute(
            """
            CREATE OR REPLACE TEMP VIEW trade_execution_price_source AS
            SELECT
                CAST(NULL AS VARCHAR) AS symbol,
                CAST(NULL AS DATE) AS trade_date,
                CAST(NULL AS DOUBLE) AS open_price
            WHERE FALSE
            """
        )
    row_count, work_unit_count = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT portfolio_id || ':' || symbol) FROM trade_plan_source_rows"
    ).fetchone()
    return _TradeSourceMetadata(
        portfolio_plan_source_path=portfolio_plan_path,
        execution_price_source_path=execution_price_path,
        row_count=int(row_count),
        work_unit_count=int(work_unit_count),
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
        if "open" not in column_names:
            return None
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
            CAST(open AS DOUBLE) AS open_price
        FROM {catalog}.{source.table_name}
        {adjust_filter}
    """


def _materialize_trade_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    phase_observer: Callable[..., None] | None = None,
    allow_fast_path: bool = True,
    write_outputs: bool = True,
    existing_catalog: str | None = None,
) -> tuple[dict[str, int], int, date | None]:
    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_source_work_unit_rows AS
        SELECT
            portfolio_id,
            symbol,
            reference_trade_date,
            hash(
                plan_snapshot_nk,
                candidate_nk,
                reference_trade_date,
                planned_entry_trade_date,
                scheduled_exit_trade_date,
                position_action_decision,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status,
                COALESCE(blocking_reason_code, ''),
                COALESCE(planned_exit_reason_code, '')
            ) AS row_hash_primary,
            hash(
                candidate_nk,
                plan_snapshot_nk,
                planned_entry_trade_date,
                scheduled_exit_trade_date,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status,
                COALESCE(blocking_reason_code, ''),
                COALESCE(planned_exit_reason_code, ''),
                position_action_decision,
                reference_trade_date
            ) AS row_hash_secondary
        FROM trade_plan_source_rows
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_source_work_unit_summary AS
        SELECT
            portfolio_id,
            symbol,
            COUNT(*) AS source_row_count,
            MAX(reference_trade_date) AS last_reference_trade_date,
            md5(
                CONCAT(
                    CAST(COUNT(*) AS VARCHAR), '|',
                    CAST(SUM(CAST(row_hash_primary AS HUGEINT)) AS VARCHAR), '|',
                    CAST(SUM(CAST(row_hash_secondary AS HUGEINT)) AS VARCHAR), '|',
                    CAST(MIN(row_hash_primary) AS VARCHAR), '|',
                    CAST(MAX(row_hash_primary) AS VARCHAR), '|',
                    CAST(MIN(row_hash_secondary) AS VARCHAR), '|',
                    CAST(MAX(row_hash_secondary) AS VARCHAR)
                )
            ) AS source_fingerprint
        FROM trade_source_work_unit_rows
        GROUP BY portfolio_id, symbol
        """
    )
    work_unit_count = _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="work_unit_summary_ready",
        started_at=phase_started,
        table_name="trade_source_work_unit_summary",
        detail=f"source_rows={_table_row_count(connection=connection, table_name='trade_source_work_unit_rows')}",
    )
    if allow_fast_path and _trade_checkpoint_fast_path_available(connection=connection):
        return _record_reused_trade_sql(
            connection=connection,
            run_id=run_id,
            phase_observer=phase_observer,
            work_unit_count=work_unit_count or 0,
        )

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_direct_blocked_source AS
        SELECT
            plan.*,
            CASE
                WHEN plan.plan_status = 'blocked' THEN COALESCE(plan.blocking_reason_code, 'plan_blocked')
                WHEN plan.plan_status NOT IN ('admitted', 'trimmed') THEN COALESCE(plan.blocking_reason_code, 'unsupported_plan_status')
                WHEN plan.position_action_decision != 'open' THEN 'unsupported_position_action'
                WHEN plan.admitted_weight <= 0 THEN 'invalid_admitted_weight'
            END AS trade_blocking_reason_code
        FROM trade_plan_source_rows AS plan
        WHERE plan.plan_status = 'blocked'
            OR plan.plan_status NOT IN ('admitted', 'trimmed')
            OR plan.position_action_decision != 'open'
            OR plan.admitted_weight <= 0
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_price_candidate_source AS
        SELECT plan.*
        FROM trade_plan_source_rows AS plan
        WHERE plan.plan_status IN ('admitted', 'trimmed')
            AND plan.position_action_decision = 'open'
            AND plan.admitted_weight > 0
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_direct_blocked_fallback_trade_date_lookup AS
        SELECT
            key_rows.symbol,
            key_rows.reference_trade_date,
            fallback_price.trade_date AS effective_planned_trade_date
        FROM (
            SELECT DISTINCT symbol, reference_trade_date
            FROM trade_direct_blocked_source
            WHERE planned_entry_trade_date IS NULL
        ) AS key_rows
        ASOF LEFT JOIN trade_execution_price_source AS fallback_price
            ON fallback_price.symbol = key_rows.symbol
            AND key_rows.reference_trade_date < fallback_price.trade_date
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_candidate_explicit_price_lookup AS
        SELECT
            key_rows.symbol,
            key_rows.planned_entry_trade_date,
            explicit_price.trade_date AS execution_trade_date,
            explicit_price.open_price AS execution_price
        FROM (
            SELECT DISTINCT symbol, planned_entry_trade_date
            FROM trade_price_candidate_source
            WHERE planned_entry_trade_date IS NOT NULL
        ) AS key_rows
        LEFT JOIN trade_execution_price_source AS explicit_price
            ON explicit_price.symbol = key_rows.symbol
            AND explicit_price.trade_date = key_rows.planned_entry_trade_date
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_candidate_fallback_price_lookup AS
        SELECT
            key_rows.symbol,
            key_rows.reference_trade_date,
            fallback_price.trade_date AS execution_trade_date,
            fallback_price.open_price AS execution_price
        FROM (
            SELECT DISTINCT symbol, reference_trade_date
            FROM trade_price_candidate_source
            WHERE planned_entry_trade_date IS NULL
        ) AS key_rows
        ASOF LEFT JOIN trade_execution_price_source AS fallback_price
            ON fallback_price.symbol = key_rows.symbol
            AND key_rows.reference_trade_date < fallback_price.trade_date
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_intent AS
        WITH blocked_intent_rows AS (
            SELECT
                CONCAT(
                    blocked.portfolio_id,
                    ':',
                    blocked.candidate_nk,
                    ':entry:',
                    COALESCE(
                        CAST(
                            COALESCE(
                                blocked.planned_entry_trade_date,
                                blocked_fallback.effective_planned_trade_date
                            ) AS VARCHAR
                        ),
                        'no_execution_date'
                    ),
                    ':',
                    ?
                ) AS order_intent_nk,
                blocked.plan_snapshot_nk,
                blocked.candidate_nk,
                blocked.portfolio_id,
                blocked.symbol,
                blocked.reference_trade_date,
                COALESCE(
                    blocked.planned_entry_trade_date,
                    blocked_fallback.effective_planned_trade_date
                ) AS planned_trade_date,
                blocked.position_action_decision,
                'blocked' AS intent_status,
                blocked.requested_weight,
                blocked.admitted_weight,
                0.0 AS execution_weight,
                blocked.trade_blocking_reason_code AS blocking_reason_code,
                blocked.scheduled_exit_trade_date,
                blocked.planned_exit_reason_code
            FROM trade_direct_blocked_source AS blocked
            LEFT JOIN trade_direct_blocked_fallback_trade_date_lookup AS blocked_fallback
                ON blocked_fallback.symbol = blocked.symbol
                AND blocked_fallback.reference_trade_date = blocked.reference_trade_date
        ),
        priced_candidates AS (
            SELECT
                candidate.*,
                COALESCE(candidate.planned_entry_trade_date, fallback_lookup.execution_trade_date) AS effective_planned_trade_date,
                CASE
                    WHEN candidate.planned_entry_trade_date IS NOT NULL THEN explicit_lookup.execution_price
                    ELSE fallback_lookup.execution_price
                END AS execution_price,
                CASE
                    WHEN candidate.planned_entry_trade_date IS NOT NULL THEN explicit_lookup.execution_trade_date
                    ELSE fallback_lookup.execution_trade_date
                END AS execution_trade_date
            FROM trade_price_candidate_source AS candidate
            LEFT JOIN trade_candidate_explicit_price_lookup AS explicit_lookup
                ON explicit_lookup.symbol = candidate.symbol
                AND explicit_lookup.planned_entry_trade_date = candidate.planned_entry_trade_date
            LEFT JOIN trade_candidate_fallback_price_lookup AS fallback_lookup
                ON fallback_lookup.symbol = candidate.symbol
                AND fallback_lookup.reference_trade_date = candidate.reference_trade_date
        ),
        candidate_intent_rows AS (
            SELECT
                CONCAT(
                    priced.portfolio_id,
                    ':',
                    priced.candidate_nk,
                    ':entry:',
                    COALESCE(CAST(priced.effective_planned_trade_date AS VARCHAR), 'no_execution_date'),
                    ':',
                    ?
                ) AS order_intent_nk,
                priced.plan_snapshot_nk,
                priced.candidate_nk,
                priced.portfolio_id,
                priced.symbol,
                priced.reference_trade_date,
                priced.effective_planned_trade_date AS planned_trade_date,
                priced.position_action_decision,
                CASE
                    WHEN priced.effective_planned_trade_date IS NULL THEN 'blocked'
                    WHEN priced.execution_trade_date IS NULL THEN 'blocked'
                    WHEN priced.execution_price IS NULL THEN 'blocked'
                    ELSE 'planned'
                END AS intent_status,
                priced.requested_weight,
                priced.admitted_weight,
                CASE
                    WHEN priced.effective_planned_trade_date IS NULL
                        OR priced.execution_trade_date IS NULL
                        OR priced.execution_price IS NULL
                        THEN 0.0
                    ELSE ROUND(priced.admitted_weight, 8)
                END AS execution_weight,
                CASE
                    WHEN priced.effective_planned_trade_date IS NULL THEN 'missing_next_execution_trade_date'
                    WHEN priced.execution_trade_date IS NULL THEN 'missing_next_execution_trade_date'
                    WHEN priced.execution_price IS NULL THEN 'missing_execution_open_price'
                    ELSE NULL
                END AS blocking_reason_code,
                priced.scheduled_exit_trade_date,
                priced.planned_exit_reason_code
            FROM priced_candidates AS priced
        )
        SELECT * FROM blocked_intent_rows
        UNION ALL
        SELECT * FROM candidate_intent_rows
        """,
        [TRADE_CONTRACT_VERSION, TRADE_CONTRACT_VERSION],
    )
    connection.execute(
        """
        ALTER TABLE trade_materialized_intent
        ADD COLUMN compare_signature UBIGINT
        """
    )
    connection.execute(
        """
        UPDATE trade_materialized_intent
        SET compare_signature = hash(
            plan_snapshot_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            reference_trade_date,
            planned_trade_date,
            position_action_decision,
            intent_status,
            requested_weight,
            admitted_weight,
            execution_weight,
            blocking_reason_code
        )
        """
    )
    blocked_row_count = _table_row_count(connection=connection, table_name="trade_direct_blocked_source")
    candidate_row_count = _table_row_count(connection=connection, table_name="trade_price_candidate_source")
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="intent_materialized",
        started_at=phase_started,
        table_name="trade_materialized_intent",
        detail=f"direct_blocked_rows={blocked_row_count} candidate_rows={candidate_row_count}",
    )

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_execution AS
        WITH blocked_execution_rows AS (
            SELECT
                CONCAT(
                    order_intent_nk,
                    ':',
                    COALESCE(CAST(planned_trade_date AS VARCHAR), 'no_execution_date'),
                    ':rejected'
                ) AS order_execution_nk,
                intent.order_intent_nk,
                intent.candidate_nk,
                intent.portfolio_id,
                intent.symbol,
                'rejected' AS execution_status,
                intent.planned_trade_date AS execution_trade_date,
                CAST(NULL AS DOUBLE) AS execution_price,
                0.0 AS executed_weight,
                intent.blocking_reason_code,
                ? AS source_price_line
            FROM trade_materialized_intent AS intent
            WHERE intent.blocking_reason_code IS NOT NULL
        ),
        actionable_execution_rows AS (
            SELECT
                CONCAT(
                    order_intent_nk,
                    ':',
                    COALESCE(CAST(planned_trade_date AS VARCHAR), 'no_execution_date'),
                    ':filled'
                ) AS order_execution_nk,
                intent.order_intent_nk,
                intent.candidate_nk,
                intent.portfolio_id,
                intent.symbol,
                'filled' AS execution_status,
                intent.planned_trade_date AS execution_trade_date,
                price.open_price AS execution_price,
                ROUND(intent.execution_weight, 8) AS executed_weight,
                intent.blocking_reason_code,
                ? AS source_price_line
            FROM trade_materialized_intent AS intent
            LEFT JOIN trade_execution_price_source AS price
                ON price.symbol = intent.symbol
                AND price.trade_date = intent.planned_trade_date
            WHERE intent.blocking_reason_code IS NULL
        )
        SELECT * FROM blocked_execution_rows
        UNION ALL
        SELECT * FROM actionable_execution_rows
        """,
        [EXECUTION_PRICE_LINE, EXECUTION_PRICE_LINE],
    )
    connection.execute(
        """
        ALTER TABLE trade_materialized_execution
        ADD COLUMN compare_signature UBIGINT
        """
    )
    connection.execute(
        """
        UPDATE trade_materialized_execution
        SET compare_signature = hash(
            order_intent_nk,
            portfolio_id,
            symbol,
            execution_status,
            execution_trade_date,
            execution_price,
            executed_weight,
            blocking_reason_code,
            source_price_line
        )
        """
    )
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="execution_materialized",
        started_at=phase_started,
        table_name="trade_materialized_execution",
    )

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_exit_execution AS
        SELECT
            CONCAT(
                intent.portfolio_id,
                ':',
                intent.candidate_nk,
                ':exit:',
                COALESCE(CAST(intent.scheduled_exit_trade_date AS VARCHAR), 'no_exit_date'),
                ':',
                ?
            ) AS exit_execution_nk,
            CONCAT(intent.portfolio_id, ':', intent.candidate_nk, ':leg') AS position_leg_nk,
            intent.candidate_nk,
            intent.portfolio_id,
            intent.symbol,
            intent.scheduled_exit_trade_date AS exit_trade_date,
            CASE
                WHEN intent.blocking_reason_code IS NOT NULL THEN 'rejected'
                WHEN exit_trade_date IS NULL THEN 'rejected'
                WHEN price.open_price IS NULL THEN 'rejected'
                ELSE 'filled'
            END AS execution_status,
            price.open_price AS execution_price,
            CASE
                WHEN intent.blocking_reason_code IS NULL AND exit_trade_date IS NOT NULL AND price.open_price IS NOT NULL
                    THEN ROUND(intent.admitted_weight, 8)
                ELSE 0.0
            END AS exited_weight,
            CASE
                WHEN intent.blocking_reason_code IS NOT NULL THEN 'entry_not_filled'
                WHEN exit_trade_date IS NULL THEN 'missing_exit_execution_trade_date'
                WHEN price.open_price IS NULL THEN 'missing_execution_open_price'
                ELSE NULL
            END AS blocking_reason_code,
            intent.planned_exit_reason_code AS exit_reason_code,
            ? AS source_price_line
        FROM trade_materialized_intent AS intent
        LEFT JOIN trade_execution_price_source AS price
            ON price.symbol = intent.symbol
            AND price.trade_date = intent.scheduled_exit_trade_date
        WHERE intent.admitted_weight > 0
            AND intent.scheduled_exit_trade_date IS NOT NULL
        """,
        [TRADE_CONTRACT_VERSION, EXECUTION_PRICE_LINE],
    )
    connection.execute(
        """
        ALTER TABLE trade_materialized_exit_execution
        ADD COLUMN compare_signature UBIGINT
        """
    )
    connection.execute(
        """
        UPDATE trade_materialized_exit_execution
        SET compare_signature = hash(
            position_leg_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            exit_trade_date,
            execution_status,
            execution_price,
            exited_weight,
            blocking_reason_code,
            exit_reason_code,
            source_price_line
        )
        """
    )
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="exit_materialized",
        started_at=phase_started,
        table_name="trade_materialized_exit_execution",
    )

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_position_leg AS
        SELECT
            CONCAT(intent.portfolio_id, ':', intent.candidate_nk, ':leg') AS position_leg_nk,
            intent.candidate_nk,
            intent.order_intent_nk,
            intent.portfolio_id,
            intent.symbol,
            intent.reference_trade_date AS entry_reference_trade_date,
            execution.execution_trade_date AS entry_trade_date,
            execution.execution_price AS entry_execution_price,
            intent.execution_weight AS position_weight,
            intent.scheduled_exit_trade_date,
            CASE
                WHEN execution.execution_status != 'filled' THEN 'entry_rejected'
                WHEN exit_execution.execution_status = 'filled' THEN 'closed'
                ELSE 'open'
            END AS position_state,
            exit_execution.exit_execution_nk,
            exit_execution.exit_trade_date,
            exit_execution.execution_price AS exit_execution_price,
            CASE
                WHEN execution.execution_status = 'filled'
                    AND COALESCE(exit_execution.execution_status, 'open') != 'filled'
                    THEN ROUND(intent.execution_weight, 8)
                ELSE 0.0
            END AS active_weight
        FROM trade_materialized_intent AS intent
        INNER JOIN trade_materialized_execution AS execution
            ON execution.order_intent_nk = intent.order_intent_nk
        LEFT JOIN trade_materialized_exit_execution AS exit_execution
            ON exit_execution.portfolio_id = intent.portfolio_id
            AND exit_execution.candidate_nk = intent.candidate_nk
        WHERE intent.admitted_weight > 0
        """
    )
    connection.execute(
        """
        ALTER TABLE trade_materialized_position_leg
        ADD COLUMN compare_signature UBIGINT
        """
    )
    connection.execute(
        """
        UPDATE trade_materialized_position_leg
        SET compare_signature = hash(
            candidate_nk,
            order_intent_nk,
            portfolio_id,
            symbol,
            entry_reference_trade_date,
            entry_trade_date,
            entry_execution_price,
            position_weight,
            scheduled_exit_trade_date,
            position_state,
            exit_execution_nk,
            exit_trade_date,
            exit_execution_price,
            active_weight
        )
        """
    )
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="position_leg_materialized",
        started_at=phase_started,
        table_name="trade_materialized_position_leg",
    )

    phase_started = perf_counter()
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_carry_snapshot AS
        SELECT
            CONCAT(position_leg_nk, ':open') AS carry_snapshot_nk,
            position_leg_nk,
            portfolio_id,
            symbol,
            entry_trade_date AS as_of_trade_date,
            'open' AS carry_status,
            CASE WHEN position_state != 'entry_rejected' THEN position_weight ELSE 0.0 END AS carried_weight
        FROM trade_materialized_position_leg
        WHERE entry_trade_date IS NOT NULL

        UNION ALL

        SELECT
            CONCAT(position_leg_nk, ':close') AS carry_snapshot_nk,
            position_leg_nk,
            portfolio_id,
            symbol,
            COALESCE(exit_trade_date, scheduled_exit_trade_date) AS as_of_trade_date,
            CASE WHEN position_state = 'closed' THEN 'closed' ELSE 'open' END AS carry_status,
            CASE WHEN position_state = 'closed' THEN 0.0 ELSE active_weight END AS carried_weight
        FROM trade_materialized_position_leg
        WHERE scheduled_exit_trade_date IS NOT NULL
        """
    )
    connection.execute(
        """
        ALTER TABLE trade_materialized_carry_snapshot
        ADD COLUMN compare_signature UBIGINT
        """
    )
    connection.execute(
        """
        UPDATE trade_materialized_carry_snapshot
        SET compare_signature = hash(
            position_leg_nk,
            portfolio_id,
            symbol,
            as_of_trade_date,
            carry_status,
            carried_weight
        )
        """
    )
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="carry_materialized",
        started_at=phase_started,
        table_name="trade_materialized_carry_snapshot",
    )

    phase_started = perf_counter()
    _classify_trade_actions(connection=connection, run_id=run_id, existing_catalog=existing_catalog)

    intent_counts = _action_counts(connection=connection, table_name="trade_materialized_intent_with_action")
    execution_counts = _action_counts(connection=connection, table_name="trade_materialized_execution_with_action")
    position_leg_counts = _action_counts(connection=connection, table_name="trade_materialized_position_leg_with_action")
    carry_counts = _action_counts(connection=connection, table_name="trade_materialized_carry_snapshot_with_action")
    exit_counts = _action_counts(connection=connection, table_name="trade_materialized_exit_execution_with_action")
    work_units_updated = int(
        connection.execute("SELECT COUNT(*) FROM trade_work_unit_actions WHERE status != 'reused'").fetchone()[0]
    )
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM trade_source_work_unit_summary"
    ).fetchone()[0]
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="action_tables_ready",
        started_at=phase_started,
        table_name="trade_work_unit_actions",
        detail=f"work_units_updated={work_units_updated}",
    )
    if write_outputs:
        if phase_observer is not None:
            phase_observer(
                "write_stage_targets_started",
                elapsed_seconds=0.0,
                row_count=work_unit_count,
                detail=f"work_units_updated={work_units_updated}",
            )
        phase_started = perf_counter()
        stage_tables = _stage_trade_target_tables(
            connection=connection,
            run_id=run_id,
            phase_observer=phase_observer,
            work_units_updated=work_units_updated,
        )
        _record_phase(
            connection=connection,
            phase_observer=phase_observer,
            phase="write_stage_targets_done",
            started_at=phase_started,
            table_name=stage_tables["trade_work_queue"],
            detail=f"work_units_updated={work_units_updated}",
        )
        cutover_started = perf_counter()
        _cutover_trade_target_tables(
            connection=connection,
            run_id=run_id,
            stage_tables=stage_tables,
            phase_observer=phase_observer,
            work_units_updated=work_units_updated,
        )
        _record_phase(
            connection=connection,
            phase_observer=phase_observer,
            phase="write_transaction_committed",
            started_at=cutover_started,
            table_name="trade_work_queue",
            detail=f"work_units_updated={work_units_updated}",
        )
    return (
        {
            "intents_inserted": int(intent_counts.get("inserted", 0)),
            "intents_reused": int(intent_counts.get("reused", 0)),
            "intents_rematerialized": int(intent_counts.get("rematerialized", 0)),
            "executions_inserted": int(execution_counts.get("inserted", 0)),
            "executions_reused": int(execution_counts.get("reused", 0)),
            "executions_rematerialized": int(execution_counts.get("rematerialized", 0)),
            "position_legs_inserted": int(position_leg_counts.get("inserted", 0)),
            "position_legs_reused": int(position_leg_counts.get("reused", 0)),
            "position_legs_rematerialized": int(position_leg_counts.get("rematerialized", 0)),
            "carry_rows_inserted": int(carry_counts.get("inserted", 0)),
            "carry_rows_reused": int(carry_counts.get("reused", 0)),
            "carry_rows_rematerialized": int(carry_counts.get("rematerialized", 0)),
            "exit_rows_inserted": int(exit_counts.get("inserted", 0)),
            "exit_rows_reused": int(exit_counts.get("reused", 0)),
            "exit_rows_rematerialized": int(exit_counts.get("rematerialized", 0)),
        },
        work_units_updated,
        latest_reference_trade_date,
    )


def _trade_run_table_token(run_id: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in run_id)


def _trade_stage_table_name(*, table_name: str, run_id: str) -> str:
    return f"{table_name}_stage_{_trade_run_table_token(run_id)}"


def _trade_backup_table_name(*, table_name: str, run_id: str) -> str:
    return f"{table_name}_backup_{_trade_run_table_token(run_id)}"


def _drop_table_if_exists(*, connection: duckdb.DuckDBPyConnection, table_name: str) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")


def _create_trade_target_table_schema(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    target_table: str,
) -> None:
    if target_table == "trade_order_intent":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                order_intent_nk TEXT PRIMARY KEY,
                plan_snapshot_nk TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                reference_trade_date DATE,
                planned_trade_date DATE,
                position_action_decision TEXT NOT NULL,
                intent_status TEXT NOT NULL,
                requested_weight DOUBLE NOT NULL,
                admitted_weight DOUBLE NOT NULL,
                execution_weight DOUBLE NOT NULL,
                blocking_reason_code TEXT,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    elif target_table == "trade_order_execution":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                order_execution_nk TEXT PRIMARY KEY,
                order_intent_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                execution_status TEXT NOT NULL,
                execution_trade_date DATE,
                execution_price DOUBLE,
                executed_weight DOUBLE NOT NULL,
                blocking_reason_code TEXT,
                source_price_line TEXT NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    elif target_table == "trade_position_leg":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                position_leg_nk TEXT PRIMARY KEY,
                candidate_nk TEXT NOT NULL,
                order_intent_nk TEXT,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                entry_reference_trade_date DATE,
                entry_trade_date DATE,
                entry_execution_price DOUBLE,
                position_weight DOUBLE NOT NULL,
                scheduled_exit_trade_date DATE,
                position_state TEXT NOT NULL,
                exit_execution_nk TEXT,
                exit_trade_date DATE,
                exit_execution_price DOUBLE,
                active_weight DOUBLE NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    elif target_table == "trade_carry_snapshot":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                carry_snapshot_nk TEXT PRIMARY KEY,
                position_leg_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                as_of_trade_date DATE,
                carry_status TEXT NOT NULL,
                carried_weight DOUBLE NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    elif target_table == "trade_exit_execution":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                exit_execution_nk TEXT PRIMARY KEY,
                position_leg_nk TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exit_trade_date DATE,
                execution_status TEXT NOT NULL,
                execution_price DOUBLE,
                exited_weight DOUBLE NOT NULL,
                blocking_reason_code TEXT,
                exit_reason_code TEXT,
                source_price_line TEXT NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    elif target_table == "trade_work_queue":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                queue_id TEXT PRIMARY KEY,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                source_row_count BIGINT NOT NULL DEFAULT 0,
                last_reference_trade_date DATE,
                source_fingerprint TEXT,
                requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                claimed_at TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
    elif target_table == "trade_checkpoint":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                last_reference_trade_date DATE,
                last_source_fingerprint TEXT,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (portfolio_id, symbol)
            )
            """
        )
    elif target_table == "trade_run_order_intent":
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
                run_id TEXT NOT NULL,
                order_intent_nk TEXT NOT NULL,
                intent_status TEXT NOT NULL,
                materialization_action TEXT NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, order_intent_nk)
            )
            """
        )
    else:
        raise ValueError(f"Unsupported trade target table: {target_table}")


def _source_work_unit_filter(*, row_alias: str) -> str:
    return f"""
        EXISTS (
            SELECT 1
            FROM trade_source_work_unit_summary AS source
            WHERE source.portfolio_id = {row_alias}.portfolio_id
                AND source.symbol = {row_alias}.symbol
        )
    """


def _stage_trade_target_tables(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    phase_observer: Callable[..., None] | None,
    work_units_updated: int,
) -> dict[str, str]:
    stage_tables = {
        table_name: _trade_stage_table_name(table_name=table_name, run_id=run_id)
        for table_name in TRADE_CUTOVER_TARGET_TABLES
    }
    for table_name in TRADE_CUTOVER_TARGET_TABLES:
        _drop_table_if_exists(connection=connection, table_name=stage_tables[table_name])
        _drop_table_if_exists(connection=connection, table_name=_trade_backup_table_name(table_name=table_name, run_id=run_id))
        _create_trade_target_table_schema(
            connection=connection,
            table_name=stage_tables[table_name],
            target_table=table_name,
        )

    stage_started = perf_counter()
    _stage_trade_order_intent(connection=connection, stage_table=stage_tables["trade_order_intent"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_order_intent_done",
        started_at=stage_started,
        table_name=stage_tables["trade_order_intent"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_order_execution(connection=connection, stage_table=stage_tables["trade_order_execution"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_order_execution_done",
        started_at=stage_started,
        table_name=stage_tables["trade_order_execution"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_position_leg(connection=connection, stage_table=stage_tables["trade_position_leg"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_position_leg_done",
        started_at=stage_started,
        table_name=stage_tables["trade_position_leg"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_carry_snapshot(connection=connection, stage_table=stage_tables["trade_carry_snapshot"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_carry_snapshot_done",
        started_at=stage_started,
        table_name=stage_tables["trade_carry_snapshot"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_exit_execution(connection=connection, stage_table=stage_tables["trade_exit_execution"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_exit_execution_done",
        started_at=stage_started,
        table_name=stage_tables["trade_exit_execution"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_work_queue(connection=connection, stage_table=stage_tables["trade_work_queue"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_work_queue_done",
        started_at=stage_started,
        table_name=stage_tables["trade_work_queue"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_checkpoint(connection=connection, stage_table=stage_tables["trade_checkpoint"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_checkpoint_done",
        started_at=stage_started,
        table_name=stage_tables["trade_checkpoint"],
        detail=f"work_units_updated={work_units_updated}",
    )
    stage_started = perf_counter()
    _stage_trade_run_order_intent(connection=connection, stage_table=stage_tables["trade_run_order_intent"], run_id=run_id)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_stage_trade_run_order_intent_done",
        started_at=stage_started,
        table_name=stage_tables["trade_run_order_intent"],
        detail=f"work_units_updated={work_units_updated}",
    )
    return stage_tables


def _stage_trade_order_intent(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    source_filter = _source_work_unit_filter(row_alias="intent")
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
            reference_trade_date, planned_trade_date, position_action_decision, intent_status,
            requested_weight, admitted_weight, execution_weight, blocking_reason_code,
            trade_contract_version, first_seen_run_id, last_materialized_run_id, created_at, updated_at
        )
        SELECT
            order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
            reference_trade_date, planned_trade_date, position_action_decision, intent_status,
            requested_weight, admitted_weight, execution_weight, blocking_reason_code,
            trade_contract_version, first_seen_run_id, last_materialized_run_id, created_at, updated_at
        FROM trade_order_intent AS intent
        WHERE NOT {source_filter}
        """
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
            reference_trade_date, planned_trade_date, position_action_decision, intent_status,
            requested_weight, admitted_weight, execution_weight, blocking_reason_code,
            trade_contract_version, first_seen_run_id, last_materialized_run_id
        )
        SELECT
            order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
            reference_trade_date, planned_trade_date, position_action_decision, intent_status,
            requested_weight, admitted_weight, execution_weight, blocking_reason_code,
            ?, COALESCE(existing_first_seen_run_id, ?), ?
        FROM trade_materialized_intent_with_action
        """,
        [TRADE_CONTRACT_VERSION, run_id, run_id],
    )


def _stage_trade_order_execution(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    source_filter = _source_work_unit_filter(row_alias="execution")
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            order_execution_nk, order_intent_nk, portfolio_id, symbol, execution_status,
            execution_trade_date, execution_price, executed_weight, blocking_reason_code,
            source_price_line, trade_contract_version, first_seen_run_id, last_materialized_run_id, created_at, updated_at
        )
        SELECT
            order_execution_nk, order_intent_nk, portfolio_id, symbol, execution_status,
            execution_trade_date, execution_price, executed_weight, blocking_reason_code,
            source_price_line, trade_contract_version, first_seen_run_id, last_materialized_run_id, created_at, updated_at
        FROM trade_order_execution AS execution
        WHERE NOT {source_filter}
        """
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            order_execution_nk, order_intent_nk, portfolio_id, symbol, execution_status,
            execution_trade_date, execution_price, executed_weight, blocking_reason_code,
            source_price_line, trade_contract_version, first_seen_run_id, last_materialized_run_id
        )
        SELECT
            order_execution_nk, order_intent_nk, portfolio_id, symbol, execution_status,
            execution_trade_date, execution_price, executed_weight, blocking_reason_code,
            source_price_line, ?, COALESCE(existing_first_seen_run_id, ?), ?
        FROM trade_materialized_execution_with_action
        """,
        [TRADE_CONTRACT_VERSION, run_id, run_id],
    )


def _stage_trade_position_leg(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    source_filter = _source_work_unit_filter(row_alias="leg")
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            position_leg_nk, candidate_nk, order_intent_nk, portfolio_id, symbol,
            entry_reference_trade_date, entry_trade_date, entry_execution_price, position_weight,
            scheduled_exit_trade_date, position_state, exit_execution_nk, exit_trade_date,
            exit_execution_price, active_weight, trade_contract_version, first_seen_run_id,
            last_materialized_run_id, created_at, updated_at
        )
        SELECT
            position_leg_nk, candidate_nk, order_intent_nk, portfolio_id, symbol,
            entry_reference_trade_date, entry_trade_date, entry_execution_price, position_weight,
            scheduled_exit_trade_date, position_state, exit_execution_nk, exit_trade_date,
            exit_execution_price, active_weight, trade_contract_version, first_seen_run_id,
            last_materialized_run_id, created_at, updated_at
        FROM trade_position_leg AS leg
        WHERE NOT {source_filter}
        """
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            position_leg_nk, candidate_nk, order_intent_nk, portfolio_id, symbol,
            entry_reference_trade_date, entry_trade_date, entry_execution_price, position_weight,
            scheduled_exit_trade_date, position_state, exit_execution_nk, exit_trade_date,
            exit_execution_price, active_weight, trade_contract_version, first_seen_run_id,
            last_materialized_run_id
        )
        SELECT
            position_leg_nk, candidate_nk, order_intent_nk, portfolio_id, symbol,
            entry_reference_trade_date, entry_trade_date, entry_execution_price, position_weight,
            scheduled_exit_trade_date, position_state, exit_execution_nk, exit_trade_date,
            exit_execution_price, active_weight, ?, COALESCE(existing_first_seen_run_id, ?), ?
        FROM trade_materialized_position_leg_with_action
        """,
        [TRADE_CONTRACT_VERSION, run_id, run_id],
    )


def _stage_trade_carry_snapshot(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    source_filter = _source_work_unit_filter(row_alias="carry")
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            carry_snapshot_nk, position_leg_nk, portfolio_id, symbol, as_of_trade_date,
            carry_status, carried_weight, trade_contract_version, first_seen_run_id,
            last_materialized_run_id, created_at, updated_at
        )
        SELECT
            carry_snapshot_nk, position_leg_nk, portfolio_id, symbol, as_of_trade_date,
            carry_status, carried_weight, trade_contract_version, first_seen_run_id,
            last_materialized_run_id, created_at, updated_at
        FROM trade_carry_snapshot AS carry
        WHERE NOT {source_filter}
        """
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            carry_snapshot_nk, position_leg_nk, portfolio_id, symbol, as_of_trade_date,
            carry_status, carried_weight, trade_contract_version, first_seen_run_id, last_materialized_run_id
        )
        SELECT
            carry_snapshot_nk, position_leg_nk, portfolio_id, symbol, as_of_trade_date,
            carry_status, carried_weight, ?, COALESCE(existing_first_seen_run_id, ?), ?
        FROM trade_materialized_carry_snapshot_with_action
        """,
        [TRADE_CONTRACT_VERSION, run_id, run_id],
    )


def _stage_trade_exit_execution(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    source_filter = _source_work_unit_filter(row_alias="exit_execution")
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            exit_execution_nk, position_leg_nk, candidate_nk, portfolio_id, symbol,
            exit_trade_date, execution_status, execution_price, exited_weight,
            blocking_reason_code, exit_reason_code, source_price_line, trade_contract_version,
            first_seen_run_id, last_materialized_run_id, created_at, updated_at
        )
        SELECT
            exit_execution_nk, position_leg_nk, candidate_nk, portfolio_id, symbol,
            exit_trade_date, execution_status, execution_price, exited_weight,
            blocking_reason_code, exit_reason_code, source_price_line, trade_contract_version,
            first_seen_run_id, last_materialized_run_id, created_at, updated_at
        FROM trade_exit_execution AS exit_execution
        WHERE NOT {source_filter}
        """
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            exit_execution_nk, position_leg_nk, candidate_nk, portfolio_id, symbol,
            exit_trade_date, execution_status, execution_price, exited_weight,
            blocking_reason_code, exit_reason_code, source_price_line, trade_contract_version,
            first_seen_run_id, last_materialized_run_id
        )
        SELECT
            exit_execution_nk, position_leg_nk, candidate_nk, portfolio_id, symbol,
            exit_trade_date, execution_status, execution_price, exited_weight,
            blocking_reason_code, exit_reason_code, source_price_line, ?,
            COALESCE(existing_first_seen_run_id, ?), ?
        FROM trade_materialized_exit_execution_with_action
        """,
        [TRADE_CONTRACT_VERSION, run_id, run_id],
    )


def _stage_trade_work_queue(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            queue_id, portfolio_id, symbol, status, source_row_count,
            last_reference_trade_date, source_fingerprint, claimed_at, finished_at
        )
        SELECT
            CONCAT(?, ':', portfolio_id, ':', symbol),
            portfolio_id,
            symbol,
            status,
            source_row_count,
            last_reference_trade_date,
            source_fingerprint,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM trade_work_unit_actions
        """,
        [run_id],
    )


def _stage_trade_checkpoint(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    source_filter = _source_work_unit_filter(row_alias="checkpoint")
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            portfolio_id, symbol, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        )
        SELECT
            portfolio_id, symbol, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        FROM trade_checkpoint AS checkpoint
        WHERE NOT {source_filter}
        """
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            portfolio_id, symbol, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        )
        SELECT portfolio_id, symbol, last_reference_trade_date, source_fingerprint, ?, CURRENT_TIMESTAMP
        FROM trade_source_work_unit_summary
        """,
        [run_id],
    )


def _stage_trade_run_order_intent(*, connection: duckdb.DuckDBPyConnection, stage_table: str, run_id: str) -> None:
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            run_id, order_intent_nk, intent_status, materialization_action, recorded_at
        )
        SELECT run_id, order_intent_nk, intent_status, materialization_action, recorded_at
        FROM trade_run_order_intent
        WHERE run_id != ?
        """,
        [run_id],
    )
    connection.execute(
        f"""
        INSERT INTO {stage_table} (
            run_id, order_intent_nk, intent_status, materialization_action
        )
        SELECT ?, order_intent_nk, intent_status, materialization_action
        FROM trade_materialized_intent_with_action
        """,
        [run_id],
    )


def _cutover_trade_target_tables(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    stage_tables: dict[str, str],
    phase_observer: Callable[..., None] | None,
    work_units_updated: int,
) -> None:
    backup_tables = {
        table_name: _trade_backup_table_name(table_name=table_name, run_id=run_id)
        for table_name in TRADE_CUTOVER_TARGET_TABLES
    }
    transaction_started = perf_counter()
    if phase_observer is not None:
        phase_observer(
            "write_cutover_transaction_started",
            elapsed_seconds=0.0,
            row_count=len(TRADE_CUTOVER_TARGET_TABLES),
            detail=f"work_units_updated={work_units_updated}",
        )
    try:
        connection.execute("BEGIN TRANSACTION")
        for index_name, _, _ in TRADE_SECONDARY_INDEXES:
            connection.execute(f"DROP INDEX IF EXISTS {index_name}")
        for table_name in TRADE_CUTOVER_TARGET_TABLES:
            connection.execute(f"ALTER TABLE {table_name} RENAME TO {backup_tables[table_name]}")
            connection.execute(f"ALTER TABLE {stage_tables[table_name]} RENAME TO {table_name}")
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_cutover_committed",
        started_at=transaction_started,
        table_name="trade_work_queue",
        detail=f"work_units_updated={work_units_updated}",
    )
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_transaction_committed",
        started_at=transaction_started,
        table_name="trade_work_queue",
        detail=f"work_units_updated={work_units_updated} cutover=1",
    )
    indexes_started = perf_counter()
    _rebuild_trade_secondary_indexes(connection=connection)
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_indexes_rebuilt",
        started_at=indexes_started,
        table_name="trade_order_intent",
        detail=f"index_count={len(TRADE_SECONDARY_INDEXES)}",
    )
    cleanup_started = perf_counter()
    for table_name in TRADE_CUTOVER_TARGET_TABLES:
        _drop_table_if_exists(connection=connection, table_name=backup_tables[table_name])
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_cutover_backups_dropped",
        started_at=cleanup_started,
        table_name="trade_work_queue",
        detail=f"backup_count={len(backup_tables)}",
    )


def _rebuild_trade_secondary_indexes(*, connection: duckdb.DuckDBPyConnection) -> None:
    for index_name, table_name, expression in TRADE_SECONDARY_INDEXES:
        connection.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}{expression}")


def _delete_trade_targets_in_batches(
    *,
    connection: duckdb.DuckDBPyConnection,
    phase_observer: Callable[..., None] | None,
    batch_size: int,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_delete_work_unit_batches AS
        SELECT
            portfolio_id,
            symbol,
            CAST(FLOOR((ROW_NUMBER() OVER (ORDER BY portfolio_id, symbol) - 1) / ?) + 1 AS INTEGER) AS batch_index
        FROM trade_source_work_unit_summary
        """,
        [batch_size],
    )
    batch_count = int(
        connection.execute("SELECT COALESCE(MAX(batch_index), 0) FROM trade_delete_work_unit_batches").fetchone()[0]
        or 0
    )
    work_unit_count = _table_row_count(connection=connection, table_name="trade_delete_work_unit_batches")
    if phase_observer is not None:
        phase_observer(
            "write_delete_batches_ready",
            elapsed_seconds=0.0,
            row_count=batch_count,
            detail=f"batch_size={batch_size} work_units={work_unit_count}",
        )
    delete_targets = [
        ("trade_carry_snapshot", "carry"),
        ("trade_exit_execution", "exit_execution"),
        ("trade_position_leg", "leg"),
        ("trade_order_execution", "execution"),
        ("trade_order_intent", "intent"),
    ]
    for table_name, alias in delete_targets:
        table_started = perf_counter()
        phase_prefix = f"write_delete_{table_name}"
        if phase_observer is not None:
            phase_observer(
                f"{phase_prefix}_started",
                elapsed_seconds=0.0,
                row_count=batch_count,
                detail=f"batch_size={batch_size}",
            )
        for batch_index in range(1, batch_count + 1):
            batch_work_units = int(
                connection.execute(
                    "SELECT COUNT(*) FROM trade_delete_work_unit_batches WHERE batch_index = ?",
                    [batch_index],
                ).fetchone()[0]
                or 0
            )
            batch_started = perf_counter()
            if phase_observer is not None:
                phase_observer(
                    f"{phase_prefix}_batch",
                    elapsed_seconds=0.0,
                    row_count=batch_work_units,
                    detail=f"batch_index={batch_index} batch_count={batch_count}",
                )
            connection.execute(
                f"""
                DELETE FROM {table_name} AS {alias}
                USING (
                    SELECT portfolio_id, symbol
                    FROM trade_delete_work_unit_batches
                    WHERE batch_index = ?
                ) AS source
                WHERE {alias}.portfolio_id = source.portfolio_id
                    AND {alias}.symbol = source.symbol
                """,
                [batch_index],
            )
            if phase_observer is not None:
                phase_observer(
                    f"{phase_prefix}_batch_done",
                    elapsed_seconds=perf_counter() - batch_started,
                    row_count=batch_work_units,
                    detail=f"batch_index={batch_index} batch_count={batch_count}",
                )
        if phase_observer is not None:
            phase_observer(
                f"{phase_prefix}_done",
                elapsed_seconds=perf_counter() - table_started,
                row_count=batch_count,
                detail=f"batch_size={batch_size}",
            )


def _classify_trade_actions(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    existing_catalog: str | None = None,
) -> None:
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_intent",
        existing_table_ref=_qualified_trade_table_name(existing_catalog=existing_catalog, table_name="trade_order_intent"),
        key_column="order_intent_nk",
        compare_columns=[
            "plan_snapshot_nk",
            "candidate_nk",
            "portfolio_id",
            "symbol",
            "reference_trade_date",
            "planned_trade_date",
            "position_action_decision",
            "intent_status",
            "requested_weight",
            "admitted_weight",
            "execution_weight",
            "blocking_reason_code",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_execution",
        existing_table_ref=_qualified_trade_table_name(existing_catalog=existing_catalog, table_name="trade_order_execution"),
        key_column="order_execution_nk",
        compare_columns=[
            "order_intent_nk",
            "portfolio_id",
            "symbol",
            "execution_status",
            "execution_trade_date",
            "execution_price",
            "executed_weight",
            "blocking_reason_code",
            "source_price_line",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_position_leg",
        existing_table_ref=_qualified_trade_table_name(existing_catalog=existing_catalog, table_name="trade_position_leg"),
        key_column="position_leg_nk",
        compare_columns=[
            "candidate_nk",
            "order_intent_nk",
            "portfolio_id",
            "symbol",
            "entry_reference_trade_date",
            "entry_trade_date",
            "entry_execution_price",
            "position_weight",
            "scheduled_exit_trade_date",
            "position_state",
            "exit_execution_nk",
            "exit_trade_date",
            "exit_execution_price",
            "active_weight",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_carry_snapshot",
        existing_table_ref=_qualified_trade_table_name(existing_catalog=existing_catalog, table_name="trade_carry_snapshot"),
        key_column="carry_snapshot_nk",
        compare_columns=[
            "position_leg_nk",
            "portfolio_id",
            "symbol",
            "as_of_trade_date",
            "carry_status",
            "carried_weight",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_exit_execution",
        existing_table_ref=_qualified_trade_table_name(existing_catalog=existing_catalog, table_name="trade_exit_execution"),
        key_column="exit_execution_nk",
        compare_columns=[
            "position_leg_nk",
            "candidate_nk",
            "portfolio_id",
            "symbol",
            "exit_trade_date",
            "execution_status",
            "execution_price",
            "exited_weight",
            "blocking_reason_code",
            "exit_reason_code",
            "source_price_line",
        ],
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_intent_with_action",
        summary_table_name="trade_intent_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_execution_with_action",
        summary_table_name="trade_execution_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_position_leg_with_action",
        summary_table_name="trade_position_leg_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_carry_snapshot_with_action",
        summary_table_name="trade_carry_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_exit_execution_with_action",
        summary_table_name="trade_exit_work_unit_change_summary",
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_work_unit_actions AS
        SELECT
            summary.portfolio_id,
            summary.symbol,
            summary.source_row_count,
            summary.last_reference_trade_date,
            summary.source_fingerprint,
            CASE
                WHEN COALESCE(intent.changed_row_count, 0) = 0
                    AND COALESCE(execution.changed_row_count, 0) = 0
                    AND COALESCE(leg.changed_row_count, 0) = 0
                    AND COALESCE(carry.changed_row_count, 0) = 0
                    AND COALESCE(exit_execution.changed_row_count, 0) = 0
                    THEN 'reused'
                ELSE 'completed'
            END AS status
        FROM trade_source_work_unit_summary AS summary
        LEFT JOIN trade_intent_work_unit_change_summary AS intent
            ON intent.portfolio_id = summary.portfolio_id
            AND intent.symbol = summary.symbol
        LEFT JOIN trade_execution_work_unit_change_summary AS execution
            ON execution.portfolio_id = summary.portfolio_id
            AND execution.symbol = summary.symbol
        LEFT JOIN trade_position_leg_work_unit_change_summary AS leg
            ON leg.portfolio_id = summary.portfolio_id
            AND leg.symbol = summary.symbol
        LEFT JOIN trade_carry_work_unit_change_summary AS carry
            ON carry.portfolio_id = summary.portfolio_id
            AND carry.symbol = summary.symbol
        LEFT JOIN trade_exit_work_unit_change_summary AS exit_execution
            ON exit_execution.portfolio_id = summary.portfolio_id
            AND exit_execution.symbol = summary.symbol
        """
    )


def _create_action_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    materialized_table: str,
    existing_table_ref: str,
    key_column: str,
    compare_columns: list[str],
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {materialized_table}_existing_projection AS
        SELECT
            {key_column} AS existing_key,
            first_seen_run_id,
            {_row_signature_sql(row_alias="existing", compare_columns=compare_columns)} AS compare_signature
        FROM {existing_table_ref} AS existing
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {materialized_table}_with_action AS
        SELECT
            materialized.*,
            existing.first_seen_run_id AS existing_first_seen_run_id,
            CASE
                WHEN existing.existing_key IS NULL THEN 'inserted'
                WHEN existing.compare_signature = materialized.compare_signature THEN 'reused'
                ELSE 'rematerialized'
            END AS materialization_action
        FROM {materialized_table} AS materialized
        LEFT JOIN {materialized_table}_existing_projection AS existing
            ON existing.existing_key = materialized.{key_column}
        """
    )


def _action_counts(*, connection: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, int]:
    return {
        key: int(value)
        for key, value in connection.execute(
            f"""
            SELECT materialization_action, COUNT(*)
            FROM {table_name}
            GROUP BY materialization_action
            """
        ).fetchall()
    }


def _create_work_unit_change_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    action_table_name: str,
    summary_table_name: str,
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {summary_table_name} AS
        SELECT
            portfolio_id,
            symbol,
            SUM(CASE WHEN materialization_action != 'reused' THEN 1 ELSE 0 END) AS changed_row_count
        FROM {action_table_name}
        GROUP BY portfolio_id, symbol
        """
    )


def _row_signature_sql(*, row_alias: str, compare_columns: list[str]) -> str:
    compare_args = ",\n            ".join(f"{row_alias}.{column}" for column in compare_columns)
    return f"hash(\n            {compare_args}\n        )"


def _trade_checkpoint_fast_path_available(*, connection: duckdb.DuckDBPyConnection) -> bool:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS work_unit_count,
            SUM(
                CASE
                    WHEN checkpoint.portfolio_id IS NOT NULL
                        AND checkpoint.last_reference_trade_date IS NOT DISTINCT FROM source.last_reference_trade_date
                        AND checkpoint.last_source_fingerprint = source.source_fingerprint
                        THEN 1
                    ELSE 0
                END
            ) AS matching_checkpoint_count
        FROM trade_source_work_unit_summary AS source
        LEFT JOIN trade_checkpoint AS checkpoint
            ON checkpoint.portfolio_id = source.portfolio_id
            AND checkpoint.symbol = source.symbol
        """
    ).fetchone()
    work_unit_count = int(row[0] or 0)
    matching_checkpoint_count = int(row[1] or 0)
    if work_unit_count == 0 or matching_checkpoint_count != work_unit_count:
        return False
    intent_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_intent AS intent
        INNER JOIN trade_source_work_unit_summary AS source
            ON source.portfolio_id = intent.portfolio_id
            AND source.symbol = intent.symbol
        """
    ).fetchone()[0]
    execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_execution AS execution
        INNER JOIN trade_source_work_unit_summary AS source
            ON source.portfolio_id = execution.portfolio_id
            AND source.symbol = execution.symbol
        """
    ).fetchone()[0]
    expected_intent_count = int(connection.execute("SELECT SUM(source_row_count) FROM trade_source_work_unit_summary").fetchone()[0] or 0)
    if int(intent_count) != int(execution_count) or int(intent_count) != expected_intent_count:
        return False
    actionable_row_count, exit_row_count = connection.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE admitted_weight > 0),
            COUNT(*) FILTER (WHERE admitted_weight > 0 AND scheduled_exit_trade_date IS NOT NULL)
        FROM trade_plan_source_rows
        """
    ).fetchone()
    position_leg_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_position_leg AS leg
        INNER JOIN trade_plan_source_rows AS source
            ON source.portfolio_id = leg.portfolio_id
            AND source.candidate_nk = leg.candidate_nk
        WHERE source.admitted_weight > 0
        """
    ).fetchone()[0]
    exit_execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_exit_execution AS exit_execution
        INNER JOIN trade_plan_source_rows AS source
            ON source.portfolio_id = exit_execution.portfolio_id
            AND source.candidate_nk = exit_execution.candidate_nk
        WHERE source.admitted_weight > 0
            AND source.scheduled_exit_trade_date IS NOT NULL
        """
    ).fetchone()[0]
    carry_snapshot_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_carry_snapshot AS carry
        INNER JOIN trade_position_leg AS leg
            ON leg.position_leg_nk = carry.position_leg_nk
        INNER JOIN trade_plan_source_rows AS source
            ON source.portfolio_id = leg.portfolio_id
            AND source.candidate_nk = leg.candidate_nk
        WHERE source.admitted_weight > 0
        """
    ).fetchone()[0]
    return (
        int(position_leg_count) == int(actionable_row_count or 0)
        and int(exit_execution_count) == int(exit_row_count or 0)
        and int(carry_snapshot_count) == int((actionable_row_count or 0) + (exit_row_count or 0))
    )


def _record_reused_trade_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    phase_observer: Callable[..., None] | None = None,
    work_unit_count: int,
) -> tuple[dict[str, int], int, date | None]:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_work_unit_actions AS
        SELECT
            portfolio_id,
            symbol,
            source_row_count,
            last_reference_trade_date,
            source_fingerprint,
            'reused' AS status
        FROM trade_source_work_unit_summary
        """
    )
    if phase_observer is not None:
        phase_observer(
            "action_tables_ready",
            elapsed_seconds=0.0,
            row_count=work_unit_count,
            detail="fast_path_reused=1",
        )
        phase_observer(
            "write_transaction_started",
            elapsed_seconds=0.0,
            row_count=work_unit_count,
            detail="work_units_updated=0 fast_path=1",
        )
    phase_started = perf_counter()
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            INSERT INTO trade_run_order_intent (
                run_id, order_intent_nk, intent_status, materialization_action
            )
            SELECT ?, intent.order_intent_nk, intent.intent_status, 'reused'
            FROM trade_order_intent AS intent
            INNER JOIN trade_source_work_unit_summary AS source
                ON source.portfolio_id = intent.portfolio_id
                AND source.symbol = intent.symbol
            """,
            [run_id],
        )
        _insert_trade_work_queue_sql(connection=connection, run_id=run_id, action_table_name="trade_work_unit_actions")
        _upsert_trade_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    _record_phase(
        connection=connection,
        phase_observer=phase_observer,
        phase="write_transaction_committed",
        started_at=phase_started,
        table_name="trade_work_queue",
        detail="work_units_updated=0 fast_path=1",
    )
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM trade_source_work_unit_summary"
    ).fetchone()[0]
    return (
        {
            "intents_inserted": 0,
            "intents_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_order_intent AS intent
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = intent.portfolio_id
                        AND source.symbol = intent.symbol
                    """
                ).fetchone()[0]
            ),
            "intents_rematerialized": 0,
            "executions_inserted": 0,
            "executions_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_order_execution AS execution
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = execution.portfolio_id
                        AND source.symbol = execution.symbol
                    """
                ).fetchone()[0]
            ),
            "executions_rematerialized": 0,
            "position_legs_inserted": 0,
            "position_legs_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_position_leg AS leg
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = leg.portfolio_id
                        AND source.symbol = leg.symbol
                    """
                ).fetchone()[0]
            ),
            "position_legs_rematerialized": 0,
            "carry_rows_inserted": 0,
            "carry_rows_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_carry_snapshot AS carry
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = carry.portfolio_id
                        AND source.symbol = carry.symbol
                    """
                ).fetchone()[0]
            ),
            "carry_rows_rematerialized": 0,
            "exit_rows_inserted": 0,
            "exit_rows_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_exit_execution AS exit_execution
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = exit_execution.portfolio_id
                        AND source.symbol = exit_execution.symbol
                    """
                ).fetchone()[0]
            ),
            "exit_rows_rematerialized": 0,
        },
        0,
        latest_reference_trade_date,
    )


def _insert_trade_work_queue_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    action_table_name: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO trade_work_queue (
            queue_id, portfolio_id, symbol, status, source_row_count,
            last_reference_trade_date, source_fingerprint, claimed_at, finished_at
        )
        SELECT
            CONCAT(?, ':', portfolio_id, ':', symbol),
            portfolio_id,
            symbol,
            status,
            source_row_count,
            last_reference_trade_date,
            source_fingerprint,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM {action_table_name}
        """,
        [run_id],
    )


def _upsert_trade_checkpoint_sql(*, connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    connection.execute(
        """
        INSERT INTO trade_checkpoint (
            portfolio_id, symbol, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        )
        SELECT portfolio_id, symbol, last_reference_trade_date, source_fingerprint, ?, CURRENT_TIMESTAMP
        FROM trade_source_work_unit_summary
        ON CONFLICT(portfolio_id, symbol) DO UPDATE
        SET
            last_reference_trade_date = excluded.last_reference_trade_date,
            last_source_fingerprint = excluded.last_source_fingerprint,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [run_id],
    )


def profile_trade_live_path(
    *,
    portfolio_id: str = "core",
    settings: WorkspaceRoots | None = None,
) -> dict[str, object]:
    workspace = settings or default_settings()
    workspace.ensure_directories()

    with duckdb.connect(":memory:") as connection:
        source = _attach_trade_source_views(connection=connection, workspace=workspace, portfolio_id=portfolio_id)
        recorder = _TradePhaseRecorder(emit_stderr=True)
        recorder.record(
            "source_attached",
            elapsed_seconds=0.0,
            row_count=source.row_count,
            detail=f"work_units={source.work_unit_count}",
        )
        _attach_trade_profile_existing_catalog(connection=connection, trade_path=workspace.databases.trade)
        counts, work_units_updated, latest_reference_trade_date = _materialize_trade_sql(
            connection=connection,
            run_id="trade-profile",
            phase_observer=recorder.record,
            allow_fast_path=False,
            write_outputs=False,
            existing_catalog="trade_existing",
        )
    dominant_phase = max(recorder.metrics, key=lambda metric: metric.elapsed_seconds).phase if recorder.metrics else None
    return {
        "runner_name": "profile_trade_live_path",
        "source_row_count": source.row_count,
        "work_units_seen": source.work_unit_count,
        "work_units_updated": work_units_updated,
        "latest_reference_trade_date": latest_reference_trade_date.isoformat()
        if latest_reference_trade_date is not None
        else None,
        "dominant_phase": dominant_phase,
        "materialization_counts": counts,
        "phase_timings": [metric.as_dict() for metric in recorder.metrics],
    }


def _attach_trade_profile_existing_catalog(
    *,
    connection: duckdb.DuckDBPyConnection,
    trade_path: Path,
) -> None:
    connection.execute(f"ATTACH {_duckdb_string_literal(trade_path)} AS trade_existing (READ_ONLY)")


def _qualified_trade_table_name(*, existing_catalog: str | None, table_name: str) -> str:
    return f"{existing_catalog}.{table_name}" if existing_catalog is not None else table_name


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


def _temp_view_exists(*, connection: duckdb.DuckDBPyConnection, view_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_views()
        WHERE view_name = ?
        """,
        [view_name],
    ).fetchone()
    return bool(row[0])


def _pick_required_column(column_names: set[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in column_names:
            return candidate
    raise ValueError(f"Could not resolve required source columns from candidates: {candidates}")


def _duckdb_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
