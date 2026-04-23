"""Read-only MALF day semantic audit for the live formal ledger."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import matplotlib
import pandas as pd

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.contracts import Timeframe
from astock_lifespan_alpha.malf.source import DAY_ADJUST_METHOD, resolve_source_table

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


_PLOT_CONTEXT_BAR_COUNT = 5
_AUTHORITY_MATERIALS = (
    r"H:\Lifespan-Validated\malf-six\001.png",
    r"H:\Lifespan-Validated\malf-six\002.png",
    r"H:\Lifespan-Validated\malf-six\003.png",
    r"H:\Lifespan-Validated\malf-six\004.png",
    r"H:\Lifespan-Validated\malf-six\005.png",
    r"H:\Lifespan-Validated\malf-six\006.png",
    r"H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf",
    r"H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf",
    r"H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf",
    r"H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf",
    r"H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf",
    r"H:\astock_lifespan-alpha\docs\02-spec\01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md",
)
_EXCLUDED_SCOPE = (
    "execution_interface",
    "structure/filter",
    "alpha consumer actions",
    "week/month full-gate rollout",
)


@dataclass(frozen=True)
class HardRuleFinding:
    rule_name: str
    description: str
    violation_count: int
    sample_rows: list[dict[str, object]] = field(default_factory=list)

    @property
    def status(self) -> str:
        return "pass" if self.violation_count == 0 else "fail"

    def as_dict(self) -> dict[str, object]:
        return {
            "rule_name": self.rule_name,
            "description": self.description,
            "status": self.status,
            "violation_count": self.violation_count,
            "sample_rows": [_normalize_record(row) for row in self.sample_rows],
        }


@dataclass(frozen=True)
class SoftObservation:
    observation_name: str
    status: str
    value: float | int | str
    threshold: str
    note: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StaleRunSummary:
    run_id: str
    status: str
    symbols_total: int
    symbols_completed: int
    started_at: str | None
    finished_at: str | None
    message: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class SampleWindow:
    sample_id: str
    category: str
    symbol: str
    direction: str
    wave_id: str
    start_bar_dt: str
    end_bar_dt: str
    bar_count: int
    final_new_count: int
    final_no_new_span: int
    reborn_bar_count: int
    secondary_wave_id: str | None = None
    context_start_bar_dt: str | None = None
    context_end_bar_dt: str | None = None
    plot_path: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TableArtifact:
    table_name: str
    row_count: int
    table_ref: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MalfSemanticAuditSummary:
    runner_name: str
    report_id: str
    generated_at: str
    timeframe: str
    target_run_id: str
    target_source_path: str | None
    target_started_at: str | None
    target_finished_at: str | None
    target_symbol_total: int
    target_symbol_completed: int
    target_snapshot_rows: int
    target_wave_rows: int
    authority_materials: tuple[str, ...]
    excluded_scope: tuple[str, ...]
    stale_run_summaries: tuple[StaleRunSummary, ...]
    running_queue_count: int
    hard_rule_findings: tuple[HardRuleFinding, ...]
    soft_observations: tuple[SoftObservation, ...]
    table_artifacts: tuple[TableArtifact, ...]
    sample_windows: tuple[SampleWindow, ...]
    verdict: str
    artifact_database_path: str
    summary_json_path: str
    summary_markdown_path: str
    message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "report_id": self.report_id,
            "generated_at": self.generated_at,
            "timeframe": self.timeframe,
            "target_run_id": self.target_run_id,
            "target_source_path": self.target_source_path,
            "target_started_at": self.target_started_at,
            "target_finished_at": self.target_finished_at,
            "target_symbol_total": self.target_symbol_total,
            "target_symbol_completed": self.target_symbol_completed,
            "target_snapshot_rows": self.target_snapshot_rows,
            "target_wave_rows": self.target_wave_rows,
            "authority_materials": list(self.authority_materials),
            "excluded_scope": list(self.excluded_scope),
            "stale_run_summaries": [item.as_dict() for item in self.stale_run_summaries],
            "running_queue_count": self.running_queue_count,
            "hard_rule_findings": [item.as_dict() for item in self.hard_rule_findings],
            "soft_observations": [item.as_dict() for item in self.soft_observations],
            "table_artifacts": [item.as_dict() for item in self.table_artifacts],
            "sample_windows": [item.as_dict() for item in self.sample_windows],
            "verdict": self.verdict,
            "artifact_database_path": self.artifact_database_path,
            "summary_json_path": self.summary_json_path,
            "summary_markdown_path": self.summary_markdown_path,
            "message": self.message,
        }


@dataclass(frozen=True)
class _ResolvedRun:
    run_id: str
    source_path: str | None
    started_at: datetime | None
    finished_at: datetime | None
    symbols_total: int
    symbols_completed: int
    message: str | None
    resolution_note: str | None = None


def audit_malf_day_semantics(
    *,
    settings: WorkspaceRoots | None = None,
    run_id: str | None = None,
    sample_count: int = 12,
    output_root: Path | None = None,
) -> MalfSemanticAuditSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    resolved_source = resolve_source_table(workspace, Timeframe.DAY)
    if resolved_source is None:
        raise FileNotFoundError("Could not resolve the MALF day source table for semantic audit.")

    report_id = f"malf-day-semantic-audit-{uuid4().hex[:12]}"
    report_root = (output_root or (workspace.module_report_root("malf") / report_id)).resolve()
    charts_root = report_root / "charts"
    report_root.mkdir(parents=True, exist_ok=True)
    charts_root.mkdir(parents=True, exist_ok=True)
    artifact_database_path = report_root / f"{report_id}.duckdb"
    summary_json_path = report_root / f"{report_id}.json"
    summary_markdown_path = report_root / f"{report_id}.md"
    if artifact_database_path.exists():
        artifact_database_path.unlink()

    with duckdb.connect(str(workspace.databases.malf_day), read_only=True) as live_connection:
        resolved_run = _resolve_target_run(live_connection=live_connection, requested_run_id=run_id)
        stale_runs = _load_stale_run_summaries(live_connection)
        running_queue_count = live_connection.execute(
            "SELECT COUNT(*) FROM malf_work_queue WHERE status = 'running'"
        ).fetchone()[0]

    with duckdb.connect(str(artifact_database_path)) as audit_connection:
        audit_connection.execute(f"ATTACH '{workspace.databases.malf_day}' AS live (READ_ONLY)")
        _materialize_required_tables(audit_connection=audit_connection, run_id=resolved_run.run_id)
        hard_rule_findings = _collect_hard_rule_findings(audit_connection=audit_connection, run_id=resolved_run.run_id)
        sample_windows = _select_sample_windows(audit_connection=audit_connection, sample_count=sample_count)
        sample_windows = _materialize_sample_outputs(
            audit_connection=audit_connection,
            workspace=workspace,
            resolved_source_path=resolved_source.source_path,
            resolved_source_table=resolved_source.table_name,
            run_id=resolved_run.run_id,
            sample_windows=sample_windows,
            charts_root=charts_root,
        )
        _write_small_tables(
            audit_connection=audit_connection,
            hard_rule_findings=hard_rule_findings,
            sample_windows=sample_windows,
        )
        soft_observations = _collect_soft_observations(audit_connection=audit_connection)
        table_artifacts = tuple(
            TableArtifact(
                table_name=table_name,
                row_count=audit_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0],
                table_ref=f"{artifact_database_path}::{table_name}",
            )
            for table_name in (
                "wave_summary",
                "state_snapshot_sample",
                "break_events",
                "reborn_windows",
            )
        )
        snapshot_row_count = audit_connection.execute("SELECT COUNT(*) FROM wave_summary").fetchone()[0]
        state_snapshot_rows = audit_connection.execute(
            "SELECT COUNT(*) FROM live.malf_state_snapshot WHERE run_id = ?",
            [resolved_run.run_id],
        ).fetchone()[0]
        verdict = _classify_verdict(hard_rule_findings=hard_rule_findings, soft_observations=soft_observations)

    resolution_note = f" {resolved_run.resolution_note}" if resolved_run.resolution_note else ""
    message = (
        "MALF day semantic audit completed with "
        f"verdict={verdict}, hard_failures={sum(item.violation_count for item in hard_rule_findings)}, "
        f"sample_windows={len(sample_windows)}.{resolution_note}"
    )
    summary = MalfSemanticAuditSummary(
        runner_name="audit_malf_day_semantics",
        report_id=report_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        timeframe=Timeframe.DAY.value,
        target_run_id=resolved_run.run_id,
        target_source_path=resolved_run.source_path,
        target_started_at=_format_datetime(resolved_run.started_at),
        target_finished_at=_format_datetime(resolved_run.finished_at),
        target_symbol_total=resolved_run.symbols_total,
        target_symbol_completed=resolved_run.symbols_completed,
        target_snapshot_rows=state_snapshot_rows,
        target_wave_rows=snapshot_row_count,
        authority_materials=_AUTHORITY_MATERIALS,
        excluded_scope=_EXCLUDED_SCOPE,
        stale_run_summaries=stale_runs,
        running_queue_count=running_queue_count,
        hard_rule_findings=hard_rule_findings,
        soft_observations=soft_observations,
        table_artifacts=table_artifacts,
        sample_windows=sample_windows,
        verdict=verdict,
        artifact_database_path=str(artifact_database_path),
        summary_json_path=str(summary_json_path),
        summary_markdown_path=str(summary_markdown_path),
        message=message,
    )
    summary_json_path.write_text(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    summary_markdown_path.write_text(_render_markdown_summary(summary), encoding="utf-8")
    return summary


def _resolve_target_run(*, live_connection: duckdb.DuckDBPyConnection, requested_run_id: str | None) -> _ResolvedRun:
    if requested_run_id is None:
        row = live_connection.execute(
            """
            SELECT
                run_id,
                source_path,
                started_at,
                finished_at,
                symbols_total,
                symbols_completed,
                message
            FROM malf_run
            WHERE timeframe = 'day' AND status = 'completed'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        requested_row = None
    else:
        requested_row = live_connection.execute(
            """
            SELECT
                run_id,
                source_path,
                started_at,
                finished_at,
                symbols_total,
                symbols_completed,
                message
            FROM malf_run
            WHERE timeframe = 'day' AND run_id = ?
            LIMIT 1
            """,
            [requested_run_id],
        ).fetchone()
        row = requested_row
    if row is None:
        raise ValueError(f"Could not resolve MALF day audit target run: {requested_run_id or 'latest completed'}")
    target_run_id = str(row[0])
    snapshot_rows = live_connection.execute(
        "SELECT COUNT(*) FROM malf_state_snapshot WHERE run_id = ?",
        [target_run_id],
    ).fetchone()[0]
    if snapshot_rows == 0:
        fallback_row = live_connection.execute(
            """
            SELECT
                run.run_id,
                run.source_path,
                run.started_at,
                run.finished_at,
                run.symbols_total,
                run.symbols_completed,
                run.message
            FROM malf_run AS run
            WHERE run.timeframe = 'day'
              AND run.status = 'completed'
              AND EXISTS (
                    SELECT 1
                    FROM malf_state_snapshot AS snapshot
                    WHERE snapshot.run_id = run.run_id
              )
            ORDER BY run.started_at DESC
            LIMIT 1
            """
        ).fetchone()
        if fallback_row is None:
            raise ValueError(f"MALF day run {target_run_id} has no materialized state snapshots and no fallback run exists.")
        resolution_note = (
            f"Requested run {target_run_id} has 0 materialized ledger rows; "
            f"fell back to latest materialized completed run {fallback_row[0]}."
        )
        row = fallback_row
    else:
        resolution_note = None
    return _ResolvedRun(
        run_id=row[0],
        source_path=row[1],
        started_at=row[2],
        finished_at=row[3],
        symbols_total=int(row[4] or 0),
        symbols_completed=int(row[5] or 0),
        message=row[6],
        resolution_note=resolution_note,
    )


def _load_stale_run_summaries(
    live_connection: duckdb.DuckDBPyConnection,
) -> tuple[StaleRunSummary, ...]:
    rows = live_connection.execute(
        """
        SELECT
            run_id,
            status,
            symbols_total,
            symbols_completed,
            started_at,
            finished_at,
            message
        FROM malf_run
        WHERE timeframe = 'day' AND status = 'running'
        ORDER BY started_at DESC
        """
    ).fetchall()
    return tuple(
        StaleRunSummary(
            run_id=row[0],
            status=row[1],
            symbols_total=int(row[2] or 0),
            symbols_completed=int(row[3] or 0),
            started_at=_format_datetime(row[4]),
            finished_at=_format_datetime(row[5]),
            message=row[6],
        )
        for row in rows
    )


def _materialize_required_tables(*, audit_connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    safe_run_id = _sql_quote(run_id)
    audit_connection.execute(
        f"""
        CREATE OR REPLACE TABLE wave_summary AS
        WITH snapshot_enriched AS (
            SELECT
                symbol,
                timeframe,
                wave_id,
                direction,
                bar_dt,
                life_state,
                guard_price,
                LAG(guard_price) OVER (
                    PARTITION BY symbol, timeframe, wave_id
                    ORDER BY bar_dt
                ) AS prev_guard_price
            FROM live.malf_state_snapshot
            WHERE run_id = '{safe_run_id}'
        ),
        snapshot_stats AS (
            SELECT
                symbol,
                timeframe,
                wave_id,
                COUNT(*) AS bar_count,
                SUM(CASE WHEN life_state = 'reborn' THEN 1 ELSE 0 END) AS reborn_bar_count,
                SUM(
                    CASE
                        WHEN prev_guard_price IS NULL THEN 0
                        WHEN guard_price <> prev_guard_price THEN 1
                        ELSE 0
                    END
                ) AS guard_update_count
            FROM snapshot_enriched
            GROUP BY 1, 2, 3
        )
        SELECT
            w.run_id,
            w.symbol,
            w.timeframe,
            w.wave_id,
            w.direction,
            w.start_bar_dt,
            w.end_bar_dt,
            w.guard_bar_dt,
            w.guard_price,
            w.extreme_price,
            w.new_count,
            w.no_new_span,
            w.life_state,
            COALESCE(snapshot_stats.bar_count, 0) AS bar_count,
            COALESCE(snapshot_stats.reborn_bar_count, 0) AS reborn_bar_count,
            COALESCE(snapshot_stats.guard_update_count, 0) AS guard_update_count
        FROM live.malf_wave_ledger AS w
        LEFT JOIN snapshot_stats
            ON snapshot_stats.symbol = w.symbol
           AND snapshot_stats.timeframe = w.timeframe
           AND snapshot_stats.wave_id = w.wave_id
        WHERE w.run_id = '{safe_run_id}'
        """
    )
    audit_connection.execute(
        f"""
        CREATE OR REPLACE TABLE break_events AS
        WITH ordered AS (
            SELECT
                symbol,
                timeframe,
                bar_dt,
                wave_id,
                direction,
                life_state,
                new_count,
                no_new_span,
                LAG(wave_id) OVER (
                    PARTITION BY symbol, timeframe
                    ORDER BY bar_dt
                ) AS prev_wave_id,
                LAG(direction) OVER (
                    PARTITION BY symbol, timeframe
                    ORDER BY bar_dt
                ) AS prev_direction,
                LAG(life_state) OVER (
                    PARTITION BY symbol, timeframe
                    ORDER BY bar_dt
                ) AS prev_life_state
            FROM live.malf_state_snapshot
            WHERE run_id = '{safe_run_id}'
        ),
        changes AS (
            SELECT *
            FROM ordered
            WHERE prev_wave_id IS NOT NULL AND wave_id <> prev_wave_id
        )
        SELECT
            changes.symbol,
            changes.timeframe,
            changes.bar_dt AS break_dt,
            changes.prev_wave_id AS old_wave_id,
            changes.prev_direction AS old_direction,
            changes.prev_life_state AS old_snapshot_life_state,
            previous_wave.life_state AS old_wave_terminal_life_state,
            changes.wave_id AS new_wave_id,
            changes.direction AS new_direction,
            changes.life_state AS new_life_state,
            changes.new_count AS new_wave_new_count,
            changes.no_new_span AS new_wave_no_new_span,
            CASE
                WHEN changes.direction = 'up' THEN 'break_up'
                ELSE 'break_down'
            END AS expected_break_pivot_type,
            pivot_row.pivot_type AS break_pivot_type,
            pivot_row.price AS break_price
        FROM changes
        LEFT JOIN live.malf_wave_ledger AS previous_wave
            ON previous_wave.wave_id = changes.prev_wave_id
           AND previous_wave.run_id = '{safe_run_id}'
        LEFT JOIN live.malf_pivot_ledger AS pivot_row
            ON pivot_row.run_id = '{safe_run_id}'
           AND pivot_row.wave_id = changes.prev_wave_id
           AND pivot_row.bar_dt = changes.bar_dt
           AND pivot_row.pivot_type = CASE
               WHEN changes.direction = 'up' THEN 'break_up'
               ELSE 'break_down'
           END
        """
    )
    audit_connection.execute(
        f"""
        CREATE OR REPLACE TABLE reborn_windows AS
        WITH grouped AS (
            SELECT
                snapshot.symbol,
                snapshot.timeframe,
                snapshot.wave_id,
                snapshot.direction,
                MIN(CASE WHEN snapshot.life_state = 'reborn' THEN snapshot.bar_dt END) AS reborn_start_dt,
                MAX(CASE WHEN snapshot.life_state = 'reborn' THEN snapshot.bar_dt END) AS reborn_end_dt,
                SUM(CASE WHEN snapshot.life_state = 'reborn' THEN 1 ELSE 0 END) AS reborn_bar_count,
                MIN(CASE WHEN snapshot.life_state = 'alive' THEN snapshot.bar_dt END) AS first_alive_dt,
                MIN(
                    CASE
                        WHEN pivot_row.pivot_type = CASE
                            WHEN snapshot.direction = 'up' THEN 'HH'
                            ELSE 'LL'
                        END THEN snapshot.bar_dt
                    END
                ) AS first_new_extreme_dt
            FROM live.malf_state_snapshot AS snapshot
            LEFT JOIN live.malf_pivot_ledger AS pivot_row
                ON pivot_row.run_id = '{safe_run_id}'
               AND pivot_row.wave_id = snapshot.wave_id
               AND pivot_row.bar_dt = snapshot.bar_dt
               AND pivot_row.pivot_type = CASE
                   WHEN snapshot.direction = 'up' THEN 'HH'
                   ELSE 'LL'
               END
            WHERE snapshot.run_id = '{safe_run_id}'
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            grouped.symbol,
            grouped.timeframe,
            grouped.wave_id,
            grouped.direction,
            wave_summary.start_bar_dt AS break_dt,
            grouped.reborn_start_dt,
            grouped.reborn_end_dt,
            grouped.reborn_bar_count,
            grouped.first_alive_dt,
            grouped.first_new_extreme_dt
        FROM grouped
        JOIN wave_summary
            ON wave_summary.wave_id = grouped.wave_id
        WHERE grouped.reborn_bar_count > 0
        """
    )


def _collect_hard_rule_findings(
    *,
    audit_connection: duckdb.DuckDBPyConnection,
    run_id: str,
) -> tuple[HardRuleFinding, ...]:
    safe_run_id = _sql_quote(run_id)
    rule_sqls = (
        (
            "new_count_transition_rule",
            "new_count 只允许在同波 HH/LL bar 上加一，换波首 bar 必须回到 0。",
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    timeframe,
                    bar_dt,
                    wave_id,
                    direction,
                    new_count,
                    LAG(wave_id) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_wave_id,
                    LAG(new_count) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_new_count
                FROM live.malf_state_snapshot
                WHERE run_id = '{safe_run_id}'
            )
            SELECT
                ordered.symbol,
                ordered.bar_dt,
                ordered.prev_wave_id,
                ordered.wave_id,
                ordered.direction,
                ordered.prev_new_count,
                ordered.new_count
            FROM ordered
            LEFT JOIN live.malf_pivot_ledger AS pivot_row
                ON pivot_row.run_id = '{safe_run_id}'
               AND pivot_row.wave_id = ordered.wave_id
               AND pivot_row.bar_dt = ordered.bar_dt
               AND pivot_row.pivot_type = CASE
                   WHEN ordered.direction = 'up' THEN 'HH'
                   ELSE 'LL'
               END
            WHERE ordered.prev_wave_id IS NOT NULL
              AND (
                    (ordered.wave_id <> ordered.prev_wave_id AND ordered.new_count <> 0)
                 OR (ordered.wave_id = ordered.prev_wave_id AND (ordered.new_count - ordered.prev_new_count) NOT IN (0, 1))
                 OR (ordered.wave_id = ordered.prev_wave_id AND (ordered.new_count - ordered.prev_new_count) = 1 AND pivot_row.pivot_nk IS NULL)
                 OR (ordered.wave_id = ordered.prev_wave_id AND (ordered.new_count - ordered.prev_new_count) = 0 AND pivot_row.pivot_nk IS NOT NULL)
              )
            """,
        ),
        (
            "no_new_span_transition_rule",
            "no_new_span 只允许在同波未创新值时递增，见新值或换波时归零。",
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    timeframe,
                    bar_dt,
                    wave_id,
                    new_count,
                    no_new_span,
                    LAG(wave_id) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_wave_id,
                    LAG(new_count) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_new_count,
                    LAG(no_new_span) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_no_new_span
                FROM live.malf_state_snapshot
                WHERE run_id = '{safe_run_id}'
            )
            SELECT
                symbol,
                bar_dt,
                prev_wave_id,
                wave_id,
                prev_new_count,
                new_count,
                prev_no_new_span,
                no_new_span
            FROM ordered
            WHERE prev_wave_id IS NOT NULL
              AND (
                    (wave_id <> prev_wave_id AND no_new_span <> 0)
                 OR (wave_id = prev_wave_id AND (new_count - prev_new_count) = 1 AND no_new_span <> 0)
                 OR (wave_id = prev_wave_id AND (new_count - prev_new_count) = 0 AND no_new_span <> prev_no_new_span + 1)
              )
            """,
        ),
        (
            "wave_id_break_rule",
            "wave_id 只允许在 break_up / break_down 发生的 bar 切换。",
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    timeframe,
                    bar_dt,
                    wave_id,
                    direction,
                    LAG(wave_id) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_wave_id
                FROM live.malf_state_snapshot
                WHERE run_id = '{safe_run_id}'
            )
            SELECT
                ordered.symbol,
                ordered.bar_dt,
                ordered.prev_wave_id,
                ordered.wave_id,
                ordered.direction
            FROM ordered
            LEFT JOIN live.malf_pivot_ledger AS pivot_row
                ON pivot_row.run_id = '{safe_run_id}'
               AND pivot_row.wave_id = ordered.prev_wave_id
               AND pivot_row.bar_dt = ordered.bar_dt
               AND pivot_row.pivot_type = CASE
                   WHEN ordered.direction = 'up' THEN 'break_up'
                   ELSE 'break_down'
               END
            WHERE ordered.prev_wave_id IS NOT NULL
              AND ordered.wave_id <> ordered.prev_wave_id
              AND pivot_row.pivot_nk IS NULL
            """,
        ),
        (
            "new_wave_reborn_rule",
            "新 wave 首 bar 必须是 reborn，且 new_count = 0 / no_new_span = 0。",
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    timeframe,
                    bar_dt,
                    wave_id,
                    life_state,
                    new_count,
                    no_new_span,
                    LAG(wave_id) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_wave_id
                FROM live.malf_state_snapshot
                WHERE run_id = '{safe_run_id}'
            )
            SELECT
                symbol,
                bar_dt,
                prev_wave_id,
                wave_id,
                life_state,
                new_count,
                no_new_span
            FROM ordered
            WHERE prev_wave_id IS NOT NULL
              AND wave_id <> prev_wave_id
              AND (
                    life_state <> 'reborn'
                 OR new_count <> 0
                 OR no_new_span <> 0
              )
            """,
        ),
        (
            "reborn_to_alive_rule",
            "从 reborn 进入 alive 的首个 bar 必须同时出现当前方向 HH/LL。",
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    timeframe,
                    bar_dt,
                    wave_id,
                    direction,
                    life_state,
                    new_count,
                    LAG(life_state) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_life_state,
                    LAG(new_count) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_new_count
                FROM live.malf_state_snapshot
                WHERE run_id = '{safe_run_id}'
            )
            SELECT
                ordered.symbol,
                ordered.bar_dt,
                ordered.wave_id,
                ordered.direction,
                ordered.prev_life_state,
                ordered.life_state,
                ordered.prev_new_count,
                ordered.new_count
            FROM ordered
            LEFT JOIN live.malf_pivot_ledger AS pivot_row
                ON pivot_row.run_id = '{safe_run_id}'
               AND pivot_row.wave_id = ordered.wave_id
               AND pivot_row.bar_dt = ordered.bar_dt
               AND pivot_row.pivot_type = CASE
                   WHEN ordered.direction = 'up' THEN 'HH'
                   ELSE 'LL'
               END
            WHERE ordered.prev_life_state = 'reborn'
              AND ordered.life_state = 'alive'
              AND (
                    ordered.prev_new_count <> 0
                 OR ordered.new_count <> 1
                 OR pivot_row.pivot_nk IS NULL
              )
            """,
        ),
        (
            "guard_update_pivot_rule",
            "guard_price 更新必须能在同波同 bar 找到 HL/LH pivot。",
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    timeframe,
                    bar_dt,
                    wave_id,
                    direction,
                    guard_price,
                    LAG(wave_id) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_wave_id,
                    LAG(guard_price) OVER (
                        PARTITION BY symbol, timeframe
                        ORDER BY bar_dt
                    ) AS prev_guard_price
                FROM live.malf_state_snapshot
                WHERE run_id = '{safe_run_id}'
            )
            SELECT
                ordered.symbol,
                ordered.bar_dt,
                ordered.wave_id,
                ordered.direction,
                ordered.prev_guard_price,
                ordered.guard_price
            FROM ordered
            LEFT JOIN live.malf_pivot_ledger AS pivot_row
                ON pivot_row.run_id = '{safe_run_id}'
               AND pivot_row.wave_id = ordered.wave_id
               AND pivot_row.bar_dt = ordered.bar_dt
               AND pivot_row.pivot_type = CASE
                   WHEN ordered.direction = 'up' THEN 'HL'
                   ELSE 'LH'
               END
               AND pivot_row.price = ordered.guard_price
            WHERE ordered.prev_wave_id IS NOT NULL
              AND ordered.wave_id = ordered.prev_wave_id
              AND ordered.prev_guard_price IS NOT NULL
              AND ordered.guard_price <> ordered.prev_guard_price
              AND pivot_row.pivot_nk IS NULL
            """,
        ),
        (
            "zone_classification_rule",
            "wave_position_zone 必须与现代码分类函数完全一致。",
            f"""
            SELECT
                symbol,
                bar_dt,
                wave_id,
                life_state,
                update_rank,
                stagnation_rank,
                wave_position_zone,
                CASE
                    WHEN life_state = 'reborn' THEN 'early_progress'
                    WHEN stagnation_rank < 50.0 AND update_rank < 50.0 THEN 'early_progress'
                    WHEN stagnation_rank < 50.0 AND update_rank >= 50.0 THEN 'mature_progress'
                    WHEN stagnation_rank >= 50.0 AND update_rank >= 50.0 THEN 'mature_stagnation'
                    ELSE 'weak_stagnation'
                END AS expected_wave_position_zone
            FROM live.malf_state_snapshot
            WHERE run_id = '{safe_run_id}'
              AND wave_position_zone <> CASE
                    WHEN life_state = 'reborn' THEN 'early_progress'
                    WHEN stagnation_rank < 50.0 AND update_rank < 50.0 THEN 'early_progress'
                    WHEN stagnation_rank < 50.0 AND update_rank >= 50.0 THEN 'mature_progress'
                    WHEN stagnation_rank >= 50.0 AND update_rank >= 50.0 THEN 'mature_stagnation'
                    ELSE 'weak_stagnation'
                END
            """,
        ),
    )
    findings: list[HardRuleFinding] = []
    for rule_name, description, sql in rule_sqls:
        violation_count = audit_connection.execute(f"SELECT COUNT(*) FROM ({sql}) AS violations").fetchone()[0]
        sample_rows = _records_from_df(
            audit_connection.execute(f"SELECT * FROM ({sql}) AS violations LIMIT 10").df()
        )
        findings.append(
            HardRuleFinding(
                rule_name=rule_name,
                description=description,
                violation_count=int(violation_count),
                sample_rows=sample_rows,
            )
        )
    return tuple(findings)


def _select_sample_windows(
    *,
    audit_connection: duckdb.DuckDBPyConnection,
    sample_count: int,
) -> tuple[SampleWindow, ...]:
    transition_count = max(1, sample_count // 3)
    remaining = max(sample_count - transition_count, 0)
    up_count = max(1, remaining // 2) if remaining else 0
    down_count = max(0, remaining - up_count)
    used_wave_ids: set[str] = set()
    selected: list[SampleWindow] = []
    selected.extend(_select_directional_windows(audit_connection, "up", up_count, used_wave_ids))
    selected.extend(_select_directional_windows(audit_connection, "down", down_count, used_wave_ids))
    selected.extend(_select_transition_windows(audit_connection, transition_count, used_wave_ids))
    if len(selected) > sample_count:
        selected = selected[:sample_count]
    return tuple(selected)


def _select_directional_windows(
    audit_connection: duckdb.DuckDBPyConnection,
    direction: str,
    count: int,
    used_wave_ids: set[str],
) -> list[SampleWindow]:
    if count <= 0:
        return []
    smooth_count = max(1, count // 2)
    stagnation_count = max(count - smooth_count, 0)
    selected: list[SampleWindow] = []
    smooth_candidates = audit_connection.execute(
        """
        SELECT
            symbol,
            wave_id,
            direction,
            start_bar_dt,
            end_bar_dt,
            bar_count,
            new_count,
            no_new_span,
            reborn_bar_count
        FROM wave_summary
        WHERE direction = ?
        ORDER BY new_count DESC, no_new_span ASC, bar_count DESC, wave_id
        LIMIT 200
        """,
        [direction],
    ).df()
    selected.extend(
        _pick_candidate_windows(
            candidates=smooth_candidates,
            category=f"{direction}_progress",
            limit=smooth_count,
            used_wave_ids=used_wave_ids,
        )
    )
    if stagnation_count > 0:
        stagnation_candidates = audit_connection.execute(
            """
            SELECT
                symbol,
                wave_id,
                direction,
                start_bar_dt,
                end_bar_dt,
                bar_count,
                new_count,
                no_new_span,
                reborn_bar_count
            FROM wave_summary
            WHERE direction = ?
            ORDER BY no_new_span DESC, guard_update_count DESC, bar_count DESC, wave_id
            LIMIT 200
            """,
            [direction],
        ).df()
        selected.extend(
            _pick_candidate_windows(
                candidates=stagnation_candidates,
                category=f"{direction}_stagnation",
                limit=stagnation_count,
                used_wave_ids=used_wave_ids,
            )
        )
    return selected


def _pick_candidate_windows(
    *,
    candidates: pd.DataFrame,
    category: str,
    limit: int,
    used_wave_ids: set[str],
) -> list[SampleWindow]:
    selected: list[SampleWindow] = []
    sample_index = 1
    for row in candidates.to_dict(orient="records"):
        if len(selected) >= limit:
            break
        if row["wave_id"] in used_wave_ids:
            continue
        used_wave_ids.add(str(row["wave_id"]))
        selected.append(
            SampleWindow(
                sample_id=f"{category}-{sample_index:02d}",
                category=category,
                symbol=str(row["symbol"]),
                direction=str(row["direction"]),
                wave_id=str(row["wave_id"]),
                start_bar_dt=_format_datetime(row["start_bar_dt"]) or "",
                end_bar_dt=_format_datetime(row["end_bar_dt"]) or "",
                bar_count=int(row["bar_count"] or 0),
                final_new_count=int(row["new_count"] or 0),
                final_no_new_span=int(row["no_new_span"] or 0),
                reborn_bar_count=int(row["reborn_bar_count"] or 0),
            )
        )
        sample_index += 1
    return selected


def _select_transition_windows(
    audit_connection: duckdb.DuckDBPyConnection,
    count: int,
    used_wave_ids: set[str],
) -> list[SampleWindow]:
    if count <= 0:
        return []
    to_up_count = count // 2
    to_down_count = count - to_up_count
    selected: list[SampleWindow] = []
    selected.extend(
        _pick_transition_direction(
            audit_connection=audit_connection,
            new_direction="up",
            limit=to_up_count,
            used_wave_ids=used_wave_ids,
        )
    )
    selected.extend(
        _pick_transition_direction(
            audit_connection=audit_connection,
            new_direction="down",
            limit=to_down_count,
            used_wave_ids=used_wave_ids,
        )
    )
    return selected


def _pick_transition_direction(
    *,
    audit_connection: duckdb.DuckDBPyConnection,
    new_direction: str,
    limit: int,
    used_wave_ids: set[str],
) -> list[SampleWindow]:
    if limit <= 0:
        return []
    candidates = audit_connection.execute(
        """
        SELECT
            break_events.symbol,
            break_events.break_dt,
            break_events.old_wave_id,
            break_events.new_wave_id,
            break_events.new_direction,
            COALESCE(reborn_windows.first_alive_dt, break_events.break_dt) AS transition_end_dt,
            COALESCE(reborn_windows.reborn_bar_count, 0) AS reborn_bar_count
        FROM break_events
        LEFT JOIN reborn_windows
            ON reborn_windows.wave_id = break_events.new_wave_id
        WHERE break_events.new_direction = ?
        ORDER BY COALESCE(reborn_windows.reborn_bar_count, 0) DESC, break_events.break_dt DESC
        LIMIT 200
        """,
        [new_direction],
    ).df()
    selected: list[SampleWindow] = []
    sample_index = 1
    for row in candidates.to_dict(orient="records"):
        if len(selected) >= limit:
            break
        if row["new_wave_id"] in used_wave_ids:
            continue
        used_wave_ids.add(str(row["new_wave_id"]))
        selected.append(
            SampleWindow(
                sample_id=f"transition_to_{new_direction}-{sample_index:02d}",
                category=f"transition_to_{new_direction}",
                symbol=str(row["symbol"]),
                direction=str(row["new_direction"]),
                wave_id=str(row["new_wave_id"]),
                secondary_wave_id=str(row["old_wave_id"]),
                start_bar_dt=_format_datetime(row["break_dt"]) or "",
                end_bar_dt=_format_datetime(row["transition_end_dt"]) or "",
                bar_count=0,
                final_new_count=0,
                final_no_new_span=0,
                reborn_bar_count=int(row["reborn_bar_count"] or 0),
            )
        )
        sample_index += 1
    return selected


def _materialize_sample_outputs(
    *,
    audit_connection: duckdb.DuckDBPyConnection,
    workspace: WorkspaceRoots,
    resolved_source_path: Path,
    resolved_source_table: str,
    run_id: str,
    sample_windows: tuple[SampleWindow, ...],
    charts_root: Path,
) -> tuple[SampleWindow, ...]:
    if not sample_windows:
        audit_connection.execute(
            """
            CREATE OR REPLACE TABLE state_snapshot_sample AS
            SELECT
                CAST(NULL AS TEXT) AS sample_id,
                CAST(NULL AS TEXT) AS category,
                CAST(NULL AS TEXT) AS symbol,
                CAST(NULL AS TIMESTAMP) AS bar_dt,
                CAST(NULL AS DOUBLE) AS open,
                CAST(NULL AS DOUBLE) AS high,
                CAST(NULL AS DOUBLE) AS low,
                CAST(NULL AS DOUBLE) AS close,
                CAST(NULL AS TEXT) AS wave_id,
                CAST(NULL AS TEXT) AS direction,
                CAST(NULL AS BIGINT) AS new_count,
                CAST(NULL AS BIGINT) AS no_new_span,
                CAST(NULL AS TEXT) AS life_state,
                CAST(NULL AS DOUBLE) AS guard_price,
                CAST(NULL AS DOUBLE) AS update_rank,
                CAST(NULL AS DOUBLE) AS stagnation_rank,
                CAST(NULL AS TEXT) AS wave_position_zone,
                CAST(NULL AS BOOLEAN) AS is_focus_window
            WHERE FALSE
            """
        )
        return sample_windows

    with duckdb.connect(str(workspace.databases.malf_day), read_only=True) as live_connection, duckdb.connect(
        str(resolved_source_path), read_only=True
    ) as source_connection:
        source_cache: dict[str, pd.DataFrame] = {}
        snapshot_cache: dict[str, pd.DataFrame] = {}
        pivot_cache: dict[str, pd.DataFrame] = {}
        sample_frames: list[pd.DataFrame] = []
        materialized_windows: list[SampleWindow] = []
        for window in sample_windows:
            source_rows = source_cache.setdefault(
                window.symbol,
                _load_source_rows(source_connection=source_connection, table_name=resolved_source_table, symbol=window.symbol),
            )
            snapshot_rows = snapshot_cache.setdefault(
                window.symbol,
                _load_snapshot_rows(live_connection=live_connection, run_id=run_id, symbol=window.symbol),
            )
            pivot_rows = pivot_cache.setdefault(
                window.symbol,
                _load_pivot_rows(live_connection=live_connection, run_id=run_id, symbol=window.symbol),
            )
            sample_frame, plotted_window = _render_sample_window(
                source_rows=source_rows,
                snapshot_rows=snapshot_rows,
                pivot_rows=pivot_rows,
                window=window,
                charts_root=charts_root,
            )
            sample_frames.append(sample_frame)
            materialized_windows.append(plotted_window)
        state_snapshot_sample = pd.concat(sample_frames, ignore_index=True)
        audit_connection.register("_state_snapshot_sample", state_snapshot_sample)
        try:
            audit_connection.execute(
                """
                CREATE OR REPLACE TABLE state_snapshot_sample AS
                SELECT *
                FROM _state_snapshot_sample
                ORDER BY sample_id, bar_dt
                """
            )
        finally:
            audit_connection.unregister("_state_snapshot_sample")
    return tuple(materialized_windows)


def _load_source_rows(
    *,
    source_connection: duckdb.DuckDBPyConnection,
    table_name: str,
    symbol: str,
) -> pd.DataFrame:
    column_info = source_connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    column_names = {row[1] for row in column_info}
    symbol_column = "symbol" if "symbol" in column_names else "code"
    date_column = "bar_dt" if "bar_dt" in column_names else ("trade_date" if "trade_date" in column_names else "date")
    params: list[object] = [symbol]
    where_clause = f"WHERE {symbol_column} = ?"
    if "adjust_method" in column_names:
        where_clause = f"WHERE {symbol_column} = ? AND adjust_method = ?"
        params.append(DAY_ADJUST_METHOD)
    source_rows = source_connection.execute(
        f"""
        SELECT
            CAST({date_column} AS TIMESTAMP) AS bar_dt,
            CAST(open AS DOUBLE) AS open,
            CAST(high AS DOUBLE) AS high,
            CAST(low AS DOUBLE) AS low,
            CAST(close AS DOUBLE) AS close
        FROM {table_name}
        {where_clause}
        ORDER BY bar_dt
        """,
        params,
    ).df()
    source_rows["bar_dt"] = pd.to_datetime(source_rows["bar_dt"])
    return source_rows


def _load_snapshot_rows(
    *,
    live_connection: duckdb.DuckDBPyConnection,
    run_id: str,
    symbol: str,
) -> pd.DataFrame:
    snapshot_rows = live_connection.execute(
        """
        SELECT
            bar_dt,
            wave_id,
            direction,
            guard_price,
            new_count,
            no_new_span,
            life_state,
            update_rank,
            stagnation_rank,
            wave_position_zone
        FROM malf_state_snapshot
        WHERE run_id = ? AND symbol = ?
        ORDER BY bar_dt
        """,
        [run_id, symbol],
    ).df()
    snapshot_rows["bar_dt"] = pd.to_datetime(snapshot_rows["bar_dt"])
    return snapshot_rows


def _load_pivot_rows(
    *,
    live_connection: duckdb.DuckDBPyConnection,
    run_id: str,
    symbol: str,
) -> pd.DataFrame:
    pivot_rows = live_connection.execute(
        """
        SELECT
            bar_dt,
            wave_id,
            pivot_type,
            price
        FROM malf_pivot_ledger
        WHERE run_id = ? AND symbol = ?
        ORDER BY bar_dt
        """,
        [run_id, symbol],
    ).df()
    pivot_rows["bar_dt"] = pd.to_datetime(pivot_rows["bar_dt"])
    return pivot_rows


def _render_sample_window(
    *,
    source_rows: pd.DataFrame,
    snapshot_rows: pd.DataFrame,
    pivot_rows: pd.DataFrame,
    window: SampleWindow,
    charts_root: Path,
) -> tuple[pd.DataFrame, SampleWindow]:
    focus_start = pd.Timestamp(window.start_bar_dt)
    focus_end = pd.Timestamp(window.end_bar_dt)
    context_start_index, context_end_index = _resolve_context_indexes(
        source_rows=source_rows,
        focus_start=focus_start,
        focus_end=focus_end,
    )
    source_slice = source_rows.iloc[context_start_index : context_end_index + 1].copy()
    if source_slice.empty:
        raise ValueError(f"Could not resolve source rows for sample window: {window.sample_id}")
    context_start = pd.Timestamp(source_slice["bar_dt"].iloc[0])
    context_end = pd.Timestamp(source_slice["bar_dt"].iloc[-1])
    snapshot_slice = snapshot_rows[
        (snapshot_rows["bar_dt"] >= context_start) & (snapshot_rows["bar_dt"] <= context_end)
    ].copy()
    pivot_slice = pivot_rows[(pivot_rows["bar_dt"] >= context_start) & (pivot_rows["bar_dt"] <= context_end)].copy()
    merged = source_slice.merge(snapshot_slice, on="bar_dt", how="left")
    merged["sample_id"] = window.sample_id
    merged["category"] = window.category
    merged["symbol"] = window.symbol
    merged["is_focus_window"] = (merged["bar_dt"] >= focus_start) & (merged["bar_dt"] <= focus_end)
    chart_path = charts_root / f"{window.sample_id}.png"
    _plot_sample_chart(
        merged=merged,
        pivot_slice=pivot_slice,
        window=window,
        chart_path=chart_path,
        focus_start=focus_start,
        focus_end=focus_end,
    )
    rendered_window = SampleWindow(
        sample_id=window.sample_id,
        category=window.category,
        symbol=window.symbol,
        direction=window.direction,
        wave_id=window.wave_id,
        secondary_wave_id=window.secondary_wave_id,
        start_bar_dt=window.start_bar_dt,
        end_bar_dt=window.end_bar_dt,
        bar_count=int(merged["wave_id"].eq(window.wave_id).sum()),
        final_new_count=int(merged.loc[merged["wave_id"] == window.wave_id, "new_count"].max() or 0),
        final_no_new_span=int(merged.loc[merged["wave_id"] == window.wave_id, "no_new_span"].max() or 0),
        reborn_bar_count=int(merged.loc[merged["wave_id"] == window.wave_id, "life_state"].eq("reborn").sum()),
        context_start_bar_dt=context_start.isoformat(),
        context_end_bar_dt=context_end.isoformat(),
        plot_path=str(chart_path),
    )
    return merged[
        [
            "sample_id",
            "category",
            "symbol",
            "bar_dt",
            "open",
            "high",
            "low",
            "close",
            "wave_id",
            "direction",
            "new_count",
            "no_new_span",
            "life_state",
            "guard_price",
            "update_rank",
            "stagnation_rank",
            "wave_position_zone",
            "is_focus_window",
        ]
    ], rendered_window


def _resolve_context_indexes(
    *,
    source_rows: pd.DataFrame,
    focus_start: pd.Timestamp,
    focus_end: pd.Timestamp,
) -> tuple[int, int]:
    eligible_indexes = source_rows.index[
        (source_rows["bar_dt"] >= focus_start) & (source_rows["bar_dt"] <= focus_end)
    ].tolist()
    if not eligible_indexes:
        source_rows = source_rows.copy()
        source_rows["distance"] = (
            (source_rows["bar_dt"] - focus_start).abs() + (source_rows["bar_dt"] - focus_end).abs()
        )
        nearest_index = int(source_rows.sort_values("distance").index[0])
        eligible_indexes = [nearest_index]
    start_index = max(min(eligible_indexes) - _PLOT_CONTEXT_BAR_COUNT, 0)
    end_index = min(max(eligible_indexes) + _PLOT_CONTEXT_BAR_COUNT, len(source_rows) - 1)
    return start_index, end_index


def _plot_sample_chart(
    *,
    merged: pd.DataFrame,
    pivot_slice: pd.DataFrame,
    window: SampleWindow,
    chart_path: Path,
    focus_start: pd.Timestamp,
    focus_end: pd.Timestamp,
) -> None:
    x_positions = list(range(len(merged)))
    x_labels = [pd.Timestamp(item).strftime("%Y-%m-%d") for item in merged["bar_dt"]]
    bar_dt_to_position = {pd.Timestamp(bar_dt): position for position, bar_dt in enumerate(merged["bar_dt"])}
    focus_start_position = next(
        (position for position, bar_dt in enumerate(merged["bar_dt"]) if pd.Timestamp(bar_dt) >= focus_start),
        0,
    )
    focus_end_position = next(
        (
            position
            for position, bar_dt in reversed(list(enumerate(merged["bar_dt"])))
            if pd.Timestamp(bar_dt) <= focus_end
        ),
        len(merged) - 1,
    )
    fig, axes = plt.subplots(6, 1, figsize=(14, 16), sharex=True, constrained_layout=True)
    for axis in axes:
        axis.axvspan(focus_start_position, focus_end_position, color="#f3f0d7", alpha=0.4)
        axis.grid(alpha=0.2)

    life_colors = {"alive": "#1f77b4", "reborn": "#ff7f0e", "broken": "#d62728"}
    zone_codes = {
        "early_progress": 0,
        "mature_progress": 1,
        "mature_stagnation": 2,
        "weak_stagnation": 3,
    }
    pivot_styles = {
        "HH": ("^", "#1f77b4"),
        "HL": ("o", "#2ca02c"),
        "LL": ("v", "#d62728"),
        "LH": ("s", "#9467bd"),
        "break_up": ("X", "#8c564b"),
        "break_down": ("P", "#7f7f7f"),
    }

    axes[0].plot(x_positions, merged["close"], color="#111111", linewidth=1.5)
    for life_state, color in life_colors.items():
        subset = merged[merged["life_state"] == life_state]
        axes[0].scatter(
            subset.index.map(lambda index: x_positions[index - merged.index[0]]),
            subset["close"],
            label=life_state,
            color=color,
            s=20,
        )
    axes[0].set_ylabel("close")
    axes[0].set_title(f"{window.sample_id} | {window.symbol} | {window.category} | {window.wave_id}")
    axes[0].legend(loc="upper left", ncol=3, fontsize=8)

    axes[1].step(x_positions, merged["new_count"].ffill().fillna(0), where="mid", color="#1f77b4")
    axes[1].set_ylabel("new_count")

    axes[2].step(
        x_positions,
        merged["no_new_span"].ffill().fillna(0),
        where="mid",
        color="#ff7f0e",
    )
    axes[2].set_ylabel("no_new_span")

    axes[3].plot(x_positions, merged["close"], color="#a0a0a0", linewidth=1.2, label="close")
    axes[3].step(
        x_positions,
        merged["guard_price"].ffill(),
        where="mid",
        color="#2ca02c",
        linewidth=1.4,
        label="guard_price",
    )
    axes[3].set_ylabel("guard")
    axes[3].legend(loc="upper left", fontsize=8)

    axes[4].plot(x_positions, merged["close"], color="#c0c0c0", linewidth=1.0)
    for pivot_type, (marker, color) in pivot_styles.items():
        subset = pivot_slice[pivot_slice["pivot_type"] == pivot_type]
        if subset.empty:
            continue
        axes[4].scatter(
            [bar_dt_to_position[pd.Timestamp(item)] for item in subset["bar_dt"] if pd.Timestamp(item) in bar_dt_to_position],
            subset["price"],
            marker=marker,
            color=color,
            label=pivot_type,
            s=40,
        )
    axes[4].set_ylabel("pivot")
    axes[4].legend(loc="upper left", ncol=3, fontsize=8)

    zone_series = merged["wave_position_zone"].map(zone_codes).fillna(-1)
    axes[5].step(x_positions, zone_series, where="mid", color="#4c72b0")
    axes[5].set_yticks([-1, 0, 1, 2, 3])
    axes[5].set_yticklabels(["na", "early", "mature+", "mature_stag", "weak_stag"])
    axes[5].set_ylabel("zone")

    axes[-1].set_xticks(x_positions)
    axes[-1].set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)


def _write_small_tables(
    *,
    audit_connection: duckdb.DuckDBPyConnection,
    hard_rule_findings: tuple[HardRuleFinding, ...],
    sample_windows: tuple[SampleWindow, ...],
) -> None:
    findings_frame = pd.DataFrame(
        [
            {
                "rule_name": finding.rule_name,
                "description": finding.description,
                "status": finding.status,
                "violation_count": finding.violation_count,
            }
            for finding in hard_rule_findings
        ]
    )
    sample_windows_frame = pd.DataFrame([item.as_dict() for item in sample_windows])
    if sample_windows_frame.empty:
        sample_windows_frame = pd.DataFrame(
            columns=[
                "sample_id",
                "category",
                "symbol",
                "direction",
                "wave_id",
                "start_bar_dt",
                "end_bar_dt",
                "bar_count",
                "final_new_count",
                "final_no_new_span",
                "reborn_bar_count",
                "secondary_wave_id",
                "context_start_bar_dt",
                "context_end_bar_dt",
                "plot_path",
            ]
        )
    audit_connection.register("_hard_rule_findings", findings_frame)
    audit_connection.register("_sample_windows", sample_windows_frame)
    try:
        audit_connection.execute(
            """
            CREATE OR REPLACE TABLE hard_rule_findings AS
            SELECT *
            FROM _hard_rule_findings
            ORDER BY rule_name
            """
        )
        audit_connection.execute(
            """
            CREATE OR REPLACE TABLE sample_windows AS
            SELECT *
            FROM _sample_windows
            ORDER BY sample_id
            """
        )
    finally:
        audit_connection.unregister("_hard_rule_findings")
        audit_connection.unregister("_sample_windows")


def _collect_soft_observations(
    *,
    audit_connection: duckdb.DuckDBPyConnection,
) -> tuple[SoftObservation, ...]:
    zone_count = audit_connection.execute(
        "SELECT COUNT(DISTINCT wave_position_zone) FROM state_snapshot_sample WHERE wave_position_zone IS NOT NULL"
    ).fetchone()[0]
    reborn_stats = audit_connection.execute(
        """
        SELECT
            COALESCE(MEDIAN(reborn_bar_count), 0),
            COALESCE(AVG(CASE WHEN reborn_bar_count = 1 THEN 1.0 ELSE 0.0 END), 0.0)
        FROM reborn_windows
        """
    ).fetchone()
    guard_ratio_p90 = audit_connection.execute(
        """
        SELECT
            COALESCE(
                QUANTILE_CONT(
                    CASE
                        WHEN bar_count > 0 THEN CAST(guard_update_count AS DOUBLE) / bar_count
                        ELSE 0.0
                    END,
                    0.9
                ),
                0.0
            )
        FROM wave_summary
        """
    ).fetchone()[0]
    observations = [
        SoftObservation(
            observation_name="zone_coverage",
            status="ok" if int(zone_count) == 4 else "flag",
            value=int(zone_count),
            threshold="expected 4 distinct zones in sampled windows",
            note="样本图层是否覆盖四个 wave_position_zone。",
        ),
        SoftObservation(
            observation_name="reborn_median_bar_count",
            status="flag" if float(reborn_stats[0] or 0.0) <= 1.0 else "ok",
            value=round(float(reborn_stats[0] or 0.0), 4),
            threshold="median reborn_bar_count > 1",
            note="用于识别 break 与确认是否过度贴合。",
        ),
        SoftObservation(
            observation_name="single_bar_reborn_share",
            status="flag" if float(reborn_stats[1] or 0.0) >= 0.8 else "ok",
            value=round(float(reborn_stats[1] or 0.0), 4),
            threshold="share of one-bar reborn windows < 0.80",
            note="用于识别 reborn 是否大多只有一根 bar。",
        ),
        SoftObservation(
            observation_name="guard_churn_p90",
            status="flag" if float(guard_ratio_p90 or 0.0) >= 0.5 else "ok",
            value=round(float(guard_ratio_p90 or 0.0), 4),
            threshold="p90(guard_update_count / bar_count) < 0.50",
            note="用于识别 guard 是否过于贴 bar 级波动。",
        ),
    ]
    return tuple(observations)


def _classify_verdict(
    *,
    hard_rule_findings: tuple[HardRuleFinding, ...],
    soft_observations: tuple[SoftObservation, ...],
) -> str:
    if any(item.violation_count > 0 for item in hard_rule_findings):
        return "不通过"
    if any(item.status == "flag" for item in soft_observations):
        return "部分通过"
    return "通过"


def _render_markdown_summary(summary: MalfSemanticAuditSummary) -> str:
    lines = [
        "# MALF day semantic audit",
        "",
        f"- report_id: `{summary.report_id}`",
        f"- timeframe: `{summary.timeframe}`",
        f"- target_run_id: `{summary.target_run_id}`",
        f"- verdict: `{summary.verdict}`",
        f"- generated_at: `{summary.generated_at}`",
        f"- target_started_at: `{summary.target_started_at}`",
        f"- target_finished_at: `{summary.target_finished_at}`",
        f"- target_symbol_total: `{summary.target_symbol_total}`",
        f"- target_symbol_completed: `{summary.target_symbol_completed}`",
        f"- target_snapshot_rows: `{summary.target_snapshot_rows}`",
        f"- target_wave_rows: `{summary.target_wave_rows}`",
        f"- running_queue_count: `{summary.running_queue_count}`",
        f"- artifact_database_path: `{summary.artifact_database_path}`",
        "",
        "## Authority Stack",
        "",
    ]
    lines.extend([f"- `{path}`" for path in summary.authority_materials])
    lines.extend(["", "## Excluded Scope", ""])
    lines.extend([f"- `{item}`" for item in summary.excluded_scope])
    lines.extend(["", "## Hard Rule Findings", ""])
    for finding in summary.hard_rule_findings:
        lines.append(
            f"- `{finding.rule_name}` status=`{finding.status}` violations=`{finding.violation_count}`: {finding.description}"
        )
    lines.extend(["", "## Soft Observations", ""])
    for observation in summary.soft_observations:
        lines.append(
            f"- `{observation.observation_name}` status=`{observation.status}` value=`{observation.value}` threshold=`{observation.threshold}`"
        )
    lines.extend(["", "## Required Tables", ""])
    for artifact in summary.table_artifacts:
        lines.append(
            f"- `{artifact.table_name}` rows=`{artifact.row_count}` ref=`{artifact.table_ref}`"
        )
    lines.extend(["", "## Sample Windows", ""])
    for window in summary.sample_windows:
        lines.append(
            f"- `{window.sample_id}` {window.category} `{window.symbol}` `{window.wave_id}` plot=`{window.plot_path}`"
        )
    lines.extend(["", "## Stale Runs", ""])
    if not summary.stale_run_summaries:
        lines.append("- none")
    else:
        for stale_run in summary.stale_run_summaries:
            lines.append(
                f"- `{stale_run.run_id}` status=`{stale_run.status}` completed=`{stale_run.symbols_completed}/{stale_run.symbols_total}` started_at=`{stale_run.started_at}`"
            )
    lines.extend(["", summary.message, ""])
    return "\n".join(lines)


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _format_datetime(value: datetime | pd.Timestamp | str | None) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _records_from_df(frame: pd.DataFrame) -> list[dict[str, object]]:
    return [_normalize_record(record) for record in frame.to_dict(orient="records")]


def _normalize_record(record: dict[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            normalized[key] = value.to_pydatetime().isoformat()
        elif isinstance(value, datetime):
            normalized[key] = value.isoformat()
        elif isinstance(value, float) and math.isnan(value):
            normalized[key] = None
        else:
            normalized[key] = value
    return normalized
