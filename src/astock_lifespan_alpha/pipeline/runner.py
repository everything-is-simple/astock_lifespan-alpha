"""Stage-eight data -> system pipeline runner."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.alpha import (
    run_alpha_bof_build,
    run_alpha_bpb_build,
    run_alpha_cpb_build,
    run_alpha_pb_build,
    run_alpha_signal_build,
    run_alpha_tst_build,
)
from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf import run_malf_day_build, run_malf_month_build, run_malf_week_build
from astock_lifespan_alpha.pipeline.contracts import (
    PIPELINE_CONTRACT_VERSION,
    PipelineResumeSummary,
    PipelineRunStatus,
    PipelineRunSummary,
    PipelineStepSummary,
)
from astock_lifespan_alpha.pipeline.schema import initialize_pipeline_schema
from astock_lifespan_alpha.portfolio_plan import run_portfolio_plan_build
from astock_lifespan_alpha.position import run_position_from_alpha_signal
from astock_lifespan_alpha.system import run_system_from_trade
from astock_lifespan_alpha.trade import run_trade_from_portfolio_plan


@dataclass(frozen=True)
class _PipelineStep:
    runner_name: str
    runner: Callable[[WorkspaceRoots, str], object]


@dataclass(frozen=True)
class _PipelineResumeContext:
    interrupted_run_id: str | None
    resume_start_step: int | None


def run_data_to_system_pipeline(
    *,
    portfolio_id: str = "core",
    settings: WorkspaceRoots | None = None,
) -> PipelineRunSummary:
    """Run the minimal data -> system orchestration sequence."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.pipeline
    initialize_pipeline_schema(target_path)

    run_id = f"pipeline-{uuid4().hex[:12]}"
    steps: list[PipelineStepSummary] = []
    message = "pipeline run completed."
    resume_context = _PipelineResumeContext(interrupted_run_id=None, resume_start_step=None)
    reused_step_count = 0
    executed_step_count = 0

    with duckdb.connect(str(target_path)) as connection:
        resume_context = _resolve_pipeline_resume_context(connection=connection, portfolio_id=portfolio_id)
        connection.execute(
            """
            INSERT INTO pipeline_run (
                run_id, status, portfolio_id, step_count, message, pipeline_contract_version
            ) VALUES (?, 'running', ?, 0, 'pipeline run started.', ?)
            """,
            [run_id, portfolio_id, PIPELINE_CONTRACT_VERSION],
        )

        try:
            for step_order, step in enumerate(_pipeline_steps(), start=1):
                if resume_context.resume_start_step is not None and step_order < resume_context.resume_start_step:
                    step_summary = _build_reused_step_summary(
                        connection=connection,
                        portfolio_id=portfolio_id,
                        step_order=step_order,
                    )
                    reused_step_count += 1
                else:
                    runner_summary = step.runner(workspace, portfolio_id)
                    step_summary = _build_step_summary(step_order=step_order, runner_summary=runner_summary)
                    executed_step_count += 1
                _insert_step_summary(connection=connection, pipeline_run_id=run_id, step_summary=step_summary)
                _upsert_step_checkpoint(
                    connection=connection,
                    portfolio_id=portfolio_id,
                    pipeline_run_id=run_id,
                    step_summary=step_summary,
                )
                steps.append(step_summary)

            connection.execute(
                """
                UPDATE pipeline_run
                SET
                    status = ?,
                    step_count = ?,
                    message = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
                """,
                [PipelineRunStatus.COMPLETED.value, len(steps), message, run_id],
            )
        except Exception as exc:
            failing_step_name = _pipeline_steps()[len(steps)].runner_name if len(steps) < len(_pipeline_steps()) else "unknown"
            connection.execute(
                """
                UPDATE pipeline_run
                SET
                    status = 'interrupted',
                    step_count = ?,
                    message = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
                """,
                [len(steps), f"pipeline interrupted at {failing_step_name}: {exc}", run_id],
            )
            raise

    return PipelineRunSummary(
        runner_name="run_data_to_system_pipeline",
        run_id=run_id,
        status=PipelineRunStatus.COMPLETED.value,
        target_path=str(target_path),
        portfolio_id=portfolio_id,
        step_count=len(steps),
        message=message,
        steps=steps,
        resume_summary=PipelineResumeSummary(
            resumed_from_run_id=resume_context.interrupted_run_id,
            resume_start_step=resume_context.resume_start_step,
            reused_step_count=reused_step_count,
            executed_step_count=executed_step_count,
        ),
    )


def _pipeline_steps() -> list[_PipelineStep]:
    return [
        _PipelineStep("run_malf_day_build", lambda workspace, _portfolio_id: run_malf_day_build(settings=workspace)),
        _PipelineStep("run_malf_week_build", lambda workspace, _portfolio_id: run_malf_week_build(settings=workspace)),
        _PipelineStep("run_malf_month_build", lambda workspace, _portfolio_id: run_malf_month_build(settings=workspace)),
        _PipelineStep("run_alpha_bof_build", lambda workspace, _portfolio_id: run_alpha_bof_build(settings=workspace)),
        _PipelineStep("run_alpha_tst_build", lambda workspace, _portfolio_id: run_alpha_tst_build(settings=workspace)),
        _PipelineStep("run_alpha_pb_build", lambda workspace, _portfolio_id: run_alpha_pb_build(settings=workspace)),
        _PipelineStep("run_alpha_cpb_build", lambda workspace, _portfolio_id: run_alpha_cpb_build(settings=workspace)),
        _PipelineStep("run_alpha_bpb_build", lambda workspace, _portfolio_id: run_alpha_bpb_build(settings=workspace)),
        _PipelineStep("run_alpha_signal_build", lambda workspace, _portfolio_id: run_alpha_signal_build(settings=workspace)),
        _PipelineStep(
            "run_position_from_alpha_signal",
            lambda workspace, _portfolio_id: run_position_from_alpha_signal(settings=workspace),
        ),
        _PipelineStep(
            "run_portfolio_plan_build",
            lambda workspace, current_portfolio_id: run_portfolio_plan_build(
                portfolio_id=current_portfolio_id,
                settings=workspace,
            ),
        ),
        _PipelineStep(
            "run_trade_from_portfolio_plan",
            lambda workspace, current_portfolio_id: run_trade_from_portfolio_plan(
                portfolio_id=current_portfolio_id,
                settings=workspace,
            ),
        ),
        _PipelineStep(
            "run_system_from_trade",
            lambda workspace, current_portfolio_id: run_system_from_trade(
                portfolio_id=current_portfolio_id,
                settings=workspace,
            ),
        ),
    ]


def _resolve_pipeline_resume_context(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
) -> _PipelineResumeContext:
    latest_run = connection.execute(
        """
        SELECT run_id, status
        FROM pipeline_run
        WHERE portfolio_id = ?
        ORDER BY started_at DESC, run_id DESC
        LIMIT 1
        """,
        [portfolio_id],
    ).fetchone()
    if latest_run is None or latest_run[1] != "interrupted":
        return _PipelineResumeContext(interrupted_run_id=None, resume_start_step=None)

    interrupted_run_id = str(latest_run[0])
    completed_step_rows = {
        int(step_order)
        for (step_order,) in connection.execute(
            """
            SELECT step_order
            FROM pipeline_step_run
            WHERE pipeline_run_id = ? AND runner_status = 'completed'
            """,
            [interrupted_run_id],
        ).fetchall()
    }
    resume_start_step: int | None = None
    for step_order, _step in enumerate(_pipeline_steps(), start=1):
        if step_order not in completed_step_rows:
            resume_start_step = step_order
            break
        checkpoint_exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM pipeline_step_checkpoint
            WHERE portfolio_id = ? AND step_order = ?
            """,
            [portfolio_id, step_order],
        ).fetchone()[0]
        if int(checkpoint_exists) == 0:
            resume_start_step = step_order
            break
    return _PipelineResumeContext(
        interrupted_run_id=interrupted_run_id,
        resume_start_step=resume_start_step,
    )


def _build_step_summary(*, step_order: int, runner_summary: object) -> PipelineStepSummary:
    summary = runner_summary.as_dict()
    return PipelineStepSummary(
        step_order=step_order,
        runner_name=str(summary["runner_name"]),
        run_id=str(summary["run_id"]),
        status=str(summary["status"]),
        target_path=str(summary["target_path"]) if summary.get("target_path") is not None else None,
        message=str(summary["message"]) if summary.get("message") is not None else None,
        summary=summary,
    )


def _build_reused_step_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    step_order: int,
) -> PipelineStepSummary:
    row = connection.execute(
        """
        SELECT runner_name, runner_run_id, runner_status, target_path, message, summary_json
        FROM pipeline_step_checkpoint
        WHERE portfolio_id = ? AND step_order = ?
        """,
        [portfolio_id, step_order],
    ).fetchone()
    if row is None:
        raise ValueError(f"Missing pipeline step checkpoint for portfolio_id={portfolio_id} step_order={step_order}")
    summary = json.loads(str(row[5]))
    summary["pipeline_action"] = "reused_checkpoint"
    return PipelineStepSummary(
        step_order=step_order,
        runner_name=str(row[0]),
        run_id=str(row[1]),
        status=str(row[2]),
        target_path=str(row[3]) if row[3] is not None else None,
        message=str(row[4]) if row[4] is not None else None,
        summary=summary,
    )


def _insert_step_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    pipeline_run_id: str,
    step_summary: PipelineStepSummary,
) -> None:
    connection.execute(
        """
        INSERT INTO pipeline_step_run (
            pipeline_run_id, step_order, runner_name, runner_run_id,
            runner_status, target_path, message, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            pipeline_run_id,
            step_summary.step_order,
            step_summary.runner_name,
            step_summary.run_id,
            step_summary.status,
            step_summary.target_path,
            step_summary.message,
            json.dumps(step_summary.summary, ensure_ascii=False, sort_keys=True, default=str),
        ],
    )


def _upsert_step_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    pipeline_run_id: str,
    step_summary: PipelineStepSummary,
) -> None:
    connection.execute(
        """
        INSERT INTO pipeline_step_checkpoint (
            portfolio_id, step_order, runner_name, runner_run_id, runner_status,
            target_path, message, summary_json, last_pipeline_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(portfolio_id, step_order) DO UPDATE
        SET
            runner_name = excluded.runner_name,
            runner_run_id = excluded.runner_run_id,
            runner_status = excluded.runner_status,
            target_path = excluded.target_path,
            message = excluded.message,
            summary_json = excluded.summary_json,
            last_pipeline_run_id = excluded.last_pipeline_run_id,
            updated_at = excluded.updated_at
        """,
        [
            portfolio_id,
            step_summary.step_order,
            step_summary.runner_name,
            step_summary.run_id,
            step_summary.status,
            step_summary.target_path,
            step_summary.message,
            json.dumps(step_summary.summary, ensure_ascii=False, sort_keys=True, default=str),
            pipeline_run_id,
        ],
    )
