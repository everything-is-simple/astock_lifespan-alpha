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

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO pipeline_run (
                run_id, status, portfolio_id, step_count, message, pipeline_contract_version
            ) VALUES (?, 'running', ?, 0, 'pipeline run started.', ?)
            """,
            [run_id, portfolio_id, PIPELINE_CONTRACT_VERSION],
        )

        for step_order, step in enumerate(_pipeline_steps(), start=1):
            runner_summary = step.runner(workspace, portfolio_id)
            step_summary = _build_step_summary(step_order=step_order, runner_summary=runner_summary)
            _insert_step_summary(connection=connection, pipeline_run_id=run_id, step_summary=step_summary)
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

    return PipelineRunSummary(
        runner_name="run_data_to_system_pipeline",
        run_id=run_id,
        status=PipelineRunStatus.COMPLETED.value,
        target_path=str(target_path),
        portfolio_id=portfolio_id,
        step_count=len(steps),
        message=message,
        steps=steps,
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
            lambda workspace, portfolio_id: run_portfolio_plan_build(portfolio_id=portfolio_id, settings=workspace),
        ),
        _PipelineStep(
            "run_trade_from_portfolio_plan",
            lambda workspace, portfolio_id: run_trade_from_portfolio_plan(portfolio_id=portfolio_id, settings=workspace),
        ),
        _PipelineStep(
            "run_system_from_trade",
            lambda workspace, portfolio_id: run_system_from_trade(portfolio_id=portfolio_id, settings=workspace),
        ),
    ]


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

