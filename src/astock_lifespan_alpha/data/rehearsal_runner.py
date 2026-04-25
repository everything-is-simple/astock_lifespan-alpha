"""Isolated stock producer rehearsal runner."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.data.contracts import (
    DataStockProducerRehearsalSummary,
    MarketBaseBuildSummary,
    TdxStockRawIngestSummary,
)
from astock_lifespan_alpha.data.market_base_runner import run_market_base_build
from astock_lifespan_alpha.data.raw_runner import DEFAULT_TDX_SOURCE_ROOT, run_tdx_stock_raw_ingest
from astock_lifespan_alpha.data.target_audit import audit_stock_producer_target

DEFAULT_REHEARSAL_SCOPES = (
    ("day", "backward"),
    ("day", "forward"),
    ("day", "none"),
    ("week", "backward"),
    ("month", "backward"),
)


def run_data_stock_producer_rehearsal(
    *,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    target_data_root: Path | str,
    scopes: tuple[tuple[str, str], ...] = DEFAULT_REHEARSAL_SCOPES,
    raw_limit: int | None = 100,
    base_limit: int | None = 1000,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> DataStockProducerRehearsalSummary:
    """Run TDX raw ingest, market_base build, and isolated target audit."""

    workspace = settings or default_settings()
    target_root = Path(target_data_root).resolve()
    source_base = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    rehearsal_run_id = run_id or f"data-stock-rehearsal-{uuid4().hex[:12]}"
    raw_summaries: list[TdxStockRawIngestSummary] = []
    base_summaries: list[MarketBaseBuildSummary] = []

    for timeframe, adjust_method in scopes:
        raw_summary = run_tdx_stock_raw_ingest(
            settings=workspace,
            source_root=source_base,
            target_data_root=target_root,
            timeframe=timeframe,
            adjust_method=adjust_method,
            limit=raw_limit,
            run_id=f"{rehearsal_run_id}-raw-{timeframe}-{adjust_method}",
        )
        raw_summaries.append(raw_summary)
        base_summary = run_market_base_build(
            settings=workspace,
            target_data_root=target_root,
            timeframe=timeframe,
            adjust_method=adjust_method,
            limit=base_limit,
            build_mode="full",
            run_id=f"{rehearsal_run_id}-base-{timeframe}-{adjust_method}",
        )
        base_summaries.append(base_summary)

    target_audit_summary = audit_stock_producer_target(settings=workspace, target_data_root=target_root)
    summary = DataStockProducerRehearsalSummary(
        runner_name="run_data_stock_producer_rehearsal",
        run_id=rehearsal_run_id,
        status="completed" if target_audit_summary.status == "completed" else "failed",
        target_data_root=str(target_root),
        raw_summaries=tuple(raw_summaries),
        base_summaries=tuple(base_summaries),
        target_audit_summary=target_audit_summary,
        gate_status=target_audit_summary.status,
        message=(
            "stock producer rehearsal completed."
            if target_audit_summary.status == "completed"
            else "stock producer rehearsal failed target audit."
        ),
    )
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary.as_dict(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    return summary
