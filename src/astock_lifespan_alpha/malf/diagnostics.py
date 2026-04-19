"""Diagnostics for MALF day real-data execution."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.contracts import Timeframe
from astock_lifespan_alpha.malf.engine import run_malf_engine
from astock_lifespan_alpha.malf.runner import _insert_result_rows, _replace_symbol_rows, _upsert_checkpoint
from astock_lifespan_alpha.malf.schema import initialize_malf_schema
from astock_lifespan_alpha.malf.source import (
    DAY_ADJUST_METHOD,
    ResolvedSourceTable,
    SourceBars,
    load_source_bars_limited,
    resolve_source_table,
)


@dataclass(frozen=True)
class SourceTableSummary:
    source_path: str
    table_name: str
    selected_adjust_method: str | None
    row_count: int
    symbol_count: int
    min_bar_dt: str | None
    max_bar_dt: str | None
    duplicate_symbol_trade_date_groups_before_filter: int
    duplicate_symbol_trade_date_examples_before_filter: list[str]
    duplicate_symbol_trade_date_groups_after_filter: int
    duplicate_symbol_trade_date_examples_after_filter: list[str]


@dataclass(frozen=True)
class PhaseTimingSummary:
    source_load_seconds: float
    engine_seconds: float
    write_seconds: float


@dataclass(frozen=True)
class SymbolTimingSummary:
    symbol: str
    bar_count: int
    engine_seconds: float
    write_seconds: float
    total_seconds: float
    error: str | None = None


@dataclass(frozen=True)
class MalfDayDiagnosticReport:
    runner_name: str
    report_id: str
    generated_at: str
    source: SourceTableSummary | None
    symbols_seen: int
    profiled_symbol_count: int
    symbol_limit: int | None
    bar_limit_per_symbol: int | None
    timings: PhaseTimingSummary
    bottleneck_stage: str
    top_slow_symbols: list[SymbolTimingSummary]
    message: str
    report_json_path: str
    report_markdown_path: str

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "report_id": self.report_id,
            "generated_at": self.generated_at,
            "source": asdict(self.source) if self.source is not None else None,
            "symbols_seen": self.symbols_seen,
            "profiled_symbol_count": self.profiled_symbol_count,
            "symbol_limit": self.symbol_limit,
            "bar_limit_per_symbol": self.bar_limit_per_symbol,
            "timings": asdict(self.timings),
            "bottleneck_stage": self.bottleneck_stage,
            "top_slow_symbols": [asdict(item) for item in self.top_slow_symbols],
            "message": self.message,
            "report_json_path": self.report_json_path,
            "report_markdown_path": self.report_markdown_path,
        }


def profile_malf_day_real_data(
    *,
    settings: WorkspaceRoots | None = None,
    top_n: int = 10,
    symbol_limit: int | None = 10,
    bar_limit_per_symbol: int | None = 1000,
) -> MalfDayDiagnosticReport:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    report_id = f"malf-day-diag-{uuid4().hex[:12]}"
    report_root = workspace.module_report_root("malf")
    report_root.mkdir(parents=True, exist_ok=True)
    report_json_path = report_root / f"{report_id}.json"
    report_markdown_path = report_root / f"{report_id}.md"

    resolved_table = resolve_source_table(workspace, Timeframe.DAY)
    source_load_start = perf_counter()
    source_bars = load_source_bars_limited(workspace, Timeframe.DAY, symbol_limit=symbol_limit)
    source_load_seconds = round(perf_counter() - source_load_start, 6)
    profiled_symbols = list(source_bars.bars_by_symbol)
    if symbol_limit is not None:
        profiled_symbols = profiled_symbols[:symbol_limit]
    source_summary = (
        _summarize_source_table(
            resolved_table,
            source_bars,
            profiled_symbols=profiled_symbols,
        )
        if resolved_table is not None
        else None
    )

    profiled_items = list(source_bars.bars_by_symbol.items())
    if symbol_limit is not None:
        profiled_items = profiled_items[:symbol_limit]

    temp_db_path = workspace.module_temp_root("malf") / f"{report_id}.duckdb"
    if temp_db_path.exists():
        temp_db_path.unlink()
    initialize_malf_schema(temp_db_path)

    engine_seconds_total = 0.0
    write_seconds_total = 0.0
    per_symbol: list[SymbolTimingSummary] = []
    symbol_errors = 0
    truncated_symbols = 0
    for symbol, bars in profiled_items:
        profiled_bars = bars[-bar_limit_per_symbol:] if bar_limit_per_symbol is not None else bars
        if len(profiled_bars) != len(bars):
            truncated_symbols += 1
        symbol_error: str | None = None
        result = None
        engine_start = perf_counter()
        try:
            result = run_malf_engine(symbol=symbol, timeframe=Timeframe.DAY, bars=profiled_bars)
            engine_seconds = round(perf_counter() - engine_start, 6)
        except Exception as exc:
            engine_seconds = round(perf_counter() - engine_start, 6)
            symbol_error = str(exc)
            symbol_errors += 1

        write_seconds = 0.0
        if result is not None:
            with duckdb.connect(str(temp_db_path)) as connection:
                try:
                    write_start = perf_counter()
                    _replace_symbol_rows(connection=connection, timeframe=Timeframe.DAY, symbol=symbol)
                    _insert_result_rows(connection=connection, run_id=report_id, result=result)
                    _upsert_checkpoint(
                        connection=connection,
                        timeframe=Timeframe.DAY,
                        symbol=symbol,
                        run_id=report_id,
                        last_bar_dt=profiled_bars[-1].bar_dt,
                    )
                    write_seconds = round(perf_counter() - write_start, 6)
                except Exception as exc:
                    write_seconds = round(perf_counter() - write_start, 6)
                    symbol_error = str(exc)
                    symbol_errors += 1

        engine_seconds_total += engine_seconds
        write_seconds_total += write_seconds
        per_symbol.append(
            SymbolTimingSummary(
                symbol=symbol,
                bar_count=len(profiled_bars),
                engine_seconds=engine_seconds,
                write_seconds=write_seconds,
                total_seconds=round(engine_seconds + write_seconds, 6),
                error=symbol_error,
            )
        )

    timings = PhaseTimingSummary(
        source_load_seconds=round(source_load_seconds, 6),
        engine_seconds=round(engine_seconds_total, 6),
        write_seconds=round(write_seconds_total, 6),
    )
    bottleneck_stage = _classify_bottleneck(timings)
    top_slow_symbols = sorted(per_symbol, key=lambda item: item.total_seconds, reverse=True)[:top_n]
    message = (
        "MALF day real-data diagnostics completed."
        if source_summary is not None
        else "MALF day diagnostics completed without a resolved source table."
    )
    if symbol_errors:
        message = f"{message} Symbol errors recorded: {symbol_errors}."
    if truncated_symbols:
        message = f"{message} Bar profiling truncated for {truncated_symbols} symbols."
    if source_bars.duplicate_symbol_trade_date_groups:
        examples = ", ".join(source_bars.duplicate_symbol_trade_date_examples) or "-"
        message = (
            f"{message} Source contract violations remain after adjust_method={source_bars.selected_adjust_method or 'unfiltered'} "
            f"formalization: duplicate_symbol_trade_date_groups={source_bars.duplicate_symbol_trade_date_groups}, "
            f"examples={examples}."
        )
    report = MalfDayDiagnosticReport(
        runner_name="profile_malf_day_real_data",
        report_id=report_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        source=source_summary,
        symbols_seen=len(source_bars.bars_by_symbol),
        profiled_symbol_count=len(profiled_items),
        symbol_limit=symbol_limit,
        bar_limit_per_symbol=bar_limit_per_symbol,
        timings=timings,
        bottleneck_stage=bottleneck_stage,
        top_slow_symbols=top_slow_symbols,
        message=message,
        report_json_path=str(report_json_path),
        report_markdown_path=str(report_markdown_path),
    )
    report_json_path.write_text(json.dumps(report.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    report_markdown_path.write_text(_render_markdown_report(report), encoding="utf-8")
    return report


def _summarize_source_table(
    resolved_table: ResolvedSourceTable,
    source_bars: SourceBars,
    *,
    profiled_symbols: list[str],
) -> SourceTableSummary:
    with duckdb.connect(str(resolved_table.source_path), read_only=True) as connection:
        column_info = connection.execute(f"PRAGMA table_info('{resolved_table.table_name}')").fetchall()
        column_names = {row[1] for row in column_info}
        symbol_column = "code" if "code" in column_names else "symbol"
        date_column = "trade_date" if "trade_date" in column_names else "bar_dt"
        row_count, symbol_count, min_bar_dt, max_bar_dt = connection.execute(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT {symbol_column}) AS symbol_count,
                MIN({date_column}) AS min_bar_dt,
                MAX({date_column}) AS max_bar_dt
            FROM {resolved_table.table_name}
            """
        ).fetchone()
        duplicate_group_count_before_filter, duplicate_examples_before_filter = _summarize_duplicate_groups(
            connection=connection,
            table_name=resolved_table.table_name,
            symbol_column=symbol_column,
            date_column=date_column,
            profiled_symbols=profiled_symbols,
        )
    return SourceTableSummary(
        source_path=str(resolved_table.source_path),
        table_name=resolved_table.table_name,
        selected_adjust_method=source_bars.selected_adjust_method or (
            DAY_ADJUST_METHOD if "adjust_method" in column_names else None
        ),
        row_count=int(row_count),
        symbol_count=int(symbol_count),
        min_bar_dt=str(min_bar_dt) if min_bar_dt is not None else None,
        max_bar_dt=str(max_bar_dt) if max_bar_dt is not None else None,
        duplicate_symbol_trade_date_groups_before_filter=duplicate_group_count_before_filter,
        duplicate_symbol_trade_date_examples_before_filter=duplicate_examples_before_filter,
        duplicate_symbol_trade_date_groups_after_filter=source_bars.duplicate_symbol_trade_date_groups,
        duplicate_symbol_trade_date_examples_after_filter=list(source_bars.duplicate_symbol_trade_date_examples),
    )


def _summarize_duplicate_groups(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    symbol_column: str,
    date_column: str,
    profiled_symbols: list[str],
    example_limit: int = 5,
) -> tuple[int, list[str]]:
    where_clause = ""
    params: list[object] = []
    if profiled_symbols:
        placeholders = ", ".join(["?"] * len(profiled_symbols))
        where_clause = f"WHERE {symbol_column} IN ({placeholders})"
        params.extend(profiled_symbols)
    duplicate_group_count = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {symbol_column}, {date_column}
            FROM {table_name}
            {where_clause}
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        )
        """,
        params,
    ).fetchone()[0]
    example_params = list(params)
    example_params.append(example_limit)
    example_rows = connection.execute(
        f"""
        SELECT {symbol_column}, {date_column}, COUNT(*) AS duplicate_count
        FROM {table_name}
        {where_clause}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
        ORDER BY {symbol_column}, {date_column}
        LIMIT ?
        """,
        example_params,
    ).fetchall()
    return int(duplicate_group_count), [
        f"{symbol}@{bar_dt.isoformat()}x{duplicate_count}"
        for symbol, bar_dt, duplicate_count in example_rows
    ]


def _classify_bottleneck(timings: PhaseTimingSummary) -> str:
    ranked = {
        "source_load_timing": timings.source_load_seconds,
        "engine_timing": timings.engine_seconds,
        "write_timing": timings.write_seconds,
    }
    return max(ranked, key=ranked.get)


def _render_markdown_report(report: MalfDayDiagnosticReport) -> str:
    lines = [
        "# MALF day real-data diagnostics",
        "",
        f"- report_id: `{report.report_id}`",
        f"- generated_at: `{report.generated_at}`",
        f"- bottleneck_stage: `{report.bottleneck_stage}`",
        f"- symbols_seen: `{report.symbols_seen}`",
        f"- profiled_symbol_count: `{report.profiled_symbol_count}`",
        f"- symbol_limit: `{report.symbol_limit}`",
        f"- bar_limit_per_symbol: `{report.bar_limit_per_symbol}`",
        "",
        "## Source",
        "",
    ]
    if report.source is None:
        lines.append("- unresolved")
    else:
        lines.extend(
            [
                f"- source_path: `{report.source.source_path}`",
                f"- table_name: `{report.source.table_name}`",
                f"- selected_adjust_method: `{report.source.selected_adjust_method}`",
                f"- row_count: `{report.source.row_count}`",
                f"- symbol_count: `{report.source.symbol_count}`",
                f"- min_bar_dt: `{report.source.min_bar_dt}`",
                f"- max_bar_dt: `{report.source.max_bar_dt}`",
                (
                    "- duplicate_symbol_trade_date_groups_before_filter: "
                    f"`{report.source.duplicate_symbol_trade_date_groups_before_filter}`"
                ),
                (
                    "- duplicate_symbol_trade_date_examples_before_filter: "
                    f"`{report.source.duplicate_symbol_trade_date_examples_before_filter}`"
                ),
                (
                    "- duplicate_symbol_trade_date_groups_after_filter: "
                    f"`{report.source.duplicate_symbol_trade_date_groups_after_filter}`"
                ),
                (
                    "- duplicate_symbol_trade_date_examples_after_filter: "
                    f"`{report.source.duplicate_symbol_trade_date_examples_after_filter}`"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Timings",
            "",
            f"- source load timing: `{report.timings.source_load_seconds}`",
            f"- engine timing: `{report.timings.engine_seconds}`",
            f"- write timing: `{report.timings.write_seconds}`",
            "",
            "## Top Slow Symbols",
            "",
        ]
    )
    if not report.top_slow_symbols:
        lines.append("- none")
    else:
        for item in report.top_slow_symbols:
            lines.append(
                f"- `{item.symbol}` bars=`{item.bar_count}` total=`{item.total_seconds}` "
                f"engine=`{item.engine_seconds}` write=`{item.write_seconds}`"
                + (f" error=`{item.error}`" if item.error else "")
            )
    lines.extend(["", report.message, ""])
    return "\n".join(lines)
