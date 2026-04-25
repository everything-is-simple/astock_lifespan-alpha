"""Contracts for the stock-only data producer and source-fact audit."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


class DataProducerSafetyError(ValueError):
    """Raised when a producer run would write to protected source fact ledgers."""


@dataclass(frozen=True)
class TdxStockRawIngestSummary:
    runner_name: str
    run_id: str
    status: str
    source_root: str
    target_raw_path: str
    timeframe: str
    adjust_method: str
    candidate_file_count: int
    processed_file_count: int
    ingested_file_count: int
    skipped_unchanged_file_count: int
    failed_file_count: int
    bar_inserted_count: int
    bar_reused_count: int
    bar_rematerialized_count: int
    message: str
    excluded_file_count: int = 0
    excluded_non_stock_file_count: int = 0
    excluded_non_stock_codes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketBaseBuildSummary:
    runner_name: str
    run_id: str
    status: str
    source_raw_path: str
    target_base_path: str
    timeframe: str
    adjust_method: str
    source_row_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    consumed_dirty_count: int
    message: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SourceFactTableAudit:
    source_key: str
    source_path: str
    table_name: str | None
    exists: bool
    row_count: int
    symbol_count: int
    min_trade_date: str | None
    max_trade_date: str | None
    adjust_method_counts: dict[str, int] = field(default_factory=dict)
    backward_duplicate_groups: int = 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DataSourceFactAuditSummary:
    runner_name: str
    status: str
    tables: dict[str, SourceFactTableAudit]
    raw_base_code_delta: dict[str, tuple[str, ...]]
    message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "status": self.status,
            "tables": {key: value.as_dict() for key, value in self.tables.items()},
            "raw_base_code_delta": dict(self.raw_base_code_delta),
            "message": self.message,
        }


@dataclass(frozen=True)
class DataStockProducerTargetAuditSummary:
    runner_name: str
    status: str
    target_data_root: str
    tables: dict[str, SourceFactTableAudit]
    raw_base_code_delta: dict[str, tuple[str, ...]]
    excluded_non_stock_codes: tuple[str, ...]
    gate_failures: tuple[str, ...]
    message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "status": self.status,
            "target_data_root": self.target_data_root,
            "tables": {key: value.as_dict() for key, value in self.tables.items()},
            "raw_base_code_delta": dict(self.raw_base_code_delta),
            "excluded_non_stock_codes": self.excluded_non_stock_codes,
            "gate_failures": self.gate_failures,
            "message": self.message,
        }


@dataclass(frozen=True)
class DataStockProducerRehearsalSummary:
    runner_name: str
    run_id: str
    status: str
    target_data_root: str
    raw_summaries: tuple[TdxStockRawIngestSummary, ...]
    base_summaries: tuple[MarketBaseBuildSummary, ...]
    target_audit_summary: DataStockProducerTargetAuditSummary
    gate_status: str
    message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "run_id": self.run_id,
            "status": self.status,
            "target_data_root": self.target_data_root,
            "raw_summaries": [summary.as_dict() for summary in self.raw_summaries],
            "base_summaries": [summary.as_dict() for summary in self.base_summaries],
            "target_audit_summary": self.target_audit_summary.as_dict(),
            "gate_status": self.gate_status,
            "message": self.message,
        }
