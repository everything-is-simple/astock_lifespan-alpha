"""Workspace and database path contracts for the reconstructed system."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


NEW_DATA_NAMESPACE = "astock_lifespan_alpha"
FORMAL_MODULES = (
    "core",
    "data",
    "malf",
    "alpha",
    "position",
    "portfolio_plan",
    "trade",
    "system",
    "pipeline",
)


def discover_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by walking upward to pyproject.toml."""

    current = (start or Path(__file__)).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Could not locate repository root from the current path.")


@dataclass(frozen=True)
class SourceFactDatabasePaths:
    """Read-only source ledgers inherited from the legacy fact layer."""

    raw_market: Path
    market_base: Path
    raw_market_week: Path
    raw_market_month: Path
    market_base_week: Path
    market_base_month: Path

    def as_dict(self) -> dict[str, Path]:
        return {
            "raw_market": self.raw_market,
            "raw_market_week": self.raw_market_week,
            "raw_market_month": self.raw_market_month,
            "market_base": self.market_base,
            "market_base_week": self.market_base_week,
            "market_base_month": self.market_base_month,
        }


@dataclass(frozen=True)
class FormalDatabasePaths:
    """New-system formal ledgers built under the reconstructed namespace."""

    namespace_root: Path
    malf_day: Path
    malf_week: Path
    malf_month: Path
    alpha_bof: Path
    alpha_tst: Path
    alpha_pb: Path
    alpha_cpb: Path
    alpha_bpb: Path
    alpha_signal: Path
    position: Path
    portfolio_plan: Path
    trade: Path
    system: Path
    pipeline: Path

    def as_dict(self) -> dict[str, Path]:
        return {
            "malf_day": self.malf_day,
            "malf_week": self.malf_week,
            "malf_month": self.malf_month,
            "alpha_bof": self.alpha_bof,
            "alpha_tst": self.alpha_tst,
            "alpha_pb": self.alpha_pb,
            "alpha_cpb": self.alpha_cpb,
            "alpha_bpb": self.alpha_bpb,
            "alpha_signal": self.alpha_signal,
            "position": self.position,
            "portfolio_plan": self.portfolio_plan,
            "trade": self.trade,
            "system": self.system,
            "pipeline": self.pipeline,
        }


@dataclass(frozen=True)
class WorkspaceRoots:
    """Five-root workspace contract and derived database paths."""

    repo_root: Path
    data_root: Path
    report_root: Path
    temp_root: Path
    validated_root: Path

    @property
    def source_databases(self) -> SourceFactDatabasePaths:
        return SourceFactDatabasePaths(
            raw_market=self.data_root / "raw" / "raw_market.duckdb",
            market_base=self.data_root / "base" / "market_base.duckdb",
            raw_market_week=self.data_root / "raw" / "raw_market_week.duckdb",
            raw_market_month=self.data_root / "raw" / "raw_market_month.duckdb",
            market_base_week=self.data_root / "base" / "market_base_week.duckdb",
            market_base_month=self.data_root / "base" / "market_base_month.duckdb",
        )

    @property
    def databases(self) -> FormalDatabasePaths:
        namespace_root = self.data_root / NEW_DATA_NAMESPACE
        return FormalDatabasePaths(
            namespace_root=namespace_root,
            malf_day=namespace_root / "malf" / "malf_day.duckdb",
            malf_week=namespace_root / "malf" / "malf_week.duckdb",
            malf_month=namespace_root / "malf" / "malf_month.duckdb",
            alpha_bof=namespace_root / "alpha" / "alpha_bof.duckdb",
            alpha_tst=namespace_root / "alpha" / "alpha_tst.duckdb",
            alpha_pb=namespace_root / "alpha" / "alpha_pb.duckdb",
            alpha_cpb=namespace_root / "alpha" / "alpha_cpb.duckdb",
            alpha_bpb=namespace_root / "alpha" / "alpha_bpb.duckdb",
            alpha_signal=namespace_root / "alpha" / "alpha_signal.duckdb",
            position=namespace_root / "position" / "position.duckdb",
            portfolio_plan=namespace_root / "portfolio_plan" / "portfolio_plan.duckdb",
            trade=namespace_root / "trade" / "trade.duckdb",
            system=namespace_root / "system" / "system.duckdb",
            pipeline=namespace_root / "pipeline" / "pipeline.duckdb",
        )

    def module_temp_root(self, module_name: str) -> Path:
        _validate_module_name(module_name)
        return self.temp_root / NEW_DATA_NAMESPACE / module_name

    def module_report_root(self, module_name: str) -> Path:
        _validate_module_name(module_name)
        return self.report_root / NEW_DATA_NAMESPACE / module_name

    def module_validated_root(self, module_name: str) -> Path:
        _validate_module_name(module_name)
        return self.validated_root / NEW_DATA_NAMESPACE / module_name

    def ensure_directories(self) -> None:
        for root in (self.repo_root, self.data_root, self.report_root, self.temp_root, self.validated_root):
            root.mkdir(parents=True, exist_ok=True)
        self.databases.namespace_root.mkdir(parents=True, exist_ok=True)
        for database_path in self.databases.as_dict().values():
            database_path.parent.mkdir(parents=True, exist_ok=True)
        for module_name in FORMAL_MODULES:
            self.module_temp_root(module_name).mkdir(parents=True, exist_ok=True)
            self.module_report_root(module_name).mkdir(parents=True, exist_ok=True)
            self.module_validated_root(module_name).mkdir(parents=True, exist_ok=True)


def _validate_module_name(module_name: str) -> None:
    if module_name not in FORMAL_MODULES:
        raise ValueError(f"Unknown formal module: {module_name}")


def _default_external_root(repo_root: Path, dirname: str) -> Path:
    return repo_root.parent / dirname


def default_settings(repo_root: Path | None = None) -> WorkspaceRoots:
    """Resolve the reconstructed five-root workspace contract."""

    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else Path(
        os.getenv("LIFESPAN_REPO_ROOT", discover_repo_root())
    ).resolve()
    data_root = Path(
        os.getenv("LIFESPAN_DATA_ROOT", _default_external_root(resolved_repo_root, "Lifespan-data"))
    ).resolve()
    report_root = Path(
        os.getenv("LIFESPAN_REPORT_ROOT", _default_external_root(resolved_repo_root, "Lifespan-report"))
    ).resolve()
    temp_root = Path(
        os.getenv("LIFESPAN_TEMP_ROOT", _default_external_root(resolved_repo_root, "Lifespan-temp"))
    ).resolve()
    validated_root = Path(
        os.getenv("LIFESPAN_VALIDATED_ROOT", _default_external_root(resolved_repo_root, "Lifespan-Validated"))
    ).resolve()
    return WorkspaceRoots(
        repo_root=resolved_repo_root,
        data_root=data_root,
        report_root=report_root,
        temp_root=temp_root,
        validated_root=validated_root,
    )
