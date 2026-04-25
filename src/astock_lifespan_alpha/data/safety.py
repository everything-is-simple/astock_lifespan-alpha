"""Safety gates for data producer writes."""

from __future__ import annotations

from pathlib import Path

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.data.contracts import DataProducerSafetyError
from astock_lifespan_alpha.data.ledger_timeframe import market_base_timeframe_ledger_path, raw_market_timeframe_ledger_path


def resolve_target_data_root(*, settings: WorkspaceRoots, target_data_root: Path | str | None) -> Path:
    if target_data_root is None:
        return settings.module_temp_root("data") / "producer"
    return Path(target_data_root).resolve()


def ensure_safe_target_data_root(*, settings: WorkspaceRoots, target_data_root: Path | str) -> Path:
    target_root = Path(target_data_root).resolve()
    source_root = settings.data_root.resolve()
    if target_root == source_root:
        raise DataProducerSafetyError(f"Refusing to write to source fact root: {target_root}")

    protected_paths = {path.resolve() for path in settings.source_databases.as_dict().values()}
    target_paths = {
        raw_market_timeframe_ledger_path(target_root, timeframe="day").resolve(),
        raw_market_timeframe_ledger_path(target_root, timeframe="week").resolve(),
        raw_market_timeframe_ledger_path(target_root, timeframe="month").resolve(),
        market_base_timeframe_ledger_path(target_root, timeframe="day").resolve(),
        market_base_timeframe_ledger_path(target_root, timeframe="week").resolve(),
        market_base_timeframe_ledger_path(target_root, timeframe="month").resolve(),
    }
    if protected_paths & target_paths:
        raise DataProducerSafetyError(f"Refusing to write to source fact root: {target_root}")
    return target_root
