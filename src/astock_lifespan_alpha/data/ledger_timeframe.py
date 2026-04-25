"""Isolated raw/base day-week-month ledger paths for the data producer."""

from __future__ import annotations

from pathlib import Path

import duckdb

TDX_TIMEFRAMES = ("day", "week", "month")


def normalize_timeframe(timeframe: str) -> str:
    normalized = str(timeframe).strip().lower()
    if normalized not in TDX_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return normalized


def raw_market_timeframe_ledger_path(target_data_root: Path | str, *, timeframe: str = "day") -> Path:
    target_root = Path(target_data_root).resolve()
    suffix = "" if normalize_timeframe(timeframe) == "day" else f"_{normalize_timeframe(timeframe)}"
    return target_root / "raw" / f"raw_market{suffix}.duckdb"


def market_base_timeframe_ledger_path(target_data_root: Path | str, *, timeframe: str = "day") -> Path:
    target_root = Path(target_data_root).resolve()
    suffix = "" if normalize_timeframe(timeframe) == "day" else f"_{normalize_timeframe(timeframe)}"
    return target_root / "base" / f"market_base{suffix}.duckdb"


def connect_raw_market_timeframe_ledger(
    target_data_root: Path | str,
    *,
    timeframe: str = "day",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    path = raw_market_timeframe_ledger_path(target_data_root, timeframe=timeframe)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def connect_market_base_timeframe_ledger(
    target_data_root: Path | str,
    *,
    timeframe: str = "day",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    path = market_base_timeframe_ledger_path(target_data_root, timeframe=timeframe)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)
