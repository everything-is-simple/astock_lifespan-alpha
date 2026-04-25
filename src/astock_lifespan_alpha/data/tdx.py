"""TDX offline stock text parser."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


_ADJUST_METHOD_FOLDER_MAP = {
    "Backward-Adjusted": "backward",
    "Forward-Adjusted": "forward",
    "Non-Adjusted": "none",
}


@dataclass(frozen=True)
class TdxStockDailyBar:
    code: str
    name: str
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    amount: float | None


@dataclass(frozen=True)
class TdxParsedStockFile:
    code: str
    name: str
    adjust_method: str
    header: str
    rows: tuple[TdxStockDailyBar, ...]


def resolve_adjust_method_folder(adjust_method: str) -> str:
    normalized = str(adjust_method).strip().lower()
    mapping = {
        "backward": "Backward-Adjusted",
        "forward": "Forward-Adjusted",
        "none": "Non-Adjusted",
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported adjust method: {adjust_method}")
    return mapping[normalized]


def resolve_adjust_method_name(folder_name: str) -> str:
    if folder_name not in _ADJUST_METHOD_FOLDER_MAP:
        raise ValueError(f"Unsupported TDX folder: {folder_name}")
    return _ADJUST_METHOD_FOLDER_MAP[folder_name]


def is_a_share_stock_code(code: str) -> bool:
    """Return whether a normalized A-share code is stock, excluding ETFs/funds."""

    candidate = str(code).strip().upper()
    if "." not in candidate:
        return False
    symbol, exchange = candidate.split(".", 1)
    if not symbol.isdigit() or len(symbol) != 6:
        return False
    if exchange == "SH":
        return symbol.startswith(("600", "601", "603", "605", "688", "689"))
    if exchange == "SZ":
        return symbol.startswith(("000", "001", "002", "003", "300", "301"))
    return False


def parse_tdx_stock_file(path: Path) -> TdxParsedStockFile:
    lines = path.read_text(encoding="gbk").splitlines()
    if len(lines) < 2:
        raise ValueError(f"Unexpected TDX file format: {path}")
    header = lines[0].strip()
    header_parts = header.split()
    if len(header_parts) < 2:
        raise ValueError(f"Cannot parse TDX header: {header}")

    code = _normalize_code_from_filename(path)
    name = header_parts[1].strip()
    adjust_method = resolve_adjust_method_name(path.parent.name)
    rows: list[TdxStockDailyBar] = []
    for raw_line in lines[2:]:
        parts = [part.strip() for part in raw_line.strip().split("\t") if part.strip()]
        if len(parts) < 7 or not _looks_like_date(parts[0]):
            continue
        rows.append(
            TdxStockDailyBar(
                code=code,
                name=name,
                trade_date=date.fromisoformat(parts[0].replace("/", "-")),
                open=_parse_float(parts[1]),
                high=_parse_float(parts[2]),
                low=_parse_float(parts[3]),
                close=_parse_float(parts[4]),
                volume=_parse_float(parts[5]),
                amount=_parse_float(parts[6]),
            )
        )
    return TdxParsedStockFile(code=code, name=name, adjust_method=adjust_method, header=header, rows=tuple(rows))


def _normalize_code_from_filename(path: Path) -> str:
    stem = path.stem
    if "#" not in stem:
        raise ValueError(f"Unexpected TDX file name: {path.name}")
    exchange, code = stem.split("#", 1)
    return f"{code}.{exchange}"


def _looks_like_date(value: str) -> bool:
    return len(value) >= 10 and value[4] in {"-", "/"} and value[7] in {"-", "/"}


def _parse_float(value: str) -> float | None:
    candidate = value.strip()
    return None if candidate == "" else float(candidate)
