"""Stock-only TDX offline raw ingest runner."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.data.contracts import TdxStockRawIngestSummary
from astock_lifespan_alpha.data.ledger_timeframe import normalize_timeframe, raw_market_timeframe_ledger_path
from astock_lifespan_alpha.data.safety import ensure_safe_target_data_root, resolve_target_data_root
from astock_lifespan_alpha.data.schema import RAW_STOCK_TABLE_BY_TIMEFRAME, initialize_raw_market_schema
from astock_lifespan_alpha.data.tdx import parse_tdx_stock_file, resolve_adjust_method_folder

DEFAULT_TDX_SOURCE_ROOT = Path("H:/tdx_offline_Data")


def run_tdx_stock_raw_ingest(
    *,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    target_data_root: Path | str | None = None,
    timeframe: str = "day",
    adjust_method: str = "backward",
    run_mode: str = "incremental",
    force_hash: bool = False,
    continue_from_last_run: bool = False,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int | None = 100,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> TdxStockRawIngestSummary:
    """Ingest local TDX stock files into an isolated raw_market ledger."""

    del continue_from_last_run  # Reserved for the later full replay card.
    workspace = settings or default_settings()
    target_root = ensure_safe_target_data_root(
        settings=workspace,
        target_data_root=resolve_target_data_root(settings=workspace, target_data_root=target_data_root),
    )
    normalized_timeframe = normalize_timeframe(timeframe)
    normalized_adjust = str(adjust_method).strip().lower()
    normalized_run_mode = _normalize_run_mode(run_mode)
    materialization_run_id = run_id or f"raw-stock-{normalized_timeframe}-{uuid4().hex[:12]}"
    source_base = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    source_folder = _resolve_tdx_source_folder(
        source_base,
        timeframe=normalized_timeframe,
        adjust_method=normalized_adjust,
    )
    raw_path = initialize_raw_market_schema(target_root, timeframe=normalized_timeframe)
    raw_table = RAW_STOCK_TABLE_BY_TIMEFRAME[normalized_timeframe]
    candidate_files = _select_candidate_files(
        source_folder=source_folder,
        instruments=_normalize_instruments(instruments),
        limit=limit,
    )

    ingested_file_count = 0
    skipped_unchanged_file_count = 0
    failed_file_count = 0
    bar_inserted_count = 0
    bar_reused_count = 0
    bar_rematerialized_count = 0

    with duckdb.connect(str(raw_path)) as connection:
        connection.execute(
            """
            INSERT INTO raw_ingest_run (
                run_id, status, timeframe, adjust_method, source_root, candidate_file_count,
                processed_file_count, message
            )
            VALUES (?, 'running', ?, ?, ?, ?, 0, 'raw ingest started.')
            """,
            [materialization_run_id, normalized_timeframe, normalized_adjust, str(source_base), len(candidate_files)],
        )
        for path in candidate_files:
            try:
                action, inserted, reused, rematerialized, row_count, file_nk, code = _ingest_one_file(
                    connection=connection,
                    raw_table=raw_table,
                    path=path,
                    timeframe=normalized_timeframe,
                    adjust_method=normalized_adjust,
                    run_id=materialization_run_id,
                    force_hash=force_hash,
                    run_mode=normalized_run_mode,
                )
                if action == "skipped_unchanged":
                    skipped_unchanged_file_count += 1
                else:
                    ingested_file_count += 1
                bar_inserted_count += inserted
                bar_reused_count += reused
                bar_rematerialized_count += rematerialized
                _record_file(connection, materialization_run_id, file_nk, code, path, action, row_count, None)
            except Exception as exc:
                failed_file_count += 1
                fallback_nk = f"{path.stem}|{normalized_adjust}|{normalized_timeframe}"
                _record_file(connection, materialization_run_id, fallback_nk, path.stem, path, "failed", 0, str(exc))
                connection.execute(
                    """
                    UPDATE raw_ingest_run
                    SET status = 'failed', processed_file_count = ?, message = ?, finished_at = CURRENT_TIMESTAMP
                    WHERE run_id = ?
                    """,
                    [
                        ingested_file_count + skipped_unchanged_file_count + failed_file_count,
                        f"raw ingest failed: {exc}",
                        materialization_run_id,
                    ],
                )
                raise

        processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
        connection.execute(
            """
            UPDATE raw_ingest_run
            SET status = 'completed', processed_file_count = ?, message = 'raw ingest completed.',
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [processed_file_count, materialization_run_id],
        )

    summary = TdxStockRawIngestSummary(
        runner_name="run_tdx_stock_raw_ingest",
        run_id=materialization_run_id,
        status="completed",
        source_root=str(source_base),
        target_raw_path=str(raw_market_timeframe_ledger_path(target_root, timeframe=normalized_timeframe)),
        timeframe=normalized_timeframe,
        adjust_method=normalized_adjust,
        candidate_file_count=len(candidate_files),
        processed_file_count=processed_file_count,
        ingested_file_count=ingested_file_count,
        skipped_unchanged_file_count=skipped_unchanged_file_count,
        failed_file_count=failed_file_count,
        bar_inserted_count=bar_inserted_count,
        bar_reused_count=bar_reused_count,
        bar_rematerialized_count=bar_rematerialized_count,
        message="raw ingest completed.",
    )
    if summary_path is not None:
        _write_summary(summary.as_dict(), summary_path)
    return summary


def _resolve_tdx_source_folder(source_root: Path, *, timeframe: str, adjust_method: str) -> Path:
    folder = source_root / f"stock-{timeframe}" / resolve_adjust_method_folder(adjust_method)
    if not folder.exists():
        raise FileNotFoundError(f"Missing TDX source directory: {folder}")
    return folder


def _select_candidate_files(*, source_folder: Path, instruments: set[str], limit: int | None) -> list[Path]:
    files = [path for path in sorted(source_folder.glob("*.txt")) if _matches_instrument(path, instruments)]
    if limit is not None and limit > 0:
        return files[:limit]
    return files


def _ingest_one_file(
    *,
    connection: duckdb.DuckDBPyConnection,
    raw_table: str,
    path: Path,
    timeframe: str,
    adjust_method: str,
    run_id: str,
    force_hash: bool,
    run_mode: str,
) -> tuple[str, int, int, int, int, str, str]:
    parsed = parse_tdx_stock_file(path)
    if parsed.adjust_method != adjust_method:
        raise ValueError(f"Adjust method mismatch: expected={adjust_method}, parsed={parsed.adjust_method}")
    stat_result = path.stat()
    source_mtime_utc = datetime.fromtimestamp(stat_result.st_mtime).replace(microsecond=0)
    source_hash = _hash_file(path)
    file_nk = _build_file_nk(code=parsed.code, adjust_method=adjust_method, timeframe=timeframe, path=path)
    existing = connection.execute(
        """
        SELECT source_size_bytes, source_mtime_utc, source_content_hash
        FROM stock_file_registry
        WHERE file_nk = ?
        """,
        [file_nk],
    ).fetchone()
    if (
        run_mode == "incremental"
        and existing is not None
        and int(existing[0]) == stat_result.st_size
        and str(existing[2]) == source_hash
        and not force_hash
    ):
        return "skipped_unchanged", 0, len(parsed.rows), 0, len(parsed.rows), file_nk, parsed.code

    inserted = 0
    reused = 0
    rematerialized = 0
    for row in parsed.rows:
        bar_nk = _build_bar_nk(code=row.code, trade_date=row.trade_date.isoformat(), adjust_method=adjust_method, timeframe=timeframe)
        existing_row = connection.execute(f"SELECT open, high, low, close FROM {raw_table} WHERE bar_nk = ?", [bar_nk]).fetchone()
        if existing_row is None:
            inserted += 1
        elif (existing_row[0], existing_row[1], existing_row[2], existing_row[3]) == (row.open, row.high, row.low, row.close):
            reused += 1
        else:
            rematerialized += 1
        connection.execute(
            f"""
            INSERT INTO {raw_table} (
                bar_nk, source_file_nk, asset_type, code, name, trade_date, adjust_method,
                open, high, low, close, volume, amount, source_path, source_mtime_utc,
                first_seen_run_id, last_ingested_run_id, timeframe, updated_at
            )
            VALUES (?, ?, 'stock', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(bar_nk) DO UPDATE SET
                name = excluded.name,
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                amount = excluded.amount,
                source_path = excluded.source_path,
                source_mtime_utc = excluded.source_mtime_utc,
                last_ingested_run_id = excluded.last_ingested_run_id,
                updated_at = excluded.updated_at
            """,
            [
                bar_nk,
                file_nk,
                row.code,
                row.name,
                row.trade_date,
                adjust_method,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.amount,
                str(path),
                source_mtime_utc,
                run_id,
                run_id,
                timeframe,
            ],
        )
    connection.execute(
        """
        INSERT INTO stock_file_registry (
            file_nk, timeframe, adjust_method, code, name, source_path, source_size_bytes,
            source_mtime_utc, source_content_hash, source_line_count, last_ingested_run_id, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(file_nk) DO UPDATE SET
            source_size_bytes = excluded.source_size_bytes,
            source_mtime_utc = excluded.source_mtime_utc,
            source_content_hash = excluded.source_content_hash,
            source_line_count = excluded.source_line_count,
            last_ingested_run_id = excluded.last_ingested_run_id,
            updated_at = excluded.updated_at
        """,
        [file_nk, timeframe, adjust_method, parsed.code, parsed.name, str(path), stat_result.st_size, source_mtime_utc, source_hash, len(parsed.rows), run_id],
    )
    if inserted > 0 or rematerialized > 0:
        _mark_dirty(connection, code=parsed.code, timeframe=timeframe, adjust_method=adjust_method, run_id=run_id, file_nk=file_nk)
    return _resolve_action(inserted=inserted, rematerialized=rematerialized), inserted, reused, rematerialized, len(parsed.rows), file_nk, parsed.code


def _record_file(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    file_nk: str,
    code: str,
    path: Path,
    action: str,
    row_count: int,
    error_message: str | None,
) -> None:
    connection.execute(
        "INSERT INTO raw_ingest_file VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
        [run_id, file_nk, code, str(path), action, row_count, error_message],
    )


def _mark_dirty(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    timeframe: str,
    adjust_method: str,
    run_id: str,
    file_nk: str,
) -> None:
    dirty_nk = _build_dirty_nk(code=code, adjust_method=adjust_method, timeframe=timeframe)
    connection.execute(
        """
        INSERT INTO base_dirty_instrument (
            dirty_nk, asset_type, timeframe, code, adjust_method, dirty_reason,
            source_run_id, source_file_nk, dirty_status, last_consumed_run_id, last_marked_at
        )
        VALUES (?, 'stock', ?, ?, ?, 'raw_ingested', ?, ?, 'pending', NULL, CURRENT_TIMESTAMP)
        ON CONFLICT(dirty_nk) DO UPDATE SET
            dirty_status = 'pending',
            source_run_id = excluded.source_run_id,
            source_file_nk = excluded.source_file_nk,
            last_marked_at = excluded.last_marked_at
        """,
        [dirty_nk, timeframe, code, adjust_method, run_id, file_nk],
    )


def _normalize_run_mode(run_mode: str) -> str:
    normalized = str(run_mode).strip().lower()
    if normalized not in {"incremental", "full"}:
        raise ValueError(f"Unsupported raw ingest run_mode: {run_mode}")
    return normalized


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> set[str]:
    return {str(item).strip().upper() for item in instruments or () if str(item).strip()}


def _matches_instrument(path: Path, instruments: set[str]) -> bool:
    if not instruments:
        return True
    stem = path.stem.upper()
    if "#" not in stem:
        return False
    exchange, code = stem.split("#", 1)
    return code in instruments or f"{code}.{exchange}" in instruments


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_file_nk(*, code: str, adjust_method: str, timeframe: str, path: Path) -> str:
    return "|".join([code, adjust_method, timeframe, str(path)])


def _build_bar_nk(*, code: str, trade_date: str, adjust_method: str, timeframe: str) -> str:
    return "|".join([code, trade_date, adjust_method, timeframe])


def _build_dirty_nk(*, code: str, adjust_method: str, timeframe: str) -> str:
    return "|".join([code, adjust_method, timeframe])


def _resolve_action(*, inserted: int, rematerialized: int) -> str:
    if inserted > 0 and rematerialized > 0:
        return "mixed"
    if inserted > 0:
        return "inserted"
    if rematerialized > 0:
        return "rematerialized"
    return "reused"


def _write_summary(payload: dict[str, object], summary_path: Path) -> None:
    import json

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
