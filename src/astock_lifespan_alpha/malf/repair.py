"""Schema repair entrypoints for MALF day ledgers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.malf.schema import (
    MALF_RUN_BACKFILL_COLUMNS,
    initialize_malf_schema,
    inspect_table_columns,
)


@dataclass(frozen=True)
class MalfRunSchemaProbe:
    path: str
    exists: bool
    has_malf_run: bool
    missing_columns: tuple[str, ...]
    compatible: bool
    columns: dict[str, dict[str, object]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "exists": self.exists,
            "has_malf_run": self.has_malf_run,
            "missing_columns": list(self.missing_columns),
            "compatible": self.compatible,
            "columns": self.columns,
        }


@dataclass(frozen=True)
class MalfDaySchemaRepairDatabaseSummary:
    path: str
    before: MalfRunSchemaProbe
    actions: tuple[str, ...]
    after: MalfRunSchemaProbe

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "before": self.before.as_dict(),
            "actions": list(self.actions),
            "after": self.after.as_dict(),
        }


@dataclass(frozen=True)
class MalfDaySchemaRepairSummary:
    runner_name: str
    status: str
    target_path: str
    scanned_database_count: int
    repaired_database_count: int
    databases: tuple[MalfDaySchemaRepairDatabaseSummary, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "status": self.status,
            "target_path": self.target_path,
            "scanned_database_count": self.scanned_database_count,
            "repaired_database_count": self.repaired_database_count,
            "databases": [database.as_dict() for database in self.databases],
        }


def repair_malf_day_schema(*, settings: WorkspaceRoots | None = None) -> MalfDaySchemaRepairSummary:
    """Repair MALF day target and building database schemas without running a build."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.malf_day
    database_paths = _discover_malf_day_databases(target_path)
    database_summaries = tuple(_repair_database_schema(path) for path in database_paths)
    repaired_database_count = sum(1 for summary in database_summaries if summary.actions)
    status = "completed" if all(summary.after.compatible for summary in database_summaries) else "failed"
    return MalfDaySchemaRepairSummary(
        runner_name="repair_malf_day_schema",
        status=status,
        target_path=str(target_path),
        scanned_database_count=len(database_summaries),
        repaired_database_count=repaired_database_count,
        databases=database_summaries,
    )


def _discover_malf_day_databases(target_path: Path) -> tuple[Path, ...]:
    building_paths = tuple(sorted(target_path.parent.glob(f"{target_path.stem}.*.building{target_path.suffix}")))
    return tuple(dict.fromkeys((target_path, *building_paths)))


def _repair_database_schema(path: Path) -> MalfDaySchemaRepairDatabaseSummary:
    before = _probe_malf_run_schema(path)
    original_stat = path.stat() if path.exists() else None
    actions: list[str] = []
    initialize_malf_schema(path, actions=actions)
    if original_stat is not None:
        os.utime(str(path), (original_stat.st_atime, original_stat.st_mtime))
    after = _probe_malf_run_schema(path)
    return MalfDaySchemaRepairDatabaseSummary(
        path=str(path),
        before=before,
        actions=tuple(actions),
        after=after,
    )


def _probe_malf_run_schema(path: Path) -> MalfRunSchemaProbe:
    required_names = tuple(column.name for column in MALF_RUN_BACKFILL_COLUMNS)
    if not path.exists():
        return MalfRunSchemaProbe(
            path=str(path),
            exists=False,
            has_malf_run=False,
            missing_columns=required_names,
            compatible=False,
        )

    with duckdb.connect(str(path), read_only=True) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "malf_run" not in table_names:
            return MalfRunSchemaProbe(
                path=str(path),
                exists=True,
                has_malf_run=False,
                missing_columns=required_names,
                compatible=False,
            )
        columns = inspect_table_columns(connection=connection, table_name="malf_run")

    missing_columns = tuple(name for name in required_names if name not in columns)
    compatible = not missing_columns and all(_column_is_compatible(columns[column.name], column) for column in MALF_RUN_BACKFILL_COLUMNS)
    return MalfRunSchemaProbe(
        path=str(path),
        exists=True,
        has_malf_run=True,
        missing_columns=missing_columns,
        compatible=compatible,
        columns={name: columns[name] for name in required_names if name in columns},
    )


def _column_is_compatible(column_info: dict[str, object], expected_column) -> bool:
    default_value = column_info["default"]
    normalized_default = None if default_value is None else str(default_value).strip().strip("'").strip('"')
    type_matches = _normalize_type(str(column_info["type"])) == _normalize_type(expected_column.type_sql)
    default_matches = expected_column.default_sql is None or normalized_default == expected_column.default_sql
    not_null_matches = not expected_column.not_null or bool(column_info["notnull"])
    return type_matches and default_matches and not_null_matches


def _normalize_type(type_sql: str) -> str:
    normalized = type_sql.upper()
    return "TEXT" if normalized == "VARCHAR" else normalized
