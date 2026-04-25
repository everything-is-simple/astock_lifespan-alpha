from __future__ import annotations

import importlib.util
from pathlib import Path


def test_data_cli_entrypoints_define_expected_arguments() -> None:
    repo_root = Path(__file__).resolve().parents[3]

    audit_cli = _load_module(repo_root / "scripts" / "data" / "audit_data_source_fact_freeze.py")
    target_audit_cli = _load_module(repo_root / "scripts" / "data" / "audit_stock_producer_target.py")
    raw_cli = _load_module(repo_root / "scripts" / "data" / "run_tdx_stock_raw_ingest.py")
    base_cli = _load_module(repo_root / "scripts" / "data" / "run_market_base_build.py")
    rehearsal_cli = _load_module(repo_root / "scripts" / "data" / "run_stock_producer_rehearsal.py")

    assert audit_cli.build_argument_parser().parse_args([]).pretty is False
    assert target_audit_cli.build_argument_parser().parse_args(["--target-data-root", "X:/tmp"]).target_data_root == Path("X:/tmp")
    assert raw_cli.build_argument_parser().parse_args(["--target-data-root", "X:/tmp"]).target_data_root == Path("X:/tmp")
    assert base_cli.build_argument_parser().parse_args(["--target-data-root", "X:/tmp"]).target_data_root == Path("X:/tmp")
    assert rehearsal_cli.build_argument_parser().parse_args(["--target-data-root", "X:/tmp"]).target_data_root == Path("X:/tmp")


def _load_module(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
