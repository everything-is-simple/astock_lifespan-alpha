from __future__ import annotations

from pathlib import Path

import pytest

from astock_lifespan_alpha.data.tdx import parse_tdx_stock_file, resolve_adjust_method_folder, resolve_adjust_method_name


def test_tdx_adjust_method_folder_mapping_supports_official_folder_names() -> None:
    assert resolve_adjust_method_folder("backward") == "Backward-Adjusted"
    assert resolve_adjust_method_folder("forward") == "Forward-Adjusted"
    assert resolve_adjust_method_folder("none") == "Non-Adjusted"
    assert resolve_adjust_method_name("Backward-Adjusted") == "backward"
    assert resolve_adjust_method_name("Forward-Adjusted") == "forward"
    assert resolve_adjust_method_name("Non-Adjusted") == "none"


def test_parse_tdx_stock_file_maps_filename_folder_and_rows(tmp_path: Path) -> None:
    source_dir = tmp_path / "stock-day" / "Backward-Adjusted"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "SH#600000.txt"
    source_file.write_text(
        "\n".join(
            [
                "600000 浦发银行 日线 前复权",
                "日期\t开盘\t最高\t最低\t收盘\t成交量\t成交额",
                "2026/04/09\t10.00\t10.50\t9.80\t10.20\t1000\t2000",
                "2026/04/10\t10.20\t10.80\t10.10\t10.70\t1100\t2200",
                "数据来源: 通达信",
            ]
        ),
        encoding="gbk",
    )

    parsed = parse_tdx_stock_file(source_file)

    assert parsed.code == "600000.SH"
    assert parsed.name == "浦发银行"
    assert parsed.adjust_method == "backward"
    assert len(parsed.rows) == 2
    assert parsed.rows[0].trade_date.isoformat() == "2026-04-09"
    assert parsed.rows[1].close == 10.7


def test_parse_tdx_stock_file_rejects_unknown_adjust_folder(tmp_path: Path) -> None:
    source_dir = tmp_path / "stock-day" / "Unknown"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "SH#600000.txt"
    source_file.write_text("600000 浦发银行\n日期\t开盘\t最高\t最低\t收盘\t成交量\t成交额\n", encoding="gbk")

    with pytest.raises(ValueError, match="Unsupported TDX folder"):
        parse_tdx_stock_file(source_file)
