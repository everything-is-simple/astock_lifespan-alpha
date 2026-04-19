# 批次 42 证据：阶段十四 MALF day 真实分段证明与阶段九重发

证据编号：`42`
日期：`2026-04-19`

## 1. 命令

```text
python -  # 内联 preflight：登记 source path / table / symbol_count / artifacts / frontier
python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100
python -  # 内联 post-failure snapshot：复核 artifacts、latest_run 与 report sidecar
```

## 2. Preflight 快照

- day source：
  - `source_path = H:\Lifespan-data\base\market_base.duckdb`
  - `table_name = stock_daily_adjusted`
  - `symbol_count = 5501`
- 正式 MALF day target：
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
  - `size_mb = 54.76`
  - `checkpoint_count = 6`
  - `run_status_counts = {"completed": 27, "running": 6}`
- active continuation artifact：
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
  - `size_mb = 4981.51`（post-failure snapshot 为 `4997.01`）
  - `checkpoint_count = 3475`
  - `run_status_counts = {"running": 1}`
- abandoned artifact：
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`
  - `size_mb = 2993.51`
  - `checkpoint_count = 1900`
  - `run_status_counts = {"running": 1}`
- active frontier：
  - `done_symbols = 3475`
  - `remaining_symbols = 2026`
  - `next_start_symbol = 600771.SH`
  - `next_5_symbols = ["600771.SH", "600773.SH", "600774.SH", "600775.SH", "600776.SH"]`

## 3. 真实 100-symbol 首轮结果

- 实际执行命令：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100
```

- 命令在约 `3` 秒内失败，未进入正式 summary/progress 写出阶段。
- 失败栈摘录：

```text
Traceback (most recent call last):
  File "H:\astock_lifespan-alpha\scripts\malf\run_malf_day_build.py", line 35, in <module>
    run_malf_day_build(
  File "H:\astock_lifespan-alpha\src\astock_lifespan_alpha\malf\runner.py", line 163, in run_malf_day_build
    return _run_malf_build(
  File "H:\astock_lifespan-alpha\src\astock_lifespan_alpha\malf\runner.py", line 292, in _run_malf_build
    initialize_malf_schema(active_target_path)
  File "H:\astock_lifespan-alpha\src\astock_lifespan_alpha\malf\schema.py", line 54, in initialize_malf_schema
    _ensure_column(
  File "H:\astock_lifespan-alpha\src\astock_lifespan_alpha\malf\schema.py", line 221, in _ensure_column
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
_duckdb.ParserException: Parser Error: Adding columns with constraints not yet supported
```

- 现场结论：
  - 失败发生在 `initialize_malf_schema(active_target_path)` 对现有真实库补 schema 时
  - 所有真实 `malf_run` 仍是旧列集，缺少：
    - `symbols_total`
    - `symbols_completed`
    - `current_symbol`
    - `elapsed_seconds`
    - `estimated_remaining_symbols`
  - 本轮没有生成 CLI summary JSON
  - 本轮没有生成 `progress_summary.progress_path`
  - `H:\Lifespan-report\astock_lifespan_alpha\malf` 下没有新的 `run_malf_day_build*-progress.json`
  - active / abandoned building 库都没有新增 checkpoint
  - 没有新增真实 `run_id`

## 4. Post-Failure 快照

- `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
  - `checkpoint_count` 仍为 `3475`
  - 最新 run 仍为：
    - `run_id = day-d48ab7015ff4`
    - `status = running`
    - `message = MALF run started.`
- `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`
  - `checkpoint_count` 仍为 `1900`
  - 最新 run 仍为：
    - `run_id = day-d696fdcd4774`
    - `status = running`
    - `message = MALF run started.`
- `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
  - `checkpoint_count` 仍为 `6`
  - `run_status_counts` 仍为 `{"running": 6, "completed": 27}`
- `H:\Lifespan-report\astock_lifespan_alpha\malf`
  - `run_malf_day_build*-progress.json` 数量：`0`

## 5. 产物

- 真实 source：`H:\Lifespan-data\base\market_base.duckdb`
- 正式 target：`H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
- active continuation artifact：`H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- abandoned artifact：`H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`
- 本批次未生成新的 progress sidecar
