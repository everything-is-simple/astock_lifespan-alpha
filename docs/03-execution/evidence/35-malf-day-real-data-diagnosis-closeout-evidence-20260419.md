# 阶段十批次 35 MALF day 真实库诊断工程收口证据

证据编号：`35`
日期：`2026-04-19`

## 1. 命令

```text
pytest -q tests/unit/core
pytest -q tests/unit/malf
pytest -q tests/unit/contracts
pytest -q tests/unit/docs
pytest -q
python -c "import runpy; runpy.run_path(r'scripts/malf/run_malf_day_build.py', run_name='malf_bootstrap_check')"
python scripts/malf/profile_malf_day_real_data.py
```

## 2. 关键结果

- 脚本直跑入口已修正，不再需要手动设置 `PYTHONPATH`。
- `profile_malf_day_real_data` 已生成真实库 JSON/Markdown 报告。
- 当前无参真实诊断报告确认：
  - `source_path = H:\Lifespan-data\base\market_base.duckdb`
  - `table_name = stock_daily_adjusted`
  - `row_count = 49044339`
  - `symbol_count = 5501`
  - `symbol_limit = 10`
  - `bar_limit_per_symbol = 1000`
  - `bottleneck_stage = engine_timing`
- 真实采样写回同时记录到重复主键异常：
  - `snapshot_nk`
  - `pivot_nk`

## 3. 产物

- `scripts/_bootstrap.py`
- `scripts/malf/profile_malf_day_real_data.py`
- `src/astock_lifespan_alpha/malf/diagnostics.py`
- `docs/03-execution/35-malf-day-real-data-diagnosis-closeout-conclusion-20260419.md`
