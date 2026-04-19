# 阶段二批次 04 MALF 契约与 Schema 证据

证据编号：`04`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python scripts\malf\run_malf_day_build.py
.\.venv\Scripts\python scripts\malf\run_malf_week_build.py
.\.venv\Scripts\python scripts\malf\run_malf_month_build.py
```

## 2. 关键结果

- `tests/unit/malf/test_runner.py::test_malf_runner_initializes_formal_schema` 通过，证明三周期数据库均能初始化 8 张 MALF 正式表。
- MALF runner 输出已升级为正式摘要，包含 `timeframe`、`run_id`、`status`、`target_path`、`materialization_counts`、`checkpoint_summary`。
- 三个 CLI 脚本均可输出 JSON 摘要，不再依赖 foundation `phase=foundation_bootstrap`。

## 3. 产物

- `src/astock_lifespan_alpha/malf/contracts.py`
- `src/astock_lifespan_alpha/malf/schema.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `tests/unit/contracts/test_runner_contracts.py`
- `tests/unit/malf/test_runner.py`
