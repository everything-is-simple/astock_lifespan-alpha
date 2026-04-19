# 批次 39 证据：阶段十二 MALF day 写路径重演 unblock 工程收口

## 1. 代码与测试

- `src/astock_lifespan_alpha/malf/contracts.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `src/astock_lifespan_alpha/malf/diagnostics.py`
- `tests/unit/malf/test_runner.py`
- `tests/unit/malf/test_diagnostics.py`
- `tests/unit/docs/test_malf_day_repair_specs.py`

## 2. 命令证据

- `pytest tests/unit/malf -q`
- `pytest tests/unit/contracts/test_module_boundaries.py tests/unit/contracts/test_runner_contracts.py -q`
- `pytest`
- `python scripts/malf/profile_malf_day_real_data.py`
- `python scripts/malf/run_malf_day_build.py`

## 3. 真实诊断报告

- 优化后 JSON：`H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-diag-996a9a2aa5e1.json`
- 优化后 Markdown：`H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-diag-996a9a2aa5e1.md`
- 对照中间报告：`H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-diag-6c4ea75966cb.json`

## 4. 真实 build 观察

- 第一次全量 build 失败于旧库 index delete fatal：`Failed to delete all rows from index`
- 第二次全量 build 进入新 building 库：`H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`
- 35 分钟观察窗结束时 building 库约 `3.14GB`
- 后台进程被手动终止，正式 `malf_day.duckdb` 未被替换
