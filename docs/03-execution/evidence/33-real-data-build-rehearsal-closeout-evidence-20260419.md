# 阶段九批次 33 真实建库演练执行收口证据

证据编号：`33`
日期：`2026-04-19`

## 1. 命令

```text
python scripts/malf/run_malf_day_build.py
$env:PYTHONPATH='H:\astock_lifespan-alpha\src'; python scripts/malf/run_malf_day_build.py
python - <<preflight/read-only duckdb inspection>>
Get-CimInstance Win32_Process -Filter "ProcessId = 10472"
```

## 2. 关键结果

- 6 个 source fact DuckDB 全部存在。
- 正式输出目录存在既有 `malf / alpha / position / portfolio_plan` DuckDB。
- 不补 `PYTHONPATH` 时，脚本入口报 `ModuleNotFoundError: No module named 'astock_lifespan_alpha'`。
- 补 `PYTHONPATH` 后，`run_malf_day_build` 超过 12 分钟未完成返回。
- 运行期间 `malf_day.duckdb` 被进程 `10472` 占用，命令行为 `"D:\\miniconda310\\python.exe" scripts/malf/run_malf_day_build.py`。
- 进程结束后，`malf_run = 28`、`malf_work_queue = 1`，其余 MALF 正式输出表仍为 `0`。

## 3. 产物

- `docs/03-execution/33-real-data-build-rehearsal-closeout-conclusion-20260419.md`
- `docs/03-execution/records/33-real-data-build-rehearsal-closeout-record-20260419.md`
