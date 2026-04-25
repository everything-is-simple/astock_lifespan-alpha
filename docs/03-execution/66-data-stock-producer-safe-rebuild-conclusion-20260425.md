# Card 66 data stock producer 安全重建工程结论

日期：`2026-04-25`
状态：`已接受`
规格：`docs/02-spec/30-data-stock-producer-safe-rebuild-freeze-spec-v1-20260425.md`

## 结论

Card 66 已完成代码落地并通过验证。

当前已落地：

- `astock_lifespan_alpha.data` 从空包升级为 stock-only producer 包。
- 新增 isolated `TDX offline -> raw_market -> market_base` runner。
- 新增 source fact read-only audit。
- producer 默认拒绝写入正式 `H:\Lifespan-data` source fact root。
- `pipeline` 默认 13 step 合同未改动。

## 当前审计发现

只读 audit 已确认：

- `market_base.stock_daily_adjusted` 覆盖 `1990-12-19 -> 2026-04-10`
- `raw_market.stock_daily_bar` 覆盖 `1990-12-19 -> 2026-04-10`
- day/week/month backward duplicate groups 均为 `0`
- `raw_market` 全 adjust_method code 集合比 `market_base` 多 `510300.SH`

`510300.SH` 差异只登记，不在本卡原地修复。

## 验证证据

已执行：

```powershell
pytest tests/unit/data -q
```

结果：`9 passed`

```powershell
pytest tests/unit/data tests/unit/core/test_paths.py tests/unit/contracts/test_module_boundaries.py tests/unit/contracts/test_runner_contracts.py tests/unit/pipeline/test_pipeline_runner.py -q
```

结果：`20 passed`

```powershell
D:\miniconda\py310\python.exe scripts/data/audit_data_source_fact_freeze.py
```

结果：`status = completed`，老库只读审计完成，`raw_only_codes = ["510300.SH"]`

```powershell
pytest
```

结果：`146 passed`

```powershell
D:\miniconda\py310\python.exe scripts/pipeline/run_data_to_system_pipeline.py
```

结果：`pipeline-f34d0b328c67`，`status = completed`，`step_count = 13`

## 最终裁决

Card 66 接受。

`data` 已从只消费 source fact 的空包，升级为具备 stock-only isolated producer 与 read-only audit 的正式模块。

现有 `H:\Lifespan-data` 老库继续保持只读审计边界；producer 默认不得写老库。
