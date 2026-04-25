# Card 66 data stock producer 安全重建执行卡

日期：`2026-04-25`
状态：`执行中`
规格：`docs/02-spec/30-data-stock-producer-safe-rebuild-freeze-spec-v1-20260425.md`

## 目标

恢复新版 `data` 模块 stock-only producer 最小闭环，同时保护现有 `H:\Lifespan-data` 老库。

## 范围

本卡纳入：

- TDX offline stock parser
- isolated raw_market bootstrap / ingest
- isolated market_base bootstrap / build
- dirty queue 最小闭环
- source fact read-only audit
- CLI 入口与单元测试

本卡不纳入：

- 写入或替换老库
- index / block
- Tushare / TdxQuant 网络日更
- pipeline 默认步骤变更

## 验收命令

最少执行：

```powershell
pytest tests/unit/data -q
pytest tests/unit/data tests/unit/core/test_paths.py tests/unit/contracts/test_module_boundaries.py tests/unit/contracts/test_runner_contracts.py tests/unit/pipeline/test_pipeline_runner.py -q
D:\miniconda\py310\python.exe scripts/data/audit_data_source_fact_freeze.py
pytest
D:\miniconda\py310\python.exe scripts/pipeline/run_data_to_system_pipeline.py
```

## 裁决规则

全部命令通过，且 audit 未写老库，才能登记为工程收口。
