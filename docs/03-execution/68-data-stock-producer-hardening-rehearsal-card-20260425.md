# Card 68 data stock producer 硬化与复演执行卡

日期：`2026-04-25`
状态：`执行中`
规格：`docs/02-spec/31-data-stock-producer-hardening-rehearsal-spec-v1-20260425.md`

## 目标

继续加强 Card 66 stock-only isolated producer，使其具备资产门禁、target audit 与 rehearsal 闭环。

## 范围

本卡纳入：

- A 股 stock-only code classifier。
- TDX stock raw ingest 非 stock 排除统计。
- isolated stock producer target audit。
- isolated stock producer rehearsal runner。
- `scripts/data/audit_stock_producer_target.py`
- `scripts/data/run_stock_producer_rehearsal.py`
- data 单元测试、契约回归与 pipeline step count 回归。

本卡不纳入：

- 写入或替换 `H:\Lifespan-data` 老库。
- 修复老库 `510300.SH` raw-only 差异。
- index / block producer。
- Tushare / TdxQuant 网络日更。
- pipeline 默认步骤变更。

## 验收命令

最少执行：

```powershell
pytest tests/unit/data -q
pytest tests/unit/data tests/unit/contracts/test_run_id_key_boundaries.py tests/unit/pipeline/test_pipeline_runner.py -q
pytest
```

## 裁决规则

全部命令通过，且 pipeline 默认仍为 13 step，才能登记为工程收口。
