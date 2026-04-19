# 阶段十二批次 39 MALF day 写路径重演 unblock 工程收口执行卡

卡片编号：`39`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段十二规格已冻结，但 `write_timing` 尚未拆分，MALF day 写路径仍会放大真实重演耗时
- 目标：完成写路径 timing 合同、ledger 插入优化、旧库索引失败隔离、真实诊断复核与阶段九重演偏差登记
- 为什么现在做：阶段十一已确认当前主瓶颈从 `engine_timing` 转为 `write_timing`

## 2. 规格输入

- `docs/02-spec/17-stage-twelve-malf-day-write-path-replay-unblock-spec-v1-20260419.md`
- `docs/03-execution/38-stage-twelve-malf-day-write-path-replay-unblock-spec-freeze-conclusion-20260419.md`

## 3. 工程输出

- `src/astock_lifespan_alpha/malf/contracts.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `src/astock_lifespan_alpha/malf/diagnostics.py`
- `tests/unit/malf/test_runner.py`
- `tests/unit/malf/test_diagnostics.py`
- `tests/unit/docs/test_malf_day_repair_specs.py`

## 4. 任务切片

1. 增加 `write_timing_summary`
2. 拆分 `delete old rows / insert ledgers / checkpoint / queue update`
3. 将 ledger 插入从 Python `executemany` 改为 DuckDB registered relation 写入
4. 对真实库遗留 `running` 状态启用新库重建与旧库 backup 隔离
5. 运行单测、全量 pytest、真实 diagnostics 与真实全量 build 观察

## 5. 收口标准

1. `profile_malf_day_real_data` 报告写出细分 write timing
2. `run_malf_day_build` summary 写出细分 write timing
3. 单元测试与全量 `pytest` 通过
4. 真实 diagnostics 证明 `insert_ledgers_seconds` 已明显下降
5. 真实全量 build 若仍不可在观察窗完成，必须登记剩余偏差
