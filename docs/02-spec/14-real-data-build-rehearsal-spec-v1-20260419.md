# 阶段九真实建库演练规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-nine-real-data-build`

## 1. 定位

本规格冻结阶段九真实建库演练边界。
阶段九不新增业务功能，不引入 Go+DuckDB 新工程，而是验证当前 Python+DuckDB 正式主线能否在真实本地 `H:\Lifespan-data` 上按既有 runner 顺序跑通。

阶段九验收主线固定为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system -> pipeline
```

## 2. 真实输入与正式输出目录

阶段九固定读取以下真实 source fact DuckDB：

- `H:\Lifespan-data\base\market_base.duckdb`
- `H:\Lifespan-data\base\market_base_week.duckdb`
- `H:\Lifespan-data\base\market_base_month.duckdb`
- `H:\Lifespan-data\raw\raw_market.duckdb`
- `H:\Lifespan-data\raw\raw_market_week.duckdb`
- `H:\Lifespan-data\raw\raw_market_month.duckdb`

阶段九允许写入正式输出目录：

- `H:\Lifespan-data\astock_lifespan_alpha`

阶段九默认就地复跑，不删除已有 DuckDB，不清空历史 run 记录。

## 3. 执行顺序

阶段九先按模块顺序做 `module-by-module build`：

1. `python scripts/malf/run_malf_day_build.py`
2. `python scripts/malf/run_malf_week_build.py`
3. `python scripts/malf/run_malf_month_build.py`
4. `python scripts/alpha/run_alpha_bof_build.py`
5. `python scripts/alpha/run_alpha_tst_build.py`
6. `python scripts/alpha/run_alpha_pb_build.py`
7. `python scripts/alpha/run_alpha_cpb_build.py`
8. `python scripts/alpha/run_alpha_bpb_build.py`
9. `python scripts/alpha/run_alpha_signal_build.py`
10. `python scripts/position/run_position_from_alpha_signal.py`
11. `python scripts/portfolio_plan/run_portfolio_plan_build.py`
12. `python scripts/trade/run_trade_from_portfolio_plan.py`
13. `python scripts/system/run_system_from_trade.py`

单模块顺序完成后，再执行 `pipeline replay`：

```text
python scripts/pipeline/run_data_to_system_pipeline.py
```

## 4. Preflight 与记录要求

执行前必须先做 read-only preflight，并记录：

1. 6 个 source fact DuckDB 是否存在。
2. `H:\Lifespan-data\astock_lifespan_alpha` 已有输出库情况。
3. 各核心表当前行数。

每组执行后必须记录：

- runner summary
- `target_path`
- status
- 核心表行数
- 是否出现 empty output

## 5. pipeline 验收口径

阶段九 `pipeline replay` 验收标准固定为：

1. `run_data_to_system_pipeline` status 为 `completed`。
2. `pipeline_step_run` 登记 13 个 step。
3. 最后一步为 `run_system_from_trade`。

## 6. 行为边界

- 阶段九只验证现有 Python+DuckDB 主线，不修改已冻结业务语义。
- 阶段九允许 runner 追加 run 记录、复用 checkpoint 或重物化表。
- 若真实建库暴露 source/schema bug，应先记录 blocker，不在同一批次随意改业务语义。
- `Go+DuckDB deferred`，阶段九完成后再按真实瓶颈评估是否单独立项。

## 7. 明确不纳入阶段九

阶段九不纳入：

- 新业务功能
- Go+DuckDB 实施
- scheduler / 定时任务
- 外部服务
- broker/session/partial fill
- pnl
- exit
- 删除正式输出库或清空正式目录

## 8. 验收标准

阶段九文档与执行收口必须满足：

1. `stage-nine-real-data-build` 规格已冻结。
2. 文档明确 `H:\Lifespan-data` 与 `H:\Lifespan-data\astock_lifespan_alpha` 的真实演练边界。
3. 文档明确 `module-by-module build` 与 `pipeline replay` 顺序。
4. 文档明确 `run_data_to_system_pipeline` 是最后 replay 入口。
5. docs 测试与全量测试通过。
