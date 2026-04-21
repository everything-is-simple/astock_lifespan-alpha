# 阶段十六 portfolio_plan / system / pipeline 正式增量与自动续跑规格 v1

日期：`2026-04-21`
状态：`冻结`
文档标识：`stage-sixteen-incremental-resume`

## 1. 目标

本规格冻结主线最后一段正式增量契约，使下列链路从“已打通”升级为“可做每日正式断点续传增量更新”：

```text
portfolio_plan -> trade -> system -> pipeline
```

本轮只补正式 ledger、checkpoint、work_queue、resume 语义，不改主线业务语义，不改 runner 名称，不改主线顺序。

## 2. portfolio_plan 增量契约

`portfolio_plan` 保持现有组合容量累计裁决语义，不改：

- `run_portfolio_plan_build`
- `plan_snapshot_nk`
- `stage4_portfolio_plan_v1`

新增正式表：

- `portfolio_plan_work_queue`
- `portfolio_plan_checkpoint`

新增稳定 summary 字段：

- `PortfolioPlanCheckpointSummary`
- `PortfolioPlanRunSummary.checkpoint_summary`

正式 work unit 固定为：

- `portfolio_id`

正式 source work unit summary 至少包含：

- `portfolio_id`
- `source_row_count`
- `last_reference_trade_date`
- `source_fingerprint`

fast path 规则冻结为：

1. `portfolio_plan_checkpoint` 存在且 `last_reference_trade_date`、`last_source_fingerprint` 匹配。
2. `portfolio_plan_snapshot` 中该 `portfolio_id` 的现有行数等于 `source_row_count`。
3. 命中后写 `portfolio_plan_work_queue.status='reused'`。
4. 命中后本轮 `portfolio_plan_run_snapshot.materialization_action='reused'`。
5. 命中后 `work_units_updated = 0`，不重写 `portfolio_plan_snapshot`。

slow path 规则冻结为：

1. 在单事务内按 `portfolio_id` 全量 rematerialize。
2. 沿用 `inserted / reused / rematerialized`。
3. 成功后 upsert `portfolio_plan_checkpoint`。
4. 事务失败时 `portfolio_plan_run.status='interrupted'`，不得留下半套 snapshot。

repair 入口冻结为：

- `portfolio_plan.repair.repair_portfolio_plan_schema()`
- `scripts/portfolio_plan/repair_portfolio_plan_schema.py`

## 3. system 增量契约

`system` 保持单向边界：

- 只读 `trade`
- 不回读 `alpha / position / portfolio_plan`
- 不触发上游 runner

不改：

- `run_system_from_trade`
- `stage6_system_v1`

新增正式表：

- `system_work_queue`
- `system_checkpoint`

新增稳定 summary 字段：

- `SystemCheckpointSummary`
- `SystemRunSummary.checkpoint_summary`

正式 work unit 固定为：

- `portfolio_id + symbol`

正式 source work unit summary 至少包含：

- `portfolio_id`
- `symbol`
- `source_row_count`
- `latest_execution_trade_date`
- `source_fingerprint`

fast path 规则冻结为：

1. `system_checkpoint` 对应 symbol 匹配。
2. `system_trade_readout` 对应 work unit 现有行数等于 `source_row_count`。
3. 命中后写 `system_work_queue.status='reused'`。
4. 命中后 `work_units_updated = 0`。
5. 命中后不重写 `system_trade_readout` 与 `system_portfolio_trade_summary`。

slow path 规则冻结为：

1. 仅删除并重写发生变化的 `portfolio_id + symbol`。
2. 变更完成后按 `portfolio_id` 重算 `system_portfolio_trade_summary`。
3. 事务失败时 `system_run.status='interrupted'`。

repair 入口冻结为：

- `system.repair.repair_system_schema()`
- `scripts/system/repair_system_schema.py`

## 4. pipeline 自动续跑契约

`pipeline` 继续只编排 public runner，不直接写业务表。

不改：

- `run_data_to_system_pipeline`
- `stage8_pipeline_v1`
- 固定 13 步顺序

新增正式表：

- `pipeline_step_checkpoint`

新增稳定 summary 字段：

- `PipelineResumeSummary`
- `PipelineRunSummary.resume_summary`

正常日跑规则冻结为：

1. 仍从 step 1 开始。
2. 不因为上一轮 completed 就整步跳过。

自动续跑规则冻结为：

1. 仅当同 `portfolio_id` 最新 `pipeline_run.status='interrupted'` 时进入 resume。
2. `resume_start_step` 为该 run 第一条缺失或未完成 step。
3. `resume_start_step` 之前的步骤从 `pipeline_step_checkpoint` 复制。
4. 复制后的 `summary_json` 必须带 `pipeline_action='reused_checkpoint'`。
5. 从 `resume_start_step` 开始正常执行 runner。
6. 每个成功 step 都 upsert `pipeline_step_checkpoint`。
7. runner 异常时 `pipeline_run.status='interrupted'`，`step_count` 写已完成步骤数。

repair 入口冻结为：

- `pipeline.repair.repair_pipeline_schema()`
- `scripts/pipeline/repair_pipeline_schema.py`

## 5. 验收

本规格冻结后，工程实施必须满足：

1. `portfolio_plan` 具备 `portfolio_id` 级 checkpoint/reused/rematerialize。
2. `system` 具备 `portfolio_id + symbol` 级 checkpoint/reused/rematerialize。
3. `pipeline` 具备 step 级 `interrupted -> resume_start_step` 自动续跑。
4. docs tests、contracts tests、模块边界测试、全量 `pytest` 通过。
