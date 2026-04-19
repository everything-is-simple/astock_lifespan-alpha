# 阶段六批次 26 system 读出规格冻结执行卡

卡片编号：`26`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段五已完成，阶段六 `system` 进入工程前必须先冻结 `trade -> system` 的最小读出边界。
- 目标：冻结 `stage-six-system` 规格，明确 `system` 只读取 `trade` 正式输出，不回读上游内部过程，不触发上游 runner。
- 为什么现在做：避免阶段六被扩大为全链路调度、真实执行、pnl 或 broker/session 范围。

## 2. 设计输入

- `docs/02-spec/10-astock-lifespan-alpha-reconstruction-plan-part2-stage-five-trade-v1-20260419.md`

## 3. 规格输入

- `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`
- `docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md`
- `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`

## 4. 任务切片

1. 新增阶段六 `system` 最小读出与 runner 规格。
2. 新增批次 `26` 的 card/evidence/record/conclusion。
3. 更新 README、docs 索引与结论目录。
4. 新增 docs 测试锁定阶段六规格冻结关键词。

## 5. 实现边界

范围内：

- docs 规格
- execution 治理闭环
- docs 索引
- docs 测试

范围外：

- `system` 工程代码
- `trade` 代码
- 全链路自动编排

## 6. 收口标准

1. `stage-six-system` 规格状态为 `冻结`。
2. 文档明确 `trade -> system` 是阶段六唯一主线。
3. 文档明确 `system` 只读取 `trade` 正式输出。
4. 文档明确阶段六不回读 `alpha / position / portfolio_plan`，不触发上游 runner。
5. docs 测试与全量测试通过。
