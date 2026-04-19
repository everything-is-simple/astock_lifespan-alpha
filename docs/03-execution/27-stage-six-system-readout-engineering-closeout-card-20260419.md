# 阶段六批次 27 system 读出工程收口执行卡

卡片编号：`27`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段六 `stage-six-system` 规格已冻结且 `system` 工程实现已完成，需要正式治理收口。
- 目标：裁决阶段六 `trade -> system` 最小读出与 runner 已完成，并把仓库状态切换为“阶段六完成，下一阶段待规划”。
- 为什么现在做：避免工程实现已经落地但文档治理仍停留在“工程待实施”。

## 2. 规格输入

- `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`
- `docs/03-execution/26-stage-six-system-readout-spec-freeze-conclusion-20260419.md`

## 3. 任务切片

1. 新增批次 `27` 的 card/evidence/record/conclusion。
2. 更新 README、docs 索引与结论目录。
3. 新增 docs 测试锁定阶段六完成结论。
4. 验证 docs 测试与全量测试。

## 4. 收口标准

1. `run_system_from_trade` 已实现。
2. `system_run / system_trade_readout / system_portfolio_trade_summary` 已落地。
3. `system` 只读取 `trade` 正式输出。
4. `system` 不回读 `alpha / position / portfolio_plan`，不触发上游 runner。
5. 阶段六完成结论已登记，下一阶段待规划。
