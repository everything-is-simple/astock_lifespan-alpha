# 阶段五批次 24 文档总收口执行卡

卡片编号：`24`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段五必须先把 `22-23` 文档、索引与结论目录统一收口，才能进入工程实施。
- 目标：确认阶段五文档已经冻结，并切换顶层状态到“阶段五工程待启动”。
- 为什么现在做：没有文档收口，阶段五仍停留在临时规划状态，而不是正式治理状态。

## 2. 设计输入

- `docs/03-execution/21-stage-four-closeout-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`
- `docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md`

## 4. 任务切片

1. 更新顶层状态说明。
2. 把阶段五文档批次纳入执行区索引与结论目录。
3. 为后续阶段五代码实施建立正式入口状态。

## 5. 实现边界

范围内：

- `README.md`
- `docs/README.md`
- `docs/03-execution/README.md`
- `docs/03-execution/00-conclusion-catalog-20260419.md`

范围外：

- 阶段五 `trade` 代码
- `system`

## 6. 收口标准

1. 阶段五被明确标记为“文档已冻结，工程待启动”。
2. `22-24` 在索引与结论目录中可追溯。
3. 顶层状态一致表达价格分线与阶段四勘误已冻结。
Implementation freeze addendum: stage-five document closeout now freezes `PathConfig.source_databases.market_base`, 次日开盘执行, `accepted` as reserved but not materialized, and `portfolio_id + symbol` as the replay work unit before trade engineering.
