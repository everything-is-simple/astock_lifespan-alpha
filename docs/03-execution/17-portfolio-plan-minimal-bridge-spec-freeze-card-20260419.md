# 阶段四批次 17 portfolio_plan 最小桥接规格冻结执行卡

卡片编号：`17`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段四不只要切到 `position`，还要同步补出 `position -> portfolio_plan` 的最小桥接。
- 目标：冻结 `portfolio_plan_run / snapshot / run_snapshot` 三表和最小 `admitted / blocked / trimmed` 规则。
- 为什么现在做：如果桥接继续悬空，`position` 上游切换后仍然无法形成组合层最小主线。

## 2. 设计输入

- `docs/03-execution/16-position-minimal-ledger-and-runner-spec-freeze-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`

## 4. 任务切片

1. 冻结 `portfolio_plan` 最小三表。
2. 冻结组合层最小裁决规则。
3. 冻结 `run_portfolio_plan_build` 与脚本入口名。

## 5. 实现边界

范围内：

- `portfolio_plan` 最小桥接规格
- `17` 的执行闭环

范围外：

- `trade` 执行账本
- 多组合治理

## 6. 收口标准

1. `portfolio_plan` 的正式上游只允许是 `position` 三表。
2. `admitted / blocked / trimmed` 规则被明确冻结。
3. 本批次只建文档，不写代码。
