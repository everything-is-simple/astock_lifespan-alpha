# 阶段五批次 23 portfolio_plan -> trade 桥接规格冻结执行卡

卡片编号：`23`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段五必须先冻结 `portfolio_plan -> trade` 的最小桥接方向、输入字段、桥接规则与禁止事项。
- 目标：明确 `trade` 只能消费 `portfolio_plan_snapshot`，并把阶段四 `reference_*` 字段的口径勘误写清。
- 为什么现在做：如果桥接规则不先冻结，后续实现会重新回读 `position` 或把阶段四参考价格误当成正式执行价格口径。

## 2. 设计输入

- `H:\lifespan-0.01\docs\01-design\modules\trade\01-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-charter-20260409.md`

## 3. 规格输入

- `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`
- `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`
- `H:\lifespan-0.01\docs\02-spec\modules\trade\01-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-spec-20260409.md`

## 4. 任务切片

1. 冻结 `portfolio_plan_snapshot` 到 `trade` 的唯一正式桥接。
2. 冻结最小输入字段与最小桥接规则。
3. 写入阶段四 `reference_trade_date / reference_price` 的勘误。

## 5. 实现边界

范围内：

- `docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md`

范围外：

- `trade` 代码实现
- `system`
- `carry / exit`

## 6. 收口标准

1. `trade` 的唯一正式上游被固定为 `portfolio_plan_snapshot`。
2. `blocked / admitted / trimmed` 的桥接行为被明确冻结。
3. 阶段四参考价格口径与阶段五正式执行价格口径被明确切开。
