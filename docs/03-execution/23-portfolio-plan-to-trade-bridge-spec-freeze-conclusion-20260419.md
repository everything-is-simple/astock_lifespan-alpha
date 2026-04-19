# 阶段五批次 23 portfolio_plan -> trade 桥接规格冻结结论

结论编号：`23`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段五 `portfolio_plan -> trade` 的唯一正式桥接已经冻结。
- 拒绝：让 `trade` 直接回读 `alpha / position` 内部过程，或让阶段四参考价格反向定义正式执行价格口径。

## 2. 原因

- `portfolio_plan_snapshot` 的最小输入字段已明确。
- 阶段四 `reference_trade_date / reference_price` 的勘误已写清。

## 3. 影响

- 阶段五实现只能消费 `portfolio_plan_snapshot` 与正式执行价格输入，不得重新打开上游边界。
