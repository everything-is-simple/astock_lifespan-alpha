# 阶段五批次 22 trade 最小执行账本与 runner 规格冻结执行卡

卡片编号：`22`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段四收口后，`trade` 仍无正式最小执行账本与 runner 合同，阶段五实现缺少正式准入面。
- 目标：冻结 `trade` 最小正式表族、最小回报、价格口径分线与正式入口。
- 为什么现在做：没有规格冻结，后续 `trade` 实施会重新把阶段范围、价格口径和回报口径拉回讨论状态。

## 2. 设计输入

- `H:\lifespan-0.01\docs\01-design\modules\trade\00-trade-module-lessons-20260409.md`
- `H:\lifespan-0.01\docs\01-design\modules\trade\01-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-charter-20260409.md`

## 3. 规格输入

- `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`
- `H:\lifespan-0.01\docs\02-spec\modules\trade\01-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-spec-20260409.md`

## 4. 任务切片

1. 冻结 `trade` 首版正式表族与 runner 合同。
2. 冻结 `accepted / rejected / filled` 最小回报口径。
3. 冻结 `analysis_price_line / execution_price_line` 的价格分线。

## 5. 实现边界

范围内：

- `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`

范围外：

- `trade` 代码实现
- `carry / exit / pnl`
- `system`

## 6. 收口标准

1. `trade` 最小正式表族被明确写入规格。
2. 正式 Python 与脚本入口被明确冻结。
3. 价格分线与最小市场规则校验被明确写入规格。
Implementation freeze addendum: stage-five engineering uses `PathConfig.source_databases.market_base` as the physical backing for `execution_price_line`, uses 次日开盘执行 for `planned_trade_date / execution_trade_date / execution_price`, keeps `accepted` reserved but not materialized, and replays by `portfolio_id + symbol`.
