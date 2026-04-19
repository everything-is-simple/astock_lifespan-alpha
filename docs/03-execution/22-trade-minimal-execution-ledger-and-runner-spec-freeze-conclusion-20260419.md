# 阶段五批次 22 trade 最小执行账本与 runner 规格冻结结论

结论编号：`22`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段五 `trade` 最小执行账本、runner 合同与最小回报口径已经冻结。
- 拒绝：在阶段五首版重新引入 `carry / exit / pnl / system` 扩展范围。

## 2. 原因

- `trade` 首版正式表族与正式入口已明确。
- `analysis_price_line / execution_price_line` 的价格分线已正式写入规格。

## 3. 影响

- 阶段五后续实现必须以最小执行账本为准，不得回退到旧版 `trade_runtime` 范围。
Implementation freeze conclusion: accepted stage-five implementation defaults are `PathConfig.source_databases.market_base`, 次日开盘执行, `filled / rejected` materialization, reserved `accepted`, and `portfolio_id + symbol` replay.
