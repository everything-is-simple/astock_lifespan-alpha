# 阶段七批次 28 data 源事实契约规格冻结结论

结论编号：`28`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段七 data 源事实契约已冻结。
- 接受：阶段七首版只读 stock。
- 拒绝：在 source fact 契约未对齐前启动 `data -> system` 全线编排。

## 2. 原因

- 当前真实本地 source fact 库分为日线、周线、月线 6 个 DuckDB。
- 真实 stock 表名为 `stock_daily_adjusted / stock_weekly_adjusted / stock_monthly_adjusted`。
- 真实字段使用 `code` 和 `trade_date`，必须映射为正式系统内部的 `symbol` 与 `bar_dt / signal_date / execution_trade_date`。

## 3. 影响

- 阶段七从本批次之后才允许进入 source adapter 工程实现。
- 工程实现必须保留旧表名 fallback。
- 阶段八 `data -> system` 编排必须等阶段七完成后再规划。

