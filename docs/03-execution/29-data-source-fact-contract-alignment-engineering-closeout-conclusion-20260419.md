# 阶段七批次 29 data 源事实契约工程收口结论

结论编号：`29`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段七 data 源事实契约对齐工程实现已完成。
- 接受：阶段七首版只读 stock。
- 拒绝：把本批次解释为 `data -> system` 全线编排已经完成。

## 2. 原因

- `SourceFactDatabasePaths` 已登记 6 个本地 source fact DuckDB。
- `malf.source` 已支持 day/week/month 对应真实 stock adjusted 表。
- `alpha.source / position.source / trade.source` 已支持 `stock_daily_adjusted`。
- `code -> symbol`、`trade_date -> bar_dt / signal_date / execution_trade_date` 已进入测试覆盖。
- 旧测试表名 fallback 保持通过。

## 3. 影响

- 阶段七完成。
- 当前仓库已具备后续 `data -> system` 最小全链路编排的 source fact 前置条件。
- 阶段八正式入口为：

```text
data -> system 最小 pipeline orchestration
```

