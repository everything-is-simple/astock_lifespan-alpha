# 阶段五批次 23 portfolio_plan -> trade 桥接规格冻结证据

证据编号：`23`
日期：`2026-04-19`

## 1. 命令

```text
参考阶段四 portfolio_plan 规格与旧版 trade bridge 设计
创建 docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md
```

## 2. 关键结果

- `trade` 的唯一正式上游已冻结为 `portfolio_plan_snapshot`。
- 阶段四 `reference_trade_date / reference_price` 只是桥接参考的勘误已写入。

## 3. 产物

- `docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md`
- `docs/03-execution/23-portfolio-plan-to-trade-bridge-spec-freeze-conclusion-20260419.md`
Implementation freeze evidence: the bridge spec now records that `portfolio_plan_snapshot` is converted through an `execution_price_line` adapter backed by `PathConfig.source_databases.market_base`, with valid rows materialized as `filled`.
