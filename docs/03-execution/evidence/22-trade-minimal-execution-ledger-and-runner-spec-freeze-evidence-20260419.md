# 阶段五批次 22 trade 最小执行账本与 runner 规格冻结证据

证据编号：`22`
日期：`2026-04-19`

## 1. 命令

```text
参考旧版 trade 设计与规格输入
创建 docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md
```

## 2. 关键结果

- `trade` 首版正式表族、runner 合同与最小回报口径已冻结。
- `analysis_price_line / execution_price_line` 价格分线已写入正式规格。

## 3. 产物

- `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`
- `docs/03-execution/22-trade-minimal-execution-ledger-and-runner-spec-freeze-conclusion-20260419.md`
Implementation freeze evidence: the trade spec now records `PathConfig.source_databases.market_base`, `planned_trade_date`, `execution_trade_date`, `execution_price`, 次日开盘执行, reserved `accepted`, and `portfolio_id + symbol`.
