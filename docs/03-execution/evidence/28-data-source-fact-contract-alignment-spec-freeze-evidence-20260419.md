# 阶段七批次 28 data 源事实契约规格冻结证据

证据编号：`28`
日期：`2026-04-19`

## 1. 命令

```text
pytest -q tests/unit/docs
pytest -q
```

## 2. 关键结果

- `stage-seven-data-source-contract` 已冻结。
- 6 个本地 DuckDB source fact 路径已登记。
- 首版范围固定为只读 stock。
- `stock_daily_adjusted / stock_weekly_adjusted / stock_monthly_adjusted` 已成为真实表名合同。
- `code -> symbol` 与 `trade_date -> bar_dt` 已成为字段映射合同。

## 3. 产物

- `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`
- `docs/03-execution/28-data-source-fact-contract-alignment-spec-freeze-conclusion-20260419.md`

