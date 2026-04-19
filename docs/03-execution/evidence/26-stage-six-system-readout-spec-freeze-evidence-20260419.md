# 阶段六批次 26 system 读出规格冻结证据

证据编号：`26`
日期：`2026-04-19`

## 1. 命令

```text
pytest -q tests/unit/docs
pytest -q
```

## 2. 关键结果

- `stage-six-system` 规格已冻结。
- `trade -> system` 已成为阶段六唯一正式主线。
- `run_system_from_trade`、`system_trade_readout`、`system_portfolio_trade_summary` 已在文档中形成工程准入合同。
- 阶段六明确只读取 `trade` 正式输出，不回读 `alpha / position / portfolio_plan`，不触发上游 runner。

## 3. 产物

- `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`
- `docs/03-execution/26-stage-six-system-readout-spec-freeze-conclusion-20260419.md`
