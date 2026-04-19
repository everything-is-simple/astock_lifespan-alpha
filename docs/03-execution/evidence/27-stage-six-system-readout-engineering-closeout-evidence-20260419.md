# 阶段六批次 27 system 读出工程收口证据

证据编号：`27`
日期：`2026-04-19`

## 1. 命令

```text
pytest -q tests/unit/system
pytest -q tests/unit/contracts
pytest -q tests/unit/docs
pytest -q
```

## 2. 关键结果

- `run_system_from_trade` 已实现并导出。
- `system_run / system_trade_readout / system_portfolio_trade_summary` 已落地。
- `system` 只读取 `trade` 正式输出。
- `system` 不回读 `alpha / position / portfolio_plan`，不触发上游 runner。
- 阶段六完成结论已登记。

## 3. 产物

- `src/astock_lifespan_alpha/system/contracts.py`
- `src/astock_lifespan_alpha/system/schema.py`
- `src/astock_lifespan_alpha/system/source.py`
- `src/astock_lifespan_alpha/system/runner.py`
- `scripts/system/run_system_from_trade.py`
- `tests/unit/system/test_system_runner.py`
- `docs/03-execution/27-stage-six-system-readout-engineering-closeout-conclusion-20260419.md`
