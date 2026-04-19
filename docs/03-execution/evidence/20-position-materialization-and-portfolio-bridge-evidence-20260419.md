# 阶段四批次 20 position 物化与最小 portfolio_plan bridge 证据

证据编号：`20`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python scripts\position\run_position_from_alpha_signal.py
.\.venv\Scripts\python scripts\portfolio_plan\run_portfolio_plan_build.py
```

## 2. 关键结果

- `position` 三层正式输出已经物化。
- `portfolio_plan` 三表与最小 `admitted / blocked / trimmed` 桥接已跑通。
- 在隔离测试工作区中，`run_portfolio_plan_build` 可稳定完成；直接读取共享 `H:\Lifespan-data` 时，若外部进程占用 `position.duckdb`，会出现 DuckDB 文件锁限制。

## 3. 产物

- `src/astock_lifespan_alpha/position/engine.py`
- `src/astock_lifespan_alpha/position/source.py`
- `src/astock_lifespan_alpha/portfolio_plan/runner.py`
