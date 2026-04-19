# 阶段四批次 19 position 契约、Schema 与 runner 证据

证据编号：`19`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python scripts\position\run_position_from_alpha_signal.py
```

## 2. 关键结果

- `run_position_from_alpha_signal` 已返回正式摘要。
- `position` 六张正式表可幂等初始化。

## 3. 产物

- `src/astock_lifespan_alpha/position/contracts.py`
- `src/astock_lifespan_alpha/position/schema.py`
- `src/astock_lifespan_alpha/position/runner.py`
