# 阶段三批次 11 Alpha 契约与 Schema 证据

证据编号：`11`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python scripts\alpha\run_alpha_bof_build.py
.\.venv\Scripts\python scripts\alpha\run_alpha_signal_build.py
```

## 2. 关键结果

- 六个 alpha runner 已升级为正式摘要接口。
- 五个 trigger 数据库与 `alpha_signal` 数据库都能幂等初始化。

## 3. 产物

- `src/astock_lifespan_alpha/alpha/contracts.py`
- `src/astock_lifespan_alpha/alpha/schema.py`
- `src/astock_lifespan_alpha/alpha/runner.py`
