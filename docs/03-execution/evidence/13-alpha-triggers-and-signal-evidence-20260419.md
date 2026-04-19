# 阶段三批次 13 Alpha 五触发器与 alpha_signal 证据

证据编号：`13`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
```

## 2. 关键结果

- 五个 trigger 都能独立运行，并产出 `alpha_trigger_event / alpha_trigger_profile`。
- `alpha_signal` 已能汇总五类 trigger 输出，并保留 `MALF` 波段位置字段。

## 3. 产物

- `src/astock_lifespan_alpha/alpha/`
- `tests/unit/alpha/`
