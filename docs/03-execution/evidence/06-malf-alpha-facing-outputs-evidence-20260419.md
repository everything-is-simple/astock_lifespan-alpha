# 阶段二批次 06 MALF 面向 Alpha 输出证据

证据编号：`06`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
```

## 2. 关键结果

- `tests/unit/malf/test_runner.py::test_malf_day_runner_materializes_semantic_outputs` 通过，证明 `malf_wave_scale_snapshot` 已输出 `direction / new_count / no_new_span / life_state / wave_position_zone`。
- `tests/unit/malf/test_runner.py` 证明 `update_rank / stagnation_rank` 与四区 `wave_position_zone` 已稳定写入 snapshot/profile。
- `alpha` 仍未改动公开接口，阶段二只提供读模型，不提前实施 `alpha_signal`。

## 3. 产物

- `src/astock_lifespan_alpha/malf/engine.py`
- `src/astock_lifespan_alpha/malf/schema.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `tests/unit/malf/test_runner.py`
