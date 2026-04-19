# 阶段二批次 05 MALF 语义引擎与 Runner 证据

证据编号：`05`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python -m pytest -q
```

## 2. 关键结果

- `tests/unit/malf/test_runner.py::test_malf_day_runner_materializes_semantic_outputs` 通过，覆盖 `HH / LL` 计数、`HL / LH` 不计数、break、reborn 与四区输出。
- `tests/unit/malf/test_runner.py::test_malf_runner_checkpoint_skips_unchanged_source` 通过，证明同一周期重跑不会重复物化未变化快照。
- `tests/unit/malf/test_runner.py::test_malf_runner_initializes_formal_schema` 通过，证明 `day / week / month` 三周期共用同一语义规则但数据库隔离。

## 3. 产物

- `src/astock_lifespan_alpha/malf/engine.py`
- `src/astock_lifespan_alpha/malf/source.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `tests/unit/malf/test_runner.py`
