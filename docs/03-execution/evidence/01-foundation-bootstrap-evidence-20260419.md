# 阶段一基础重构证据

证据编号：`01`
日期：`2026-04-19`

## 1. 命令

```text
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe scripts\malf\run_malf_day_build.py
.\.venv\Scripts\python.exe scripts\alpha\run_alpha_signal_build.py
.\.venv\Scripts\python.exe scripts\position\run_position_from_alpha_signal.py
git log --oneline -n 12
```

## 2. 关键结果

- `pytest` 共收集 3 个测试，全部通过。
- `tests/unit/core/test_paths.py` 证明五根目录与新命名空间路径契约可解析。
- `tests/unit/contracts/test_module_boundaries.py` 证明源码与脚本中不存在 `astock_lifespan_alpha.structure` 与 `astock_lifespan_alpha.filter` 引用。
- `tests/unit/contracts/test_runner_contracts.py` 证明 `malf / alpha / position` 的 runner 名称、`status=stub` 与 `phase=foundation_bootstrap` 已冻结。
- `scripts/malf/run_malf_day_build.py` 输出目标路径：`H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
- `scripts/alpha/run_alpha_signal_build.py` 输出目标路径：`H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`
- `scripts/position/run_position_from_alpha_signal.py` 输出目标路径：`H:\Lifespan-data\astock_lifespan_alpha\position\position.duckdb`
- 当前仓库历史只有 1 个提交：`71e0d15 Bootstrap reconstructed repository foundation`

## 3. 产物

- `src/astock_lifespan_alpha/core/paths.py`
- `src/astock_lifespan_alpha/core/contracts.py`
- `src/astock_lifespan_alpha/malf/runner.py`
- `src/astock_lifespan_alpha/alpha/runner.py`
- `src/astock_lifespan_alpha/position/runner.py`
- `tests/unit/core/test_paths.py`
- `tests/unit/contracts/test_module_boundaries.py`
- `tests/unit/contracts/test_runner_contracts.py`
