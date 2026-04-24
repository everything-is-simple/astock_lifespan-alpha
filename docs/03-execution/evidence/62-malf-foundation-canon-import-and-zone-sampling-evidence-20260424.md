# 批次 62 证据：MALF 地基 Canon、包入口修复与 zone sampling 收口

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/62-malf-foundation-canon-import-and-zone-sampling-card-20260424.md`

## 1. 工作区纠偏

本轮正式工作区固定为五根目录：

- `H:\astock_lifespan-alpha`
- `H:\Lifespan-data`
- `H:\Lifespan-report`
- `H:\Lifespan-Validated`
- `H:\Lifespan-temp`

Python 固定为：

- `D:\miniconda\py310\python.exe`

不再把 `C:\Users\Administrator\.codex\worktrees\03ef\astock_lifespan-alpha` 作为正式项目根目录。

## 2. 代码证据

本轮最小修复：

- `src/astock_lifespan_alpha/malf/__init__.py`
  - 移除 `malf.audit` 顶层导入
  - public package 只保留 runner、contracts、repair、recover 等轻量导出
- `pyproject.toml`
  - `dev` extra 补 `matplotlib>=3.8.0`
  - `pandas>=2.2.0` 继续作为 audit/dev 依赖
- `src/astock_lifespan_alpha/malf/audit.py`
  - `_select_sample_windows()` 先选择四区 coverage window
  - 再用原有方向与 transition window 填充剩余 sample

新增契约测试：

- `tests/unit/malf/test_public_imports.py`
- `tests/unit/malf/test_audit_sampling.py`
- `tests/unit/docs/test_malf_specs.py`

新增正式规格：

- `docs/02-spec/26-malf-foundation-canon-v1-20260424.md`

## 3. 验证结果

环境验证：

```powershell
D:\miniconda\py310\python.exe -m pip list
```

关键包：

- `duckdb 1.5.2`
- `pyarrow 24.0.0`
- `pytest 9.0.3`
- `pandas 2.3.3`
- `matplotlib 3.10.9`

新增契约门：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/malf/test_public_imports.py tests/unit/malf/test_audit_sampling.py tests/unit/docs/test_malf_specs.py -q
```

结果：

- `6 passed in 2.66s`

MALF 单测：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/malf -q
```

结果：

- `30 passed in 33.75s`

文档契约：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_malf_specs.py -q
```

结果：

- `4 passed in 0.05s`

模块边界：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```

结果：

- `4 passed in 0.11s`

## 4. forced audit 结果

执行：

```powershell
$env:LIFESPAN_REPO_ROOT='H:\astock_lifespan-alpha'
$env:LIFESPAN_DATA_ROOT='H:\Lifespan-data'
$env:LIFESPAN_REPORT_ROOT='H:\Lifespan-report'
$env:LIFESPAN_TEMP_ROOT='H:\Lifespan-temp'
$env:LIFESPAN_VALIDATED_ROOT='H:\Lifespan-Validated'
D:\miniconda\py310\python.exe scripts/malf/audit_malf_day_semantics.py --run-id day-e687a8277f61 --sample-count 12
```

报告：

- `report_id = malf-day-semantic-audit-ad35dcbbae62`
- `target_run_id = day-e687a8277f61`
- `generated_at = 2026-04-24 16:30:47`
- `summary_json_path = H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-ad35dcbbae62\malf-day-semantic-audit-ad35dcbbae62.json`
- `artifact_database_path = H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-ad35dcbbae62\malf-day-semantic-audit-ad35dcbbae62.duckdb`

目标 run：

- `target_symbol_completed = 5501 / 5501`
- `target_snapshot_rows = 16348113`
- `target_wave_rows = 2448048`
- `running_queue_count = 0`

7 项硬规则：

- `new_count_transition_rule = pass`
- `no_new_span_transition_rule = pass`
- `wave_id_break_rule = pass`
- `new_wave_reborn_rule = pass`
- `reborn_to_alive_rule = pass`
- `guard_update_pivot_rule = pass`
- `zone_classification_rule = pass`

软观察：

- `zone_coverage = ok (4)`
- `reborn_median_bar_count = ok (2)`
- `single_bar_reborn_share = ok (0.3119)`
- `guard_churn_p90 = ok (0.25)`

sample 前四个窗口已固定为四区覆盖：

- `zone_coverage_early_progress`
- `zone_coverage_mature_progress`
- `zone_coverage_mature_stagnation`
- `zone_coverage_weak_stagnation`

最终裁决：

- `verdict = 通过`
