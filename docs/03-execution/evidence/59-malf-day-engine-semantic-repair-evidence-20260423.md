# 批次 59 证据：MALF day engine 纯语义修复

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/59-malf-day-engine-semantic-repair-card-20260423.md`

## 1. 代码改动范围

- `src/astock_lifespan_alpha/malf/engine.py`
- `tests/unit/malf/test_engine.py`

## 2. 单测命令

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-stage19
```

结果：

- `25 passed`

## 3. live semantic audit 复跑

执行：

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id day-a1c965e1f7a9 --sample-count 12
```

结果：

- `report_id = malf-day-semantic-audit-199020ae473f`
- `requested_run_id = day-a1c965e1f7a9`
- `effective_run_id = day-fc56ff5e5441`
- `verdict = 部分通过`

## 4. live audit 关键输出

- 7 项硬规则仍全部 `pass`
- 4 项软观察仍全部 `flag`
  - `zone_coverage = 2`
  - `reborn_median_bar_count = 1.0`
  - `single_bar_reborn_share = 0.8085`
  - `guard_churn_p90 = 0.75`

## 5. 结果解释

本轮复跑的 audit 是对现存 formal ledger 的只读审计，不会触发 `day` live build。

因此：

- 本轮已经验证新 `engine.py` 的本地单测语义
- 但 live formal ledger 仍是旧 run 物化结果
- audit 结论未变化，不代表本轮 engine 代码未落地，只代表正式账本还未用新 engine 重算

## 6. 审计产物

- JSON：`H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-199020ae473f\malf-day-semantic-audit-199020ae473f.json`
- Markdown：`H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-199020ae473f\malf-day-semantic-audit-199020ae473f.md`
- DuckDB：`H:\Lifespan-report\astock_lifespan_alpha\malf\malf-day-semantic-audit-199020ae473f\malf-day-semantic-audit-199020ae473f.duckdb`
