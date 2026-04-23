# 批次 58 记录：MALF day 纯语义重验 gate

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/58-malf-day-semantic-revalidation-gate-card-20260423.md`

## 1. 代码变更

- 新增 `src/astock_lifespan_alpha/malf/audit.py`
- 新增 `scripts/malf/audit_malf_day_semantics.py`
- 新增 `tests/unit/malf/test_audit.py`
- 更新 `src/astock_lifespan_alpha/malf/__init__.py`

## 2. 测试

执行：

```powershell
pytest tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-audit
pytest tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-stage18
```

结果：

- `tests/unit/malf/test_audit.py`：`2 passed`
- `tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py`：`20 passed`

## 3. live 审计

执行：

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id day-a1c965e1f7a9 --sample-count 12
```

结果：

- `report_id = malf-day-semantic-audit-b7ea18c043f9`
- `requested_run_id = day-a1c965e1f7a9`
- `effective_run_id = day-fc56ff5e5441`
- `verdict = 部分通过`

## 4. 本轮正式登记

- 区分了“最新 completed run”与“最新已物化 completed run”
- `running` run 与 queue 只做 stale bookkeeping，不参与语义评分
- 四张标准导出表统一落到审计 DuckDB，而不是散落 CSV
- 固定输出 `12` 张样本图，避免手挑样本

## 5. 本轮未实现事项

本轮明确未做：

- 不改 `engine.py`
- 不改 `schema.py`
- 不改 `runner.py`
- 不开 `week / month`
- 不回引 `execution_interface / structure / filter`
- 不把 `runner / queue / build` 工程问题混入语义结论
