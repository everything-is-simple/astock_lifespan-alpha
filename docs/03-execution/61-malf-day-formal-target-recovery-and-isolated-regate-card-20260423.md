# 批次 61 卡片：MALF day formal target 恢复与 isolated regate

- 日期：`2026-04-23`
- 对应规格：`docs/02-spec/25-stage-twenty-malf-day-formal-target-recovery-and-isolated-regate-spec-v1-20260423.md`

## 1. 目标

恢复被 `day-107059a919fc` 污染的 `malf_day.duckdb`，把 `day` full-universe `--no-resume` rebuild 收敛为新的 `.building.duckdb -> promote` 正式路径，并直接用新 formal run 重跑 `audit_malf_day_semantics`。

## 2. 本卡边界

本卡允许：

- 修改 `runner.py` 的 `day + full_universe + --no-resume` 选路
- 新增 formal target recovery 模块与 CLI
- 新增/调整对应单测
- 只读核实 MALF authority 材料
- 执行受控 recovery / rebuild / audit
- 补齐 `card / evidence / record / conclusion`

本卡明确排除：

- 修改 `engine.py`
- 修改 `audit.py` 7 条硬规则定义
- 修改 `week / month`
- 修改下游模块或 `pipeline`
- 因 rebuild 结果顺手重开 MALF 纯语义设计

## 3. 执行命令

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-card61
python scripts/malf/recover_malf_day_formal_target.py --baseline-run-id day-fc56ff5e5441
python scripts/malf/run_malf_day_build.py --no-resume --progress-path H:\Lifespan-report\astock_lifespan_alpha\malf\card61-malf-day-formal-target-recovery-and-isolated-regate-progress.json
python scripts/malf/audit_malf_day_semantics.py --run-id day-e687a8277f61 --sample-count 12
```

## 4. 验收

本卡工程验收通过条件：

- MALF 单测通过
- canonical target 成功恢复为干净 baseline
- polluted target 被隔离为 quarantine
- 新 `day` rebuild 走 `.building.duckdb`
- 新 run `completed`
- `promoted_to_target = true`
- `malf_checkpoint.last_run_id` 全量切到新 run
- forced audit 直接命中新 run，不 fallback

本卡语义登记条件：

- 若 7 项硬规则全 `pass`，则记录 Stage 19 新 formal run 已被正式审计
- 若仍有软观察 flag，必须如实登记为下一张卡的唯一入口
