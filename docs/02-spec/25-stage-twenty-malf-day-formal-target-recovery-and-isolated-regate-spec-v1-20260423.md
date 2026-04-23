# 阶段二十 MALF day formal target 恢复与 isolated regate 规格 v1

- 日期：`2026-04-23`
- 状态：`冻结`
- 文档 ID：`stage-twenty-malf-day-formal-target-recovery-and-isolated-regate`

## 1. 目标

本轮正式工作定义为：先把被 `day-107059a919fc` 污染的 `malf_day.duckdb` 从 canonical formal target 降级为待隔离证物，再在干净 baseline 上完成一次受控 `day` full-universe rebuild，并用新 formal run 直接重跑 forced audit。

本轮要回答的唯一问题是：

`在不改 MALF 纯语义 public contract 的前提下，是否能把 day formal rebuild 恢复为“双库隔离 + 原子 cutover”的正式工程路径。`

## 2. authority stack

### 2.1 语义 authority

本轮语义 authority 固定为：

1. 图版与正式图版 PDF：
   - `H:\Lifespan-Validated\malf-six\001.png` 至 `006.png`
   - `H:\Lifespan-Validated\malf-six.zip`
   - `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
   - `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
   - `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
   - `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
2. 仓库冻结文本规格：
   - `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
   - `docs/02-spec/23-stage-eighteen-malf-day-semantic-revalidation-spec-v1-20260423.md`
   - `docs/02-spec/24-stage-nineteen-malf-day-engine-semantic-repair-spec-v1-20260423.md`
3. 补充解释材料：
   - `H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf`

补充材料只用于术语对照和歧义解释，不得覆盖图版主定义。

### 2.2 工程 authority

本轮工程 authority 固定为：

- `src/astock_lifespan_alpha/malf/runner.py`
- `src/astock_lifespan_alpha/malf/recover.py`
- `scripts/malf/recover_malf_day_formal_target.py`
- `tests/unit/malf/test_runner.py`

图版 authority 只约束语义边界，不越权决定 baseline run 选取规则、DuckDB 双库恢复流程、quarantine/cutover 实施细节。

## 3. 本轮边界

本轮只允许修改：

- `day` 周期 `runner` 的 full-universe `--no-resume` 选路
- `formal target recovery` 入口与 CLI
- 对应单测、文档和索引

本轮明确排除：

- `engine.py`
- `audit.py` 7 条硬规则定义
- `week / month`
- 下游 `alpha / position / portfolio_plan / trade / system / pipeline`
- 因恢复/重建而顺手扩展新的语义设计

## 4. 正式工程合同

### 4.1 isolated rebuild

当满足以下条件时：

- `timeframe = day`
- `full_universe = true`
- `resume = false`

正式合同冻结为：

- runner 必须强制写入新的 `.building.duckdb`
- canonical `malf_day.duckdb` 在 promotion 前不得写入 `malf_run / malf_work_queue / malf_checkpoint / malf_*`
- rebuild 成功后只允许通过 rename-based cutover 替换 canonical target
- 失败时只允许留下 staging / quarantine 证物，不允许再次把 canonical target 写脏

### 4.2 formal target recovery

正式 recovery 入口冻结为：

```powershell
python scripts/malf/recover_malf_day_formal_target.py --baseline-run-id day-fc56ff5e5441
```

默认 baseline 规则冻结为：

- 只接受 `completed` 且存在 materialized `malf_state_snapshot` rows 的 `day` run
- 显式跳过 `completed` 但 0 ledger rows 的 run
- 当前现场下默认 baseline 为 `day-fc56ff5e5441`

正式恢复动作冻结为：

1. 只读解析污染 target 中的 materialized baseline
2. 基于 baseline run 重建新的 recovery DB
3. 复制 baseline `malf_run`、baseline 非 `running` queue、五张 `malf_*` materialized 表
4. 从 baseline `malf_state_snapshot` 重建 `malf_checkpoint`
5. 确保 recovered DB 中 `running run / running queue = 0`
6. 将旧 canonical target rename 为 quarantine
7. 用 recovery DB 原子替换 canonical `malf_day.duckdb`

## 5. 执行命令

本轮执行顺序冻结为：

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-card61
python scripts/malf/recover_malf_day_formal_target.py --baseline-run-id day-fc56ff5e5441
python scripts/malf/run_malf_day_build.py --no-resume --progress-path H:\Lifespan-report\astock_lifespan_alpha\malf\card61-malf-day-formal-target-recovery-and-isolated-regate-progress.json
python scripts/malf/audit_malf_day_semantics.py --run-id <new_formal_run_id> --sample-count 12
```

## 6. 验收

本轮工程验收通过条件：

- MALF 本地门通过
- recovery 成功完成并形成 quarantine
- 新的 `day` full-universe rebuild 走 `.building.duckdb`
- 新 run `completed`
- `promoted_to_target = true`
- canonical target 只保留新 formal run 的 `last_run_id`
- forced audit 直接命中新 formal run，不 fallback
- 7 项硬规则继续全 `pass`

若 forced audit 仍存在软观察 flag：

- 必须如实登记为新的语义入口
- 但不得把 recovery / isolated rebuild 工程合同误判为失败

## 7. 冻结结论

本文冻结以下结论：

1. Card 60 暴露的问题是 formal target 恢复与 rebuild 工程路径，不是再次重开 MALF 纯语义设计。
2. `day + full_universe + --no-resume` 的正式合同是 isolated staging build，而不是直写 canonical target。
3. baseline run 只从 materialized completed runs 中选取，`completed` 但 0 ledger rows 的 run 不得作为恢复基线。
4. quarantine 是正式证物，不得在本轮删除。
5. Stage 20 只解决恢复与重建工程口径；若 forced audit 仍留软 flag，下一张卡只允许围绕该软 flag 继续。
