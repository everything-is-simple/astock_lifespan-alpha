# 批次 60 卡片：MALF day live formal rebuild 与 Stage 19 重验收

- 日期：`2026-04-23`
- 对应规格：`docs/02-spec/24-stage-nineteen-malf-day-engine-semantic-repair-spec-v1-20260423.md`

## 1. 目标

在不改 `engine / runner / schema / audit threshold` 的前提下，使用现有 `day` CLI 完成一次 full-universe live formal rebuild，并据此重验 `Stage 19` 是否真正推动 `MALF day` live formal gate 前进。

## 2. 本卡边界

本卡允许：

- 复跑 MALF 相关既有本地门
- 执行 `python scripts/malf/run_malf_day_build.py --no-resume`
- 对 live formal DB 做只读 preflight / postflight 核验
- 在 run 明确 stale / 无法继续时做正式 bookkeeping 治理
- 补齐 `card / evidence / record / conclusion`

本卡明确排除：

- 修改 `engine.py / runner.py / schema.py / audit.py`
- 修改 `week / month`
- 修改 `pipeline` 或任何下游模块
- 因 rebuild 失败而顺手混入新的代码修补

## 3. 执行命令

本卡执行顺序：

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-stage20
python scripts/malf/run_malf_day_build.py --no-resume --progress-path H:\Lifespan-report\astock_lifespan_alpha\malf\card60-malf-day-live-formal-regate-progress.json
```

若 live rebuild 无法完成：

- 不继续强行复跑 audit
- 不顺手修代码
- 只把 stale run 与 queue 收为正式 `interrupted`
- 将 formal target 污染范围写入证据并以 blocker 收口

## 4. 验收

通过条件：

- live rebuild 形成新的 `completed` full-universe run
- `promoted_to_target = true`
- 新 run 在 `malf_state_snapshot` 中有正式 rows
- forced audit 可以直接命中新 run，且不 fallback
- 7 项硬规则继续全 `pass`

本卡 blocker 收口条件：

- rebuild 停在 stale `running`
- target 直写路径长期无推进
- 无新的可接受 `completed` run 可供 forced audit
- formal target 出现 interrupted run 局部写入，需要下一张卡单独恢复
