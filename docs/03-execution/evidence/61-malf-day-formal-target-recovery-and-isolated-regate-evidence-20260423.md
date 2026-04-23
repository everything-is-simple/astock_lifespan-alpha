# 批次 61 证据：MALF day formal target 恢复与 isolated regate

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/61-malf-day-formal-target-recovery-and-isolated-regate-card-20260423.md`

## 1. authority 与 preflight

本轮只读核实的 MALF authority 材料：

- `H:\Lifespan-Validated\malf-six\001.png` 至 `006.png`
- `H:\Lifespan-Validated\malf-six.zip`
- `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
- `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
- `H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf`

图版再次确认：

- `new-count × no-new-span × life-state` 是 MALF 核心三元
- `HH / LL = 推进`，`HL / LH = 守护`，`break = 转移`
- `WavePosition = (direction, update-rank, stagnation-rank, life-state)`

这些材料只用于冻结语义边界，不越权决定 recovery / cutover 细节。

live preflight 继续确认：

- `day-107059a919fc` 已于 `2026-04-23 21:42:27.924976` 收口为 `interrupted`
- `malf_checkpoint.last_run_id` 仍处于混合状态：
  - `day-fc56ff5e5441 = 3501`
  - `day-107059a919fc = 1875`
  - `day-02686332592b = 100`
  - `day-d696fdcd4774 = 25`
- `malf_work_queue.status = running` 仍残留：
  - `day-d696fdcd4774 = 14`
- 最近 `completed 5501/5501` 但无 materialized rows 的 run 仍包括：
  - `day-a1c965e1f7a9`
  - `day-81f3c2abdb3f`
  - `day-ac2ace0accbf`

因此 recovery baseline 继续锁定为：

- `day-fc56ff5e5441`

## 2. 本地门

执行：

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-card61
```

结果：

- `28 passed in 20.47s`

其中新增 contract 覆盖：

- `day + full_universe + --no-resume` 强制写入新的 `.building.duckdb`
- polluted target recovery 后只保留 baseline materialized rows
- baseline 解析显式跳过 `completed` 但 0 ledger rows 的 run

## 3. formal target recovery

执行：

```powershell
python scripts/malf/recover_malf_day_formal_target.py --baseline-run-id day-fc56ff5e5441
```

结果摘要：

- `runner_name = recover_malf_day_formal_target`
- `status = completed`
- `resolved_baseline.run_id = day-fc56ff5e5441`
- `resolved_baseline.queue_rows = 5501`
- `resolved_baseline.pivot_rows = 11151643`
- `resolved_baseline.wave_rows = 3300393`
- `resolved_baseline.state_snapshot_rows = 8138049`
- `resolved_baseline.wave_scale_snapshot_rows = 8138049`
- `resolved_baseline.wave_scale_profile_rows = 3300393`
- `quarantine_path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.quarantine-2892d82b7c0d.duckdb`
- `recovered_running_run_count = 0`
- `recovered_running_queue_count = 0`

recovery 后 canonical target 只读核实：

- `malf_run` 只剩 `day-fc56ff5e5441`
- `malf_checkpoint.last_run_id = day-fc56ff5e5441`：`3501`
- `malf_work_queue.status` 只剩：
  - `completed = 3501`
  - `skipped = 2000`
- `malf_state_snapshot(run_id = day-fc56ff5e5441) = 8138049`

目录状态：

- `malf_day.duckdb`
- `malf_day.quarantine-2892d82b7c0d.duckdb`
- 历史 backup 保留

## 4. isolated rebuild

执行：

```powershell
python scripts/malf/run_malf_day_build.py --no-resume --progress-path H:\Lifespan-report\astock_lifespan_alpha\malf\card61-malf-day-formal-target-recovery-and-isolated-regate-progress.json
```

结果摘要：

- `run_id = day-e687a8277f61`
- `status = completed`
- `message = MALF day full-universe build completed and building database is ready for promotion.`
- `segment_summary.resume = false`
- `segment_summary.full_universe = true`
- `artifact_summary.active_build_path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-e687a8277f61.building.duckdb`
- `artifact_summary.promoted_to_target = true`
- `symbols_seen = 5501`
- `symbols_completed = 5501`
- `symbols_updated = 5501`
- `latest_bar_dt = 2026-04-10T00:00:00`
- `elapsed_seconds = 3449.151315`
- `insert_ledgers_seconds = 1166.497280`
- `checkpoint_seconds = 35.727946`
- `queue_update_seconds = 19.889686`

materialization：

- `pivot_rows = 7797482`
- `wave_rows = 2448048`
- `state_snapshot_rows = 16348113`
- `wave_scale_snapshot_rows = 16348113`
- `wave_scale_profile_rows = 2448048`

postflight 只读核实：

- `malf_run` 最新正式 run = `day-e687a8277f61`
- `status = completed`
- `symbols_completed = 5501 / 5501`
- `malf_checkpoint.last_run_id = day-e687a8277f61`：`5501`
- `malf_work_queue.status = completed`：`5501`
- `malf_state_snapshot(run_id = day-e687a8277f61) = 16348113`
- `malf_day.backup-day-e687a8277f61.duckdb` 已留下 cutover 前 canonical target

这证明本轮正式走通的是：

- `.building.duckdb -> backup old target -> promote new target`

而不是 Card 60 的 target 直写路径。

## 5. forced audit

执行：

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id day-e687a8277f61 --sample-count 12
```

结果摘要：

- `target_run_id = day-e687a8277f61`
- 本轮未 fallback
- `target_snapshot_rows = 16348113`
- `target_wave_rows = 2448048`
- `running_queue_count = 0`
- `stale_run_summaries = []`
- `verdict = 部分通过`

7 项硬规则：

- `new_count_transition_rule = pass`
- `no_new_span_transition_rule = pass`
- `wave_id_break_rule = pass`
- `new_wave_reborn_rule = pass`
- `reborn_to_alive_rule = pass`
- `guard_update_pivot_rule = pass`
- `zone_classification_rule = pass`

软观察：

- `zone_coverage = flag (3)`
- `reborn_median_bar_count = ok (2.0)`
- `single_bar_reborn_share = ok (0.3119)`
- `guard_churn_p90 = ok (0.25)`

## 6. 本轮正式证据结论

- Card 60 的 formal target 污染已被 recovery/quarantine 正式收口
- `day + full_universe + --no-resume` 已重新回到 isolated staging build 正式路径
- Stage 19 新 formal run 已成功生成并被 forced audit 直接命中
- 当前 `MALF day` 的剩余问题不再是 recovery/build 基础设施，而是 `zone_coverage = 3` 的唯一软观察残留
