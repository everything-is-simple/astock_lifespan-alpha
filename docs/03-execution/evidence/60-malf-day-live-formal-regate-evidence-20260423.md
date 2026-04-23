# 批次 60 证据：MALF day live formal rebuild 与 Stage 19 重验收

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/60-malf-day-live-formal-regate-card-20260423.md`

## 1. preflight 基线

formal target：

- `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`

preflight 目录状态：

- 仅见 `malf_day.duckdb` 与历史 backup
- 未发现新的 `.building.duckdb`

Card 59 之后的候选 `completed` run：

- `day-81f3c2abdb3f`
- `day-a1c965e1f7a9`
- `day-ac2ace0accbf`

上述 3 个 run 均满足：

- `symbols_total = 5501`
- `symbols_completed = 5501`
- `inserted_pivots / inserted_waves / inserted_state_snapshots / inserted_wave_scale_snapshots / inserted_wave_scale_profiles = 0`
- 在 `malf_state_snapshot` 中无 rows

当前最新已物化 `completed` run：

- `day-fc56ff5e5441`
- `malf_state_snapshot = 8138049`

preflight stale bookkeeping：

- `malf_run.status = running`
  - `day-d696fdcd4774`
  - `day-3343b24d0f0b`
- `malf_work_queue.status = running`
  - `day-d696fdcd4774:* = 14`

## 2. 本地门

执行：

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-stage20
```

结果：

- `25 passed in 54.99s`

## 3. live rebuild 触发

执行：

```powershell
python scripts/malf/run_malf_day_build.py --no-resume --progress-path H:\Lifespan-report\astock_lifespan_alpha\malf\card60-malf-day-live-formal-regate-progress.json
```

新 run：

- `run_id = day-107059a919fc`
- `started_at = 2026-04-23 18:16:37.324559`

progress sidecar 冻结前最后状态：

- `status = running`
- `symbols_total = 5501`
- `symbols_seen = 1899`
- `symbols_completed = 1875`
- `current_symbol = 300445.SZ`
- `elapsed_seconds = 4422.490618`
- `estimated_remaining_symbols = 3626`
- `pivot_rows = 3606896`
- `wave_rows = 1135183`
- `state_snapshot_rows = 7476366`
- `wave_scale_snapshot_rows = 7476366`
- `wave_scale_profile_rows = 1135183`

## 4. stale / 卡死证据

目标写入路径：

- 本轮未产生新的 `.building.duckdb`
- 实际发生的是 target 直写

冻结时点证据：

- `card60-malf-day-live-formal-regate-progress.json` 最后 mtime 停在 `2026-04-23 19:30` 左右
- `malf_day.duckdb` 最后推进到 `2026-04-23 19:30:10`
- `malf_day.duckdb.wal` 最后推进到 `2026-04-23 19:30:24`
- `card60-malf-day-live-formal-regate-build.stdout.log = 0 bytes`
- `card60-malf-day-live-formal-regate-build.stderr.log = 0 bytes`

进程证据：

- 早前高 CPU 的实际 worker 已消失
- 仅残留同命令行的空挂 `python.exe`
- `Stop-Process -Id 23428 -Force` 后该壳进程仍保持极小工作集，不再代表有效执行推进

结论：

- 本轮不是“继续慢跑”，而是停在固定进度不再前进的 stale `running`

## 5. blocker closeout 治理

closeout 前：

- `day-107059a919fc.status = running`
- `symbols_completed = 1875 / 5501`
- `day-107059a919fc:* running queue = 25`

治理动作：

- `malf_run.run_id = day-107059a919fc` 改为 `interrupted`
- 写入 closeout message
- `finished_at = 2026-04-23 21:42:27.924976`
- `day-107059a919fc:* running queue` 全部改为 `interrupted`

closeout 后：

- `day-107059a919fc.status = interrupted`
- `day-107059a919fc:* running queue = 0`
- `day-107059a919fc:* interrupted queue = 25`

本轮未改动的历史 stale run：

- `day-d696fdcd4774`
- `day-3343b24d0f0b`

## 6. formal target 污染范围

本轮失败后，target 已出现 interrupted run 的局部写入：

- `malf_pivot_ledger(run_id = day-107059a919fc) = 3606925`
- `malf_wave_ledger(run_id = day-107059a919fc) = 1135184`
- `malf_state_snapshot(run_id = day-107059a919fc) = 7479018`
- `malf_wave_scale_snapshot(run_id = day-107059a919fc) = 7414169`
- `malf_wave_scale_profile(run_id = day-107059a919fc) = 1128255`

checkpoint 也被改成混合状态：

- `last_run_id = day-107059a919fc`：`1875` symbols
- `last_run_id = day-fc56ff5e5441`：`3501` symbols
- `last_run_id = day-02686332592b`：`100` symbols
- `last_run_id = day-d696fdcd4774`：`25` symbols

因此当前 formal target 不再满足“单一正式 day run”口径，不能把本轮结果当作 Stage 19 live formal pass 使用。

## 7. forced audit 处理

本卡没有继续执行：

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id <new_run_id> --sample-count 12
```

原因：

- 没有新的 `completed` full-universe run
- `day-107059a919fc` 已被正式登记为 `interrupted`
- target 已被 interrupted run 局部写入，若强行对该 run 审计会把“不完整账本”误当成 formal ledger
