# 批次 48 证据：position 阶段十七 live freeze gate 与放行

证据编号：`48`
日期：`2026-04-22`
文档标识：`position-stage-seventeen-live-freeze-gate-and-release`

## 1. 代码与回归测试

- 代码补丁：
  - `src/astock_lifespan_alpha/position/runner.py`
  - `tests/unit/position/test_position_runner.py`
- 关键行为变化：
  - 检测到 `position_exit_plan / position_exit_leg` 缺失、`planned_entry_trade_date` 缺失或 exit 行数为 `0` 时，强制 full refresh
  - exit 规划从未来自连接改为窗口式“下一个 exit 日期”算法

回归结果：

```text
pytest tests/unit/position -q
7 passed in 10.77s
```

```text
pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.07s
```

## 2. bounded real-data replay

真实样本窗口：

- symbol：`600653.SH`
- signal window：`1991-01-01 -> 1991-12-31`
- market window：`1991-01-01 -> 1992-01-31`

bounded replay 摘要：

```json
{
  "runner_name": "run_position_from_alpha_signal",
  "status": "completed",
  "candidate_rows": 39,
  "capacity_rows": 39,
  "sizing_rows": 39,
  "exit_plan_rows": 31,
  "exit_leg_rows": 31,
  "work_units_seen": 1,
  "work_units_updated": 1,
  "latest_signal_date": "1991-12-30"
}
```

样本对账：

- `candidate_status`：`admitted = 33`，`blocked = 6`
- `exit_reason_code`：`signal_not_confirmed_exit = 10`，`time_stop_no_new_span_exit = 21`
- `planned_entry_trade_date`：`1991-01-22 -> 1991-12-31`
- `planned_exit_trade_date`：`1991-03-08 -> 1991-12-27`
- `MAX(final_allowed_position_weight) = 0.096`

## 3. 正式库 live cutover

第一次正式 repair：

- `position-a9f946c50d22`
- 由于交互超时留下 `running` 残留
- 复核时无活跃后台进程，因此显式改为 `interrupted`

长跑友好 live repair：

- 启动时间：`2026-04-22 10:58:46`
- CLI：`python scripts/position/run_position_from_alpha_signal.py`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\position\position-freeze-live-repair-20260422-105846.stdout.log`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\position\position-freeze-live-repair-20260422-105846.stderr.log`

正式 `stdout` 摘要：

```json
{
  "runner_name": "run_position_from_alpha_signal",
  "run_id": "position-acda303305c7",
  "status": "completed",
  "target_path": "H:\\Lifespan-data\\astock_lifespan_alpha\\position\\position.duckdb",
  "message": "position run completed with contract-drift full refresh.",
  "materialization_counts": {
    "candidate_rows": 5892934,
    "capacity_rows": 5892934,
    "sizing_rows": 5892934,
    "exit_plan_rows": 2564635,
    "exit_leg_rows": 2564635
  },
  "checkpoint_summary": {
    "work_units_seen": 5497,
    "work_units_updated": 5497,
    "latest_signal_date": "2026-04-10"
  }
}
```

## 4. 正式 DuckDB 对账

最新 `position_run`：

- `position-acda303305c7`
- `status = completed`
- `started_at = 2026-04-22 10:58:50`
- `finished_at = 2026-04-22 11:16:18`
- `inserted_candidates = 5892934`
- `inserted_capacity_rows = 5892934`
- `inserted_sizing_rows = 5892934`
- `inserted_exit_plan_rows = 2564635`
- `inserted_exit_leg_rows = 2564635`

正式表计数：

- `position_candidate_audit = 5892934`
- `position_capacity_snapshot = 5892934`
- `position_sizing_snapshot = 5892934`
- `position_exit_plan = 2564635`
- `position_exit_leg = 2564635`
- `planned_entry_trade_date IS NOT NULL = 5889479`

`exit_reason_code` 摘要：

- `signal_not_confirmed_exit = 2280390`
- `direction_not_long_exit = 196803`
- `time_stop_no_new_span_exit = 87442`
