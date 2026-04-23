# 批次 52 证据：trade live slow-path diagnosis and unblock

证据编号：`52`
日期：`2026-04-23`
文档标识：`trade-live-slow-path-diagnosis-and-unblock`

## 1. 本轮代码修复

本轮 `trade` 代码只在模块内部继续缩小 live slow path 计算与写入范围：

- `trade_run.message` 与 stderr 已增加阶段性进度输出
- 新增只读诊断入口：`scripts/trade/profile_trade_live_path.py`
- `trade_source_work_unit_summary` 使用固定宽度哈希聚合，替代旧的长串 `string_agg`
- `trade_materialized_intent` 拆成：
  - `trade_direct_blocked_source`
  - `trade_price_candidate_source`
- `trade_materialized_execution` 拆成：
  - 阻塞行直接 `rejected`
  - 仅 actionable 行做价格联接
- action classification 改为 `compare_signature` 窄比较
- 正式写事务 delete 改为按 `(portfolio_id, symbol)` 成对联接
- 写事务新增子阶段：
  - `write_targets_cleared`
  - `write_output_tables_loaded`
  - `write_tracking_tables_loaded`

## 2. 本地测试结果

```text
pytest tests/unit/trade -q
11 passed in 15.87s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.20s

pytest tests/unit/docs/test_trade_specs.py -q
2 passed in 0.14s
```

## 3. 只读 profile 结果

`python scripts/trade/profile_trade_live_path.py`

stdout 结果确认：

- `source_row_count = 5892934`
- `work_units_seen = 5497`
- `work_units_updated = 5497`
- `latest_reference_trade_date = 2026-04-10`
- `dominant_phase = intent_materialized`

关键阶段耗时：

- `work_unit_summary_ready = 4.578615s`
- `intent_materialized = 42.960675s`
  - `direct_blocked_rows = 5883494`
  - `candidate_rows = 9440`
- `execution_materialized = 12.601592s`
- `exit_materialized = 0.868226s`
- `position_leg_materialized = 0.537525s`
- `carry_materialized = 0.143526s`
- `action_tables_ready = 40.940951s`

关键行数：

- `trade_materialized_intent = 5892934`
- `trade_materialized_execution = 5892934`
- `trade_materialized_position_leg = 9440`
- `trade_materialized_exit_execution = 9434`
- `trade_materialized_carry_snapshot = 18874`

结论：旧的 work-unit 指纹 OOM 已消除，主瓶颈正式收敛到 `intent_materialized` 与 `action_tables_ready`。

## 4. 正式 live rerun 结果

### 4.1 第一次 Card 52 live rerun

- 正式 run：`trade-5b93a1f466f8`
- 日志：
  - `H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card52-20260423-092635.stderr.log`
  - `H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card52-20260423-092635.stdout.log`
- 已推进到：
  - `source_attached`
  - `work_unit_summary_ready`
  - `intent_materialized`
  - `execution_materialized`
  - `exit_materialized`
  - `position_leg_materialized`
  - `carry_materialized`
  - `action_tables_ready`
  - `write_transaction_started`
- 之后连续两个观察窗内：
  - `CPU delta = 0`
  - stderr 无新增
  - `trade.duckdb` mtime 无变化
- 最终已标记 `interrupted`

### 4.2 第二次 Card 52 live rerun

- 正式 run：`trade-b68fe7bc930e`
- 日志：
  - `H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card52b-20260423-093312.stderr.log`
  - `H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card52b-20260423-093312.stdout.log`
- delete rewrite 后仍然推进到 `write_transaction_started`
- 但两个观察窗内依旧无 `CPU / stderr / db mtime` 进展
- 最终已标记 `interrupted`

### 4.3 第三次 Card 52 live rerun

- 正式 run：`trade-dbb7397cbd43`
- 日志：
  - `H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card52c-20260423-093924.stderr.log`
  - `H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card52c-20260423-093924.stdout.log`
- 新增写事务子阶段后，正式日志仍只到：
  - `write_transaction_started`
- `write_targets_cleared` 没有出现
- 连续两个观察窗内：
  - `CPU delta = 0`
  - stderr 长度不变
  - `trade.duckdb` mtime 不变
- 最终已标记 `interrupted`

## 5. 当前正式库 closeout

正式库回查确认：

- `trade_checkpoint.last_run_id = trade-012abd340b1b`
- `trade_work_queue = 0`
- `trade_order_intent`
  - `planned = 2`
  - `blocked = 5892932`
- `trade_order_execution`
  - `filled = 2`
  - `rejected = 5892932`
- `trade_position_leg = 0`
- `trade_exit_execution = 0`
- `trade_carry_snapshot = 0`

## 6. 证据裁决

本轮证据已经把 Card 52 的 live blocker 收敛到具体写路径：

- 不是旧的 work-unit 指纹 OOM
- 不是 action classification 之前的黑盒无进展
- 不是 output insert 或 checkpoint/work_queue
- 当前 blocker 位于 `write_targets_cleared` 之前，也就是正式 target-table delete 路径

因此 Card 52 只能正式登记为 `trade = 待修`。
