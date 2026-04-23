# 批次 52 结论：trade live slow-path diagnosis and unblock

结论编号：`52`
日期：`2026-04-23`
文档标识：`trade-live-slow-path-diagnosis-and-unblock`

## 裁决

`已记录，trade 待修`

## 结论

Card 52 已在 `trade` 模块内完成本轮 slow-path 诊断与最小修复，正式结论如下：

- `trade` phase-level 可观测性已经建立，正式 live rerun 不再停在 `trade run started.`
- 只读 `profile_trade_live_path` 已能完整返回正式体量 slow path 的阶段耗时与行数
- 当前主瓶颈已从旧的 work-unit 指纹 OOM 收敛为：
  - `intent_materialized = 42.960675s`
  - `action_tables_ready = 40.940951s`
- 正式 writer 已能稳定推进到 `write_transaction_started`
- 新增写事务子阶段后，正式 blocker 已进一步收敛为：
  - `write_targets_cleared` 之前没有任何进展
  - 即当前问题位于正式 target-table delete 路径，而不是 action classification、output insert 或 checkpoint/work_queue

因此本轮不能把 `trade` 登记为 `放行`，正式裁决维持：

- `trade = 待修`
- `system` 继续冻结
- 下一轮继续留在 `trade`，优先只处理正式 delete 路径 unblock

## 正式 gate 结果

- 只读 profile：
  - `runner_name = profile_trade_live_path`
  - `source_row_count = 5892934`
  - `work_units_seen = 5497`
  - `work_units_updated = 5497`
  - `latest_reference_trade_date = 2026-04-10`
  - `dominant_phase = intent_materialized`
- Card 52 正式 rerun 诊断链：
  - `trade-5b93a1f466f8`
    - 已推进到 `write_transaction_started`
    - 两个观察窗无 `CPU / stderr / db mtime` 进展
    - 已标记 `interrupted`
  - `trade-b68fe7bc930e`
    - 在 delete rewrite 后再次推进到 `write_transaction_started`
    - 仍在两个观察窗内无进展
    - 已标记 `interrupted`
  - `trade-dbb7397cbd43`
    - 在写事务子阶段补点后，仍停在 `write_transaction_started`
    - `write_targets_cleared` 未出现
    - 已标记 `interrupted`

## 当前正式状态

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

## 后续边界

- 不进入 `system`
- 不回退到 stage-five 语义讨论
- 只继续在 `trade` 模块内处理正式 delete 路径
- 下一轮优先把 delete 路径继续拆到具体目标表或批次策略，再重跑正式 gate
