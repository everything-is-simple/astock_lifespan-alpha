# 批次 54 证据：trade commit 前短事务 cutover 收口

证据编号：`54`
日期：`2026-04-23`
文档标识：`trade-commit-cutover`

## 1. 本轮代码修复

本轮只在 `trade` 内部写路径做收口修复：

- 将原先大事务 `delete + insert outputs + insert tracking + COMMIT` 改为 staged target table replacement
- 新增 run-scoped staging tables，表名由当前 `run_id` 派生
- staging tables 使用显式 DDL，保留 `NOT NULL / PRIMARY KEY`
- source work unit 外正式旧行在 staging 中保留
- `trade_run_order_intent` 改为 staged full replacement：历史 run 保留，当前 run mapping 在 staging 内追加
- 最终 cutover 短事务只做 index drop 与 table rename
- cutover 后重建：
  - `idx_trade_intent_work_unit`
  - `idx_trade_execution_work_unit`
  - `idx_trade_position_leg_work_unit`
- 成功后 drop 本 run backup tables

## 2. 本地测试结果

```text
pytest tests/unit/trade -q
13 passed in 9.09s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.07s

pytest tests/unit/docs/test_trade_specs.py -q
2 passed in 0.03s

pytest -q
117 passed in 43.52s
```

## 3. 正式 live gate 结果

正式执行：

- CLI：`python scripts/trade/run_trade_from_portfolio_plan.py`
- run：`trade-558802e7f7a4`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card54-20260423-110924.stderr.log`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card54-20260423-110924.stdout.log`

stdout summary：

- `status = completed`
- `work_units_seen = 5497`
- `work_units_updated = 5497`
- `latest_reference_trade_date = 2026-04-10`
- `trade_order_intent = 5892934`
- `trade_order_execution = 5892934`
- `trade_position_leg = 9440`
- `trade_carry_snapshot = 18874`
- `trade_exit_execution = 9434`

关键 stderr phase：

- `write_stage_trade_order_intent_done elapsed_seconds=62.251301 rows=5892934`
- `write_stage_trade_order_execution_done elapsed_seconds=68.031351 rows=5892934`
- `write_stage_trade_run_order_intent_done elapsed_seconds=505.591686 rows=53036406`
- `write_stage_targets_done elapsed_seconds=636.362316 rows=5497`
- `write_cutover_committed elapsed_seconds=0.019545 rows=5497`
- `write_transaction_committed elapsed_seconds=0.022597 rows=5497 cutover=1`
- `write_indexes_rebuilt elapsed_seconds=10.556678 rows=5892934 index_count=3`
- `write_cutover_backups_dropped elapsed_seconds=0.205477 rows=5497 backup_count=8`
- `write_transaction_committed elapsed_seconds=10.795482 rows=5497`

## 4. 正式库验收

正式库只读回查结果：

- 最新 run：`trade-558802e7f7a4`
- `trade_run.status = completed`
- `trade_work_queue = 5497`
- `trade_checkpoint = 5497`
- `trade_checkpoint.last_run_id = trade-558802e7f7a4` 覆盖全部 `5497` 个 work unit
- `trade_order_intent = 5892934`
- `trade_order_execution = 5892934`
- `trade_position_leg = 9440`
- `trade_carry_snapshot = 18874`
- `trade_exit_execution = 9434`
- `trade_run_order_intent = 53036406`
- 无 `stage / backup` 残留表
- 三个 trade secondary indexes 已恢复
- 无残留 `trade` Python 进程

## 5. 证据裁决

Card 54 已完成 `trade` commit 前短事务 cutover 收口，并通过正式 live gate。

因此：

- `trade = 放行`
- `system` 可以作为下一活跃模块进入 live freeze gate
