# 批次 53 证据：trade delete path unblock

证据编号：`53`
日期：`2026-04-23`
文档标识：`trade-delete-path-unblock`

## 1. 本轮代码修复

本轮只在 `trade` 内部写路径做最小修复：

- 新增 `TRADE_DELETE_WORK_UNIT_BATCH_SIZE = 250`
- 将目标表 delete 改为 `_delete_trade_targets_in_batches`
- 先生成 `trade_delete_work_unit_batches`
- 按 `portfolio_id + symbol` 分批删除正式目标表旧行
- 删除顺序固定为：
  - `trade_carry_snapshot`
  - `trade_exit_execution`
  - `trade_position_leg`
  - `trade_order_execution`
  - `trade_order_intent`
- stderr 增加表级和 batch 级 phase
- 单测用 batch size `1` 强制覆盖多批 delete

## 2. 本地测试结果

```text
pytest tests/unit/trade -q
12 passed in 7.30s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.06s

pytest tests/unit/docs/test_trade_specs.py -q
2 passed in 0.03s
```

## 3. 正式 live gate 结果

正式执行：

- CLI：`python scripts/trade/run_trade_from_portfolio_plan.py`
- run：`trade-258bd7bafa7d`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card53-20260423-103832.stderr.log`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card53-20260423-103832.stdout.log`

正式日志确认：

- `write_delete_trade_carry_snapshot_done elapsed_seconds=0.170719`
- `write_delete_trade_exit_execution_done elapsed_seconds=0.166957`
- `write_delete_trade_position_leg_done elapsed_seconds=0.167680`
- `write_delete_trade_order_execution_done elapsed_seconds=54.151770`
- `write_delete_trade_order_intent_done elapsed_seconds=77.904093`
- `write_targets_cleared elapsed_seconds=132.588952 rows=5497`
- `write_output_tables_loaded elapsed_seconds=83.854912 rows=9434`
- `write_tracking_tables_loaded elapsed_seconds=23.324166 rows=5497`

关键结论：

- Card 52 的 `write_targets_cleared` 前 blocker 已解除
- delete path 不再是当前主 blocker
- 新 blocker 位于 `write_tracking_tables_loaded` 之后、`write_transaction_committed` 之前

## 4. 正式库 closeout

由于 run 在 commit 前被中断，事务回滚后正式库仍保持旧态：

- `trade_checkpoint.last_run_id = trade-012abd340b1b`
- `trade_work_queue = 0`
- `trade_order_intent = planned 2 / blocked 5892932`
- `trade_order_execution = filled 2 / rejected 5892932`
- `trade_position_leg = 0`
- `trade_exit_execution = 0`
- `trade_carry_snapshot = 0`

## 5. 证据裁决

Card 53 已完成 delete path unblock，但没有完成正式 live pass。

因此：

- `trade = 待修`
- 下一轮不应继续把 delete 当作主 blocker
- 下一轮应聚焦 `write_transaction_committed` 前的正式事务收口路径
