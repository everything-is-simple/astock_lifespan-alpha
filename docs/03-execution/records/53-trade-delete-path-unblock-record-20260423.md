# 批次 53 记录：trade delete path unblock

记录编号：`53`
日期：`2026-04-23`
文档标识：`trade-delete-path-unblock`

## 1. 执行顺序

1. 在 Card 52 提交后新建分支 `lifespan0.01/card53-trade-delete-path-unblock`。
2. 将正式目标表 delete 替换为 `_delete_trade_targets_in_batches`。
3. 固定 batch size 为 `250` 个 work unit。
4. 按下游到上游顺序删除：
   - `trade_carry_snapshot`
   - `trade_exit_execution`
   - `trade_position_leg`
   - `trade_order_execution`
   - `trade_order_intent`
5. 为每张表和每个 batch 输出 stderr phase。
6. 新增单测强制 batch size 为 `1`，验证多批 delete 仍能正确替换旧目标行。
7. 运行本地三组 gate。
8. 正式 preflight 确认无活跃 writer，无 stale `running` 行。
9. 后台启动正式 live gate。
10. 观察到正式 run 越过 `write_targets_cleared`、`write_output_tables_loaded`、`write_tracking_tables_loaded`。
11. 连续两个观察窗口内未出现 `write_transaction_committed`，且无 `CPU / stderr / trade.duckdb mtime` 进展。
12. 终止正式 runner，将 `trade-258bd7bafa7d` 标记为 `interrupted`。
13. 回查正式库，确认事务未提交，正式表仍保持旧态。

## 2. 偏差项

- Card 53 没有达到正式 live pass 线。
- 原 blocker 已解除，但 blocker 后移到 commit 前事务收口。
- `trade_run.message` 在中断后由 closeout 脚本修正为 commit 前 blocker；详细阶段证据以 stderr 日志为准。

## 3. 备注

- 本轮没有进入 `system`。
- 本轮没有改 public runner 名称。
- 本轮没有 bump `trade_contract_version`。
- 下一轮应继续只在 `trade` 内处理正式事务收口，不回退到前半段 materialization。
