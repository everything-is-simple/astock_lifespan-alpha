# 批次 54 记录：trade commit 前短事务 cutover 收口

记录编号：`54`
日期：`2026-04-23`
文档标识：`trade-commit-cutover`

## 1. 执行顺序

1. 从 `lifespan0.01/card53-trade-delete-path-unblock` 新建分支 `lifespan0.01/card54-trade-commit-cutover`。
2. 保留 `run_trade_from_portfolio_plan` public runner 名称和参数。
3. 在 `trade` runner 内新增 staged target table replacement 写路径。
4. 使用显式 DDL 创建 run-scoped staging tables。
5. 将正式输出表、`trade_work_queue`、`trade_checkpoint`、`trade_run_order_intent` 先写入 staging。
6. 在短事务中 drop secondary indexes，并将 staging tables rename 为正式表。
7. cutover 后重建 secondary indexes，drop backup tables。
8. 改写 Card 53 delete-batching 单测为 Card 54 staged cutover 单测。
9. 新增非 source work unit 保留测试。
10. 运行本地 trade、boundary、docs、全量 pytest。
11. 正式 live preflight 确认无残留 writer，最新 run 为 Card 53 `interrupted`。
12. 后台启动正式 live gate。
13. 观察到正式 run `trade-558802e7f7a4` 完成 staged build、短事务 cutover、index rebuild、backup cleanup。
14. 只读回查正式库，确认 run completed、checkpoint 全量切新、下游正式表非空、无 stage/backup 残留。

## 2. 偏差项

- `trade_run_order_intent` staged full replacement 是本轮最长阶段，用时约 `505.59` 秒。
- 最终 cutover 本身用时约 `0.02` 秒，符合 Card 54 的短事务目标。
- backup drop 未成为 blocker，用时约 `0.21` 秒。

## 3. 备注

- 本轮没有进入 `system`。
- 本轮没有改 public runner 名称。
- 本轮没有 bump `trade_contract_version`。
- 本轮将 `trade` 从 `待修` 改为 `放行`，下一活跃模块切到 `system`。
