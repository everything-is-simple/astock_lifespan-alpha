# 批次 54 卡片：trade commit 前短事务 cutover 收口

卡片编号：`54`
日期：`2026-04-23`
文档标识：`trade-commit-cutover`

## 目标

在 Card 53 已证明 delete path 不再是主 blocker 的前提下，只处理 `trade` 正式写入尾段，不进入 `system`，不重开 stage-five 语义，不 bump `trade_contract_version`。

本轮目标固定为：

- 将正式输出与 tracking 写入拆成 run-scoped staging table 构建
- 最终只用短事务完成正式表 rename cutover
- cutover 前 drop 旧 secondary indexes，cutover 后重建固定 indexes
- 输出 `write_cutover_committed` 与兼容性的 `write_transaction_committed`
- 正式 live gate 必须确认 `trade` 是否可以从 `待修` 改为 `放行`

## 验收口径

- staging 表必须使用显式 DDL，保留正式表 `NOT NULL / PRIMARY KEY` 约束。
- staging 覆盖以下正式表：
  - `trade_order_intent`
  - `trade_order_execution`
  - `trade_position_leg`
  - `trade_carry_snapshot`
  - `trade_exit_execution`
  - `trade_work_queue`
  - `trade_checkpoint`
  - `trade_run_order_intent`
- source work unit 内使用当前 materialized 结果；source work unit 外保留正式旧行。
- `trade_work_queue` 替换为当前 run 的完整 work-unit queue。
- `trade_checkpoint.last_run_id` 必须全量切到最新 run。
- 成功后不得残留本 run staging / backup tables。

## 本轮边界

- 只改 `trade` 写路径、`trade` 单测和治理文档。
- 不进入 `system`。
- 不修改 public runner 名称。
- 不修改模块主链路和 stage-five 执行语义。
