# 批次 53 卡片：trade delete path unblock

卡片编号：`53`
日期：`2026-04-23`
文档标识：`trade-delete-path-unblock`

## 目标

在 Card 52 已经把正式 blocker 收敛到 `write_targets_cleared` 之前的前提下，只处理 `trade` 正式写事务 delete 路径，不进入 `system`，不重开 stage-five 语义。

本轮目标固定为：

- 将五张正式目标表 delete 拆成单表阶段
- 每张表按 `portfolio_id + symbol` work unit 分批 delete
- 用 stderr phase 精确记录目标表与 batch index
- 重跑正式 live gate，确认 blocker 是否仍在 delete 路径

## 验收口径

- `run_trade_from_portfolio_plan` public runner 不改名、不改参数
- `trade` 读取边界保持 `portfolio_plan_snapshot + execution_price_line`
- 默认 delete batch size 为 `250` 个 work unit
- delete 顺序固定为：
  - `trade_carry_snapshot`
  - `trade_exit_execution`
  - `trade_position_leg`
  - `trade_order_execution`
  - `trade_order_intent`
- 每个表必须输出：
  - `write_delete_<table>_started`
  - `write_delete_<table>_batch`
  - `write_delete_<table>_batch_done`
  - `write_delete_<table>_done`
- 若正式 gate 仍失败，必须精确登记最新失败阶段

## 本轮边界

- 只改 `trade` 写路径、测试和治理文档
- 不处理 `system`
- 不 bump `trade_contract_version`
- 不把 Card 52 回写成放行
- 若 delete 被证明通过但 blocker 后移，本卡结论仍可登记为 `trade = 待修`
