# 批次 50 结论：portfolio_plan live 0.50 cutover 性能修复与重验收

结论编号：`50`
日期：`2026-04-22`
文档标识：`portfolio-plan-live-050-cutover-performance-repair-and-regate`

## 裁决

`已记录，portfolio_plan 待修`

## 结论

Card 50 已经完成 `portfolio_plan` live `0.50` cutover 的主要性能修复，并把前序黑盒显式收缩为最终 committed replace 尾段。

本轮正式确认：

- `portfolio_plan` slow path 已不再使用整表递归累计 active gross
- live stderr progress 已恢复，不再是空日志
- 正式 rerun 已能跑到：
  - `dates=8531/8531`
  - `materialized_with_action`
  - `old snapshot deleted`
  - `snapshot inserted`
  - `run_snapshot inserted`

但本轮仍未满足放行条件：

- 最新验证 run `portfolio-plan-0875345c4aa5` 未完成最终提交，已按 `interrupted` 收口
- `portfolio_plan_checkpoint.last_run_id` 仍未切到新的 `0.50` run
- 当前正式 `portfolio_plan_snapshot` 仍由旧 `0.15` run 主导

因此：

- `portfolio_plan = 待修`
- 本轮不进入 `trade`
- 下一步固定为继续压缩 committed replace 尾段后，再重跑正式 `0.50` gate

## 正式 gate 结果

- 最新验证 run：`portfolio-plan-0875345c4aa5`
- `status = interrupted`
- 最新 stderr 进度日志：
  `H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-card50-20260422-160527.stderr.log`
- 当前正式 `portfolio_plan_checkpoint.last_run_id`：
  `portfolio-plan-bd3a42d2fafe`
- 当前正式 `plan_status`：
  - `blocked = 5892932`
  - `admitted = 1`
  - `trimmed = 1`

## 后续边界

在 `portfolio_plan` 真正完成正式 `0.50` cutover 之前：

- 不进入 `trade` freeze card
- 不把 `position` 从 `放行` 升级为 `冻结`
- 不把 Card 50 误记为通过
