# 批次 50 记录：portfolio_plan live 0.50 cutover 性能修复与重验收

记录编号：`50`
日期：`2026-04-22`
文档标识：`portfolio-plan-live-050-cutover-performance-repair-and-regate`

## 1. 执行顺序

1. 将 `portfolio_plan` slow path 从全表递归改为按 `planned_entry_trade_date` 分批物化。
2. 保持 `run_portfolio_plan_build / portfolio_id / checkpoint / work_queue` 合同不变。
3. 为 slow path 增加 progress message 与 stderr 日志。
4. 补充三组单测：
   - 同日顺序容量消耗
   - `scheduled_exit_trade_date` 边界释放
   - `0.50` 非旧形态回归
5. 在正式 `H:\Lifespan-data` 上重跑 live `0.50` gate。
6. 观察到主段 `dates=8531/8531` 可完成，随后继续跑到：
   - `materialized_with_action`
   - `old snapshot deleted`
   - `snapshot inserted`
   - `run_snapshot inserted`
7. 在最终事务提交仍未完成的情况下，停止进程并将最新 run 收口为 `interrupted`。
8. 回写 Card 50 证据、记录、结论与治理索引。

## 2. 关键结论

- 本轮已经确认：
  - 旧的“整表递归 + 无日志”瓶颈被替换并显著推进
  - live 运行中 progress 已经可直接观察
  - 当前剩余瓶颈收缩到正式 committed replace 的最终提交尾段
- 本轮尚未确认：
  - `portfolio_plan_checkpoint` 已切到新的 `0.50` run
  - 正式 `portfolio_plan_snapshot` 已从旧 `0.15` 结果切换

## 3. 当前观察

- `position` 继续维持 `放行`
- 当前唯一活跃模块继续是 `portfolio_plan`
- `trade / system` 仍不得提前启动 freeze gate
- Card 50 之后的下一步固定为：
  - 继续削减 committed replace 的最终提交尾段
  - 重跑正式 `0.50` gate
