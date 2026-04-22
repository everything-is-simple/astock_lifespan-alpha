# 批次 49 记录：portfolio_plan 阶段十七 live freeze gate

记录编号：`49`
日期：`2026-04-22`
文档标识：`portfolio-plan-stage-seventeen-live-freeze-gate`

## 1. 执行顺序

1. 复核阶段四、阶段十六、阶段十七对 `portfolio_plan` 的正式口径。
2. 检查当前 `portfolio_plan` 单测覆盖，确认是否已经存在 active-cap 与容量释放验证。
3. 只读核对正式 `portfolio_plan.duckdb` 的最新 run、表计数、关键字段与 `plan_status` 聚合。
4. 做 bounded real-data replay，对照 `cap = 0.15` 与 `cap = 0.50` 的窗口结果。
5. 运行 `repair_portfolio_plan_schema()` 对齐正式 schema。
6. 以长跑方式正式重跑 `run_portfolio_plan_build()`。
7. 观察到 live 进程长期持锁、日志为空、CPU 持续累加但 I/O 不再推进。
8. 将异常 run 显式改写为 `interrupted`，避免留下 stale `running`。
9. 回写治理面板与本批次结论。

## 2. 关键偏差

### 正式库仍停在旧 run

正式库最新 run 仍是：

- `portfolio-plan-bd3a42d2fafe`
- `portfolio_gross_cap_weight = 0.15`
- `admitted = 1`
- `trimmed = 1`
- `blocked = 5892932`

这说明正式库还没有完成阶段十七要求的 live cutover。

### bounded replay 与 live cutover 分离

bounded replay 已证明：

- `1991-09-10 -> 1991-09-20` 窗口内，`0.50` 相比 `0.15` 会减少 trimmed
- `1991-09-16` 当天由 `6 admitted + 2 trimmed + 2 blocked` 提升到 `8 admitted + 0 trimmed + 2 blocked`
- 阶段十七 active-cap 语义本身不是 blocker

### live rerun 发生 stall

本轮 live `0.50` rerun：

- `run_id = portfolio-plan-21b6ab8747f7`
- 启动后长期持锁正式库
- stdout / stderr 均为空
- CPU 时间继续增长，但读写计数停止推进

因此本轮将其按异常长跑处理：

- 强制停止进程
- 将 run 显式标为 `interrupted`

当前代码和单测已经具备：

- `portfolio_gross_cap_weight = 0.50` 默认值
- `planned_entry_trade_date / scheduled_exit_trade_date` 驱动的 active-cap 裁决
- scheduled exit 后释放容量的本地证明

因此当前缺口主要不是模块边界或纯实现空白，而是：

- live `portfolio_plan` 在全量正式库上的 cutover 仍无法完成
- 旧 `0.15` 正式快照仍然主导当前结果

## 3. 本轮观察

- `position` 已完成并固定为 `放行`
- 当前唯一活跃模块已切换为 `portfolio_plan`
- `portfolio_plan` 当前入口判定为 `待修`
- bounded replay 已通过
- 是否进入 `放行` 必须等待正式 live cutover 真正完成
