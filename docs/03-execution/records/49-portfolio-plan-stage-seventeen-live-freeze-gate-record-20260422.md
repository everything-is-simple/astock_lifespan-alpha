# 批次 49 记录：portfolio_plan 阶段十七 live freeze gate

记录编号：`49`
日期：`2026-04-22`
文档标识：`portfolio-plan-stage-seventeen-live-freeze-gate`

## 1. 执行顺序

1. 复核阶段四、阶段十六、阶段十七对 `portfolio_plan` 的正式口径。
2. 检查当前 `portfolio_plan` 单测覆盖，确认是否已经存在 active-cap 与容量释放验证。
3. 只读核对正式 `portfolio_plan.duckdb` 的最新 run、表计数、关键字段与 `plan_status` 聚合。
4. 将治理面板当前活跃模块切换为 `portfolio_plan`。
5. 基于 preflight 结果给出本轮入口判定。

## 2. 关键偏差

### 正式库仍停在旧 run

正式库最新 run 仍是：

- `portfolio-plan-bd3a42d2fafe`
- `portfolio_gross_cap_weight = 0.15`
- `admitted = 1`
- `trimmed = 1`
- `blocked = 5892932`

这说明正式库还没有完成阶段十七要求的 live cutover。

### 代码与正式库出现 cutover 裂缝

当前代码和单测已经具备：

- `portfolio_gross_cap_weight = 0.50` 默认值
- `planned_entry_trade_date / scheduled_exit_trade_date` 驱动的 active-cap 裁决
- scheduled exit 后释放容量的本地证明

因此当前缺口主要不是模块边界或纯实现空白，而是：

- 正式库仍停在旧口径
- 尚未完成 bounded real-data replay 与正式 live gate

## 3. 本轮观察

- `position` 已完成并固定为 `放行`
- 当前唯一活跃模块已切换为 `portfolio_plan`
- `portfolio_plan` 当前入口判定为 `待修`
- 是否进入 `放行` 必须等待正式 live cutover 与真实 replay 一起通过
