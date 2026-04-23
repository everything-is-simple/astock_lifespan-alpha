# 批次 51 结论：trade stage-seventeen live freeze gate

结论编号：`51`
日期：`2026-04-23`
文档标识：`trade-stage-seventeen-live-freeze-gate`

## 裁决

`已记录，trade 待修`

## 结论

`trade` 本轮已经按 Card 51 进入 stage-seventeen live freeze gate，但当前不能写 `放行`。
本轮正式确认：

- 当前唯一活跃模块保持为 `trade`
- `position` 继续维持 `放行`
- `portfolio_plan` 继续维持 `放行`
- `trade` public runner 与模块边界保持不变，修复仍局限在 `trade` 内部
- 本轮代码侧修复已经收口：
  - exit/leg/carry 改为基于 `intent` 的等值联接
  - work-unit action 汇总改为先按 `portfolio_id + symbol` 聚合
  - checkpoint fast path 现在会强校验 `trade_position_leg / trade_carry_snapshot / trade_exit_execution`
  - `trade_source_work_unit_summary` 指纹构建已改成两阶段聚合，避免直接 `string_agg` 原始长串
- 代码侧 gate 已通过：
  - `pytest tests/unit/trade -q`
  - `pytest tests/unit/contracts/test_module_boundaries.py -q`
  - `pytest tests/unit/docs/test_trade_specs.py -q`
- 正式库 preflight 已确认：
  - 最新上游正式 run 仍是 `portfolio-plan-68ab0db998ad`
  - 正式 `portfolio_plan_snapshot` 状态分布仍是 `admitted = 6638 / trimmed = 2802 / blocked = 5883494`
  - 旧 `trade` 正式状态仍是 `planned = 2 / blocked = 5892932` 与 `filled = 2 / rejected = 5892932`
  - `trade_checkpoint.last_run_id` 仍停在 `trade-012abd340b1b`
  - `trade_work_queue` 为空
- 本轮 live rerun 启动了新正式 run `trade-6f780ccc1005`，但后台监控显示：
  - 两个连续观察窗口内 `CPU / stderr / trade.duckdb mtime` 都无进展
  - 正式表、checkpoint、work queue 都没有推进
  - 因此该 run 已按规则标记为 `interrupted`

因此：

- `trade = 待修`
- `system` 不允许提前进入 live freeze gate
- 下一轮必须继续留在 `trade`，聚焦当前 live slow path 的无进展 blocker，再重跑正式 gate

## 正式 gate 结果

- 最新失败 run：`trade-6f780ccc1005`
- 当前状态：`interrupted`
- 中断原因：`trade live gate run interrupted after two observation windows showed no CPU, stderr, or database-write progress.`
- 本轮后台日志：
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card51-20260423-075932.stderr.log`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card51-20260423-075932.stdout.log`
- 当前正式 `trade_checkpoint.last_run_id`：`trade-012abd340b1b`
- 当前正式 `trade_work_queue`：`0`
- 当前正式 `trade_order_intent`：
  - `planned = 2`
  - `blocked = 5892932`
- 当前正式 `trade_order_execution`：
  - `filled = 2`
  - `rejected = 5892932`
- 当前正式下游账表：
  - `trade_position_leg = 0`
  - `trade_exit_execution = 0`
  - `trade_carry_snapshot = 0`

## 后续边界

在 `trade` 本轮仍为 `待修` 的前提下：

- 不进入 `system`
- 不把 `trade` 升级为 `冻结`
- 不回退到 stage-five 语义讨论
- 只允许继续做 `trade` 内部 live slow path 修复与正式 rerun
