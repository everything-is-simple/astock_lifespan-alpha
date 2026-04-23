# 批次 51 卡片：trade stage-seventeen live freeze gate

卡片编号：`51`
日期：`2026-04-23`
文档标识：`trade-stage-seventeen-live-freeze-gate`

## 目标

以已经放行的 `portfolio_plan` live `0.50` snapshot 为唯一上游输入，对正式 `trade` 库执行 stage-seventeen live freeze gate。
本轮只允许在 `trade` 模块内做最小兼容修复，不重开 stage-five 语义讨论，不进入 `system`。

## 验收口径

- `pytest tests/unit/trade -q` 通过。
- `pytest tests/unit/contracts/test_module_boundaries.py -q` 通过。
- `pytest tests/unit/docs/test_trade_specs.py -q` 通过。
- `trade` public runner 继续保持为 `run_trade_from_portfolio_plan`，不改名、不改模块边界。
- 正式库 preflight 固定确认：
  - 无活跃 writer 持有 `trade.duckdb`
  - `portfolio_plan` 最新正式 run 为 `portfolio-plan-68ab0db998ad`
  - `trade` 旧正式结果仍停在 `trade-012abd340b1b`
  - 正式 `trade_order_intent / trade_order_execution` 仍是旧分布
  - `trade_position_leg / trade_carry_snapshot / trade_exit_execution` 尚未正式落表
- live rerun 必须以后台受控方式执行，并记录 stdout/stderr 日志。
- 只有当以下条件全部满足时，才允许写 `trade = 放行`：
  - 最新 `trade_run` 为新 run 且 `completed`
  - `input_rows = 5892934`
  - `work_units_seen = 5497`
  - `trade_work_queue` 回写 5497 行当前 run 的 work unit
  - `trade_checkpoint.last_run_id` 全量切到新 run
  - `trade_order_intent` 状态为 `planned = 9440 / blocked = 5883494`
  - `trade_order_execution` 状态为 `filled = 9440 / rejected = 5883494`
  - `trade_position_leg = 9440`
  - `trade_exit_execution = 9434`
  - `trade_carry_snapshot` 非零，若口径未变优先校验 `18874`

## 本轮边界

- 只允许修改 `trade` 内部 runner SQL、快路径校验与对应测试。
- 不改 `portfolio_plan`、`system`、`pipeline` 的实现。
- 不 bump `trade_contract_version`。
- 本轮结论只允许写 `放行` 或 `待修`。

## 修复方案冻结

- 保留已有两处 `trade` 本地修复：
  - exit/leg/carry 改为基于 `intent` 的等值联接
  - checkpoint fast path 必须同时校验下游账表形态
- 新增一处 runner-local 修复：
  - 把 `trade_source_work_unit_summary` 的指纹构建改成两阶段聚合
  - 先对 `trade_plan_source_rows` 逐行生成固定宽度 `row_fingerprint`
  - 再按 `portfolio_id + symbol` 聚合生成 `source_fingerprint`
- 新增单测覆盖同一 work unit 多行输入下的 reuse/rematerialize 路径。

## 本轮预期输出

- Card 51 `card / evidence / record / conclusion`
- `47` 治理板状态更新
- 若 live gate 失败，则精确登记 blocker，并保持 `system` 关闭
