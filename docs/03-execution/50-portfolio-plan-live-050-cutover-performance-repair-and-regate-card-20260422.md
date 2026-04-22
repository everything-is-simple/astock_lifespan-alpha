# 批次 50 卡片：portfolio_plan live 0.50 cutover 性能修复与重验收

卡片编号：`50`
日期：`2026-04-22`
文档标识：`portfolio-plan-live-050-cutover-performance-repair-and-regate`

## 目标

在不改 `portfolio_plan` 正式对外合同的前提下，修复 live `0.50` cutover 的全量性能路径，并重跑正式 gate，给出本轮唯一正式判定：`放行` 或 `待修`。

## 验收口径

- `pytest tests/unit/portfolio_plan -q` 通过。
- `pytest tests/unit/contracts/test_module_boundaries.py -q` 通过。
- `pytest tests/unit/docs/test_portfolio_plan_specs.py -q` 通过。
- `pytest tests/unit/docs/test_position_specs.py -q` 通过。
- `pytest -q` 通过。
- `portfolio_plan` slow path 已从全表递归改为按 `planned_entry_trade_date` 分批的正式路径。
- live 正式 `run_portfolio_plan_build(portfolio_gross_cap_weight=0.50)` 满足：
  - 最新 `portfolio_plan_run` 为 `completed`
  - `portfolio_plan_checkpoint.last_run_id` 切换到新的 `0.50` run
  - `portfolio_plan_snapshot.portfolio_gross_cap_weight = 0.50`
  - `plan_status` 不再停留在旧的 `blocked = 5892932 / admitted = 1 / trimmed = 1`
  - 运行中 progress 信息可在 `portfolio_plan_run.message` 与 stderr 日志中观察

## 本轮边界

- 只继续 `portfolio_plan`，不切到 `trade / system`
- 不改 public runner 名称 `run_portfolio_plan_build`
- 不改 `portfolio_id` work unit 与 `portfolio_plan_checkpoint / portfolio_plan_work_queue` 契约
- 不 bump `portfolio_plan_contract_version`
- 本轮结论只允许写 `放行` 或 `待修`

## 修复方案冻结

- slow path 固定改为两层物化：
  - 一次性构建稳定排序的 `portfolio_plan_ordered_source`
  - 以 `planned_entry_trade_date` 为序推进 active-cap carry
  - 同日内用 DuckDB window SQL 按行序分配容量
- live 可观测性固定补齐：
  - `portfolio_plan_run.message` 周期性刷新
  - stderr 日志输出 progress
  - 最终 summary 写入 phase timing
- 本轮不重新设计语义，只修执行路径与可观测性
