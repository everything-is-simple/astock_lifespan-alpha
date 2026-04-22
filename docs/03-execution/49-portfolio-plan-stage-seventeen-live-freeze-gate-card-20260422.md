# 批次 49 卡片：portfolio_plan 阶段十七 live freeze gate

卡片编号：`49`
日期：`2026-04-22`
文档标识：`portfolio-plan-stage-seventeen-live-freeze-gate`

## 目标

验证并切换当前 `portfolio_plan` 到阶段十七的 live active-cap accounting，并给出本轮唯一正式判定：`放行` 或 `待修`。

## 验收口径

- `pytest tests/unit/portfolio_plan -q` 通过。
- `pytest tests/unit/contracts/test_module_boundaries.py -q` 通过。
- bounded real-data replay 能证明同日 full exit 释放容量后，后续 candidate 可重新 admitted。
- live `portfolio_plan.duckdb` 的正式 gate 满足：
  - 最新 `portfolio_plan_run.portfolio_gross_cap_weight = 0.50`
  - `portfolio_plan_snapshot` 稳定包含并实际使用：
    - `planned_entry_trade_date`
    - `scheduled_exit_trade_date`
    - `current_portfolio_gross_weight`
    - `remaining_portfolio_capacity_weight`
  - `plan_status` 分布不再停在“几乎全 blocked，仅 1 admitted + 1 trimmed”的旧累计口径
  - 最新正式 run 为 `completed`

## 本轮边界

- 只继续 `portfolio_plan`，不切到 `trade / system`
- 不改 public runner 名称
- 不改 `portfolio_id` work unit、checkpoint、`reused / rematerialized` 契约
- 本轮结论只允许写 `放行` 或 `待修`
