# 阶段四批次 20 position 物化与最小 portfolio_plan bridge 执行卡

卡片编号：`20`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段四不能只停留在 `position` 骨架，必须打通 `alpha_signal -> position -> portfolio_plan` 的最小主线。
- 目标：完成三层 `position` 物化，并同步实现 `run_portfolio_plan_build`。
- 为什么现在做：这是阶段四“最小切换版 + 同步最小 bridge 实现”的核心交付。

## 2. 设计输入

- `docs/03-execution/19-position-contracts-schema-and-runner-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`
- `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`

## 4. 任务切片

1. 物化 `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot`。
2. 新增 `portfolio_plan` 三表与正式 runner。
3. 建立端到端 smoke 测试。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/position/`
- `src/astock_lifespan_alpha/portfolio_plan/`
- `scripts/portfolio_plan/run_portfolio_plan_build.py`
- 相关测试

范围外：

- `trade`
- 多组合治理

## 6. 收口标准

1. `position` 能从 `alpha_signal` 生成三层正式事实。
2. `portfolio_plan` 三表可以幂等初始化并完成最小 bridge。
3. 端到端 smoke 覆盖 `alpha_signal -> position -> portfolio_plan`。
