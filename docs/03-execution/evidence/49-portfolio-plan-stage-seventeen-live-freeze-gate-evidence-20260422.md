# 批次 49 证据：portfolio_plan 阶段十七 live freeze gate

证据编号：`49`
日期：`2026-04-22`
文档标识：`portfolio-plan-stage-seventeen-live-freeze-gate`

## 1. 阶段十七目标口径

`docs/02-spec/22-stage-seventeen-rolling-backtest-minimal-v1-spec-20260421.md` 已冻结以下 `portfolio_plan` 目标：

- 默认 `portfolio_gross_cap_weight = 0.50`
- 组合容量语义切到 `live active-cap accounting`
- `portfolio_plan_snapshot` 需要稳定表达：
  - `planned_entry_trade_date`
  - `scheduled_exit_trade_date`
  - `current_portfolio_gross_weight`
  - `remaining_portfolio_capacity_weight`
- 同日 full exit 后容量可重新分配给后续 candidate

## 2. 当前代码与单测 readiness

当前 `portfolio_plan` runner 已具备阶段十七前置条件：

- `run_portfolio_plan_build()` 默认值已经是 `portfolio_gross_cap_weight = 0.50`
- runner 已消费 `planned_entry_trade_date / scheduled_exit_trade_date`
- 现有单测已经覆盖：
  - checkpoint fast path
  - scheduled exit 后释放容量
  - rematerialize 失败回滚

回归结果：

```text
pytest tests/unit/portfolio_plan -q
6 passed
```

```text
pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed
```

```text
pytest tests/unit/docs/test_portfolio_plan_specs.py -q
2 passed
```

```text
pytest -q
105 passed
```

## 3. 正式库 preflight

正式 `portfolio_plan.duckdb` 已具备表族：

- `portfolio_plan_run`
- `portfolio_plan_snapshot`
- `portfolio_plan_run_snapshot`
- `portfolio_plan_work_queue`
- `portfolio_plan_checkpoint`

最新正式 run 摘要：

- `run_id = portfolio-plan-bd3a42d2fafe`
- `status = completed`
- `portfolio_gross_cap_weight = 0.15`
- `bounded_candidate_count = 5892934`
- `admitted_count = 1`
- `blocked_count = 5892932`
- `trimmed_count = 1`
- `message = portfolio_plan run completed.`

正式表计数：

- `portfolio_plan_run = 9`
- `portfolio_plan_snapshot = 5892934`
- `portfolio_plan_run_snapshot = 53036406`
- `portfolio_plan_work_queue = 1`
- `portfolio_plan_checkpoint = 1`

`portfolio_plan_snapshot` 已存在阶段十七关键字段：

- `planned_entry_trade_date`
- `scheduled_exit_trade_date`
- `current_portfolio_gross_weight`
- `remaining_portfolio_capacity_weight`

当前 `plan_status` 聚合：

- `blocked = 5892932`
- `admitted = 1`
- `trimmed = 1`

## 4. 证据裁决

当前证据表明：

- `portfolio_plan` 代码与单测已经接近阶段十七口径
- 正式库仍停在 `portfolio_gross_cap_weight = 0.15` 的旧 run
- 因此本轮首先要完成 live gate 与正式重跑，不能直接判 `放行`
