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

## 2. bounded real-data replay

bounded replay 固定使用临时 harness，不新增仓内脚本。

临时工作区：

- `H:\Lifespan-temp\astock_lifespan_alpha\portfolio_plan_freeze49_probe_015`
- `H:\Lifespan-temp\astock_lifespan_alpha\portfolio_plan_freeze49_probe_05`

观测窗口：

- `planned_entry_trade_date = 1991-09-10 -> 1991-09-20`

subset 规则：

- 所有 `planned_entry_trade_date` 落在窗口内的 candidate
- 加上窗口开始日前已开仓、且在窗口内仍 active 的 candidate

对照组 `cap = 0.15`：

```json
{
  "snapshot_rows": 57,
  "admitted_count": 27,
  "blocked_count": 18,
  "trimmed_count": 12,
  "latest_reference_trade_date": "1991-09-19"
}
```

`1991-09-16` 当天：

- `6 admitted`
- `2 trimmed`
- `2 blocked`

目标组 `cap = 0.50`：

```json
{
  "snapshot_rows": 57,
  "admitted_count": 39,
  "blocked_count": 18,
  "trimmed_count": 0,
  "latest_reference_trade_date": "1991-09-19"
}
```

`1991-09-16` 当天：

- `8 admitted`
- `0 trimmed`
- `2 blocked`

关键观测：

- 同一窗口下，`0.50` 相比 `0.15` 明显减少容量截断
- `1991-09-16` 的 `current_portfolio_gross_weight` 与 `remaining_portfolio_capacity_weight` 已能反映“同日 exit 后释放容量”
- 这证明阶段十七 `live active-cap accounting` 语义在 bounded replay 中成立

## 3. 当前代码与单测 readiness

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

## 4. 正式库 preflight

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

preflight 时 `portfolio_plan_snapshot` 仍缺阶段十七关键字段：

- `planned_entry_trade_date`
- `scheduled_exit_trade_date`
- `planned_exit_reason_code`
- `current_portfolio_gross_weight`
- `remaining_portfolio_capacity_weight`

当前 `plan_status` 聚合：

- `blocked = 5892932`
- `admitted = 1`
- `trimmed = 1`

## 5. schema repair 与 live cutover

schema repair：

```json
{
  "runner_name": "repair_portfolio_plan_schema",
  "status": "completed",
  "target_path": "H:\\Lifespan-data\\astock_lifespan_alpha\\portfolio_plan\\portfolio_plan.duckdb",
  "checkpoint_rows_backfilled": 0
}
```

repair 后正式库已补出：

- `planned_entry_trade_date`
- `scheduled_exit_trade_date`
- `planned_exit_reason_code`
- `current_portfolio_gross_weight`
- `remaining_portfolio_capacity_weight`

live long-run：

- CLI：`python scripts/portfolio_plan/run_portfolio_plan_build.py`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-repair-20260422-124041.stdout.log`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-repair-20260422-124041.stderr.log`
- 最新 run：`portfolio-plan-21b6ab8747f7`

live 观察结果：

- 进程开始后长期持锁 `portfolio_plan.duckdb`
- 期间日志始终为空文件
- CPU 时间持续增长，但读写计数不再推进
- 最终按 `stall / timeout` 口径人工中断，并将 run 改写为 `interrupted`

中断后正式库状态：

- 最新 run：`portfolio-plan-21b6ab8747f7`
- `status = interrupted`
- `portfolio_gross_cap_weight = 0.50`
- `admitted_count = 0`
- `blocked_count = 0`
- `trimmed_count = 0`
- `message = portfolio_plan run interrupted after live freeze gate timeout/stall.`

中断后正式业务结果仍未切换：

- `plan_status` 仍是 `blocked = 5892932`、`admitted = 1`、`trimmed = 1`
- `portfolio_plan_checkpoint.last_run_id` 仍是 `portfolio-plan-bd3a42d2fafe`
- `planned_entry_trade_date / scheduled_exit_trade_date / planned_exit_reason_code` 虽已补列，但当前正式快照仍全部为 `NULL`

## 6. 证据裁决

当前证据表明：

- `portfolio_plan` 的阶段十七语义已经被 bounded replay 证明
- live schema repair 已完成
- 正式 `0.50` rerun 未完成 cutover，而是停在 `interrupted`
- 当前正式结果仍由旧 `0.15` run 主导

因此本轮不能判 `放行`，`portfolio_plan` 继续维持 `待修`
