# 批次 46 证据：阶段十六正式增量与自动续跑工程收口

证据编号：`46`
日期：`2026-04-21`
文档标识：`stage-sixteen-incremental-resume-engineering-closeout`

## 1. 代码与测试落地

- 新增 `portfolio_plan_work_queue / portfolio_plan_checkpoint`
- 新增 `system_work_queue / system_checkpoint`
- 新增 `pipeline_step_checkpoint`
- 新增 repair CLI：
  - `scripts/portfolio_plan/repair_portfolio_plan_schema.py`
  - `scripts/system/repair_system_schema.py`
  - `scripts/pipeline/repair_pipeline_schema.py`
- 新增与扩展测试：
  - `tests/unit/portfolio_plan/test_portfolio_plan_runner.py`
  - `tests/unit/system/test_system_runner.py`
  - `tests/unit/pipeline/test_pipeline_runner.py`
  - `tests/unit/contracts/test_runner_contracts.py`
  - `tests/unit/docs/test_stage_sixteen_incremental_specs.py`

## 2. repair CLI 真实 proof

### `repair_portfolio_plan_schema`

```json
{
  "runner_name": "repair_portfolio_plan_schema",
  "status": "completed",
  "target_path": "H:\\Lifespan-data\\astock_lifespan_alpha\\portfolio_plan\\portfolio_plan.duckdb",
  "checkpoint_rows_backfilled": 1
}
```

### `repair_system_schema`

```json
{
  "runner_name": "repair_system_schema",
  "status": "completed",
  "target_path": "H:\\Lifespan-data\\astock_lifespan_alpha\\system\\system.duckdb",
  "checkpoint_rows_backfilled": 5497
}
```

### `repair_pipeline_schema`

```json
{
  "runner_name": "repair_pipeline_schema",
  "status": "completed",
  "target_path": "H:\\Lifespan-data\\astock_lifespan_alpha\\pipeline\\pipeline.duckdb",
  "checkpoint_rows_backfilled": 13
}
```

## 3. portfolio_plan / system 真实 proof

### `run_portfolio_plan_build`

首轮：

```json
{
  "run_id": "portfolio-plan-40a4ac919683",
  "status": "completed",
  "snapshot_rows": 5892934,
  "admitted_count": 1,
  "blocked_count": 5892932,
  "trimmed_count": 1,
  "work_units_seen": 1,
  "work_units_updated": 1,
  "latest_reference_trade_date": "2026-04-10"
}
```

复跑：

```json
{
  "run_id": "portfolio-plan-690c1d907cbe",
  "status": "completed",
  "snapshot_rows": 5892934,
  "work_units_seen": 1,
  "work_units_updated": 0,
  "latest_reference_trade_date": "2026-04-10"
}
```

### `run_system_from_trade`

首轮：

```json
{
  "run_id": "system-85dc5a2a8004",
  "status": "completed",
  "readout_rows": 5892934,
  "summary_rows": 1,
  "work_units_seen": 5497,
  "work_units_updated": 0,
  "latest_execution_trade_date": "2026-04-10"
}
```

复跑：

```json
{
  "run_id": "system-d1da93169a6a",
  "status": "completed",
  "readout_rows": 5892934,
  "summary_rows": 1,
  "work_units_seen": 5497,
  "work_units_updated": 0,
  "latest_execution_trade_date": "2026-04-10"
}
```

## 4. pipeline 双重 proof

先治理 orphan：

```json
{
  "run_id": "pipeline-cb3824690208",
  "before_status": "running",
  "after_status": "interrupted",
  "after_step_count": 0
}
```

第 1 次 proof：

```json
{
  "run_id": "pipeline-9f4b9d2af256",
  "status": "completed",
  "step_count": 13,
  "resumed_from_run_id": "pipeline-cb3824690208",
  "resume_start_step": 1,
  "reused_step_count": 0,
  "executed_step_count": 13
}
```

第 2 次 proof：

```json
{
  "run_id": "pipeline-e401bf172a23",
  "status": "completed",
  "step_count": 13,
  "resumed_from_run_id": null,
  "resume_start_step": null,
  "reused_step_count": 0,
  "executed_step_count": 13
}
```

proof 完成后最新 `pipeline_run` 序列为：

- `pipeline-e401bf172a23` `completed`
- `pipeline-9f4b9d2af256` `completed`
- `pipeline-cb3824690208` `interrupted`

## 5. 全量验证

```text
pytest
97 passed in 79.31s
```
