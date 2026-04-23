# 批次 56 证据：pipeline live freeze gate

证据编号：`56`
日期：`2026-04-23`
文档标识：`pipeline-live-freeze-gate`

## 1. preflight

正式 preflight 确认：

- 工作区：`H:\astock_lifespan-alpha`
- 起始分支：`lifespan0.01/card55-system-live-freeze-gate`
- Card 56 分支：`lifespan0.01/card56-pipeline-live-freeze-gate`
- 起始 git 状态只有未跟踪 `.vscode/`
- 无 `run_data_to_system_pipeline.py` Python 进程
- `pipeline_run` 无 `running` 残留
- Card 56 前最新正式 pipeline run：
  - `pipeline-e401bf172a23`
  - `status = completed`
  - `step_count = 13`
  - `pipeline_contract_version = stage8_pipeline_v1`
  - step 12 = `trade-012abd340b1b`
  - step 13 = `system-c97d6c383908`
- Card 56 前 `pipeline_step_checkpoint`：
  - `count = 13`
  - `min(step_order) = 1`
  - `max(step_order) = 13`
  - `last_pipeline_run_id = pipeline-e401bf172a23`
- Card 54 已放行的正式 `trade` run：
  - `trade-558802e7f7a4`
  - `status = completed`
  - `work_units_seen = 5497`
  - `work_units_updated = 5497`
  - `inserted_order_executions = 5892934`
  - `inserted_exit_rows = 9434`
- Card 55 已放行的正式 `system` run：
  - `system-080b8ac3bf8d`
  - `status = completed`
  - `readout_rows = 5902368`
  - `summary_rows = 1`

## 2. 局部 gate

本轮先执行 pipeline 与模块边界局部测试：

```text
pytest tests/unit/pipeline tests/unit/contracts/test_module_boundaries.py -q
9 passed in 17.44s
```

该结果确认：

- `pipeline` runner 单测通过。
- `pipeline` step resume / checkpoint 单测通过。
- business modules 仍不反向依赖 `pipeline`。
- `system` 仍不调用上游 runner。
- `pipeline` 仍不直接写业务表。

## 3. live gate

正式执行：

- CLI：`python scripts/pipeline/run_data_to_system_pipeline.py`
- run：`pipeline-88b35c7e6e8a`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\pipeline\pipeline-live-card56-20260423-123054.stdout.log`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\pipeline\pipeline-live-card56-20260423-123054.stderr.log`

关键 stderr phase：

- `trade phase source_attached elapsed_seconds=0.000000 rows=5892934 work_units=5497`
- `trade phase work_unit_summary_ready elapsed_seconds=4.804347 rows=5497 source_rows=5892934`
- `trade phase action_tables_ready elapsed_seconds=0.000000 rows=5497 fast_path_reused=1`
- `trade phase write_transaction_started elapsed_seconds=0.000000 rows=5497 work_units_updated=0 fast_path=1`
- `trade phase write_transaction_committed elapsed_seconds=97.776957 rows=5497 work_units_updated=0 fast_path=1`
- `system phase source_attached elapsed_seconds=5.330717 rows=5902368 work_units=5497`
- `system phase work_unit_summary_ready elapsed_seconds=10.792758 rows=5497 source_rows=5902368`
- `system phase write_reused_tracking_committed elapsed_seconds=0.173374 rows=5497 work_units_updated=0`
- `system phase system_run_completed elapsed_seconds=0.184377 rows=1 readout_rows=5902368`

stdout summary：

- `runner_name = run_data_to_system_pipeline`
- `run_id = pipeline-88b35c7e6e8a`
- `status = completed`
- `target_path = H:\Lifespan-data\astock_lifespan_alpha\pipeline\pipeline.duckdb`
- `portfolio_id = core`
- `step_count = 13`
- `message = pipeline run completed.`
- `resume_summary.resumed_from_run_id = null`
- `resume_summary.resume_start_step = null`
- `resume_summary.reused_step_count = 0`
- `resume_summary.executed_step_count = 13`

## 4. step 验收

正式 `pipeline_step_run` 回查结果：

| step | runner | run | status |
| --- | --- | --- | --- |
| 1 | `run_malf_day_build` | `day-a1c965e1f7a9` | `completed` |
| 2 | `run_malf_week_build` | `week-89df644f84f1` | `completed` |
| 3 | `run_malf_month_build` | `month-a5d23ecb1144` | `completed` |
| 4 | `run_alpha_bof_build` | `bof-2bfa3f351665` | `completed` |
| 5 | `run_alpha_tst_build` | `tst-c986697a870d` | `completed` |
| 6 | `run_alpha_pb_build` | `pb-172c010e4ba2` | `completed` |
| 7 | `run_alpha_cpb_build` | `cpb-abede9a0e185` | `completed` |
| 8 | `run_alpha_bpb_build` | `bpb-aff8057c665c` | `completed` |
| 9 | `run_alpha_signal_build` | `alpha-signal-a16700405abf` | `completed` |
| 10 | `run_position_from_alpha_signal` | `position-c2c3b1d40a52` | `completed` |
| 11 | `run_portfolio_plan_build` | `portfolio-plan-3ba8c89e0472` | `completed` |
| 12 | `run_trade_from_portfolio_plan` | `trade-594d80dfdf1d` | `completed` |
| 13 | `run_system_from_trade` | `system-7d34ce3dad1f` | `completed` |

下游消费结果：

- step 12 `trade-594d80dfdf1d`
  - `status = completed`
  - `work_units_seen = 5497`
  - `work_units_updated = 0`
  - `fast_path = 1`
  - `executions_reused = 5892934`
  - `exit_rows_reused = 9434`
- step 13 `system-7d34ce3dad1f`
  - `status = completed`
  - `readout_rows = 5902368`
  - `summary_rows = 1`
  - `work_units_seen = 5497`
  - `work_units_updated = 0`

## 5. 正式库验收

正式 `pipeline.duckdb` 只读回查结果：

- 最新 run：`pipeline-88b35c7e6e8a`
- `pipeline_run.status = completed`
- `pipeline_run.portfolio_id = core`
- `pipeline_run.step_count = 13`
- `pipeline_run.pipeline_contract_version = stage8_pipeline_v1`
- `pipeline_run.message = pipeline run completed.`
- `pipeline_step_checkpoint`：
  - `count = 13`
  - `min(step_order) = 1`
  - `max(step_order) = 13`
  - `COUNT(DISTINCT last_pipeline_run_id) = 1`
  - `last_pipeline_run_id = pipeline-88b35c7e6e8a`
- 无残留 `pipeline_run.status = running`
- 无残留 `run_data_to_system_pipeline.py` Python 进程

## 6. 证据裁决

本地验证结果：

```text
pytest tests/unit/pipeline tests/unit/contracts/test_module_boundaries.py -q
9 passed in 17.44s

pytest tests/unit/docs/test_pipeline_specs.py -q
3 passed in 0.05s

pytest
117 passed in 76.22s (0:01:16)
```

Card 56 已完成 `pipeline` live freeze gate，并通过正式 live gate。

因此：

- `pipeline = 放行`
- 本轮不升级或重判 `trade / system`
- 下一轮可回到主线冻结治理面板，决定是否把已放行链路升级为更高层级冻结裁决
