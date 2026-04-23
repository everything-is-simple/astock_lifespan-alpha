# 批次 56 卡片：pipeline live freeze gate

卡片编号：`56`
日期：`2026-04-23`
文档标识：`pipeline-live-freeze-gate`

## 目标

在 Card 54 已确认 `trade = 放行`、Card 55 已确认 `system = 放行` 的前提下，只处理 `pipeline` 的正式 live freeze gate。

本轮目标固定为：

- 验收 `pipeline` orchestration 能消费当前已放行的业务模块输出。
- 验收正式 `run_data_to_system_pipeline` 能完成 `data -> malf -> alpha -> position -> portfolio_plan -> trade -> system` 的 13 步编排。
- 验收 `pipeline_run / pipeline_step_run / pipeline_step_checkpoint` 正式账本切到本轮最新 run。
- 不反向修改 `trade`。
- 不反向修改 `system`。
- 不用 `system` 放行结论反推 `pipeline` 放行。

## 验收口径

- 最新 `pipeline_run.status = completed`
- `pipeline_run.step_count = 13`
- `pipeline_run.pipeline_contract_version = stage8_pipeline_v1`
- `pipeline_step_run` 顺序固定为：
  - `run_malf_day_build`
  - `run_malf_week_build`
  - `run_malf_month_build`
  - `run_alpha_bof_build`
  - `run_alpha_tst_build`
  - `run_alpha_pb_build`
  - `run_alpha_cpb_build`
  - `run_alpha_bpb_build`
  - `run_alpha_signal_build`
  - `run_position_from_alpha_signal`
  - `run_portfolio_plan_build`
  - `run_trade_from_portfolio_plan`
  - `run_system_from_trade`
- step 12 `run_trade_from_portfolio_plan` 必须 `completed`
- step 13 `run_system_from_trade` 必须 `completed`
- `pipeline_step_checkpoint` 对 `portfolio_id = core` 覆盖 step `1..13`
- `pipeline_step_checkpoint.last_pipeline_run_id` 全部切到本轮最新 `pipeline` run
- `pipeline` 继续只写 pipeline 自身账本，不直接写业务表

## 本轮边界

- 只处理 `pipeline` gate 与治理文档。
- 不修复 `trade` / `system` 代码。
- 不修改业务模块 schema repair。
- 不扩展 pipeline runner 顺序或业务语义。
- 若 live gate 暴露业务模块 blocker，本卡只登记阻塞，并将修复移交到对应业务模块卡。
