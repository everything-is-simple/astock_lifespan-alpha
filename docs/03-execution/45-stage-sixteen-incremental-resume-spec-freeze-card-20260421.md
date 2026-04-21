# 批次 45 卡片：阶段十六正式增量与自动续跑规格冻结

卡片编号：`45`
日期：`2026-04-21`
文档标识：`stage-sixteen-incremental-resume`

## 目标

冻结 `portfolio_plan / system / pipeline` 的正式增量与自动续跑规格，使主线从“已打通”升级到“具备正式每日增量契约”的最后一段进入可实施状态。

## 范围

- `portfolio_plan`：`portfolio_id` 级 checkpoint / work_queue / fast path / interrupted
- `system`：`portfolio_id + symbol` 级 checkpoint / work_queue / fast path / interrupted
- `pipeline`：`pipeline_step_checkpoint`、step 级 interrupted / resume
- repair CLI、contracts tests、docs tests、真实 proof 与执行闭环
