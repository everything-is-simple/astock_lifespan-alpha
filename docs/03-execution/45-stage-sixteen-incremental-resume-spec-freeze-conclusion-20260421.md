# 批次 45 结论：阶段十六正式增量与自动续跑规格冻结

结论编号：`45`
日期：`2026-04-21`
文档标识：`stage-sixteen-incremental-resume`

## 裁决

`已接受，进入工程实施`

## 结论

阶段十六正式冻结以下增量契约：

- `portfolio_plan` 采用 `portfolio_id` 级 checkpoint / work_queue / reused fast path
- `system` 采用 `portfolio_id + symbol` 级 checkpoint / work_queue / selective rematerialize
- `pipeline` 采用 `pipeline_step_checkpoint` 与 `interrupted -> resume_start_step` 自动续跑

冻结边界同时确认：

- 不改 runner 名称
- 不改主线顺序
- 不改 `portfolio_plan` 组合容量累计裁决语义
- `interrupted` 只进入 run ledger，不改变成功返回的 public summary 状态
