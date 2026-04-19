# 阶段十批次 34 MALF day 真实库诊断规格冻结执行卡

卡片编号：`34`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段九已在真实库 `run_malf_day_build` 首步暴露阻塞，但尚未形成正式诊断规格。
- 目标：冻结阶段十 `stage-ten-malf-day-diagnosis` 规格，明确入口修正、只读诊断和不改 MALF 业务语义的边界。
- 为什么现在做：真实库阻塞已经出现，下一步必须先把诊断范围写清，而不是直接扩大到 Go+DuckDB 或全链路重演。

## 2. 设计输入

- `docs/02-spec/14-real-data-build-rehearsal-spec-v1-20260419.md`
- `docs/03-execution/33-real-data-build-rehearsal-closeout-conclusion-20260419.md`

## 3. 规格输出

- `docs/02-spec/15-malf-day-real-data-diagnosis-spec-v1-20260419.md`

## 4. 任务切片

1. 新增阶段十 MALF day 真实库诊断规格。
2. 新增批次 `34` 的 card/evidence/record/conclusion。
3. 更新 README、docs 索引与结论目录。
4. 新增 docs 测试锁定阶段十诊断边界。

## 5. 收口标准

1. `stage-ten-malf-day-diagnosis` 状态为 `冻结`。
2. 文档登记 `stock_daily_adjusted`、`PYTHONPATH`、`source load timing / engine timing / write timing`。
3. 文档明确不修改 MALF 业务语义。
4. 文档明确阶段九重演待重新发起。
