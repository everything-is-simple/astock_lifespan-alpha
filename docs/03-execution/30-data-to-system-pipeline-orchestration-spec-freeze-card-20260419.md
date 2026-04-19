# 阶段八批次 30 data -> system pipeline 编排规格冻结执行卡

卡片编号：`30`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段七已经完成 data 源事实契约对齐，但尚无统一入口把 `data -> system` 主线串起来。
- 目标：冻结阶段八 `stage-eight-pipeline`，新增最小 pipeline orchestration 规格。
- 为什么现在做：全线打通应通过薄编排层实现，而不是把业务逻辑混入 `system` 或任一上游模块。

## 2. 设计输入

- `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`
- `docs/03-execution/29-data-source-fact-contract-alignment-engineering-closeout-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md`

## 4. 任务切片

1. 新增阶段八 `data -> system` 最小 pipeline 编排规格。
2. 新增批次 `30` 的 card/evidence/record/conclusion。
3. 更新 README、docs 索引与结论目录。
4. 新增 docs 测试锁定阶段八规格关键词。

## 5. 实现边界

范围内：

- docs 规格
- execution 治理闭环
- docs 索引
- docs 测试

范围外：

- pipeline 工程代码
- scheduler / 定时任务
- 外部服务
- pnl / exit / broker / partial fill

## 6. 收口标准

1. `stage-eight-pipeline` 规格状态为 `冻结`。
2. 文档明确 `run_data_to_system_pipeline`、`pipeline_run`、`pipeline_step_run`。
3. 文档明确固定 runner 顺序。
4. 文档明确 pipeline 不直接写业务表。
5. docs 测试与全量测试通过。

