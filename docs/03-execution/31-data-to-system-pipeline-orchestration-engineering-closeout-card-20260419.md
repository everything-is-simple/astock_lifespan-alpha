# 阶段八批次 31 data -> system pipeline 编排工程收口执行卡

卡片编号：`31`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段八 `stage-eight-pipeline` 规格已冻结且 pipeline 工程实现已完成，需要正式治理收口。
- 目标：裁决阶段八 `data -> system` 最小 pipeline orchestration 已完成，并把仓库状态切换为“阶段八完成，下一阶段待规划”。
- 为什么现在做：避免工程实现已落地但文档治理仍停留在“工程待实施”。

## 2. 规格输入

- `docs/02-spec/13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md`
- `docs/03-execution/30-data-to-system-pipeline-orchestration-spec-freeze-conclusion-20260419.md`

## 3. 任务切片

1. 新增批次 `31` 的 card/evidence/record/conclusion。
2. 更新 README、docs 索引与结论目录。
3. 新增 docs 测试锁定阶段八完成结论。
4. 验证 docs 测试与全量测试。

## 4. 收口标准

1. `run_data_to_system_pipeline` 已实现。
2. `pipeline_run / pipeline_step_run` 已落地。
3. pipeline 只调用 public runner，不直接写业务表。
4. pipeline 测试、contracts 测试、docs 测试与全量测试通过。
5. 阶段八完成结论已登记，下一阶段待规划。

