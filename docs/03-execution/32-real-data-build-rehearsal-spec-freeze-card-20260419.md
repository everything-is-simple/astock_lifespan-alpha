# 阶段九批次 32 真实建库演练规格冻结执行卡

卡片编号：`32`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段八已经完成 `data -> system` 最小 pipeline 编排，但尚未验证现有 Python+DuckDB 主线能否在真实 `H:\Lifespan-data` 上跑通。
- 目标：冻结阶段九 `stage-nine-real-data-build` 规格，明确真实建库演练顺序、真实输入输出目录与验收口径。
- 为什么现在做：真实建库属于高成本执行动作，必须先把写入边界、验证顺序和 blocker 处理方式写成正式规格。

## 2. 设计输入

- `docs/02-spec/13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md`
- `docs/03-execution/31-data-to-system-pipeline-orchestration-engineering-closeout-conclusion-20260419.md`

## 3. 规格输出

- `docs/02-spec/14-real-data-build-rehearsal-spec-v1-20260419.md`

## 4. 任务切片

1. 新增阶段九真实建库演练规格。
2. 新增批次 `32` 的 card/evidence/record/conclusion。
3. 更新 README、docs 索引与结论目录。
4. 新增 docs 测试锁定阶段九规格边界。

## 5. 实施边界

范围内：

- docs 规格
- execution 治理闭环
- docs 索引
- docs 测试

范围外：

- 真实建库执行
- source/schema 修复
- Go+DuckDB 新工程
- scheduler / 外部服务 / pnl / exit / broker/session / partial fill

## 6. 收口标准

1. `stage-nine-real-data-build` 规格状态为 `冻结`。
2. 文档明确 `H:\Lifespan-data`、`H:\Lifespan-data\astock_lifespan_alpha`、`module-by-module build` 与 `pipeline replay`。
3. 文档明确 `run_data_to_system_pipeline` 是最后 replay 入口。
4. 文档明确 `Go+DuckDB deferred`。
5. docs 测试与全量测试通过。
