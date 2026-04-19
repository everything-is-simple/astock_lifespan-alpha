# 阶段七批次 28 data 源事实契约规格冻结执行卡

卡片编号：`28`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段六完成后，下一步若直接做 `data -> system` 编排，会因真实本地 source fact 库路径、表名和字段名未进入契约而无法稳定读取数据。
- 目标：冻结阶段七 `stage-seven-data-source-contract`，先对齐 6 个本地 DuckDB、stock 表名和字段映射。
- 为什么现在做：全线编排前必须先保证各模块 source adapter 能读取真实本地 stock 数据。

## 2. 设计输入

- `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`

## 3. 规格输入

- `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`

## 4. 任务切片

1. 新增阶段七 data 源事实契约规格。
2. 新增批次 `28` 的 card/evidence/record/conclusion。
3. 更新 README、docs 索引与结论目录。
4. 新增 docs 测试锁定阶段七规格关键词。

## 5. 实现边界

范围内：

- docs 规格
- execution 治理闭环
- docs 索引
- docs 测试

范围外：

- source adapter 工程实现
- `data -> system` 编排
- index / block
- raw ingest

## 6. 收口标准

1. `stage-seven-data-source-contract` 规格状态为 `冻结`。
2. 文档登记 6 个本地 DuckDB source fact 路径。
3. 文档冻结 stock-only 首版范围。
4. 文档冻结 `stock_*_adjusted` 表名与 `code -> symbol` 字段映射。
5. docs 测试与全量测试通过。

