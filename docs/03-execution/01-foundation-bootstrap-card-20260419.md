# 阶段一基础重构执行卡

卡片编号：`01`
日期：`2026-04-19`
状态：`已补记`

## 1. 需求

- 问题：仓库已经完成第一阶段基础重构，但几乎没有正式执行留痕，当前文档也没有形成 `design -> spec -> execution` 的治理闭环。
- 目标：把已经落地的第一阶段基础设施补成正式、可追溯、全中文的执行批次。
- 为什么现在做：如果不先补齐阶段一，后续 `MALF` 与 `alpha` 的正式实现会继续建立在口头共识和零散改动之上，后面无法审计。

## 2. 设计输入

- `docs/01-design/00-foundation-bootstrap-design-notes-20260419.md`
- `docs/01-design/01-doc-governance-charter-20260419.md`

## 3. 规格输入

- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
- `docs/02-spec/01-doc-governance-and-execution-spec-20260419.md`

## 4. 任务切片

1. 复原第一阶段实际交付范围，明确哪些代码已经存在。
2. 建立中文文档治理总入口与执行纪律。
3. 为第一阶段补齐 `card / evidence / record / conclusion` 闭环。
4. 把现有英文占位文档改写为中文正式文档或中文占位说明。

## 5. 实现边界

范围内：

- `README.md`
- `docs/`
- 对现有代码、测试、脚本的事实复盘与引用

范围外：

- 变更 `src/` 下任何业务逻辑
- 新增 `MALF` 语义实现
- 新增 `alpha` 触发器实现

## 6. 执行约束

- 本次属于“治理恢复 + 阶段一补记”，允许回填已发生的执行事实。
- 结论必须基于当前代码与实际测试，而不是基于回忆。
- 所有正式文档正文必须为中文。

## 7. 收口标准

1. `docs/README.md` 能解释当前治理结构。
2. `03-execution` 拥有模板、纪律、结论目录与子目录约束。
3. 阶段一拥有完整 `evidence / record / conclusion`。
4. 文档内容与当前代码、测试结果一致。
