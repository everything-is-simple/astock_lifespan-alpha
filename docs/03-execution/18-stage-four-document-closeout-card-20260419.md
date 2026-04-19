# 阶段四批次 18 文档总收口执行卡

卡片编号：`18`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段四必须先把 `15-17` 全部文档建完，才能进入任何 `position / portfolio_plan` 实施。
- 目标：完成阶段四文档准入面总收口，并把索引状态切到“规格冻结完成，工程待启动”。
- 为什么现在做：这是阶段四实施的治理前置条件。

## 2. 设计输入

- `docs/03-execution/15-alpha-signal-to-position-bridge-spec-freeze-conclusion-20260419.md`
- `docs/03-execution/16-position-minimal-ledger-and-runner-spec-freeze-conclusion-20260419.md`
- `docs/03-execution/17-portfolio-plan-minimal-bridge-spec-freeze-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/05-alpha-signal-to-position-bridge-spec-v1-20260419.md`
- `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`
- `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`

## 4. 任务切片

1. 补齐阶段四文档闭环。
2. 更新阅读入口和执行区索引。
3. 明确阶段四实施的前置条件已经满足。

## 5. 实现边界

范围内：

- `docs/README.md`
- `docs/03-execution/README.md`
- `docs/03-execution/00-conclusion-catalog-20260419.md`
- 根 `README.md`

范围外：

- `position` 与 `portfolio_plan` 代码

## 6. 收口标准

1. 阶段四文档准入面完整。
2. 索引层能明确看到 `15-17` 已冻结。
3. 直到本批次完成前，不允许进入阶段四代码实施。
