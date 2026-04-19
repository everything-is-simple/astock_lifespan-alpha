# 阶段四批次 15 alpha_signal -> position 桥接规格冻结执行卡

卡片编号：`15`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段三已经有 `alpha_signal`，但阶段四还没有新仓版本的 `alpha_signal -> position` 正式桥接合同。
- 目标：冻结 `position` 首版只认哪些 `alpha_signal` 字段，以及哪些旧 admission 字段明确不回引。
- 为什么现在做：如果桥接合同不先冻结，后续 `position` 实施会再次依赖旧仓口径。

## 2. 设计输入

- `docs/03-execution/14-stage-three-closeout-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`

## 4. 任务切片

1. 冻结 `alpha_signal -> position` 最小字段组。
2. 冻结字段职责边界。
3. 冻结旧 admission 字段不回引的禁止项。

## 5. 实现边界

范围内：

- 阶段四桥接规格文档
- `15` 的执行闭环

范围外：

- `position` 代码实现
- `portfolio_plan` 代码实现

## 6. 收口标准

1. `position` 的唯一正式上游被写成 `alpha_signal`。
2. 阶段四字段组与禁止项被明确冻结。
3. 本批次只建文档，不改 `src/astock_lifespan_alpha/position/`。
