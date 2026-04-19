# 阶段二批次 02 MALF 文本规格冻结执行卡

卡片编号：`02`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：`MALF` 文本规格仍是占位稿，后续实现缺少正式中文真值契约。
- 目标：冻结 `MALF` 正式文本规格，清空占位状态。
- 为什么现在做：阶段二实现必须先有正式语义文本，不允许代码先猜规则。

## 2. 设计输入

- `docs/01-design/01-doc-governance-charter-20260419.md`
- `docs/03-execution/01-foundation-bootstrap-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
- `docs/02-spec/01-doc-governance-and-execution-spec-20260419.md`

## 4. 任务切片

1. 把占位稿替换为正式中文文本规格。
2. 明确 `HH / HL / LH / LL`、生命表达、guard、reborn 与三周期独立性。
3. 为核心规则补齐最小验收样例。

## 5. 实现边界

范围内：

- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`

范围外：

- 代码实现
- 图版整理
- `alpha` 接口变更

## 6. 收口标准

1. 文本规格状态从“占位”变为“冻结”。
2. 总方案第 6 章核心规则均有独立条款承接。
3. 至少覆盖 6 个最小验收样例。
