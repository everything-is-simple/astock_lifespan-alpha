# 阶段二批次 03 MALF 图版规格冻结执行卡

卡片编号：`03`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：仓库已有多份 MALF PDF 图稿，但还没有统一图版口径。
- 目标：冻结图版规格，并建立“文本条款 -> 图版位置”的对照规则。
- 为什么现在做：如果图版不先归一，代码会被迫在多份图稿间猜测。

## 2. 设计输入

- `docs/03-execution/02-malf-semantic-spec-freeze-card-20260419.md`
- `docs/03-execution/01-foundation-bootstrap-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`

## 4. 任务切片

1. 统一三份现有 PDF 图稿的权威顺序。
2. 建立图版必须表达的对象与图层。
3. 增加“文本条款 -> 图版位置”对照表。

## 5. 实现边界

范围内：

- `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`

范围外：

- PDF 重绘
- 代码实现
- `alpha` 读取实现

## 6. 收口标准

1. 图版状态从“占位”变为“冻结”。
2. 文本优先、图版回修的裁决顺序明确。
3. 每条核心文本规则都存在图版映射要求。
