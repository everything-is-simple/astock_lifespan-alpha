# 阶段三批次 08 Alpha PAS 触发器规格冻结执行卡

卡片编号：`08`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：仓库内还没有 `BOF / TST / PB / CPB / BPB` 的正式语义规格，阶段三实现没有权威输入。
- 目标：冻结五个 PAS trigger 的正式中文文本规格。
- 为什么现在做：阶段三要求先建文档，再实施；不允许 `alpha` 代码先猜语义。

## 2. 设计输入

- `docs/03-execution/07-stage-two-closeout-conclusion-20260419.md`
- `docs/03-execution/00-card-execution-discipline-20260419.md`

## 3. 规格输入

- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`

## 4. 任务切片

1. 固定阶段三唯一输入边界。
2. 定义五个 trigger 的正式语义、状态与唯一键。
3. 补齐最小验收样例。

## 5. 实现边界

范围内：

- `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`

范围外：

- `src/astock_lifespan_alpha/alpha/`
- `alpha_signal` 汇总逻辑

## 6. 收口标准

1. 五个 trigger 都有正式定义。
2. 输入边界、失效边界、唯一键和样例齐全。
3. 文档状态为“冻结”。
