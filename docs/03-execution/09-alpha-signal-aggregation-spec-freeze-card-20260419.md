# 阶段三批次 09 alpha_signal 汇总规格冻结执行卡

卡片编号：`09`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：即使五个 trigger 有了正式定义，也缺少统一汇总账本的正式读取规则。
- 目标：冻结 `alpha_signal` 的字段、状态、追溯关系和汇总规则。
- 为什么现在做：如果不先冻结 `alpha_signal`，实施阶段会把聚合规则写进代码猜测。

## 2. 设计输入

- `docs/03-execution/08-alpha-pas-trigger-spec-freeze-card-20260419.md`
- `docs/03-execution/07-stage-two-closeout-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`

## 4. 任务切片

1. 冻结 `formal_signal_status` 口径。
2. 冻结 `signal_nk` 与追溯关系。
3. 冻结五类 trigger 到 `alpha_signal` 的汇总规则。

## 5. 实现边界

范围内：

- `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`

范围外：

- `src/astock_lifespan_alpha/alpha/`
- `position` 接口切换

## 6. 收口标准

1. `alpha_signal` 最小字段集齐全。
2. 汇总规则、状态和追溯关系明确冻结。
3. 文档状态为“冻结”。
