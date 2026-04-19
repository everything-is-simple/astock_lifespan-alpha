# 阶段三批次 11 Alpha 契约与 Schema 执行卡

卡片编号：`11`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：`alpha` 当前仍停留在 foundation stub，缺少正式契约和数据库结构。
- 目标：建立 `alpha` 的共享契约层和六类账本 schema。
- 为什么现在做：没有统一契约和表结构，后续 trigger 与 `alpha_signal` 无法共用骨架。

## 2. 设计输入

- `docs/03-execution/10-stage-three-document-closeout-card-20260419.md`
- `docs/03-execution/09-alpha-signal-aggregation-spec-freeze-card-20260419.md`

## 3. 规格输入

- `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
- `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`

## 4. 任务切片

1. 固定 trigger 类型、状态和正式摘要类型。
2. 建立五个 trigger 账本 schema。
3. 建立 `alpha_signal` 独立 schema。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/alpha/`
- Alpha 相关测试

范围外：

- Trigger 细则实现
- `position` 切换

## 6. 收口标准

1. 六个 alpha runner 保留原入口名。
2. Trigger DB 与 `alpha_signal` DB 都能幂等初始化。
3. Alpha runner 不再返回 foundation stub。
