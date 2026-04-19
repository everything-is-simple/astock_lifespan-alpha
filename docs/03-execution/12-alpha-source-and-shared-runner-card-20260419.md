# 阶段三批次 12 Alpha 输入适配与共享骨架执行卡

卡片编号：`12`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：五个 trigger 如果各自独立拼装输入和生命周期，会快速分叉。
- 目标：建立统一输入适配层和共享 runner 生命周期骨架。
- 为什么现在做：阶段三必须把共同问题收敛在一层，而不是复制五份实现。

## 2. 设计输入

- `docs/03-execution/11-alpha-contracts-and-schema-card-20260419.md`
- `docs/03-execution/08-alpha-pas-trigger-spec-freeze-card-20260419.md`

## 3. 规格输入

- `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
- `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`

## 4. 任务切片

1. 统一读取 `market_base_day + malf_day.malf_wave_scale_snapshot`。
2. 固定 symbol/date 对齐与 checkpoint 比较规则。
3. 提供所有 alpha runner 复用的 queue/replay 骨架。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/alpha/`
- Alpha 相关测试

范围外：

- Trigger 语义裁决以外的下游逻辑

## 6. 收口标准

1. 五个 trigger 共用同一输入适配层。
2. Checkpoint 与 replay 机制统一。
3. 共享骨架不绑定任何单个 trigger 语义。
