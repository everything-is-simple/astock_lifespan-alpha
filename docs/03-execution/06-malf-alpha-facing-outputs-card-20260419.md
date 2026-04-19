# 阶段二批次 06 MALF 面向 Alpha 输出执行卡

卡片编号：`06`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：即使语义账本建成，若没有正式 snapshot/profile 读模，`alpha` 仍无法稳定接入。
- 目标：物化 `malf_wave_scale_snapshot` 与 `malf_wave_scale_profile`。
- 为什么现在做：阶段二的收口标准就是给 `alpha` 提供稳定读模型。

## 2. 设计输入

- `docs/03-execution/05-malf-engine-and-runner-card-20260419.md`
- `docs/03-execution/03-malf-diagram-spec-freeze-card-20260419.md`

## 3. 规格输入

- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`

## 4. 任务切片

1. 输出对齐正式字段集的 `malf_wave_scale_snapshot`。
2. 计算同标的 / 同周期 / 同方向样本口径下的 rank。
3. 生成四区 `wave_position_zone` 并写入 profile。

## 5. 实现边界

范围内：

- `malf_wave_scale_snapshot`
- `malf_wave_scale_profile`
- 对应测试

范围外：

- `alpha` 账本实现
- 下游消费逻辑切换

## 6. 收口标准

1. `alpha` 所需最小字段集全部物化。
2. `update_rank / stagnation_rank` 与四区映射可被测试验证。
3. `alpha` 后续只需读取 `malf_wave_scale_snapshot` 即可进入下一阶段。
