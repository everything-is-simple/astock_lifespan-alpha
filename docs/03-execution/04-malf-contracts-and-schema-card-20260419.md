# 阶段二批次 04 MALF 契约与 Schema 执行卡

卡片编号：`04`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段一只提供 runner stub，尚无 MALF 正式类型契约和数据库结构。
- 目标：建立 `MALF` 的共享契约层与 DuckDB schema 初始化逻辑。
- 为什么现在做：没有统一契约，后续状态机和快照输出无法稳定。

## 2. 设计输入

- `docs/03-execution/02-malf-semantic-spec-freeze-card-20260419.md`
- `docs/03-execution/03-malf-diagram-spec-freeze-card-20260419.md`

## 3. 规格输入

- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
- `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`

## 4. 任务切片

1. 固定 `Timeframe / LifeState / WaveDirection / WavePositionZone`。
2. 升级 MALF runner 返回类型为正式运行摘要。
3. 建立 8 张 MALF 正式表的初始化逻辑。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/malf/`
- MALF 相关测试

范围外：

- `alpha / position` 公开接口变更
- 完整语义回放

## 6. 收口标准

1. 三周期数据库都能幂等初始化 8 张正式表。
2. MALF runner 返回正式运行摘要，而非 foundation stub。
3. 表名、字段与三周期路径隔离可被测试验证。
