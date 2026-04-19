# 阶段二批次 05 MALF 语义引擎与 Runner 执行卡

卡片编号：`05`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：MALF 还没有真正把事实层 OHLC bars 物化成正式语义账本。
- 目标：实现共享语义状态机和 `day / week / month` runner。
- 为什么现在做：没有正式 runner，阶段二仍停留在文档层。

## 2. 设计输入

- `docs/03-execution/04-malf-contracts-and-schema-card-20260419.md`
- `docs/03-execution/02-malf-semantic-spec-freeze-card-20260419.md`

## 3. 规格输入

- `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
- `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`

## 4. 任务切片

1. 从事实层加载日线 bars，并为周/月提供聚合适配。
2. 以共享状态机物化 pivot、wave、state 三类正式账本。
3. 接入 work_queue 与 checkpoint，保证三周期互不污染。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/malf/`
- MALF 相关测试与脚本验证

范围外：

- `alpha_signal`
- `position` 切换
- 交易与系统层

## 6. 收口标准

1. 三周期 runner 都能完成 source -> queue -> replay -> ledger -> checkpoint。
2. 语义测试覆盖 `HH / LL` 计数、`HL / LH` 不计数、break、reborn。
3. 重跑不会在同一周期内产生重复快照。
