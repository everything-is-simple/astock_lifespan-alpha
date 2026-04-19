# 阶段三批次 13 Alpha 五触发器与 alpha_signal 执行卡

卡片编号：`13`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段三最终目标是五个 trigger 事件库和统一 `alpha_signal`，当前仓库还没有任何正式实现。
- 目标：实现五个 trigger runner 和 `run_alpha_signal_build`。
- 为什么现在做：前置规格与工程骨架已经完成，阶段三需要正式进入可运行状态。

## 2. 设计输入

- `docs/03-execution/12-alpha-source-and-shared-runner-card-20260419.md`
- `docs/03-execution/09-alpha-signal-aggregation-spec-freeze-card-20260419.md`

## 3. 规格输入

- `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
- `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`

## 4. 任务切片

1. 实现 `BOF / TST / PB / CPB / BPB` 五个 trigger。
2. 为五个 trigger 物化 `alpha_trigger_event / alpha_trigger_profile`。
3. 汇总五类输出，物化 `alpha_signal`。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/alpha/`
- `scripts/alpha/`
- Alpha 相关测试

范围外：

- `position` 切换
- `trade / system` 调整

## 6. 收口标准

1. 五个 trigger 都能独立运行。
2. `alpha_signal` 能稳定汇总五类输出。
3. `alpha_signal` 保留 `MALF` 波段位置字段。
