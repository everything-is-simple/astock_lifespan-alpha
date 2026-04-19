# 阶段四批次 19 position 契约、Schema 与 runner 执行卡

卡片编号：`19`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：`position` 仍停留在 foundation stub，没有正式契约、schema 和 data-grade runner。
- 目标：补齐 `PositionRunSummary`、六表 schema 和正式 runner 骨架。
- 为什么现在做：`18` 已完成，阶段四终于满足实施前置条件。

## 2. 设计输入

- `docs/03-execution/18-stage-four-document-closeout-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/05-alpha-signal-to-position-bridge-spec-v1-20260419.md`
- `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`

## 4. 任务切片

1. 新增 `position` 契约层。
2. 新增 `position` schema 初始化。
3. 升级 `run_position_from_alpha_signal` 为正式 runner。

## 5. 实现边界

范围内：

- `src/astock_lifespan_alpha/position/`
- `scripts/position/run_position_from_alpha_signal.py`
- 相关测试

范围外：

- `portfolio_plan` 物化
- `trade / system`

## 6. 收口标准

1. `run_position_from_alpha_signal` 不再返回 foundation stub。
2. `position` 六表可以幂等初始化。
3. 输入只允许来自 `alpha_signal` 与 `market_base_day`。
