# 阶段四批次 16 position 最小账本与 runner 规格冻结执行卡

卡片编号：`16`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段四需要把 `position` 从 foundation stub 升级为正式 runner，但最小表族和 replay 合同尚未冻结。
- 目标：冻结 `position_run / work_queue / checkpoint / candidate_audit / capacity_snapshot / sizing_snapshot` 六表与 runner 合同。
- 为什么现在做：不先冻结表族，后续实现会反复漂移。

## 2. 设计输入

- `docs/03-execution/15-alpha-signal-to-position-bridge-spec-freeze-conclusion-20260419.md`

## 3. 规格输入

- `docs/02-spec/05-alpha-signal-to-position-bridge-spec-v1-20260419.md`

## 4. 任务切片

1. 冻结 `position` 六张正式表。
2. 冻结最小状态口径。
3. 冻结 queue / checkpoint / replay 规则与 runner 名称。

## 5. 实现边界

范围内：

- `position` 最小账本规格
- `16` 的执行闭环

范围外：

- 实际 DuckDB schema
- `portfolio_plan` 代码

## 6. 收口标准

1. 六张正式表全部明确命名。
2. `run_position_from_alpha_signal` 被冻结为正式入口名。
3. 阶段四明确不做 `position_exit_plan / position_exit_leg`。
