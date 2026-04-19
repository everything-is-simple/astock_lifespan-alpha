# 阶段四批次 16 position 最小账本与 runner 规格冻结记录

记录编号：`16`
日期：`2026-04-19`

## 1. 做了什么

1. 冻结了 `position` 六表表族。
2. 冻结了 `candidate_status / capacity_status / position_action_decision` 的首版口径。
3. 冻结了 `run_position_from_alpha_signal` 的 bounded replay 合同。

## 2. 偏差项

- 阶段四明确不扩展到完整 exit 体系，这是有意收敛。

## 3. 备注

- 该规格为阶段四最小切换版服务，不代表旧仓全部 `position` 能力已恢复。
