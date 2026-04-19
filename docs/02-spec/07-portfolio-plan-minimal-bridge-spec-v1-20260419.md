# portfolio_plan 最小桥接规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档冻结阶段四 `position -> portfolio_plan` 的最小正式桥接。
它只覆盖：

- `portfolio_plan_run`
- `portfolio_plan_snapshot`
- `portfolio_plan_run_snapshot`
- 最小 `admitted / blocked / trimmed` 裁决

它不覆盖：

- `trade` 执行账本
- 多组合、多账户治理
- 组合回测与绩效分析

## 2. 正式桥接方向

阶段四固定桥接方向为：

```text
position_candidate_audit + position_capacity_snapshot + position_sizing_snapshot
-> portfolio_plan_snapshot
```

明确禁止：

- `portfolio_plan` 直接读取 `alpha_signal` 或五个 trigger 库。
- `portfolio_plan` 回写 `position` 的 sizing 主语义。
- `portfolio_plan` 自动触发 `trade / system`。

## 3. 正式表族

### 3.1 `portfolio_plan_run`

至少包含：

- `run_id`
- `status`
- `portfolio_id`
- `source_position_path`
- `bounded_candidate_count`
- `admitted_count`
- `blocked_count`
- `trimmed_count`
- `portfolio_gross_cap_weight`
- `message`

### 3.2 `portfolio_plan_snapshot`

至少包含：

- `plan_snapshot_nk`
- `candidate_nk`
- `portfolio_id`
- `symbol`
- `reference_trade_date`
- `position_action_decision`
- `requested_weight`
- `admitted_weight`
- `trimmed_weight`
- `plan_status`
- `blocking_reason_code`
- `portfolio_gross_cap_weight`
- `portfolio_gross_used_weight`
- `portfolio_gross_remaining_weight`
- `first_seen_run_id`
- `last_materialized_run_id`

### 3.3 `portfolio_plan_run_snapshot`

至少包含：

- `run_id`
- `plan_snapshot_nk`
- `candidate_nk`
- `plan_status`
- `materialization_action`

`materialization_action` 首版冻结为：

- `inserted`
- `reused`
- `rematerialized`

## 4. 最小裁决规则

阶段四首版固定以下最小规则：

1. 若 `candidate_status != 'admitted'`，则 `plan_status='blocked'`。
2. 若 `final_allowed_position_weight <= 0`，则 `plan_status='blocked'`。
3. 若组合剩余容量足够，则 `plan_status='admitted'`。
4. 若组合剩余容量大于 `0` 但不足，则 `plan_status='trimmed'`。
5. 若组合剩余容量已经耗尽，则 `plan_status='blocked'`，并记录 `portfolio_capacity_exhausted`。

## 5. 自然键与 selective rebuild

`plan_snapshot_nk` 首版固定由以下语义字段稳定生成：

- `portfolio_id`
- `candidate_nk`
- `reference_trade_date`
- `portfolio_plan_contract_version`

runner 必须支持：

- bounded build
- `reused`
- `rematerialized`

但不允许为了图省事先清空整个正式账本。

## 6. Python 入口

正式 Python 入口名固定为：

`run_portfolio_plan_build`

正式脚本入口固定为：

`scripts/portfolio_plan/run_portfolio_plan_build.py`

## 7. 最小验收样例

### 样例 1：阻断候选不得被组合放行

- 给定：`position_candidate_audit.candidate_status='blocked'`
- 则：`portfolio_plan_snapshot.plan_status='blocked'`

### 样例 2：容量不足触发 trimmed

- 给定：`final_allowed_position_weight=0.10`，组合剩余容量 `0.03`
- 则：`plan_status='trimmed'`，`admitted_weight=0.03`

### 样例 3：重复构建允许复用

- 给定：同一输入重复运行且结果未变化
- 则：`portfolio_plan_run_snapshot.materialization_action='reused'`

## 8. 冻结结论

本文冻结以下结论：

1. `portfolio_plan` 的唯一正式上游是 `position` 的三张最小正式输出。
2. 阶段四只做三表与最小组合裁决。
3. `trade / system` 在阶段四不参与任何自动联动。
