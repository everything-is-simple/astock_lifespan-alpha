# position 最小账本与 runner 规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档冻结阶段四 `position` 的最小正式账本、自然键、运行摘要与 bounded runner 合同。
它只覆盖：

- `position_run`
- `position_work_queue`
- `position_checkpoint`
- `position_candidate_audit`
- `position_capacity_snapshot`
- `position_sizing_snapshot`

它不覆盖：

- `position_exit_plan`
- `position_exit_leg`
- 完整资金管理与分批退出体系

## 2. 正式职责

`position` 在阶段四只负责三层事实：

1. `candidate_audit`：表达上游信号是否进入最小候选。
2. `capacity_snapshot`：表达单标最大允许容量。
3. `sizing_snapshot`：表达最小目标权重与减仓需求。

2026-04-24 补记：

- `position` 读取到的 `MALF` 字段来自 `alpha_signal` 的正式传递。
- `position` 可以基于这些字段派生候选、容量与 sizing。
- 下游只消费 MALF 事实，不反向定义 MALF。

## 3. 正式表族

### 3.1 `position_run`

记录一次 `run_position_from_alpha_signal` 的运行摘要，至少包含：

- `run_id`
- `status`
- `alpha_source_path`
- `market_source_path`
- `input_rows`
- `symbols_seen`
- `symbols_updated`
- `inserted_candidates`
- `inserted_capacity_rows`
- `inserted_sizing_rows`
- `latest_signal_date`
- `message`

### 3.2 `position_work_queue`

按 `symbol` 管理 bounded replay，至少包含：

- `queue_id`
- `symbol`
- `status`
- `source_row_count`
- `last_signal_date`

### 3.3 `position_checkpoint`

按 `symbol` 记录增量进度，至少包含：

- `symbol`
- `last_signal_date`
- `last_run_id`
- `updated_at`

### 3.4 `position_candidate_audit`

正式表达候选裁决，至少包含：

- `candidate_nk`
- `signal_nk`
- `symbol`
- `signal_date`
- `trigger_type`
- `formal_signal_status`
- `candidate_status`
- `blocked_reason_code`
- `source_trigger_event_nk`
- `wave_id`
- `direction`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`
- `reference_trade_date`
- `reference_price`

### 3.5 `position_capacity_snapshot`

正式表达单标容量上限，至少包含：

- `capacity_nk`
- `candidate_nk`
- `symbol`
- `signal_date`
- `policy_id`
- `capacity_status`
- `capacity_ceiling_weight`
- `reference_trade_date`
- `reference_price`

### 3.6 `position_sizing_snapshot`

正式表达最小 sizing 结果，至少包含：

- `sizing_nk`
- `candidate_nk`
- `symbol`
- `signal_date`
- `policy_id`
- `position_action_decision`
- `requested_weight`
- `final_allowed_position_weight`
- `required_reduction_weight`
- `candidate_status`
- `reference_trade_date`
- `reference_price`

## 4. 最小状态口径

阶段四首版冻结以下状态：

- `candidate_status`
  - `admitted`
  - `blocked`
- `capacity_status`
  - `enabled`
  - `blocked`
- `position_action_decision`
  - `open`
  - `blocked`

## 5. queue / checkpoint / replay 规则

`run_position_from_alpha_signal` 必须遵守：

1. 以 `symbol` 为最小 work unit。
2. 若 `position_checkpoint.last_signal_date >= source_last_signal_date`，允许跳过该 `symbol`。
3. 若上游有变更，必须整标的重放 `candidate / capacity / sizing` 三张表。
4. 不允许为方便 replay 清空整个正式账本后重写。

## 6. bounded runner 合同

正式 Python 入口名固定为：

`run_position_from_alpha_signal`

首版 runner 行为固定为：

```text
load alpha_signal + market_base_day
-> enqueue by symbol
-> replay candidate/capacity/sizing
-> update checkpoint
-> return PositionRunSummary
```

## 7. 最小验收样例

### 样例 1：未确认信号只保留审计

- 给定：某 `symbol` 只有 `formal_signal_status='candidate'`
- 则：`position_candidate_audit` 必须有记录，但 `position_sizing_snapshot.final_allowed_position_weight=0`

### 样例 2：相同源数据重复运行

- 给定：同一批 `alpha_signal` 再运行一次
- 则：runner 可以返回 `completed`，但 `checkpoint_summary.work_units_updated=0`

### 样例 3：容量与 sizing 不可脱钩

- 给定：某记录 `capacity_ceiling_weight=0.10`
- 则：对应 `final_allowed_position_weight` 不得大于 `0.10`

## 8. 冻结结论

本文冻结以下结论：

1. 阶段四 `position` 只做最小账本，不做完整 exit。
2. `run_position_from_alpha_signal` 从 foundation stub 升级为正式 runner。
3. replay 的最小工作单元固定为 `symbol`。
