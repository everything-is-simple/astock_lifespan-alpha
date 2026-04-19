# alpha_signal -> position 桥接规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档冻结阶段四 `alpha_signal -> position` 的唯一正式桥接合同。
它回答三个问题：

1. `position` 首版到底读取 `alpha_signal` 的哪些正式字段。
2. 哪些字段职责继续留在 `alpha_signal`，哪些职责进入 `position`。
3. 阶段四明确禁止哪些旧仓 admission 口径被直接搬回新仓。

## 2. 正式桥接方向

阶段四固定桥接方向为：

```text
alpha_signal -> position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot
```

明确禁止：

- `position` 直接读取五个 trigger 库。
- `position` 直接读取 `alpha` 内部未冻结的 detector payload。
- 把旧仓 `alpha formal signal` admission 字段整包回引到新仓。

## 3. 最小输入字段组

阶段四 `position` 首版只认当前 `alpha_signal` 已正式输出的字段：

- `signal_nk`
- `symbol`
- `signal_date`
- `trigger_type`
- `formal_signal_status`
- `source_trigger_event_nk`
- `wave_id`
- `direction`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

辅助输入固定来自 `market_base_day`，只负责补齐：

- `reference_trade_date`
- `reference_price`

## 4. 字段职责冻结

### 4.1 `alpha_signal` 负责

- 信号是否成立。
- 信号属于哪类 `trigger_type`。
- `MALF` 波段上下文表达。
- 对 `source_trigger_event_nk` 的可追溯关系。

### 4.2 `position` 负责

- `candidate_status` 的最小裁决。
- 单标容量上限。
- 最小 `sizing` 结果。

## 5. 明确不回引的旧仓字段

阶段四首版明确不要求 `alpha_signal` 提供以下旧仓 admission 侧字段：

- `malf_context_4`
- `lifecycle_rank_high`
- `lifecycle_rank_total`
- `admission_verdict_code`
- `admission_reason_code`
- `admission_audit_note`
- `filter_gate_code`
- `filter_reject_reason_code`

如果阶段四实施需要等价信息，只能在 `position` 内基于现有字段显式派生，不允许隐式假定这些字段仍然存在。

## 6. 阶段四最小派生规则

`position` 首版允许的最小派生只有三类：

1. 基于 `formal_signal_status / direction / wave_position_zone` 派生 `candidate_status`。
2. 基于 `wave_position_zone / update_rank / stagnation_rank` 派生 `capacity_ceiling_weight` 与 `final_allowed_position_weight`。
3. 基于 `signal_nk / policy_id / reference_trade_date` 生成 `position` 自身自然键。

除此之外，阶段四不做完整资金管理和 exit 规划。

## 7. 最小验收样例

### 样例 1：确认信号进入最小候选

- 给定：`alpha_signal.formal_signal_status='confirmed'`，`direction='up'`，`wave_position_zone='early_progress'`
- 则：`position_candidate_audit.candidate_status='admitted'`

### 样例 2：候选信号不得被提前放行

- 给定：`alpha_signal.formal_signal_status='candidate'`
- 则：`position` 可以保留审计记录，但 `final_allowed_position_weight=0`

### 样例 3：弱停滞区不得被解释为可建仓

- 给定：`wave_position_zone='weak_stagnation'`
- 则：`position` 必须把该条记录表达为阻断或零容量结果

## 8. 冻结结论

本文冻结以下结论：

1. `position` 的唯一正式上游是 `alpha_signal`。
2. 阶段四首版桥接字段组只允许使用当前 `alpha_signal` 已正式输出的字段。
3. 旧仓 admission 侧字段不回引；如需等价能力，只能在新规格中显式派生。
