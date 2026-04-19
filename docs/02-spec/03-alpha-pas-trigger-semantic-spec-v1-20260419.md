# Alpha PAS 触发器正式语义规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档是阶段三 `alpha` 五触发器体系的正式中文语义规格。

它回答四个问题：

1. `BOF / TST / PB / CPB / BPB` 的正式输入边界是什么。
2. 每个 trigger 的触发条件、失效条件和最小字段是什么。
3. 五个 trigger 如何保持工程骨架一致。
4. 进入实现前，哪些样例必须被测试覆盖。

## 2. 输入边界

阶段三 `alpha` 的唯一正式输入固定为：

- `market_base_day`
- `malf_day.malf_wave_scale_snapshot`

明确禁止：

- 恢复旧 `structure`
- 恢复旧 `filter`
- 恢复旧 `family`
- 把旧 `formal_signal` 当作上游权威

`alpha` 只能消费日线事实层与日线 `MALF` 快照，不扩展到周线或月线。

## 3. 共用事件字段

五个 trigger 的最小正式事件字段固定为：

- `event_nk`
- `symbol`
- `signal_date`
- `trigger_type`
- `formal_signal_status`
- `wave_id`
- `direction`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

其中：

- `trigger_type` 固定为 `bof / tst / pb / cpb / bpb`
- `formal_signal_status` 首版固定为 `candidate / confirmed`

## 4. 共用工程纪律

五个 trigger 必须共享同一工程骨架：

- 独立账本
- 独立 runner
- 独立 work_queue
- 独立 checkpoint
- 独立 trigger_event
- 独立 trigger_profile

差异只允许出现在触发语义，不允许出现在生命周期管理、队列处理或 checkpoint 机制上。

## 5. 触发器定义

### 5.1 BOF

`BOF` 的正式含义冻结为：

> 在上升波推进区内，价格对前一日高点形成向上突破，并具备最小跟随确认

候选条件：

- `direction = up`
- `life_state = alive 或 reborn`
- `wave_position_zone = early_progress 或 mature_progress`
- 当日 `high >` 前一日 `high`

确认条件：

- 满足候选条件
- 当日 `close >=` 前一日 `high`

失效边界：

- `direction != up`
- `wave_position_zone = weak_stagnation`

### 5.2 TST

`TST` 的正式含义冻结为：

> 上升波突破后，价格对前一日高点或等价突破位进行最小回测，并保持收盘恢复

候选条件：

- `direction = up`
- `no_new_span >= 1`
- 当日 `low <=` 前一日 `high`
- `wave_position_zone != weak_stagnation`

确认条件：

- 满足候选条件
- 当日 `close >=` 前一日 `high`
- 当日 `close >= open`

失效边界：

- `direction != up`
- `life_state = broken`

### 5.3 PB

`PB` 的正式含义冻结为：

> 上升波内出现正式回撤，但尚未脱离 `MALF` 上升波生命约束

候选条件：

- `direction = up`
- `life_state = alive`
- `no_new_span >= 1`
- `wave_position_zone = mature_progress 或 mature_stagnation`
- 当日 `close <` 前一日 `close`

确认条件：

- 满足候选条件
- 当日 `low >=` 前一日 `low`

失效边界：

- `direction != up`
- `life_state != alive`

### 5.4 CPB

`CPB` 的正式含义冻结为：

> 上升波回撤后的建设性恢复，强调前一日回撤后出现正向收复

候选条件：

- `direction = up`
- `life_state = alive`
- `no_new_span >= 1`
- `wave_position_zone = mature_progress 或 mature_stagnation`
- 前一日 `close < open`
- 当日 `close >= open`

确认条件：

- 满足候选条件
- 当日 `close >` 前一日 `close`

失效边界：

- `direction != up`
- `life_state = broken`

### 5.5 BPB

`BPB` 的正式含义冻结为：

> 下降波中的延续性下破触发，强调下行方向的 pullback 后再向下推进

候选条件：

- `direction = down`
- `life_state = alive 或 reborn`
- `no_new_span >= 1`
- `wave_position_zone = mature_progress / mature_stagnation / weak_stagnation`
- 当日 `close <` 前一日 `close`

确认条件：

- 满足候选条件
- 当日 `low <` 前一日 `low`

失效边界：

- `direction != down`
- `life_state = broken`

## 6. 去重与唯一键

五个 trigger 的唯一键口径冻结为：

```text
event_nk = symbol + trigger_type + signal_date + wave_id
```

同一 `symbol / trigger_type / signal_date / wave_id` 只允许存在一条正式事件。

## 7. 最小验收样例

### 样例 1：BOF 确认

- 给定：上升波处于 `early_progress`
- 当：当日 `high >` 前一日 `high` 且 `close >=` 前一日 `high`
- 则：产生 `bof.confirmed`

### 样例 2：TST 确认

- 给定：上升波停滞至少 1 根 bar
- 当：当日回测前一日突破位且收盘恢复
- 则：产生 `tst.confirmed`

### 样例 3：PB 确认

- 给定：上升波处于 `mature_progress`
- 当：收盘回撤但低点仍高于前一日低点
- 则：产生 `pb.confirmed`

### 样例 4：CPB 确认

- 给定：前一日为回撤日
- 当：次日收出正向恢复 bar 且收盘强于前一日
- 则：产生 `cpb.confirmed`

### 样例 5：BPB 确认

- 给定：下降波仍有效
- 当：当日 `close <` 前一日 `close` 且 `low <` 前一日 `low`
- 则：产生 `bpb.confirmed`

## 8. 冻结结论

本文冻结以下结论：

1. 阶段三 `alpha` 只消费 `market_base_day + malf_day.malf_wave_scale_snapshot`。
2. 五个 trigger 的正式事件字段、唯一键和状态口径已经固定。
3. 五个 trigger 必须共享同一工程骨架。
4. 触发语义的差异只来自本文定义的条件，不允许实现阶段自行扩展。
