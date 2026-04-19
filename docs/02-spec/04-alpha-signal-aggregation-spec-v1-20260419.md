# alpha_signal 正式汇总规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档冻结 `alpha_signal` 的正式汇总规则。

它回答三个问题：

1. 五类 trigger 如何汇总进入统一账本。
2. `formal_signal_status` 与 `signal_nk` 如何定义。
3. `alpha_signal` 对 `position` 暴露什么最小字段。

## 2. 正式职责

`alpha_signal` 是阶段三唯一正式输出账本。

它的职责固定为：

- 汇总五类 trigger 输出
- 为 `position` 提供统一读取面
- 保留 `MALF` 波段位置字段，避免下游重复回查

明确禁止：

- 在 `alpha_signal` 中混入 `position` 逻辑
- 直接把五个 trigger 账本暴露给 `position` 作为正式上游

## 3. 正式字段集

`alpha_signal` 的最小字段集冻结为：

- `signal_nk`
- `symbol`
- `signal_date`
- `trigger_type`
- `formal_signal_status`
- `source_trigger_db`
- `source_trigger_event_nk`
- `wave_id`
- `direction`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

## 4. 状态口径

`formal_signal_status` 首版固定为：

- `candidate`
- `confirmed`

语义冻结为：

- `candidate`：已满足 trigger 的最小语义条件，但未进入完全确认
- `confirmed`：满足 trigger 的正式确认条件

`alpha_signal` 只汇总 `candidate / confirmed`，不在首版引入其他状态。

## 5. 汇总规则

### 5.1 数据来源

`alpha_signal` 只读取五个 trigger 数据库中的 `alpha_trigger_event`：

- `alpha_bof`
- `alpha_tst`
- `alpha_pb`
- `alpha_cpb`
- `alpha_bpb`

### 5.2 唯一键

`signal_nk` 固定为：

```text
signal_nk = trigger_type + ":" + source_trigger_event_nk
```

因此：

- 一个 trigger 事件最多对应一个 `alpha_signal`
- `alpha_signal` 必须可追溯回原始 `source_trigger_event_nk`

### 5.3 冲突处理

首版汇总规则冻结为：

- 不做跨 trigger 去重
- 不做跨 trigger 优先级压缩
- 若同一 `symbol / signal_date` 出现多个 trigger，则全部保留

`alpha_signal` 的职责是完整表达正式事件，不是提前裁决下游取舍。

## 6. 读取边界

阶段三后，下游正式读取边界固定为：

- `position` 只能读取 `alpha_signal`
- `alpha_trigger_event / alpha_trigger_profile` 只属于 `alpha` 内部实现面

## 7. 最小验收样例

### 样例 1：单 trigger 汇总

- 给定：`alpha_bof.alpha_trigger_event` 出现一条 `confirmed`
- 则：`alpha_signal` 新增一条同日 `bof` 事件

### 样例 2：多 trigger 并存

- 给定：同一 `symbol / signal_date` 同时出现 `bof.confirmed` 与 `tst.confirmed`
- 则：`alpha_signal` 保留两条记录，不做合并裁决

### 样例 3：追溯一致

- 给定：任意 `alpha_signal` 记录
- 则：必须能通过 `source_trigger_db + source_trigger_event_nk` 回到原 trigger 事件

## 8. 冻结结论

本文冻结以下结论：

1. `alpha_signal` 是阶段三唯一正式输出账本。
2. `signal_nk` 与 `formal_signal_status` 口径已经固定。
3. 首版不做跨 trigger 去重或优先级裁决。
4. `position` 后续只能读取 `alpha_signal`，不能直接读取五个 trigger 账本。
