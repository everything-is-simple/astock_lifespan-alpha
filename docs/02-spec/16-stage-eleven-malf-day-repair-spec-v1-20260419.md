# 阶段十一 MALF day repair 规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-eleven-malf-day-repair`

## 1. 定位

本规格冻结阶段十一对 `MALF day` 的最小修复边界。
阶段十一只处理两类确定性问题：

1. 把 `stock_daily_adjusted` formalize 成正式 `day source contract`
2. 把 `engine` 内部 `rank / profile` 的重复扫描改成可复用计算

阶段十一不是 MALF 语义重写阶段，不引入新的 runner，不改 public runner 名称，不改正式表名。

## 2. Source contract

- `Timeframe.DAY` 读取 `stock_daily_adjusted` 时，正式固定 `adjust_method = backward`
- `none / forward` 不进入正式 MALF day 账本
- 正式输出必须满足：`symbol + trade_date -> 1 day bar`
- `trade_date -> bar_dt`
- 输出顺序必须是每个 symbol 内 `bar_dt` 严格升序

如果过滤到 `adjust_method = backward` 之后仍出现重复：

- 不做静默 dedup
- diagnostics 必须登记重复事实
- 正式 runner 必须 fail-fast

阶段十一明确拒绝通过给 `snapshot_nk / pivot_nk` 追加随机序号来掩盖 source uniqueness 问题。

## 3. Engine contract

- `run_malf_engine()` 保持现有签名不变
- bars 只 materialize 一次
- 已按日期升序且唯一时，不再无条件 `sorted(list(...))`
- 输入无序时允许只排序一次
- 发现重复 `bar_dt` 时直接拒绝推进状态机

性能修复限定为：

- `_rank_snapshots()` 先按 `(symbol, timeframe, direction)` 预建 sample pool
- `_build_profiles()` 复用同一组 sample pool
- 不再对每个 snapshot / wave 重复筛一遍 `waves`

本阶段不做 dataclass-heavy 到 tuple accumulator 的结构重写。

## 4. Diagnostics 与验收

- `profile_malf_day_real_data` 必须登记：
  - `selected_adjust_method`
  - 过滤前重复事实
  - 过滤后是否仍违反 source contract
  - `source load timing / engine timing / write timing`
- 验收必须覆盖：
  - `backward` 口径只读
  - 重复 `symbol + trade_date + backward` fail-fast
  - `engine` 重复日期输入拒绝
  - `_rank_snapshots()` / `_build_profiles()` 缓存后语义不变

## 5. 非目标

- 不把 `rank/profile` 拆成下游独立 materialization
- 不处理历史谱系库
- 不扩改 `week / month` 正式边界
- 不在阶段十一内重写 `guard anchor / reborn window`

阶段十一完成后，正式表达应为：

> `stock_daily_adjusted` 已被 formalize 为 `adjust_method = backward` 的唯一 day bars；`snapshot_nk / pivot_nk` 主键冲突根因已被封堵；`engine` 内部重复扫描已被移除；阶段九重演待在新瓶颈上重新发起。
