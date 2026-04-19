# 阶段十一批次 37 MALF day repair 工程收口结论

结论编号：`37`
日期：`2026-04-19`
状态：`已接收`

## 1. 裁决

- 接受：阶段十一 `MALF day repair` 工程实现已完成
- 接受：`stock_daily_adjusted` 已被 formalize 为 `adjust_method = backward` 的唯一 day bars 合同
- 接受：`engine_timing` 主瓶颈已被压降，不再是当前主瓶颈

## 2. 原因

- source 层已经固定 `backward`，并把过滤后仍重复的 `symbol + trade_date` 视为正式 contract violation
- engine 已移除 `_rank_snapshots()` 与 `_build_profiles()` 的重复 sample 扫描
- 真实诊断报告 `malf-day-diag-68cc9d425b1d` 证明在相同窗口：
  - `symbol_limit = 10`
  - `bar_limit_per_symbol = 1000`
  - `source_load_seconds = 3.02114`
  - `engine_seconds = 1.419344`
  - `write_seconds = 99.106712`
  - `bottleneck_stage = write_timing`
- 同一窗口下，过滤前重复事实已被明确登记：
  - `duplicate_symbol_trade_date_groups_before_filter = 79300`
  - 示例含 `000001.SZ@1991-04-03x3`
  - `duplicate_symbol_trade_date_groups_after_filter = 0`

## 3. 影响

- 阶段十一完成
- `snapshot_nk / pivot_nk` 的真实主键冲突根因已被从 source contract 侧封堵
- 阶段九重演仍未重新发起；下一轮应把焦点从 `engine_timing` 转向 `write_timing` 与真实全量 build 持续时长
- `python scripts/malf/run_malf_day_build.py` 的真实手动执行已不再在 source load 阶段 OOM，但在 10 分钟观察窗内未完成，当前作为阶段九重演前的已知偏差保留
