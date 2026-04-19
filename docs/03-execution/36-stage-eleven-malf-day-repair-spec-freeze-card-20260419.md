# 阶段十一批次 36 MALF day repair 规格冻结执行卡

卡片编号：`36`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段十已经确认 `snapshot_nk / pivot_nk` 重复与 `engine_timing` 主瓶颈，但阶段十一修复边界尚未冻结
- 目标：冻结 `stage-eleven-malf-day-repair` 的 source contract、engine contract、诊断口径与验收标准
- 为什么现在做：避免先改实现再回填正式边界

## 2. 规格输入

- `docs/02-spec/15-malf-day-real-data-diagnosis-spec-v1-20260419.md`
- `docs/03-execution/35-malf-day-real-data-diagnosis-closeout-conclusion-20260419.md`

## 3. 规格输出

- `docs/02-spec/16-stage-eleven-malf-day-repair-spec-v1-20260419.md`

## 4. 任务切片

1. 冻结 `adjust_method = backward` 的 MALF day source contract
2. 冻结重复 `symbol + trade_date` 的 fail-fast 边界
3. 冻结 `_rank_snapshots()` / `_build_profiles()` 的缓存复用边界
4. 登记 `36` 的 card / evidence / record / conclusion

## 5. 收口标准

1. `stage-eleven-malf-day-repair` 已正式登记
2. 文档明确 `stock_daily_adjusted -> backward -> 唯一 day bars`
3. 文档明确拒绝靠修改 `snapshot_nk / pivot_nk` 规避 source 问题
4. 文档明确 `profile_malf_day_real_data` 要登记过滤前后重复事实与 timing
