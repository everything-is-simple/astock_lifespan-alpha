# 阶段十二批次 38 MALF day 写路径重演 unblock 规格冻结执行卡

卡片编号：`38`
日期：`2026-04-19`
状态：`已完成`

## 1. 需求

- 问题：阶段十一已把 `engine_timing` 压降并确认新主瓶颈为 `write_timing`，但阶段九真实重演仍未重新跑通
- 目标：冻结 `stage-twelve-malf-day-write-path-replay-unblock` 的写路径诊断、优化与真实重演 unblock 边界
- 为什么现在做：避免继续转向 guard anchor、reborn window 或历史谱系 profile，导致偏离当前真实 blocker

## 2. 规格输入

- `docs/02-spec/16-stage-eleven-malf-day-repair-spec-v1-20260419.md`
- `docs/03-execution/37-stage-eleven-malf-day-repair-engineering-closeout-conclusion-20260419.md`

## 3. 规格输出

- `docs/02-spec/17-stage-twelve-malf-day-write-path-replay-unblock-spec-v1-20260419.md`

## 4. 任务切片

1. 冻结 `write_timing` 细分 phase
2. 冻结 MALF day 写路径优化允许范围
3. 冻结真实全量 `run_malf_day_build` 可完成性验收顺序
4. 登记 `38` 的 card / evidence / record / conclusion

## 5. 实现边界

范围内：

- MALF day 写路径诊断
- MALF day 写入批量化或事务边界优化
- checkpoint 与 work_queue 更新耗时拆解
- 阶段九真实重演 unblock

范围外：

- MALF 语义状态机改造
- `guard anchor / reborn window / 历史谱系 profile`
- public runner 改名
- 正式表改名或删除

## 6. 收口标准

1. `stage-twelve-malf-day-write-path-replay-unblock` 已正式登记
2. 规格明确下一阶段只处理 `write_timing` 与真实重演 unblock
3. 规格明确 `write_timing` 至少拆成 `delete old rows / insert ledgers / checkpoint / queue update`
4. 规格明确拒绝继续修改 MALF 语义来绕过真实写路径瓶颈
