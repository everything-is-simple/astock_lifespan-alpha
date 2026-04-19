# 批次 38 记录：阶段十二 MALF day 写路径重演 unblock 规格冻结

## 1. 决策

- 新阶段名固定为 `stage-twelve-malf-day-write-path-replay-unblock`
- 阶段十二只覆盖 `MALF day` 写路径与阶段九真实重演 unblock
- 当前主瓶颈按阶段十一真实诊断结果登记为 `write_timing`

## 2. 实施边界

- 保留 MALF 语义状态机
- 保留 public runner 名称
- 保留正式表名与主键生成语义
- 允许拆分 `write_timing` phase
- 允许减少逐 symbol 的重复 `DELETE + executemany`
- 允许调整 checkpoint 与 work_queue 更新粒度

## 3. 后续批次

- `39` 负责工程实施、测试、真实全量 build 验证与阶段九重演发起
