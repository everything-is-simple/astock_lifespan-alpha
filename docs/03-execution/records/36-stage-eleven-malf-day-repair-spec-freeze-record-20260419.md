# 批次 36 记录：阶段十一 MALF day repair 规格冻结

## 1. 决策

- 新阶段名固定为 `stage-eleven-malf-day-repair`
- 阶段十一只覆盖 `MALF day`
- `backward` 被冻结为正式 day source 口径

## 2. 实施边界

- 不改 public runner 名称
- 不改 `MalfRunSummary / CheckpointSummary / RunStatus`
- 不改 MALF 正式表名
- `rank/profile` 仍由 engine 产出，本阶段只优化内部实现

## 3. 后续批次

- `37` 负责工程实施、测试、真实诊断复核与执行结论
