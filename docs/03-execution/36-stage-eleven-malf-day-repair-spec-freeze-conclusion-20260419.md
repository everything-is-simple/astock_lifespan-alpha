# 阶段十一批次 36 MALF day repair 规格冻结结论

结论编号：`36`
日期：`2026-04-19`
状态：`已接收`

## 1. 裁决

- 接受：`stage-eleven-malf-day-repair` 已冻结
- 接受：MALF day source contract 固定为 `stock_daily_adjusted + adjust_method = backward`
- 接受：重复 `symbol + trade_date` 必须 fail-fast，不允许靠修改 `snapshot_nk / pivot_nk` 绕开

## 2. 原因

- 阶段十真实诊断已经确认：
  - 主键冲突根因在 source uniqueness 缺口
  - `engine_timing` 是当时主瓶颈
- 阶段十一只需在既有 MALF 语义边界内完成 formalization 与重复计算移除

## 3. 影响

- 阶段十一规格冻结完成
- 下一批次 `37` 进入工程实施与真实验证
- 阶段九重演仍待阶段十一工程结果落地后重新发起
