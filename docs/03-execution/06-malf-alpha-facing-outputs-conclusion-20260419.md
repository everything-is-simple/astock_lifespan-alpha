# 阶段二批次 06 MALF 面向 Alpha 输出结论

结论编号：`06`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：MALF 面向 `alpha` 的 snapshot/profile 读模型已经成立。
- 拒绝：让 `alpha` 直接依赖 MALF 内部 ledger 作为正式读取面的做法。

## 2. 原因

- `malf_wave_scale_snapshot` 已对齐正式字段集。
- `update_rank / stagnation_rank / wave_position_zone` 已被物化并通过测试验证。

## 3. 影响

- 阶段三可以把 `malf_wave_scale_snapshot` 当作稳定上游输入。
- `alpha_signal` 仍留在下一阶段，不在本批次提前实现。
