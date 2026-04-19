# 阶段二批次 06 MALF 面向 Alpha 输出记录

记录编号：`06`
日期：`2026-04-19`

## 1. 做了什么

1. 物化 `malf_wave_scale_snapshot` 与 `malf_wave_scale_profile`。
2. 以同标的 / 同周期 / 同方向历史波为样本口径计算 rank。
3. 生成 `early_progress / mature_progress / mature_stagnation / weak_stagnation` 四区。

## 2. 偏差项

- 本批次只提供 `alpha` 读取面，没有提前实施 `alpha_signal`。

## 3. 备注

- 阶段二的正式交付物现在已经具备进入阶段三的上游形态。
