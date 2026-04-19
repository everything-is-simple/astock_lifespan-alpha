# 阶段四批次 19 position 契约、Schema 与 runner 记录

记录编号：`19`
日期：`2026-04-19`

## 1. 做了什么

1. 为 `position` 新增正式契约与 checkpoint 摘要。
2. 新增 `position` 六表 schema。
3. 把 `run_position_from_alpha_signal` 从 stub 升级为正式 runner。

## 2. 偏差项

- 这一批次先搭正式骨架，完整组合桥接留到 `20`。

## 3. 备注

- `position` 的唯一上游仍然固定为 `alpha_signal`。
