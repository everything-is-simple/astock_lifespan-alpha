# 阶段四批次 15 alpha_signal -> position 桥接规格冻结记录

记录编号：`15`
日期：`2026-04-19`

## 1. 做了什么

1. 冻结了 `position` 首版桥接字段组。
2. 冻结了 `alpha_signal` 与 `position` 的职责边界。
3. 明确列出了旧 admission 字段不回引的禁止项。

## 2. 偏差项

- 没有直接照搬旧仓桥接字段，而是按新仓 `alpha_signal` 实际输出重写。

## 3. 备注

- 这是阶段四实施前的前置合同，不代表 `position` 已经开始编码。
