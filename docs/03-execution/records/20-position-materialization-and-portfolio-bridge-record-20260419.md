# 阶段四批次 20 position 物化与最小 portfolio_plan bridge 记录

记录编号：`20`
日期：`2026-04-19`

## 1. 做了什么

1. 实现了 `position` 三层正式物化。
2. 实现了 `portfolio_plan` 三表与组合层最小裁决。
3. 增加了端到端 smoke 测试与脚本入口。

## 2. 偏差项

- 阶段四只覆盖最小桥接，不扩展到 `trade`。

## 3. 备注

- `portfolio_plan` 只消费 `position` 正式输出，不再越层读取 `alpha`。
