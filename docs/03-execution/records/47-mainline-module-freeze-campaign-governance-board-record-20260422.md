# 批次 47 记录：主线模块冻结战役治理面板启动

记录编号：`47`
日期：`2026-04-22`
文档标识：`mainline-module-freeze-campaign-governance-board`

## 1. 执行顺序

1. 复核现有主线顺序与 stage-seventeen rolling backtest 边界。
2. 固定冻结战役只允许单模块推进。
3. 固定状态枚举为 `待测 / 放行 / 冻结 / 待修`。
4. 将 `position` 的 live cutover 结果登记为 `放行`。
5. 将 `portfolio_plan` 提升为当前活跃模块。
6. 将 `portfolio_plan` 的正式库旧口径 preflight 登记为 `待修`。

## 2. 关键裁决

- 当前这张治理面板是本轮主线冻结战役的唯一真相源。
- `position` 本轮只写 `放行`，不提前写 `冻结`。
- `portfolio_plan` 进入当前唯一活跃修复模块。
- `trade / system / pipeline / alpha / malf / data` 继续维持非活跃状态，避免并行扩散。
