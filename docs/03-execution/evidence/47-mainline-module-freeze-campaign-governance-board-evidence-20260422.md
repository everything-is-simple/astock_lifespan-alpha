# 批次 47 证据：主线模块冻结战役治理面板启动

证据编号：`47`
日期：`2026-04-22`
文档标识：`mainline-module-freeze-campaign-governance-board`

## 1. 治理规则落点

- 冻结战役顺序固定为：
  `position -> portfolio_plan -> trade -> system -> pipeline -> alpha -> malf -> data`
- 状态枚举固定为：
  `待测 / 放行 / 冻结 / 待修`
- 当前只允许一个活跃模块。
- `pipeline` 不作为业务模块健康证明，只在业务模块放行后做编排验收。

## 2. 当前治理面板快照

| 模块 | 当前状态 | 最近一次 gate | 下一动作 | 阻断原因/备注 |
| --- | --- | --- | --- | --- |
| `position` | `放行` | `2026-04-22 position live freeze gate` | 等待 `portfolio_plan` gate | 已完成 stage-seventeen live cutover |
| `portfolio_plan` | `待修` | `2026-04-22 portfolio_plan Card 50 regate` | 继续压缩 committed replace 尾段后重跑正式 `0.50` gate | 已完成按日分批 slow path 与 progress logging，但最新 `0.50` run 仍未完成最终提交 |
| `trade` | `待测` | `stage-five engineering closeout` | 等待上游放行 | 等待 `portfolio_plan` |
| `system` | `待测` | `stage-six engineering closeout` | 等待上游放行 | 等待 `trade` |
| `pipeline` | `待测` | `stage-sixteen incremental resume` | 等待下游模块放行 | orchestration-only |
| `alpha` | `待测` | `stage-three closeout` | 后置冻结审计 | 本轮不主动开修 |
| `malf` | `待测` | `stage-fourteen replay closeout` | 后置冻结审计 | 本轮不主动开修 |
| `data` | `待测` | `stage-seven engineering closeout` | 后置冻结审计 | 本轮不主动开修 |
