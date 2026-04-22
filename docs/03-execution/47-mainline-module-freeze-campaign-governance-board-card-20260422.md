# 批次 47 卡片：主线模块冻结战役治理面板启动

卡片编号：`47`
日期：`2026-04-22`
文档标识：`mainline-module-freeze-campaign-governance-board`

## 目标

建立当前仓库的主线模块冻结战役治理面板，作为 `position -> portfolio_plan -> trade -> system -> pipeline -> alpha -> malf -> data` 的唯一真相源。

## 验收口径

- 治理面板只允许使用 `待测 / 放行 / 冻结 / 待修` 四种状态。
- 每个模块都要登记 `当前状态 / 最近一次 gate / 下一动作 / 阻断原因`。
- 同一时刻只允许一个活跃修复模块。
- `pipeline` 不得被用于反推业务模块已经健康。
- 本轮治理面板必须明确：
  - 当前活跃模块是 `position`
  - `position` 本轮判定只允许写到 `放行` 或 `待修`
  - 下一锤模块是 `portfolio_plan`

## 治理面板快照

| 模块 | 当前状态 | 最近一次 gate | 下一动作 | 阻断原因/备注 |
| --- | --- | --- | --- | --- |
| `position` | `放行` | `2026-04-22 position live freeze gate` | 等待 `portfolio_plan` 验证是否反向打破 | 已完成 stage-seventeen live cutover，本轮不写 `冻结` |
| `portfolio_plan` | `待测` | `stage-sixteen incremental resume` | 开 `portfolio_plan` freeze card 并做真实 gate | 需验证 `live active-cap accounting` 与释放容量语义 |
| `trade` | `待测` | `stage-five engineering closeout` | 等待 `portfolio_plan` 放行后进入 | 需验证 `open + carry + full-exit` 正式落表 |
| `system` | `待测` | `stage-six engineering closeout` | 等待 `trade` 放行后进入 | 需验证只读 `trade` 的 rolling readout |
| `pipeline` | `待测` | `stage-sixteen incremental resume` | 等待下游业务模块放行后进入 | 只做 orchestration gate，不反推主线健康 |
| `alpha` | `待测` | `stage-three closeout` | 后置冻结审计 | 本轮不主动开修 |
| `malf` | `待测` | `stage-fourteen replay closeout` | 后置冻结审计 | 本轮不主动开修 |
| `data` | `待测` | `stage-seven engineering closeout` | 后置冻结审计 | 本轮不主动开修 |
