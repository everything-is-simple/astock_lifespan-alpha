# 阶段六批次 26 system 读出规格冻结结论

结论编号：`26`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段六 `trade -> system` 最小读出与 runner 规格已冻结。
- 拒绝：在规格冻结前启动 `system` 工程实现。
- 拒绝：把阶段六 v1 扩大为全链路自动编排、pnl、broker/session 或 partial fill。

## 2. 原因

- 阶段五已经裁决完成，下一阶段入口是阶段六 `system`。
- `system` 首版必须以 `trade` 正式输出为唯一上游，才能保持主线边界清晰。
- `run_system_from_trade` 的职责是最小读出与 summary 物化，不负责触发上游 runner。

## 3. 影响

- 阶段六从本批次之后才允许进入 `system` 代码实施。
- 工程实现必须新增 `system_trade_readout` 与 `system_portfolio_trade_summary`。
- 工程实现必须保证 `system` 不回读 `alpha / position / portfolio_plan`，不触发上游 runner。
