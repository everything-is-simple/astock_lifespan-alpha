# 批次 55 卡片：system live freeze gate

卡片编号：`55`
日期：`2026-04-23`
文档标识：`system-live-freeze-gate`

## 目标

在 Card 54 已确认 `trade = 放行` 的前提下，只处理 `system` 消费正式 `trade` 输出的 live freeze gate。

本轮目标固定为：

- `system` 只读消费正式 `trade.duckdb`
- 验收 `open_entry + full_exit` 的正式 readout
- 补齐 live system v2 schema 后按 `portfolio_id + symbol` 增量重算
- 不进入 `pipeline`
- 不回头修改 `trade`
- 不新增 PnL / Sharpe 等未正式化统计

## 验收口径

- 最新 `system_run.status = completed`
- `system_run.source_trade_path` 指向正式 `trade.duckdb`
- `system_run.readout_rows = 5902368`
- `system_work_queue = 5497`
- `system_checkpoint.last_run_id` 全量切到最新 `system` run
- `system_trade_readout` 同时包含：
  - `open_entry = 5892934`
  - `full_exit = 9434`
  - `system_contract_version = stage6_system_v2`
- `system_portfolio_trade_summary = 1`
- 当前 summary 字段只登记 operational counts，不扩展为收益统计。

## 本轮边界

- 只改 `system` 模块、`system` 单测和治理文档。
- 不进入 `pipeline`。
- 不修改 `trade`。
- 若首次 live gate 失败，只允许在 `system` 内做最小修复，并登记失败 run。
