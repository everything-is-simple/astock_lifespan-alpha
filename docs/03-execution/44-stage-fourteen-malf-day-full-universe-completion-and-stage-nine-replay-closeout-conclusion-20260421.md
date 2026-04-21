# 批次 44 结论：阶段十四 MALF day full-universe completion 与阶段九 replay 收口

结论编号：`44`
日期：`2026-04-21`
文档标识：`stage-fourteen-malf-day-full-universe-completion-and-stage-nine-replay-closeout`

## 1. 裁决

`已接受，阶段九 replay 完成`

## 2. 结论

阶段十四 MALF day full-universe completion 已完成，阶段九真实 replay 已完整收口到 system。

当前正式 `malf_day.duckdb` 被接受为正式结果。其与 `malf_day.backup-day-fc56ff5e5441.duckdb` 在核心业务表与 checkpoint 业务列上完全一致；`day-fc56...` 与 `day-d696...building` 相关差异登记为谱系归因偏差。

最新 `data -> system` pipeline 已完成 13 步：

- pipeline run：`pipeline-4a2a2455df18`
- status：`completed`
- step_count：`13`
- trade step：`trade-38bf18c8918c`
- system step：`system-1bc072d08b83`

## 3. 关键修复

- MALF day artifact 选择逻辑改为完整正式 target 优先
- MALF incomplete work 判定不再被已 checkpoint 覆盖的 stale running queue 误导
- alpha / position replay 改为 set-based 路径
- trade replay 增加 checkpoint reused fast path
- trade 慢路径删除/插入改为事务保护，避免中断后主订单表留空

## 4. 验证

- 最新 trade 主表：
  - `trade_order_intent = 5892934`
  - `trade_order_execution = 5892934`
  - `trade_checkpoint = 5497`
- 最新 system：
  - `system_trade_readout = 5892934`
  - `system_portfolio_trade_summary = 1`
- `pytest`：`86 passed`

## 5. 后续

阶段九 replay 已完成登记。

后续若继续优化性能，应优先评估：

- portfolio_plan replay no-op 快路径
- trade `trade_run_order_intent` 全量 reused 审计映射的增长成本

这些优化不影响本批次完成裁决。
