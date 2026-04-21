# 批次 44 记录：阶段十四 MALF day full-universe completion 与阶段九 replay 收口

记录编号：`44`
日期：`2026-04-21`

## 1. 执行记录

本批次从阶段十五完成后的 remaining `426` symbols 继续执行，完成 MALF day full-universe promotion，并重启阶段九 replay。

真实 replay 中发现 alpha、position、trade 在 full-universe 输入下存在 Python 全量加载或 SQL reused 路径过重的问题。本批次按不改变 public contract 的原则，改为 set-based / checkpoint fast path，并保持既有 schema 与 runner summary 口径。

## 2. 偏差记录

### 2.1 MALF day 谱系偏差

`2026-04-20 22:11` 的 pipeline run 曾触发 `day-fc56ff5e5441`，并记录从 `day-d696...building` promote。

后续确认：

- 当前正式库与 07:52 backup 的核心业务列完全一致
- checkpoint 业务列完全一致
- 差异集中在 `malf_run`、`malf_work_queue` 与 checkpoint `last_run_id` 归因

处理裁决：

- 接受当前正式 `malf_day.duckdb`
- 不恢复、不重建
- 将差异登记为谱系归因偏差

### 2.2 Orphan running

本批次显式治理多个无进程对应的 `running` run：

- `trade-3f485b2b81cf`
- `pipeline-537e38c0c12e`
- `trade-3a93ba69841d`
- `trade-f9b520455f05`
- `pipeline-5bcbb03d612f`

均标记为 `interrupted` 并写入说明。

### 2.3 Trade 中断恢复

`pipeline-5bcbb03d612f` 在 step 12 trade 阶段被停止后，`trade_order_intent` 与 `trade_order_execution` 曾变为 `0` 行。

处理：

- 标记 `trade-f9b520455f05` 为 `interrupted`
- 修复 trade runner，使慢路径删除/插入在事务内完成
- 重新运行 trade，生成 `trade-5d820b9084ce`
- 恢复 `trade_order_intent = 5892934`
- 恢复 `trade_order_execution = 5892934`

## 3. 最终状态

最终 pipeline：

- `pipeline-4a2a2455df18`
- `status = completed`
- `step_count = 13`

最终 trade：

- `trade-38bf18c8918c`
- `status = completed`
- `work_units_seen = 5497`
- `work_units_updated = 0`
- `reused_order_intents = 5892934`
- `reused_order_executions = 5892934`

最终 system：

- `system-1bc072d08b83`
- `status = completed`
- `readout_rows = 5892934`
- `summary_rows = 1`

## 4. 验证

- `pytest`：`86 passed`
- 模块边界测试通过
- 最新 pipeline 已完整记录 step 1 到 step 13
- MALF day 没有新建 stale building artifact
