# 批次 55 证据：system live freeze gate

证据编号：`55`
日期：`2026-04-23`
文档标识：`system-live-freeze-gate`

## 1. preflight

正式 preflight 确认：

- 无残留 `run_system_from_trade` Python 进程
- 最新正式 `trade` run：
  - `trade-558802e7f7a4`
  - `status = completed`
  - `work_units_seen = 5497`
  - `work_units_updated = 5497`
- 正式 trade source rows：
  - `trade_order_execution = 5892934`
  - `trade_exit_execution = 9434`
  - expected system source rows = `5902368`
- Card 55 前最新正式 system run：
  - `system-c97d6c383908`
  - `status = completed`
  - `readout_rows = 5892934`
- Card 55 前正式 system live schema 缺少 v2 readout/summary columns。

## 2. 首次 live gate

首次正式执行：

- CLI：`python scripts/system/run_system_from_trade.py`
- run：`system-2bebfbed66cb`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\system\system-live-card55-20260423-113436.stdout.log`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\system\system-live-card55-20260423-113436.stderr.log`

观察结果：

- 运行超过 `16` 分钟
- CPU 持续增长
- 但无 stdout / stderr 进展
- `system.duckdb` mtime 长时间未推进
- 该 run 已按规则标记为 `interrupted`

结论：首次 live gate 暴露 `system` source summary / materialization prewrite 前段缺少可观测性，并且原 `string_agg` fingerprint 路径在正式体量下不可接受。

## 3. system-only 修复

本轮只在 `system` 内做最小修复：

- `system` runner 增加 phase stderr 与 `system_run.message` 进度：
  - `source_attached`
  - `work_unit_summary_ready`
  - `write_materialized_committed`
  - `system_run_completed`
- 将 source fingerprint 从大体量 `md5(string_agg(CONCAT(...)))` 改为 aggregate hash：
  - `COUNT(*)`
  - `MAX(execution_trade_date)`
  - `bit_xor(row_hash)`
  - `SUM(row_hash)`
- `repair_system_schema` 使用同一 fingerprint 口径，避免后续 repair 重新落回慢路径。
- 单测补充 `system_run_completed` message 断言。

## 4. 第二次 live gate

第二次正式执行：

- CLI：`python scripts/system/run_system_from_trade.py`
- run：`system-080b8ac3bf8d`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\system\system-live-card55b-20260423-115442.stdout.log`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\system\system-live-card55b-20260423-115442.stderr.log`

关键 stderr phase：

- `source_attached elapsed_seconds=2.143111 rows=5902368 work_units=5497`
- `work_unit_summary_ready elapsed_seconds=2.458784 rows=5497 source_rows=5902368`
- `write_materialized_committed elapsed_seconds=321.912415 rows=5902368 work_units_updated=5497`
- `system_run_completed elapsed_seconds=321.924372 rows=1 readout_rows=5902368`

stdout summary：

- `status = completed`
- `readout_rows = 5902368`
- `summary_rows = 1`
- `work_units_seen = 5497`
- `work_units_updated = 5497`
- `latest_execution_trade_date = 2026-04-10`

## 5. 正式库验收

正式库只读回查结果：

- 最新 run：`system-080b8ac3bf8d`
- `system_run.status = completed`
- `system_work_queue = 5497`
- `system_checkpoint = 5497`
- `system_checkpoint.last_run_id = system-080b8ac3bf8d` 覆盖全部 `5497` 个 work unit
- `system_trade_readout = 5902368`
- `system_portfolio_trade_summary = 1`
- `system_trade_readout.trade_action`：
  - `open_entry = 5892934`
  - `full_exit = 9434`
- `system_trade_readout.system_contract_version`：
  - `stage6_system_v2 = 5902368`
- `system_work_queue.status`：
  - `completed = 5497`
- 无残留 `system` Python 进程

当前 `system_portfolio_trade_summary` operational counts：

- `open_entry_count = 9440`
- `full_exit_count = 9434`
- `active_symbol_count = 6`
- `execution_count = 5902368`
- `filled_count = 18874`
- `rejected_count = 5883494`
- `symbol_count = 5497`
- `gross_executed_weight = 1415.2280000000007`
- `latest_execution_trade_date = 2026-04-10`

## 6. 证据裁决

本地验证结果：

```text
pytest tests/unit/system -q
7 passed in 4.24s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.08s

pytest -q
117 passed in 65.11s (0:01:05)
```

Card 55 已完成 `system` live freeze gate，并通过正式 live gate。

因此：

- `system = 放行`
- 下一活跃模块切到 `pipeline`
