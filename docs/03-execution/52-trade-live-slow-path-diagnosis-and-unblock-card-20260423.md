# 批次 52 卡片：trade live slow-path diagnosis and unblock

卡片编号：`52`
日期：`2026-04-23`
文档标识：`trade-live-slow-path-diagnosis-and-unblock`

## 目标

在 `trade = 待修` 的前提下，继续只在 `trade` 模块内推进正式 live slow path 诊断与 unblock，不重开 stage-five 语义讨论，不提前进入 `system`。

本轮目标固定为：

- 给 `trade` runner 补齐 phase-level 可观测性
- 新增只读 `profile_trade_live_path` 入口，输出完整 slow-path 阶段耗时与主瓶颈
- 基于诊断结果继续做 `trade` 模块内最小性能修复
- 重跑正式 live gate，把 blocker 从“无进展”收敛到具体阶段或 SQL 路径

## 验收口径

- `trade` public runner 继续保持为 `run_trade_from_portfolio_plan`
- `trade` 读取边界不变，仍只读 `portfolio_plan_snapshot + execution_price_line`
- `pytest tests/unit/trade -q` 通过
- `pytest tests/unit/contracts/test_module_boundaries.py -q` 通过
- `pytest tests/unit/docs/test_trade_specs.py -q` 通过
- `python scripts/trade/profile_trade_live_path.py` 能输出完整 JSON，而不是停在旧的 OOM 或空白阶段
- 正式 live rerun 必须至少推进到明确 phase message，而不能再次只停在 `trade run started.`
- 若正式 live gate 仍失败，结论必须精确写出失败阶段；本轮不允许只写“整体很慢”

## 本轮边界

- 只允许修改 `src/astock_lifespan_alpha/trade/` 与 `scripts/trade/`、对应测试和治理文档
- 不改 `portfolio_plan`、`system`、`pipeline`
- 不改 public runner 名称
- 不 bump `trade_contract_version`
- `system` 继续冻结，直到 `trade` 获得正式 `放行`

## 修复方案冻结

- `trade_run.message` 与 stderr 补固定 phase：
  - `source_attached`
  - `work_unit_summary_ready`
  - `intent_materialized`
  - `execution_materialized`
  - `position_leg_materialized`
  - `carry_materialized`
  - `exit_materialized`
  - `action_tables_ready`
  - `write_transaction_started`
  - `write_transaction_committed`
- 新增只读脚本：`python scripts/trade/profile_trade_live_path.py`
- `trade_source_work_unit_summary` 继续使用两阶段 work-unit 指纹
- `trade_materialized_intent / execution` 拆成：
  - 确定阻塞行直接落表
  - 仅对可能成为 `planned / filled` 的候选行做价格联接
- action classification 改为：
  - materialized 侧预写 `compare_signature`
  - existing 侧只投影 `key + first_seen_run_id + compare_signature`
- 写事务补写阶段子点：
  - `write_targets_cleared`
  - `write_output_tables_loaded`
  - `write_tracking_tables_loaded`
- 正式 delete 路径改为按 `(portfolio_id, symbol)` 成对联接删写，不再使用独立 `IN (...) AND IN (...)`

## 本轮预期输出

- Card 52 `card / evidence / record / conclusion`
- `47` 治理面板 active card 切到 `52`
- 若正式 gate 继续失败，则明确登记：
  - 当前 blocker 位于写事务 delete 路径
  - `trade` 继续保持 `待修`
  - `system` 不进入 live freeze gate
