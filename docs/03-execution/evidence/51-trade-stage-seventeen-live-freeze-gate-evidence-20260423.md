# 批次 51 证据：trade stage-seventeen live freeze gate

证据编号：`51`
日期：`2026-04-23`
文档标识：`trade-stage-seventeen-live-freeze-gate`

## 1. 本轮代码修复

本轮 `trade` 代码只保留并扩展模块内最小兼容修复：

- `trade` exit/leg/carry 路径继续使用基于 `intent` 的等值联接，去掉旧的 `LIKE CONCAT(...)` 大范围匹配。
- work-unit 状态汇总继续使用每张 action 表的变更摘要，不再直接把五张 action 表按行相乘。
- `trade_source_work_unit_summary` 新增两阶段指纹路径：
  - 先从 `trade_plan_source_rows` 逐行生成固定宽度 `row_fingerprint`
  - 再按 `portfolio_id + symbol` 聚合生成 `source_fingerprint`
- `trade` 定向测试新增多行 work unit 回归，证明同一 work unit 下相同输入仍可 `reused`，单行变化仍能触发受影响行 `rematerialized`。

## 2. 本地测试结果

```text
pytest tests/unit/trade -q
8 passed in 71.12s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed

pytest tests/unit/docs/test_trade_specs.py -q
2 passed
```

## 3. 正式库 preflight

正式 preflight 查询确认：

- 没有活跃 `trade` writer 进程持有 `trade.duckdb`
- 旧孤儿 run `trade-5a83d9f388af` 已由 `running` 改成 `interrupted`
- 最新上游正式 run：
  - `portfolio-plan-68ab0db998ad`
  - `status = completed`
  - `portfolio_gross_cap_weight = 0.50`
- 当前正式 `portfolio_plan_snapshot.plan_status`：
  - `admitted = 6638`
  - `trimmed = 2802`
  - `blocked = 5883494`
- 当前正式 `trade` 旧态：
  - `trade_checkpoint.last_run_id = trade-012abd340b1b`
  - `trade_work_queue = 0`
  - `trade_order_intent: planned = 2 / blocked = 5892932`
  - `trade_order_execution: filled = 2 / rejected = 5892932`
  - `trade_position_leg = 0`
  - `trade_exit_execution = 0`
  - `trade_carry_snapshot = 0`
- `market_base` 覆盖到：`2026-04-10`

## 4. 正式 live rerun 观测

本轮正式 live rerun 采用后台受控方式：

- CLI：`python scripts/trade/run_trade_from_portfolio_plan.py`
- stderr：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card51-20260423-075932.stderr.log`
- stdout：`H:\Lifespan-report\astock_lifespan_alpha\trade\trade-live-card51-20260423-075932.stdout.log`
- 新正式 run：`trade-6f780ccc1005`

后台观测结果：

- 第一个窗口内，工作集从约 `4.3GB` 增长到约 `6.4GB`
- 但 stderr 仍为空，`trade.duckdb` mtime 无变化
- 第二个连续窗口内：
  - `CPU delta = 0`
  - stderr 仍为 `0` 字节
  - `trade.duckdb` mtime 仍不变
- 终止进程后回查正式库：
  - `trade-6f780ccc1005` 只停在 `trade run started.`
  - 正式表、checkpoint、work queue 均未推进

## 5. 证据裁决

本轮证据表明：

- `trade` 代码侧最小兼容修复已经成立
- 但正式 live gate 仍未跑通
- 当前 blocker 已经从旧的 schema/快路径误判收敛为 live slow path 无进展
- 因此 Card 51 只能正式登记为 `trade = 待修`
