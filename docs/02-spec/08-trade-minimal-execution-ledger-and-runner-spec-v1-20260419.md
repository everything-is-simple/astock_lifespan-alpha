# trade 最小执行账本与 runner 规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档冻结阶段五 `trade` 的最小正式执行账本、自然键、运行摘要、bounded runner 合同与最小执行回报口径。

它只覆盖：

- `trade_run`
- `trade_work_queue`
- `trade_checkpoint`
- `trade_order_intent`
- `trade_order_execution`
- `trade_run_order_intent`
- `run_trade_from_portfolio_plan`
- 最小 `accepted / rejected / filled` 回报

它不覆盖：

- `carry`
- `open leg`
- `exit`
- `pnl`
- broker/session/partial fill
- `system` 自动联动

## 2. 正式职责

`trade` 在阶段五只负责三层事实：

1. `order_intent`：表达组合计划进入执行层后的正式下单意图。
2. `order_execution`：表达最小执行回报与阻断结果。
3. `run / queue / checkpoint`：表达 bounded replay、增量推进与物化审计。

`trade` 当前不负责：

1. 持仓延续事实。
2. 完整退出引擎。
3. 实盘券商会话。
4. `system` 汇总读出。

## 3. 价格口径分线

阶段五正式冻结两条价格口径：

- `analysis_price_line`
- `execution_price_line`

其中：

- `analysis_price_line` 服务于 `malf / alpha`
- `execution_price_line` 服务于 `portfolio_plan / trade / system`

本阶段实现允许暂时复用同一物理 `market_base` 来源，但语义上必须分线。

明确禁止：

1. 把 `analysis_price_line` 的语义直接当作 `trade` 正式执行价格口径。
2. 用 `malf / alpha` 内部价格推导替代 `trade` 的正式输入适配。

## 4. 正式表族

### 4.1 `trade_run`

记录一次 `run_trade_from_portfolio_plan` 的运行摘要，至少包含：

- `run_id`
- `status`
- `portfolio_id`
- `source_portfolio_plan_path`
- `source_execution_price_path`
- `input_rows`
- `work_units_seen`
- `work_units_updated`
- `inserted_order_intents`
- `inserted_order_executions`
- `latest_reference_trade_date`
- `message`

### 4.2 `trade_work_queue`

按最小 work unit 管理 bounded replay，至少包含：

- `queue_id`
- `portfolio_id`
- `symbol`
- `status`
- `source_row_count`
- `last_reference_trade_date`

### 4.3 `trade_checkpoint`

按最小 work unit 记录增量进度，至少包含：

- `portfolio_id`
- `symbol`
- `last_reference_trade_date`
- `last_run_id`
- `updated_at`

### 4.4 `trade_order_intent`

正式表达进入执行层的指令意图，至少包含：

- `order_intent_nk`
- `plan_snapshot_nk`
- `candidate_nk`
- `portfolio_id`
- `symbol`
- `reference_trade_date`
- `planned_trade_date`
- `position_action_decision`
- `intent_status`
- `requested_weight`
- `admitted_weight`
- `execution_weight`
- `blocking_reason_code`
- `first_seen_run_id`
- `last_materialized_run_id`

### 4.5 `trade_order_execution`

正式表达最小执行回报，至少包含：

- `order_execution_nk`
- `order_intent_nk`
- `portfolio_id`
- `symbol`
- `execution_status`
- `execution_trade_date`
- `execution_price`
- `executed_weight`
- `blocking_reason_code`
- `source_price_line`
- `first_seen_run_id`
- `last_materialized_run_id`

### 4.6 `trade_run_order_intent`

桥接某次 `run` 与本次触达的执行意图，至少包含：

- `run_id`
- `order_intent_nk`
- `intent_status`
- `materialization_action`

`materialization_action` 首版冻结为：

- `inserted`
- `reused`
- `rematerialized`

## 5. 最小状态口径

阶段五首版固定以下状态：

- `intent_status`
  - `planned`
  - `blocked`
- `execution_status`
  - `accepted`
  - `rejected`
  - `filled`

## 6. 最小市场规则校验

`trade` 首版只做最小可执行性校验，固定包括：

1. 合法交易日可解析。
2. 正式执行参考价可得。
3. 明显不可执行输入必须阻断。

首版不要求：

1. 完整 A 股规则引擎。
2. 完整涨跌停与撮合仿真。
3. 卖出侧 `T+1` 全规则。
4. 部分成交与撤单状态机。

## 7. queue / checkpoint / replay 规则

`run_trade_from_portfolio_plan` 必须遵守：

1. 最小 replay 单位固定为 `portfolio_id + symbol`。
2. 若 `trade_checkpoint.last_reference_trade_date >= source_last_reference_trade_date`，允许跳过该 work unit。
3. 若上游有变化，必须整组重放该 work unit 对应的 `order_intent / order_execution`。
4. 不允许为了 replay 方便清空整个正式账本后重写。

## 8. bounded runner 合同

正式 Python 入口名固定为：

`run_trade_from_portfolio_plan`

正式脚本入口固定为：

`scripts/trade/run_trade_from_portfolio_plan.py`

首版 runner 行为固定为：

```text
load portfolio_plan_snapshot + execution_price_line
-> enqueue by portfolio_id + symbol
-> replay order_intent / order_execution
-> update checkpoint
-> return TradeRunSummary
```

## 9. 最小验收样例

### 样例 1：阻断计划只保留拒绝型记录

- 给定：`plan_status='blocked'`
- 则：`trade_order_intent.intent_status='blocked'`
- 且：`trade_order_execution.execution_status='rejected'`

### 样例 2：trimmed 只按 admitted_weight 下发

- 给定：`requested_weight=0.10`，`admitted_weight=0.03`
- 则：`trade_order_intent.execution_weight=0.03`

### 样例 3：缺失正式执行价格必须拒绝

- 给定：存在合法计划，但 `execution_price_line` 缺失对应交易日价格
- 则：`trade_order_execution.execution_status='rejected'`

### 样例 4：重复构建允许复用

- 给定：同一输入重复运行且结果未变化
- 则：`trade_run_order_intent.materialization_action='reused'`

## 10. 冻结结论

本文冻结以下结论：

1. 阶段五 `trade` 只做最小执行账本，不做持仓延续与退出引擎。
2. `trade` 的正式价格口径属于 `execution_price_line`，与 `malf / alpha` 的 `analysis_price_line` 分线。
3. `run_trade_from_portfolio_plan` 是阶段五唯一正式 Python 入口。

## 11. Implementation Freeze Addendum

This addendum freezes the stage-five engineering defaults before code implementation.

1. `execution_price_line` is physically backed by `PathConfig.source_databases.market_base` in stage five.
2. The physical reuse of `market_base` does not merge semantics with `analysis_price_line`.
3. `planned_trade_date` and `execution_trade_date` are the first available trading day after `reference_trade_date` for the same `symbol`.
4. `execution_price` is the `open` price from that planned execution day.
5. Missing next trading day or missing `open` price must produce `trade_order_execution.execution_status='rejected'`.
6. `accepted` is a reserved execution status in stage five; the first implementation only materializes `filled / rejected`.
7. The bounded replay work unit is fixed as `portfolio_id + symbol`.
8. `order_intent_nk` is based on `portfolio_id / candidate_nk / planned_trade_date / trade_contract_version`.
9. 次日开盘执行 is the frozen stage-five execution-price convention.
