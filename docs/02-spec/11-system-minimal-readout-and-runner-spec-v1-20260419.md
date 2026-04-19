# 阶段六 system 最小读出与 runner 规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-six-system`

## 1. 定位

本规格冻结阶段六 `trade -> system` 的最小正式边界。

阶段六 v1 的 `system` 不是全链路调度器，也不是交易执行器。它只读取 `trade` 正式输出，生成面向系统层的最小 read model、组合级 summary 与 runner 摘要。

阶段六 v1 不回读 `alpha / position / portfolio_plan`，不触发上游 runner，不重开阶段五已经冻结的 `execution_price_line` 物理口径。

## 2. 上游边界

`system` 的唯一正式上游固定为：

```text
trade -> system
```

`system` source 只允许读取 `settings.databases.trade`。

允许读取的 `trade` 正式表为：

- `trade_order_intent`
- `trade_order_execution`

不允许读取：

- `alpha`
- `position`
- `portfolio_plan`
- `market_base`
- `raw_market`
- 任何临时文件或上游内部过程表

## 3. 正式接口

阶段六新增 runner：

```text
run_system_from_trade(portfolio_id: str = "core", settings: WorkspaceRoots | None = None) -> SystemRunSummary
```

稳定契约：

- `SYSTEM_CONTRACT_VERSION = "stage6_system_v1"`
- `stage6_system_v1`
- `SystemRunStatus.COMPLETED`
- `SystemRunSummary`

runner 必须返回：

- `runner_name`
- `run_id`
- `status`
- `target_path`
- `source_paths`
- `message`
- `readout_rows`
- `summary_rows`

缺失 `trade.duckdb` 或缺失必要 `trade` 表时，runner 只初始化 `system` schema，返回 `completed` empty summary，不报错。

## 4. system 表族

阶段六新增三张正式表：

- `system_run`
- `system_trade_readout`
- `system_portfolio_trade_summary`

### `system_run`

记录每次 `run_system_from_trade` 的运行元数据。

必须记录：

- `run_id`
- `status`
- `portfolio_id`
- `source_trade_path`
- `readout_rows`
- `summary_rows`
- `message`
- `started_at`
- `finished_at`

### `system_trade_readout`

`system_trade_readout` 是 `trade_order_intent` 与 `trade_order_execution` 的最小正式投影。

必须包含：

- `system_readout_nk`
- `order_intent_nk`
- `order_execution_nk`
- `portfolio_id`
- `symbol`
- `reference_trade_date`
- `planned_trade_date`
- `execution_trade_date`
- `position_action_decision`
- `intent_status`
- `execution_status`
- `requested_weight`
- `admitted_weight`
- `execution_weight`
- `executed_weight`
- `execution_price`
- `blocking_reason_code`
- `source_price_line`
- `system_contract_version`
- `last_materialized_run_id`

### `system_portfolio_trade_summary`

`system_portfolio_trade_summary` 按 `portfolio_id` 汇总 `system_trade_readout`。

必须包含：

- `portfolio_id`
- `execution_count`
- `filled_count`
- `rejected_count`
- `symbol_count`
- `gross_executed_weight`
- `latest_execution_trade_date`
- `system_contract_version`
- `last_materialized_run_id`

## 5. 重跑规则

阶段六 v1 的 bounded replay 单位为 `portfolio_id`。

每次运行时：

1. 初始化 `system` schema。
2. 读取 `trade` 正式输出。
3. 删除目标 `portfolio_id` 既有 `system_trade_readout` 与 `system_portfolio_trade_summary`。
4. 重新物化该 `portfolio_id` 的 readout 与 summary。
5. 写入 `system_run`。

重跑不得重复堆积旧 readout。

## 6. 明确不纳入阶段六 v1

阶段六 v1 不纳入：

- 全链路自动编排
- 调用 `run_alpha_*`
- 调用 `run_position_from_alpha_signal`
- 调用 `run_portfolio_plan_build`
- 调用 `run_trade_from_portfolio_plan`
- carry
- open leg
- exit
- pnl
- broker/session/partial fill
- `execution_price_line` 物理拆分

阶段六 v1 的“编排层”只表示 `system` 最小读出入口成立，不表示端到端调度器已经成立。

## 7. 验收标准

阶段六规格冻结后，工程实施必须满足：

1. `system` 只读取 `trade` 正式输出。
2. `system_trade_readout` 可以从 `filled / rejected` trade 输出稳定生成。
3. `system_portfolio_trade_summary` 可以按 `portfolio_id` 汇总最小执行状态。
4. 缺失 trade 数据时 runner completed 且 empty summary。
5. 重跑不会重复堆积旧 readout。
6. docs 测试与 system 单元测试锁定上述边界。
