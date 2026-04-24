# portfolio_plan -> trade 最小桥接规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档冻结阶段五 `portfolio_plan -> trade` 的最小正式桥接。

它只覆盖：

- `portfolio_plan_snapshot -> trade_order_intent -> trade_order_execution`
- 最小 `planned / blocked`
- 最小 `accepted / rejected / filled`

它不覆盖：

- `carry`
- `open leg`
- `exit`
- `pnl`
- `system` 自动读出

## 2. 正式桥接方向

阶段五固定桥接方向为：

```text
portfolio_plan_snapshot
-> trade_order_intent
-> trade_order_execution
```

明确禁止：

1. `trade` 直接读取 `alpha` 或 `position` 内部过程。
2. `trade` 回写 `portfolio_plan` 的组合主语义。
3. `trade` 自动触发 `system`。
4. `trade` 用执行状态反向定义 `MALF` 或 `alpha` 的上游事实。

2026-04-24 补记：

- `trade` 是执行层，不是结构事实层。
- 若执行链路间接携带 `MALF` 上下文，也只作为来源谱系与审计字段使用。
- 下游只消费 MALF 事实，不反向定义 MALF。

## 3. 正式上游与输入字段

`trade` 的唯一正式上游固定为：

- `portfolio_plan_snapshot`

首版最小输入字段固定为：

- `plan_snapshot_nk`
- `candidate_nk`
- `portfolio_id`
- `symbol`
- `reference_trade_date`
- `position_action_decision`
- `requested_weight`
- `admitted_weight`
- `trimmed_weight`
- `plan_status`
- `blocking_reason_code`

字段职责冻结为：

1. `portfolio_plan` 负责回答组合层最终放行了多少。
2. `trade` 负责回答这些放行结果如何进入执行层。
3. `trade` 不得反向定义 `portfolio_plan` 的裁决逻辑。

## 4. 阶段四勘误

阶段四 `portfolio_plan` 中的：

- `reference_trade_date`
- `reference_price`

在阶段五之后被正式限定为：

> 仅服务于阶段四最小桥接与审计参考，不等于阶段五之后的正式执行价格口径。

因此：

1. `reference_trade_date / reference_price` 不能反向约束 `trade / system` 的正式价格语义。
2. 阶段五起正式执行价格口径由 `execution_price_line` 约束。

## 5. 价格口径分线

阶段五桥接固定遵守以下分线：

- `analysis_price_line`：服务于 `malf / alpha`
- `execution_price_line`：服务于 `portfolio_plan / trade / system`

本阶段实现允许暂时复用当前同一物理 `market_base` 来源，但必须明确：

1. 这是物理来源复用，不是语义合并。
2. `trade` 消费的是 `execution_price_line` 的正式输入适配结果。

## 6. 最小桥接规则

阶段五首版固定以下最小规则：

1. 若 `plan_status='blocked'`，则：
   - `trade_order_intent.intent_status='blocked'`
   - `trade_order_execution.execution_status='rejected'`
2. 若 `plan_status in ('admitted', 'trimmed')` 且 `admitted_weight > 0`，则：
   - `trade_order_intent.intent_status='planned'`
   - `trade_order_intent.execution_weight = admitted_weight`
3. 若 `position_action_decision != 'open'`，则首版按阻断处理。
4. `trimmed` 不得按 `requested_weight` 全量下发。
5. `trade_order_execution.execution_status='filled'` 仅代表首版最小形式化回报，不代表真实券商成交系统已经接入。

## 7. 自然键与 selective rebuild

`order_intent_nk` 首版固定由以下语义字段稳定生成：

- `plan_snapshot_nk`
- `planned_trade_date`
- `trade_contract_version`

`order_execution_nk` 首版固定由以下语义字段稳定生成：

- `order_intent_nk`
- `execution_status`
- `trade_contract_version`

runner 必须支持：

- bounded build
- `reused`
- `rematerialized`

但不允许为了图省事先清空整个正式账本。

## 8. 最小验收样例

### 样例 1：阻断计划不得生成有效指令

- 给定：`portfolio_plan_snapshot.plan_status='blocked'`
- 则：`trade_order_intent.intent_status='blocked'`

### 样例 2：trimmed 只按 admitted_weight 生成执行意图

- 给定：`requested_weight=0.10`，`admitted_weight=0.03`
- 则：`trade_order_intent.execution_weight=0.03`

### 样例 3：trade 不得回读 position 内部过程

- 给定：实现阶段存在 `position` 额外中间字段
- 则：`trade` 仍只允许消费 `portfolio_plan_snapshot`

## 9. 冻结结论

本文冻结以下结论：

1. `trade` 的唯一正式上游是 `portfolio_plan_snapshot`。
2. 阶段五只做最小桥接，不扩展到 `carry / exit / system`。
3. 阶段四 `reference_trade_date / reference_price` 只是最小桥接参考，不等于正式执行价格口径。

## 10. Implementation Freeze Addendum

This addendum freezes the bridge implementation defaults before code implementation.

1. `trade` consumes `portfolio_plan_snapshot` plus the `execution_price_line` adapter only.
2. The stage-five `execution_price_line` adapter reads `PathConfig.source_databases.market_base`.
3. `planned_trade_date` is the first available `market_base_day` date after `reference_trade_date` for the same `symbol`.
4. `execution_trade_date` equals `planned_trade_date`.
5. `execution_price` is the `open` price on `execution_trade_date`.
6. `accepted` is reserved but not materialized by the first stage-five runner.
7. Valid `open` intents materialize `filled`; blocked or invalid inputs materialize `rejected`.
8. Selective rebuild remains bounded by `portfolio_id + symbol`.
9. 次日开盘执行 is the frozen bridge convention from `portfolio_plan` into `trade`.
