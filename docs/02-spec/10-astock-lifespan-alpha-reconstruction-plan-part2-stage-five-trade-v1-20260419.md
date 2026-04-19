# astock-lifespan-alpha 重构计划 Part 2：第五阶段文档先行与工程实施计划 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`reconstruction-plan-part2`

## 1. 文档定位

本文是 `astock_lifespan-alpha` 重构计划的 Part 2，专门记录第五阶段 `trade` 从文档冻结到工程实现的完整实施计划与验收结论。

Part 1 即 `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`，已经覆盖并完成阶段一到阶段五的最小正式主线：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade
```

本文不重开 Part 1 的裁决，只对阶段五工程收口做正式落档，并把下一阶段入口固定为阶段六 `system`。

## 2. 阶段五最终交付

阶段五最终交付主线固定为：

```text
portfolio_plan -> trade
```

阶段五已经完成：

1. `trade` 最小执行账本与 runner 规格冻结。
2. `portfolio_plan -> trade` 最小桥接规格冻结。
3. `run_trade_from_portfolio_plan` 工程实现。
4. `trade_run / trade_work_queue / trade_checkpoint / trade_order_intent / trade_order_execution / trade_run_order_intent` 正式表族落地。
5. docs 测试、trade 单元测试、全量测试通过。

## 3. 第五阶段文档先行与工程实施计划

第五阶段采用“文档先行，工程后置”的实施顺序：

1. 先冻结 `trade` 表族、runner 合同、桥接规则和价格口径。
2. 再实现 `trade` contracts / schema / source / engine / runner。
3. 最后补齐脚本入口、docs 测试、trade runner 测试与全量验证。

阶段五关键提交已经形成闭环：

- `b513dce`：阶段五 trade 文档冻结。
- `5da5856`：阶段五 `portfolio_plan -> trade` 工程实现。
- `a3d03bf`：portfolio_plan rematerialization 测试补齐。

## 4. 阶段五工程口径

阶段五工程口径正式冻结为：

1. `execution_price_line` 物理上复用 `PathConfig.source_databases.market_base`。
2. `execution_price_line` 语义上独立于 `malf / alpha` 的 `analysis_price_line`。
3. 合法 `open` 指令按次日开盘执行。
4. `planned_trade_date` 和 `execution_trade_date` 取 `reference_trade_date` 后首个可交易日。
5. `execution_price` 取该执行日 `market_base_day.open`。
6. 首版实际物化 `filled / rejected`。
7. `accepted` 保留为正式枚举，但阶段五首版不物化。
8. bounded replay 单位固定为 `portfolio_id + symbol`。
9. `trimmed` 只按 `admitted_weight` 下发，不按 `requested_weight` 全量下发。

## 5. 明确不纳入阶段五

阶段五不纳入：

- `carry`
- `open leg`
- `exit`
- `pnl`
- broker/session/partial fill
- `system` 自动联动

这些能力不得反向解释阶段五已完成内容。阶段五完成的含义仅限于 `portfolio_plan -> trade` 最小正式执行账本成立。

## 6. 验收结论

阶段五验收以以下事实为准：

1. 文档闭环 `22-24` 已完成。
2. 工程闭环已由 `run_trade_from_portfolio_plan`、正式表族、CLI 入口和测试覆盖。
3. `pytest -q tests/unit/docs tests/unit/trade` 已通过。
4. `pytest -q` 已通过。
5. 当前主线可从阶段六继续推进，不需要重开阶段五裁决。

因此，阶段五完成。

## 7. 下一阶段入口

阶段五完成后的下一阶段固定为：

```text
阶段六 system 最小读出与编排层
```

阶段六的初始方向是：

1. `system` 只读取 `trade` 正式输出，不回读 `alpha / position / portfolio_plan` 内部过程。
2. 建立 `system` 最小读出账本、summary/read model 与 runner。
3. 完成 `trade -> system` 的最小正式闭环。
4. 暂不重开 `execution_price_line` 物理拆分，该问题留给后续专门阶段。

## 8. 冻结结论

本文冻结以下结论：

1. Part 1 主重构计划已经完成阶段一到阶段五的最小正式主线。
2. Part 2 正式记录第五阶段文档先行与工程实施计划。
3. 阶段五完成的正式表述是：`portfolio_plan -> trade` 最小执行主线已完成。
4. 阶段六 system 是下一阶段正式入口。
