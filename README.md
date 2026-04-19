# astock-lifespan-alpha

`astock-lifespan-alpha` 是从 `lifespan-0.01` 重构出来的新系统仓库。

正式主链路为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

当前仓库明确移除了 `structure` 与 `filter` 作为正式系统架构的一部分。

首版实现技术栈冻结为：

- Python
- DuckDB
- Arrow

## 当前阶段

当前已完成阶段七 data 源事实契约对齐；阶段八 `data -> system` 最小全链路编排待规划。重点是：

- `alpha_signal -> position` 桥接规格、`position` 最小账本规格、`portfolio_plan` 最小桥接规格已冻结
- `run_position_from_alpha_signal` 已从 foundation stub 升级为正式 runner
- `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot` 已形成正式输出
- `run_portfolio_plan_build` 与 `portfolio_plan_run / snapshot / run_snapshot` 已落地
- 阶段四执行闭环 `15-21` 已补齐
- 阶段五文档闭环 `22-24` 已补齐
- 阶段五工程收口闭环 `25` 已补齐
- `run_trade_from_portfolio_plan` 与 `trade_run / work_queue / checkpoint / order_intent / order_execution / run_order_intent` 已落地
- 阶段六规格冻结闭环 `26` 已补齐
- 阶段六 v1 主线固定为 `trade -> system`
- 阶段六工程收口闭环 `27` 已补齐
- `run_system_from_trade` 与 `system_run / system_trade_readout / system_portfolio_trade_summary` 已落地
- 阶段七规格冻结闭环 `28` 已补齐
- 阶段七首版固定只读 stock，并对齐 6 个本地 source fact DuckDB
- 阶段七工程收口闭环 `29` 已补齐
- `malf / alpha / position / trade` source adapter 已支持真实 stock adjusted 表

这不代表完整资金管理、完整 exit、真实 broker/session/partial fill 或 `system` 已实现完成。

当前阶段更准确的含义是：

> `alpha -> position -> portfolio_plan -> trade -> system` 最小正式主线已经成立；阶段七 data 源事实契约对齐已完成，阶段八 `data -> system` 编排待规划。

阶段五起正式冻结以下价格口径分线：

- `malf / alpha` 属于 `analysis_price_line`
- `portfolio_plan / trade / system` 属于 `execution_price_line`

阶段四中的 `reference_trade_date / reference_price` 只是最小桥接参考，不等于阶段五之后的正式执行价格口径。

Stage-five implementation defaults are now frozen before engineering work:
- `execution_price_line` is backed by `PathConfig.source_databases.market_base`.
- Valid `open` intents use 次日开盘执行: the first later `market_base_day.open`.
- The first `trade` runner materializes `filled / rejected`; `accepted` is reserved but not written.

重构计划 Part 2 已正式落档：
- `docs/02-spec/10-astock-lifespan-alpha-reconstruction-plan-part2-stage-five-trade-v1-20260419.md`
- 文档标识：`reconstruction-plan-part2`
- 主题：第五阶段文档先行与工程实施计划

阶段六 system 规格已正式冻结：
- `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`
- 文档标识：`stage-six-system`
- 主题：`trade -> system` 最小读出与 runner
- 口径：只读取 `trade` 正式输出，不回读 `alpha / position / portfolio_plan`，不触发上游 runner

阶段六 system 工程已完成：
- `run_system_from_trade`
- `system_run / system_trade_readout / system_portfolio_trade_summary`
- 收口结论：`docs/03-execution/27-stage-six-system-readout-engineering-closeout-conclusion-20260419.md`

阶段七 data 源事实契约已冻结：
- `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`
- 文档标识：`stage-seven-data-source-contract`
- 主题：真实本地 stock source fact 路径、表名与字段映射对齐
- 下一阶段：阶段八 `data -> system` 最小全链路编排待规划

阶段七 data 源事实契约工程已完成：
- `SourceFactDatabasePaths` 已登记 6 个 source fact 路径
- `stock_daily_adjusted / stock_weekly_adjusted / stock_monthly_adjusted`
- `code -> symbol`
- `trade_date -> bar_dt / signal_date / execution_trade_date`

## 文档入口

正式文档请从 [docs/README.md](H:\astock_lifespan-alpha\docs\README.md) 开始阅读。
