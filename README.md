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

当前已完成阶段四 `position` 接口切换与 `portfolio_plan` 最小桥接；阶段五 `trade` 文档已冻结，工程待启动。重点是：

- `alpha_signal -> position` 桥接规格、`position` 最小账本规格、`portfolio_plan` 最小桥接规格已冻结
- `run_position_from_alpha_signal` 已从 foundation stub 升级为正式 runner
- `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot` 已形成正式输出
- `run_portfolio_plan_build` 与 `portfolio_plan_run / snapshot / run_snapshot` 已落地
- 阶段四执行闭环 `15-21` 已补齐
- 阶段五文档闭环 `22-24` 已补齐

这不代表 `trade` 代码、完整资金管理、完整 exit 或 `system` 已实现完成。

当前阶段更准确的含义是：

> `alpha -> position -> portfolio_plan` 最小正式主线已经成立；`portfolio_plan -> trade` 的阶段五文档已经冻结，工程实施待启动。

阶段五起正式冻结以下价格口径分线：

- `malf / alpha` 属于 `analysis_price_line`
- `portfolio_plan / trade / system` 属于 `execution_price_line`

阶段四中的 `reference_trade_date / reference_price` 只是最小桥接参考，不等于阶段五之后的正式执行价格口径。

Stage-five implementation defaults are now frozen before engineering work:
- `execution_price_line` is backed by `PathConfig.source_databases.market_base`.
- Valid `open` intents use 次日开盘执行: the first later `market_base_day.open`.
- The first `trade` runner materializes `filled / rejected`; `accepted` is reserved but not written.

## 文档入口

正式文档请从 [docs/README.md](H:\astock_lifespan-alpha\docs\README.md) 开始阅读。
