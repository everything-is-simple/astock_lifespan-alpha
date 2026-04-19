# 阶段七 data 源事实契约对齐规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-seven-data-source-contract`

## 1. 定位

本规格冻结阶段七 data source fact contract 对齐范围。

阶段七不做 `data -> system` 全线编排，而是先把真实本地 DuckDB 源事实库纳入正式路径、表名和字段映射契约，使现有 `malf / alpha / position / trade` source adapter 可以读取真实 stock 数据。

阶段七完成后，阶段八再规划 `data -> system` 最小全链路编排。

## 2. 本地源库

阶段七正式登记 6 个本地 source fact DuckDB：

- `H:\Lifespan-data\base\market_base.duckdb`
- `H:\Lifespan-data\base\market_base_week.duckdb`
- `H:\Lifespan-data\base\market_base_month.duckdb`
- `H:\Lifespan-data\raw\raw_market.duckdb`
- `H:\Lifespan-data\raw\raw_market_week.duckdb`
- `H:\Lifespan-data\raw\raw_market_month.duckdb`

source fact basename 合同为：

- `market_base.duckdb`
- `market_base_week.duckdb`
- `market_base_month.duckdb`
- `raw_market.duckdb`
- `raw_market_week.duckdb`
- `raw_market_month.duckdb`

`SourceFactDatabasePaths` 必须保留日线兼容字段：

- `market_base`
- `raw_market`

并新增：

- `market_base_week`
- `market_base_month`
- `raw_market_week`
- `raw_market_month`

`as_dict()` 必须返回全部 6 个路径。

## 3. 首版资产范围

阶段七只读 stock。

首版不读取：

- `index_*`
- `block_*`

index 与 block 后续作为市场环境或板块上下文扩展，不进入本轮。

## 4. 表名契约

真实本地 stock 表名固定为：

- day: `stock_daily_adjusted`
- week: `stock_weekly_adjusted`
- month: `stock_monthly_adjusted`

为保持既有测试与旧接口兼容，source adapter 必须继续支持旧表名 fallback：

- `market_base_day`
- `market_base_week`
- `market_base_month`
- `bars_day`
- `bars_week`
- `bars_month`
- `price_bar_day`
- `price_bar_week`
- `price_bar_month`
- `market_day`
- `market_week`
- `market_month`

## 5. 字段映射契约

真实本地 stock 表字段映射固定为：

- `code -> symbol`
- `trade_date -> bar_dt`
- `trade_date -> signal_date`
- `trade_date -> execution_trade_date`
- `open / high / low / close` 保持 OHLC 语义

旧测试表字段仍继续支持：

- `symbol`
- `bar_dt`
- `date`

## 6. 模块读取规则

`malf.source`：

- day 读取 `market_base.duckdb` 的 `stock_daily_adjusted`
- week 读取 `market_base_week.duckdb` 的 `stock_weekly_adjusted`
- month 读取 `market_base_month.duckdb` 的 `stock_monthly_adjusted`
- 如果对应周期库不可用，允许回退到日线库聚合

`alpha.source`：

- 读取 `market_base.duckdb` 的 `stock_daily_adjusted`
- 使用 `code -> symbol` 与 `trade_date -> signal_date`

`position.source`：

- 读取 `market_base.duckdb` 的 `stock_daily_adjusted`
- 使用 `code -> symbol` 与 `trade_date` 作为参考交易日来源

`trade.source`：

- 读取 `market_base.duckdb` 的 `stock_daily_adjusted`
- 使用 `code -> symbol` 与 `trade_date` 作为执行价格日期来源

## 7. 明确不纳入阶段七

阶段七不纳入：

- index / block 读取
- raw ingest 实施
- 全链路 orchestration
- `system` 自动触发上游 runner
- pnl
- exit
- broker/session/partial fill
- 已冻结业务语义修改

阶段七只修正 data source path / table / column contract。

## 8. 验收标准

阶段七工程实施必须满足：

1. `SourceFactDatabasePaths.as_dict()` 返回 6 个 source fact 路径。
2. `malf` day/week/month 可以读取 `stock_daily_adjusted / stock_weekly_adjusted / stock_monthly_adjusted`。
3. `alpha / position / trade` 可以读取 `stock_daily_adjusted`。
4. `code -> symbol` 与 `trade_date -> bar_dt` 映射被测试锁定。
5. 旧表名 fallback 测试继续通过。
6. docs 测试与全量测试通过。
