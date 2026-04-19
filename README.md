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

当前已完成阶段八 `data -> system` 最小 pipeline orchestration，阶段九真实建库演练已发现阻塞，阶段十 MALF day 真实库诊断已完成；阶段九重演待重新发起。重点是：

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
- 阶段八规格冻结闭环 `30` 已补齐
- 阶段八主线固定为 `data -> system` 最小 pipeline orchestration
- 阶段八工程收口闭环 `31` 已补齐
- `run_data_to_system_pipeline` 与 `pipeline_run / pipeline_step_run` 已落地
- 阶段九规格冻结闭环 `32` 已补齐
- 阶段九主线固定为真实 `H:\Lifespan-data` 建库演练
- 阶段九执行收口闭环 `33` 已补齐
- 首个 blocker 出现在 `run_malf_day_build` 真实库复跑
- 阶段十规格冻结闭环 `34` 已补齐
- 阶段十主线固定为 MALF day 真实库诊断与脚本入口修正
- 阶段十工程收口闭环 `35` 已补齐
- `profile_malf_day_real_data` 已确认当前真实瓶颈落在 `engine_timing`

这不代表完整资金管理、完整 exit、真实 broker/session/partial fill 或 `system` 已实现完成。

当前阶段更准确的含义是：

> `data -> malf -> alpha -> position -> portfolio_plan -> trade -> system` 最小正式主线已经具备统一 pipeline 入口；阶段九阻塞已定位到真实 MALF day 首步，阶段十诊断已完成，阶段九重演待重新发起。

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

阶段八 data -> system pipeline 规格已冻结：
- `docs/02-spec/13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md`
- 文档标识：`stage-eight-pipeline`
- 主题：最小 pipeline orchestration
- 边界：只调用 public runner 并记录 step summary，不直接写业务表

阶段八 data -> system pipeline 工程已完成：
- `run_data_to_system_pipeline`
- `pipeline_run / pipeline_step_run`
- 收口结论：`docs/03-execution/31-data-to-system-pipeline-orchestration-engineering-closeout-conclusion-20260419.md`

阶段九真实建库演练规格已冻结：
- `docs/02-spec/14-real-data-build-rehearsal-spec-v1-20260419.md`
- 文档标识：`stage-nine-real-data-build`
- 主题：真实 `H:\Lifespan-data` module-by-module build 与 pipeline replay
- 边界：先做 read-only preflight，再做真实建库演练；`Go+DuckDB deferred`

阶段九真实建库演练执行收口：
- `docs/03-execution/33-real-data-build-rehearsal-closeout-conclusion-20260419.md`
- 裁决：阶段九真实建库演练发现阻塞，待修复
- 阻塞点：`PYTHONPATH` 修正后，`run_malf_day_build` 真实库复跑超过 12 分钟未完成返回，并占用 `malf_day.duckdb`

阶段十 MALF day 真实库诊断规格已冻结：
- `docs/02-spec/15-malf-day-real-data-diagnosis-spec-v1-20260419.md`
- 文档标识：`stage-ten-malf-day-diagnosis`
- 主题：`run_malf_day_build` 真实库卡点诊断与脚本入口修正
- 诊断表：`stock_daily_adjusted`
- 边界：只做 `PYTHONPATH` 入口修正与真实库只读诊断，不修改 MALF 业务语义

阶段十 MALF day 真实库诊断工程已完成：
- `scripts/_bootstrap.py`
- `scripts/malf/profile_malf_day_real_data.py`
- 诊断结论：`engine_timing`
- 收口结论：`docs/03-execution/35-malf-day-real-data-diagnosis-closeout-conclusion-20260419.md`
- 下一步：阶段九重演待重新发起

## 文档入口

正式文档请从 [docs/README.md](H:\astock_lifespan-alpha\docs\README.md) 开始阅读。
