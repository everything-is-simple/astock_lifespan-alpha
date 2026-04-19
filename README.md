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

当前已完成阶段八 `data -> system` 最小 pipeline orchestration，阶段九真实建库演练已发现阻塞，阶段十一 MALF day repair 已完成，阶段十二 MALF day 写路径重演 unblock 已冻结；阶段九重演待在写路径工程结果落地后重新发起。重点是：

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
- 阶段十一规格冻结闭环 `36` 已补齐
- 阶段十一工程收口闭环 `37` 已补齐
- `profile_malf_day_real_data` 已确认当前真实瓶颈已从 `engine_timing` 转为 `write_timing`
- 阶段十二规格冻结闭环 `38` 已补齐
- 下一轮固定处理 MALF day 写路径诊断、写入优化与阶段九真实重演 unblock
- 阶段十二工程收口闭环 `39` 已补齐
- `write_timing_summary` 已进入 MALF runner 与 diagnostics 输出
- 安装 `pyarrow 23.0.1` 后真实采样窗口 `write_seconds` 已降到 `0.911749`
- 真实全量 `run_malf_day_build` 在 60 分钟观察窗内仍未完成，阶段九重演尚未登记为完成

这不代表完整资金管理、完整 exit、真实 broker/session/partial fill 或 `system` 已实现完成。

当前阶段更准确的含义是：

> `data -> malf -> alpha -> position -> portfolio_plan -> trade -> system` 最小正式主线已经具备统一 pipeline 入口；阶段九阻塞已定位到真实 MALF day 首步，阶段十二已冻结写路径 unblock 边界，阶段九重演待写路径工程结果落地后重新发起。

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
## 阶段十一更新

阶段十一 `stage-eleven-malf-day-repair` 已完成并正式登记：

- `stock_daily_adjusted` 的 MALF day source contract 已冻结为 `adjust_method = backward`
- 过滤前的 `code + trade_date` 重复事实已在真实诊断中登记，过滤后唯一性恢复
- `run_malf_engine()` 已拒绝重复 `bar_dt` 输入
- `_rank_snapshots()` 与 `_build_profiles()` 已改为 sample pool 复用
- 同一真实诊断窗口下，`engine_seconds` 已从 `6.789267` 降到 `1.419344`
- 当前真实瓶颈已从 `engine_timing` 转到 `write_timing`

阶段九真实重演尚未重新发起；下一轮关注点不再是 `snapshot_nk / pivot_nk` 主键冲突，而是全量真实 build 的写入耗时与持续时长。

## 阶段十二更新

阶段十二 `stage-twelve-malf-day-write-path-replay-unblock` 已冻结并正式登记：

- 新规格：`docs/02-spec/17-stage-twelve-malf-day-write-path-replay-unblock-spec-v1-20260419.md`
- 规格冻结结论：`docs/03-execution/38-stage-twelve-malf-day-write-path-replay-unblock-spec-freeze-conclusion-20260419.md`
- `write_timing` 至少拆成 `delete old rows / insert ledgers / checkpoint / queue update`
- 真实全量 `run_malf_day_build` 可完成性优先
- 阶段十二不修改 MALF 语义，不处理 `guard anchor / reborn window / 历史谱系 profile`

阶段十二工程已完成首轮写路径修复：

- `write_timing_summary` 输出 `delete_old_rows_seconds / insert_ledgers_seconds / checkpoint_seconds / queue_update_seconds`
- 安装 `pyarrow 23.0.1` 后 diagnostics 真实采样窗口 `write_seconds = 0.911749`
- runner 已避免旧库 index delete fatal，并在旧库遗留 `running` 状态时走 building 库重建
- 剩余偏差：真实全量 build 仍超过 60 分钟观察窗，阶段九真实重演尚未完成
