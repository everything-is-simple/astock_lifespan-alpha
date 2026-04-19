# 文档入口

`docs/` 是 `astock-lifespan-alpha` 的正式治理入口。

从 `2026-04-19` 起，当前仓库采用中文文档治理，并固定按下面的顺序组织：

```text
01-design -> 02-spec -> 03-execution
```

执行层继续展开为：

```text
card -> evidence -> record -> conclusion
```

## 推荐阅读顺序

如果要快速进入当前项目，建议按下面顺序读：

1. `docs/01-design/01-doc-governance-charter-20260419.md`
2. `docs/01-design/00-foundation-bootstrap-design-notes-20260419.md`
3. `docs/02-spec/01-doc-governance-and-execution-spec-20260419.md`
4. `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
5. `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
6. `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`
7. `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
8. `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`
9. `docs/02-spec/05-alpha-signal-to-position-bridge-spec-v1-20260419.md`
10. `docs/02-spec/06-position-minimal-ledger-and-runner-spec-v1-20260419.md`
11. `docs/02-spec/07-portfolio-plan-minimal-bridge-spec-v1-20260419.md`
12. `docs/02-spec/08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md`
13. `docs/02-spec/09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md`
14. `docs/02-spec/10-astock-lifespan-alpha-reconstruction-plan-part2-stage-five-trade-v1-20260419.md`
15. `docs/02-spec/11-system-minimal-readout-and-runner-spec-v1-20260419.md`
16. `docs/02-spec/12-data-source-fact-contract-alignment-spec-v1-20260419.md`
17. `docs/02-spec/13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md`
18. `docs/02-spec/14-real-data-build-rehearsal-spec-v1-20260419.md`
19. `docs/02-spec/15-malf-day-real-data-diagnosis-spec-v1-20260419.md`
20. `docs/03-execution/README.md`
21. `docs/03-execution/30-data-to-system-pipeline-orchestration-spec-freeze-conclusion-20260419.md`
22. `docs/03-execution/31-data-to-system-pipeline-orchestration-engineering-closeout-conclusion-20260419.md`
23. `docs/03-execution/32-real-data-build-rehearsal-spec-freeze-conclusion-20260419.md`
24. `docs/03-execution/33-real-data-build-rehearsal-closeout-conclusion-20260419.md`
25. `docs/03-execution/34-malf-day-real-data-diagnosis-spec-freeze-conclusion-20260419.md`
26. `docs/03-execution/35-malf-day-real-data-diagnosis-closeout-conclusion-20260419.md`
27. `docs/02-spec/16-stage-eleven-malf-day-repair-spec-v1-20260419.md`
28. `docs/02-spec/17-stage-twelve-malf-day-write-path-replay-unblock-spec-v1-20260419.md`
29. `docs/03-execution/38-stage-twelve-malf-day-write-path-replay-unblock-spec-freeze-conclusion-20260419.md`
30. `docs/03-execution/39-stage-twelve-malf-day-write-path-replay-unblock-engineering-closeout-conclusion-20260419.md`
31. `docs/02-spec/18-stage-thirteen-malf-day-segmented-build-completion-spec-v1-20260419.md`
32. `docs/03-execution/40-stage-thirteen-malf-day-segmented-build-completion-spec-freeze-conclusion-20260419.md`
33. `docs/03-execution/41-stage-thirteen-malf-day-segmented-build-completion-engineering-closeout-conclusion-20260419.md`
34. `docs/02-spec/19-stage-fourteen-malf-day-real-segmented-proof-and-stage-nine-replay-restart-spec-v1-20260419.md`
35. `docs/03-execution/42-stage-fourteen-malf-day-real-segmented-proof-and-stage-nine-replay-restart-conclusion-20260419.md`

## 目录职责

### `docs/01-design/`

回答：

- 为什么这样设计
- 当前生效的治理原则是什么
- 当前阶段的边界和非目标是什么

### `docs/02-spec/`

回答：

- 正式系统边界是什么
- 重构总方案是什么
- 文档治理和执行准入规则是什么

### `docs/03-execution/`

回答：

- 这次批次做了什么
- 用什么命令和测试验证
- 执行过程中有哪些偏差
- 最终裁决是什么

执行层的正式闭环是：

`card -> evidence -> record -> conclusion`

其中：

- `evidence` 只允许放在 `docs/03-execution/evidence/`
- `record` 只允许放在 `docs/03-execution/records/`

## 当前状态

当前仓库已经完成阶段八 `data -> system` 最小 pipeline orchestration，并已完成阶段九真实建库演练首轮执行记录；阶段十一 MALF day repair 已完成，阶段十二 MALF day 写路径重演 unblock 已完成，阶段十三 MALF day segmented build completion 已完成首轮工程落地。阶段十四已启动真实分段完成性验证，但首轮 frontier proof 阻塞于真实库 schema backfill 兼容性，阶段九重演仍待重新发起。

这意味着：

- 阶段一到阶段四的实际交付范围都已经进入正式治理闭环
- `position` 已具备正式规格、独立账本、queue/checkpoint、正式 runner 与三层最小输出
- `portfolio_plan` 已具备最小三表、最小组合裁决与正式 runner
- `trade` 已具备最小执行账本、桥接规格、正式 runner、CLI 入口与测试闭环
- 阶段六规格冻结闭环 `26` 已补齐
- `stage-six-system` 已冻结 `trade -> system` 最小读出边界
- 阶段六工程收口闭环 `27` 已补齐
- `run_system_from_trade` 与 `system_run / system_trade_readout / system_portfolio_trade_summary` 已落地
- 阶段七规格冻结闭环 `28` 已补齐
- `stage-seven-data-source-contract` 已冻结真实本地 stock source fact 路径、表名与字段映射
- 阶段七工程收口闭环 `29` 已补齐
- `malf / alpha / position / trade` source adapter 已支持真实 stock adjusted 表
- 阶段八规格冻结闭环 `30` 已补齐
- `stage-eight-pipeline` 已冻结 `data -> system` 最小 pipeline 编排边界
- 阶段八工程收口闭环 `31` 已补齐
- `run_data_to_system_pipeline` 与 `pipeline_run / pipeline_step_run` 已落地
- 阶段九规格冻结闭环 `32` 已补齐
- `stage-nine-real-data-build` 已冻结真实 `H:\Lifespan-data` 建库演练边界
- 阶段九执行收口闭环 `33` 已补齐
- `run_malf_day_build` 真实库复跑已暴露首个 blocker
- 阶段十规格冻结闭环 `34` 已补齐
- `stage-ten-malf-day-diagnosis` 已冻结 MALF day 真实库诊断边界
- 阶段十工程收口闭环 `35` 已补齐
- `profile_malf_day_real_data` 已确认当前真实瓶颈落在 `engine_timing`
- 阶段十一规格冻结闭环 `36` 已补齐
- 阶段十一工程收口闭环 `37` 已补齐
- `adjust_method = backward` 已 formalize 为 MALF day source contract
- 同一真实诊断窗口下 `engine_seconds` 已从 `6.789267` 降到 `1.419344`
- 当前真实主瓶颈已转为 `write_timing`
- 阶段十二规格冻结闭环 `38` 已补齐
- `stage-twelve-malf-day-write-path-replay-unblock` 已冻结写路径诊断、优化与阶段九重演 unblock 边界
- 阶段十二工程收口闭环 `39` 已补齐
- `write_timing_summary` 已进入 MALF runner 与 diagnostics 输出
- 安装 `pyarrow 23.0.1` 后真实采样窗口 `write_seconds = 0.911749`
- 真实全量 build 在 60 分钟观察窗内仍未完成，阶段九重演尚未登记为完成
- 阶段十三规格冻结闭环 `40` 已补齐
- 阶段十三工程收口闭环 `41` 已补齐
- `run_malf_day_build` 已支持 `segmented build`、`resume`、`progress` 与 `abandoned build artifacts`
- 真实 build 推进顺序已固定为 `100 / 500 / 1000 symbol` 分段证明，再进入 full-universe segmented build
- 阶段十四规格与执行闭环 `42` 已补齐
- 首轮真实 `100 symbol` frontier proof 失败于 `initialize_malf_schema` 对现有真实库的 schema 补齐
- 本轮未生成新的 summary / progress sidecar，`500 / 1000 / full-universe / replay` 均未启动
- 阶段九 replay 继续保持待重发状态
- 阶段五之后正式冻结价格分线：
  - `malf / alpha` 属于 `analysis_price_line`
  - `portfolio_plan / trade / system` 属于 `execution_price_line`
- 阶段四 `reference_trade_date / reference_price` 只是最小桥接参考，不等于正式执行价格口径

Stage-five engineering defaults are frozen:
- `execution_price_line` reads `PathConfig.source_databases.market_base` through the `trade` adapter.
- `planned_trade_date / execution_trade_date / execution_price` use 次日开盘执行.
- `accepted` is reserved; the first implementation materializes only `filled / rejected`.

重构计划 Part 2：
- 文档标识：`reconstruction-plan-part2`
- 主题：第五阶段文档先行与工程实施计划
- 裁决：阶段五完成，阶段六 system 待规划/待实施

阶段六 system 规格：
- 文档标识：`stage-six-system`
- 主题：`trade -> system` 最小读出与 runner
- 裁决：阶段六完成，下一阶段待规划
- 边界：只读取 `trade` 正式输出，不回读 `alpha / position / portfolio_plan`，不触发上游 runner

阶段七 data 源事实契约：
- 文档标识：`stage-seven-data-source-contract`
- 主题：6 个本地 source fact DuckDB、stock 表名和 `code -> symbol` / `trade_date -> bar_dt` 字段映射
- 裁决：阶段七完成，阶段八待规划
- 下一阶段入口：阶段八 `data -> system` 最小全链路编排

阶段八 pipeline 规格：
- 文档标识：`stage-eight-pipeline`
- 主题：`data -> system` 最小 pipeline orchestration
- 裁决：阶段八完成，下一阶段待规划
- 边界：pipeline 只调用 public runner，不直接写业务表

阶段九真实建库演练规格：
- 文档标识：`stage-nine-real-data-build`
- 主题：真实 `H:\Lifespan-data` module-by-module build 与 pipeline replay
- 裁决：阶段九真实建库演练发现阻塞，待修复
- 边界：允许写入 `H:\Lifespan-data\astock_lifespan_alpha`，不删除正式库，`Go+DuckDB deferred`

阶段十 MALF day 真实库诊断规格：
- 文档标识：`stage-ten-malf-day-diagnosis`
- 主题：`run_malf_day_build` 真实库卡点诊断与脚本入口修正
- 裁决：阶段十完成，阶段九重演待重新发起
- 诊断表：`stock_daily_adjusted`
- 边界：只做 `PYTHONPATH` 入口修正与只读诊断，不修改 MALF 业务语义
- 结果：`engine_timing` 已确认，真实采样写回同时暴露 `snapshot_nk / pivot_nk` 重复主键异常
## 阶段十一补充

- 新规格：`docs/02-spec/16-stage-eleven-malf-day-repair-spec-v1-20260419.md`
- 规格冻结结论：`docs/03-execution/36-stage-eleven-malf-day-repair-spec-freeze-conclusion-20260419.md`
- 工程收口结论：`docs/03-execution/37-stage-eleven-malf-day-repair-engineering-closeout-conclusion-20260419.md`

阶段十一正式冻结并实现了以下口径：

- `stock_daily_adjusted` 仅以 `adjust_method = backward` 进入 MALF day
- `symbol + trade_date -> 1 day bar`
- 重复 `symbol + trade_date + backward` 视为 source contract violation
- 真实诊断窗口下 `engine_timing` 已不再是主瓶颈，当前转为 `write_timing`

当前正式状态为：阶段十一完成，阶段九重演待在 `write_timing` 与真实全量 build 持续时长的新瓶颈上重新发起。

## 阶段十二补充

- 新规格：`docs/02-spec/17-stage-twelve-malf-day-write-path-replay-unblock-spec-v1-20260419.md`
- 规格冻结结论：`docs/03-execution/38-stage-twelve-malf-day-write-path-replay-unblock-spec-freeze-conclusion-20260419.md`

阶段十二正式冻结以下口径：

- 下一轮只处理 MALF day 写路径与阶段九真实重演 unblock
- `write_timing` 至少拆成 `delete old rows / insert ledgers / checkpoint / queue update`
- 真实全量 `run_malf_day_build` 可完成性优先于实现优雅度
- `guard anchor / reborn window / 历史谱系 profile` 不进入阶段十二

当前正式状态为：阶段十二规格冻结完成，下一批次进入写路径工程实施、真实全量 build 验证与阶段九重演发起。

阶段十二工程收口：

- 工程收口结论：`docs/03-execution/39-stage-twelve-malf-day-write-path-replay-unblock-engineering-closeout-conclusion-20260419.md`
- `write_timing` 已拆分为 `delete_old_rows_seconds / insert_ledgers_seconds / checkpoint_seconds / queue_update_seconds`
- runner 已支持旧真实库 running 状态下的新库重建与旧库 backup promotion
- 当前正式状态为：阶段十二工程完成但保留真实全量 build 剩余偏差，阶段九重演尚未完成

## 阶段十三补充

- 新规格：`docs/02-spec/18-stage-thirteen-malf-day-segmented-build-completion-spec-v1-20260419.md`
- 规格冻结结论：`docs/03-execution/40-stage-thirteen-malf-day-segmented-build-completion-spec-freeze-conclusion-20260419.md`
- 工程收口结论：`docs/03-execution/41-stage-thirteen-malf-day-segmented-build-completion-engineering-closeout-conclusion-20260419.md`

阶段十三正式冻结并实现了以下口径：

- `run_malf_day_build` 支持 `start_symbol / end_symbol / symbol_limit / resume / progress_path`
- day build 支持 `segmented build` 与 checkpoint-based resume
- `progress_summary` 与 sidecar progress 已进入正式 runner 输出
- `artifact_summary` 可登记 active build path 与 abandoned build artifacts

当前正式状态为：阶段十三工程实现已完成，真实 `100 / 500 / 1000 symbol` 分段证明与 full-universe completion 仍待执行，阶段九 replay 待阶段十三完成后重新发起。
