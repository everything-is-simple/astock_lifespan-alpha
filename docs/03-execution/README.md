# 执行区说明

`docs/03-execution/` 不是普通笔记目录，而是正式执行闭环目录。

本目录默认闭环为：

```text
card -> evidence -> record -> conclusion
```

## 阅读顺序

建议先读：

1. `00-card-execution-discipline-20260419.md`
2. `00-conclusion-catalog-20260419.md`
3. 当前批次对应的 `card`
4. 对应 `evidence`
5. 对应 `record`
6. 对应 `conclusion`

## 目录规则

根目录只放：

- `card`
- `conclusion`
- 模板
- 执行纪律
- 结论目录
- `README`

子目录规则：

- 证据放在 `docs/03-execution/evidence/`
- 记录放在 `docs/03-execution/records/`

如果把 `record` 或 `evidence` 直接放到根目录，属于治理违规，需要先回收整理再继续。

## 当前批次

当前已补齐批次：

- `01` 阶段一基础重构
- `02` MALF 文本规格冻结
- `03` MALF 图版规格冻结
- `04` MALF 契约与 Schema
- `05` MALF 语义引擎与 Runner
- `06` MALF 面向 Alpha 输出
- `07` 阶段二本地收口
- `08` Alpha PAS 触发器规格冻结
- `09` alpha_signal 汇总规格冻结
- `10` 阶段三文档收口
- `11` Alpha 契约与 Schema
- `12` Alpha 输入适配与共享骨架
- `13` Alpha 五触发器与 alpha_signal
- `14` 阶段三本地收口
- `15` alpha_signal -> position 桥接规格冻结
- `16` position 最小账本与 runner 规格冻结
- `17` portfolio_plan 最小桥接规格冻结
- `18` 阶段四文档总收口
- `19` position 契约、Schema 与 runner
- `20` position 物化与最小 portfolio_plan bridge
- `21` 阶段四本地收口
- `22` trade 最小执行账本与 runner 规格冻结
- `23` portfolio_plan -> trade 桥接规格冻结
- `24` 阶段五文档总收口
- `25` 阶段五 trade 工程收口
- `26` 阶段六 system 读出规格冻结
- `27` 阶段六 system 读出工程收口
- `28` 阶段七 data 源事实契约规格冻结
- `29` 阶段七 data 源事实契约工程收口
- `30` 阶段八 data -> system pipeline 编排规格冻结
- `31` 阶段八 data -> system pipeline 编排工程收口
- `32` 阶段九真实建库演练规格冻结
- `33` 阶段九真实建库演练执行收口
- `34` 阶段十 MALF day 真实库诊断规格冻结
- `35` 阶段十 MALF day 真实库诊断工程收口
- `36` 阶段十一 MALF day repair 规格冻结
- `37` 阶段十一 MALF day repair 工程收口
- `38` 阶段十二 MALF day 写路径重演 unblock 规格冻结
- `39` 阶段十二 MALF day 写路径重演 unblock 工程收口
- `40` 阶段十三 MALF day segmented build completion 规格冻结
- `41` 阶段十三 MALF day segmented build completion 工程收口
- `42` 阶段十四 MALF day 真实分段证明与阶段九重发
- `43` 阶段十五 MALF day schema backfill 兼容修复

Stage-five implementation defaults are frozen for engineering:
- `execution_price_line` is backed by `PathConfig.source_databases.market_base`.
- The replay work unit is `portfolio_id + symbol`.
- Valid `open` rows use 次日开盘执行 and materialize `filled`.
- `accepted` remains a reserved status and is not materialized by the first runner.

当前状态：
- 阶段五完成。
- `reconstruction-plan-part2` 已落档。
- `stage-six-system` 已冻结。
- 阶段六完成。
- `stage-seven-data-source-contract` 已冻结。
- 阶段七完成。
- `stage-eight-pipeline` 已冻结。
- 阶段八完成。
- `stage-nine-real-data-build` 已冻结。
- 阶段九真实建库演练发现阻塞，待修复。
- `stage-ten-malf-day-diagnosis` 已冻结。
- 阶段十完成。
- 阶段十三 segmented build completion 已冻结并完成首轮工程落地。
- 阶段十四真实分段证明已启动，但首轮 frontier proof 阻塞于真实库 schema backfill 兼容性。
- 阶段十五已完成 schema backfill 兼容修复，并恢复通过 `100 / 500 / resume / 1000 symbol` proof。
- 阶段九 replay 继续保持待重发状态。
## 阶段十一补充

- `36` 阶段十一 MALF day repair 规格冻结
- `37` 阶段十一 MALF day repair 工程收口
- `stage-eleven-malf-day-repair` 已冻结 `adjust_method = backward` 的 MALF day source contract
- 同一真实诊断窗口下 `engine_seconds` 已从 `6.789267` 降到 `1.419344`
- 当前真实主瓶颈已转到 `write_timing`
- 阶段九重演仍待在新瓶颈上重新发起

## 阶段十二补充

- `38` 阶段十二 MALF day 写路径重演 unblock 规格冻结
- `stage-twelve-malf-day-write-path-replay-unblock` 已冻结
- 下一轮只处理 MALF day 写路径与阶段九真实重演 unblock
- `write_timing` 至少拆成 `delete old rows / insert ledgers / checkpoint / queue update`
- `guard anchor / reborn window / 历史谱系 profile` 明确排除在阶段十二之外

## 阶段十二工程补充

- `39` 阶段十二 MALF day 写路径重演 unblock 工程收口
- `write_timing_summary` 已进入 MALF runner 与 diagnostics 输出
- 安装 `pyarrow 23.0.1` 后真实采样窗口 `write_seconds = 0.911749`
- 当前剩余偏差：真实全量 build 在 60 分钟观察窗内仍未完成，阶段九重演尚未登记为完成

## 阶段十三补充

- `40` 阶段十三 MALF day segmented build completion 规格冻结
- `41` 阶段十三 MALF day segmented build completion 工程收口
- `stage-thirteen-malf-day-segmented-build-completion` 已冻结 `segmented build` / `resume` / `progress` / `abandoned build artifacts`
- `run_malf_day_build` 已支持 `start_symbol / end_symbol / symbol_limit / resume / progress_path`
- `segment_summary / progress_summary / artifact_summary` 已进入正式 runner 合同
- 真实推进顺序固定为 `100 / 500 / 1000 symbol` 分段证明，再进入 full-universe segmented build
- 阶段九 replay 待阶段十三完成后重新发起

## 阶段十四补充

- `42` 阶段十四 MALF day 真实分段证明与阶段九重发
- `stage-fourteen-malf-day-real-segmented-proof-and-stage-nine-replay-restart` 已冻结真实执行顺序与 blocker 兜底口径
- 真实 preflight 已确认：
  - day source 为 `H:\Lifespan-data\base\market_base.duckdb::stock_daily_adjusted`
  - symbol 总量为 `5501`
  - active frontier 为 `600771.SH`
  - active continuation artifact 为 `malf_day.day-d48ab7015ff4.building.duckdb`
  - abandoned artifact 为 `malf_day.day-d696fdcd4774.building.duckdb`
- 首轮命令 `python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100` 失败于 `initialize_malf_schema`
- 当前精确 blocker 为：DuckDB 不支持在现有真实 `malf_run` 表上用带约束定义直接 `ADD COLUMN`
- 本轮未生成新的 summary / progress sidecar；`500 / 1000 / full-universe / replay` 均未启动

## 阶段十五补充

- `43` 阶段十五 MALF day schema backfill 兼容修复
- `stage-fifteen-malf-day-schema-backfill-compatibility` 已冻结真实库 schema repair 口径
- `initialize_malf_schema()` 已兼容旧版 `malf_run` 的分步 backfill
- `repair_malf_day_schema` 已完成 target + building artifacts 的显式 repair/probe
- 正确 active artifact 上的 `100 / 500 / resume / 1000 symbol` proof 已通过
- 当前 remaining symbols 为 `426`，下一 frontier 为 `688618.SH`
- 阶段十五不登记 full-universe completion 或阶段九 replay 完成

## 阶段十四 / 阶段九收口补充

- `44` 阶段十四 MALF day full-universe completion 与阶段九 replay 收口
- `stage-fourteen-malf-day-full-universe-completion-and-stage-nine-replay-closeout` 已完成
- 当前正式 `malf_day.duckdb` 已接受为正式结果；其与 07:52 backup 在核心业务列上双向 `EXCEPT = 0`
- `day-fc56...` / `day-d696...building` 相关差异登记为谱系归因偏差，不恢复、不重建
- alpha / position / trade full-universe replay 已完成 set-based / fast-path 修复
- 最新 pipeline `pipeline-4a2a2455df18` 已完成 13 步，阶段九 replay 完成登记
## 阶段十六补充

- `45` 阶段十六正式增量与自动续跑规格冻结
- `46` 阶段十六正式增量与自动续跑工程收口
- `stage-sixteen-incremental-resume` 已完成工程收口与真实 proof
- `portfolio_plan / system / pipeline` 已补齐正式增量与 step 级自动续跑契约

## 主线模块冻结战役补充

- `47` 主线模块冻结战役治理面板启动
- `48` position 阶段十七 live freeze gate 与放行
- `49` portfolio_plan 阶段十七 live freeze gate
- `50` portfolio_plan live `0.50` cutover 性能修复与重验收
- 当前唯一真相源为 `docs/03-execution/47-mainline-module-freeze-campaign-governance-board-conclusion-20260422.md`
- 当前活跃模块已切换为 `trade`
- `position = 放行`
- `portfolio_plan = 放行`
- `portfolio_plan` 的 bounded replay、schema repair、Card 50 regate 已全部通过
- 最新正式 run `portfolio-plan-68ab0db998ad` 已 `completed`，正式 snapshot 已切到 `portfolio_gross_cap_weight = 0.50`
- `position` 尚未升级为 `冻结`；只有 `portfolio_plan` 后续 gate 未反向打破当前口径时才允许升级
- 下一锤模块切换为 `trade`
- `pipeline` 继续只承担 orchestration gate，不倒推业务模块健康
## Card 51 addendum

- `51` trade stage-seventeen live freeze gate 宸茶褰?
- 褰撳墠 `trade = 寰呬慨`
- 鏈€鏂?live run `trade-6f780ccc1005` 宸叉爣璁颁负 `interrupted`
- `system` 缁х画鍐荤粨锛岀瓑寰?trade 鏀捐鍚庡啀杩涘叆 live freeze gate

## Card 52 addendum

- `52` trade live slow-path diagnosis and unblock 已登记
- 当前唯一活跃模块继续保持为 `trade`
- 只读 `profile_trade_live_path` 已能完整返回正式体量 phase timings
- 当前正式主瓶颈已收敛到 `intent_materialized / action_tables_ready`
- 正式 writer 已能推进到 `write_transaction_started`
- 当前 live blocker 已进一步收敛为 `write_targets_cleared` 之前的 target-table delete 路径
- `trade` 继续保持 `待修`
- `system` 继续冻结，等待 `trade` 放行

## Card 53 addendum

- `53` trade delete path unblock 已登记
- target-table delete 已拆成表级与 batch 级阶段
- 正式 run `trade-258bd7bafa7d` 已越过 `write_targets_cleared`
- 正式 run 同时已越过 `write_output_tables_loaded` 与 `write_tracking_tables_loaded`
- 当前 blocker 已移动到 `write_transaction_committed` 之前
- `trade` 继续保持 `待修`
- `system` 继续冻结，等待 `trade` 放行

## Card 54 addendum

- `54` trade commit cutover 已登记
- 正式写入尾段已改为 staged target table replacement
- 正式 run `trade-558802e7f7a4` 已 `completed`
- 正式日志已出现 `write_cutover_committed` 与 `write_transaction_committed`
- `trade_checkpoint.last_run_id` 已全量切到 `trade-558802e7f7a4`
- `trade_position_leg / trade_carry_snapshot / trade_exit_execution` 已落正式表
- 正式库无 staging / backup 残留表，secondary indexes 已恢复
- `trade` 改为 `放行`
- 下一活跃模块切到 `system`

## Card 55 addendum

- `55` system live freeze gate 已登记
- 首次 run `system-2bebfbed66cb` 已标记为 `interrupted`
- `system` source fingerprint 慢路径已改为 aggregate hash
- `system` runner 已补齐 phase-level stderr/message
- 第二次正式 run `system-080b8ac3bf8d` 已 `completed`
- `system_trade_readout = 5902368`
- `open_entry = 5892934`
- `full_exit = 9434`
- `system_checkpoint.last_run_id` 已全量切到 `system-080b8ac3bf8d`
- `system` 改为 `放行`
- 下一活跃模块切到 `pipeline`

## Card 56 addendum

- `56` pipeline live freeze gate 已登记
- 正式 run `pipeline-88b35c7e6e8a` 已 `completed`
- `pipeline_run.step_count = 13`
- `pipeline_contract_version = stage8_pipeline_v1`
- 13 个 `pipeline_step_run` 全部 `completed`
- step 12 = `run_trade_from_portfolio_plan / trade-594d80dfdf1d / completed`
- step 13 = `run_system_from_trade / system-7d34ce3dad1f / completed`
- `pipeline_step_checkpoint.last_pipeline_run_id` 已全量切到 `pipeline-88b35c7e6e8a`
- `pipeline` 改为 `放行`
- 本轮不反向修改或重判 `trade / system`

## Card 57 addendum

- `57` alpha live freeze audit 已登记
- 正式 audit runs：
  - `bof-7f0155fe8bf0`
  - `tst-6eb9d845971d`
  - `pb-ced2863032cf`
  - `cpb-d3670031d272`
  - `bpb-6bb1d9858cf2`
  - `alpha-signal-755796862970`
- 所有 runs 均 `completed`
- `alpha_signal = 5892934`
- `alpha_signal distinct symbol = 5497`
- `position_run.alpha_source_path` 继续指向正式 `alpha_signal.duckdb`
- `alpha` 改为 `放行`
- legacy delta 已正式登记，但本轮不吸收历史 PAS 因子体系
- 下一顺序切到 `malf`
