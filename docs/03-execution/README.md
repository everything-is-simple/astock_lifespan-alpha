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
- 阶段九 replay 待阶段十三完成后重新发起。
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
