# 阶段十二 MALF day 写路径重演 unblock 规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-twelve-malf-day-write-path-replay-unblock`

## 1. 定位

本规格冻结阶段十二的最小执行边界。

阶段十一已经完成两件前置修复：

1. `stock_daily_adjusted` 的 MALF day source contract 已 formalize 为 `adjust_method = backward`
2. 同一真实诊断窗口下，`engine_seconds` 已从 `6.789267` 降到 `1.419344`

因此阶段十二的主问题不再是 MALF 语义或 engine 计算，而是 `write_timing` 与真实全量 build 可完成性。

阶段十二只做 `MALF day` 写路径诊断、写入优化和阶段九真实重演 unblock，不改 `guard anchor / reborn window / 历史谱系 profile`，不扩展 MALF 语义。

## 2. 问题陈述

阶段十一真实诊断报告 `malf-day-diag-68cc9d425b1d` 已确认：

- `source_load_seconds = 3.02114`
- `engine_seconds = 1.419344`
- `write_seconds = 99.106712`
- `bottleneck_stage = write_timing`

同时，`python scripts/malf/run_malf_day_build.py` 在真实全量库上 10 分钟观察窗内仍未完成。

当前阶段十二的核心判断是：

> 真实重演被阻塞在 MALF day 写路径与全量落库持续时长，而不是 source uniqueness 或 engine timing。

## 3. 写路径诊断合同

`profile_malf_day_real_data` 的 `write_timing` 必须拆成更细 phase，至少区分：

- `delete_old_rows_seconds`
- `insert_ledgers_seconds`
- `checkpoint_seconds`
- `queue_update_seconds`

诊断报告必须继续保留阶段十、阶段十一已有字段：

- `source_load_seconds`
- `engine_seconds`
- `write_seconds`
- `bottleneck_stage`
- `selected_adjust_method`
- 过滤前后 duplicate source facts

新增写路径 phase 必须能回答：

- 慢点是否来自逐 symbol 多表 `DELETE`
- 慢点是否来自多表 `executemany`
- 慢点是否来自 `malf_checkpoint` upsert
- 慢点是否来自 `malf_work_queue` insert/update
- 慢点是否来自 DuckDB 单表写入、约束或索引维护

## 4. 工程优化边界

阶段十二允许的优化：

- 合并或批量化同一 run 内的旧行删除
- 减少逐 symbol 重复 `DELETE + executemany`
- 将 ledger 插入从逐 symbol 多次写入改为更粗粒度批量写入
- 调整 checkpoint 与 work_queue 的更新粒度
- 在不改变正式表合同的前提下，优化 DuckDB 写事务边界
- 为真实全量 build 引入可观测的阶段性 timing summary

阶段十二不允许的优化：

- 不修改 MALF 语义状态机
- 不修改 `snapshot_nk / pivot_nk / wave_id / profile_nk` 生成语义
- 不删除或改名 MALF 正式表
- 不改变 public runner 名称
- 不让 `malf` 依赖 `pipeline`
- 不通过跳过 ledger 写入来伪造 build 完成

## 5. 阶段九重演 unblock 合同

阶段十二的目标不是让实现更优雅，而是让阶段九真实重演重新可执行。

验收顺序固定为：

1. 先用诊断报告确认写路径细分瓶颈
2. 再实施最小写路径优化
3. 再运行 `python scripts/malf/run_malf_day_build.py` 真实全量 build
4. build 可稳定完成后，再重新发起阶段九 module-by-module replay

如果全量 build 仍然不可完成，阶段十二必须登记明确偏差：

- 卡在哪个 write phase
- 已处理 symbol 数
- 已写入各 ledger 行数
- checkpoint 与 work_queue 是否放大耗时
- 是否需要进入下一轮批量提交、分段落库或更粗 checkpoint 粒度

## 6. 验收标准

阶段十二工程收口必须满足：

1. `profile_malf_day_real_data` 报告能拆出写路径 phase
2. 单元测试覆盖新增 timing 字段与 markdown/json 输出
3. `pytest` 通过
4. `python scripts/malf/run_malf_day_build.py` 在真实全量库上稳定完成，或登记明确的剩余 write phase blocker
5. 阶段九真实重演被重新发起，或以阶段十二证据说明仍无法重演的精确原因

阶段十二完成后的正式表达应为：

> `write_timing` 已被拆解并针对性优化；`run_malf_day_build` 真实全量 build 已重新具备完成性，或剩余阻塞已精确落到单一写路径 phase；阶段九真实重演重新启动。
