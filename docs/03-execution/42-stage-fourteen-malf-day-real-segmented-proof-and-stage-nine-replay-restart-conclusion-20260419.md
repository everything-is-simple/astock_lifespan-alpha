# 阶段十四批次 42 MALF day 真实分段证明与阶段九重发结论

结论编号：`42`
日期：`2026-04-19`
状态：`已记录阻塞`

## 1. 裁决

- 接受：阶段十四只读 preflight 已完成，真实 source、active frontier 和 active/abandoned artifacts 已登记。
- 记录阻塞：首轮真实 `100 symbol` frontier proof 在 schema 初始化阶段失败，未进入 summary/progress 写出。
- 拒绝：把本批次解释为 `100 / 500 / 1000 symbol` 分段证明已经成立。
- 拒绝：把本批次解释为 full-universe segmented completion 已完成或阶段九 replay 已重发。

## 2. 原因

- 真实 source 与 artifact 现场与阶段十四计划一致：
  - day source 为 `H:\Lifespan-data\base\market_base.duckdb::stock_daily_adjusted`
  - symbol 总量为 `5501`
  - active frontier 为 `600771.SH`
  - active continuation artifact 为 `malf_day.day-d48ab7015ff4.building.duckdb`
  - abandoned artifact 为 `malf_day.day-d696fdcd4774.building.duckdb`
- 首轮命令
  - `python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100`
  在约 `3` 秒内失败于 `initialize_malf_schema(active_target_path)`。
- DuckDB 对现有真实 `malf_run` 表执行补列时返回：
  - `Parser Error: Adding columns with constraints not yet supported`
- 失败发生在 run stub、summary 与 sidecar progress 写出之前，因此：
  - 没有新的 summary JSON
  - 没有新的 progress JSON
  - 没有新增真实 `run_id`
  - checkpoint 数未推进

## 3. 影响

- 阶段十四当前状态切换为：真实 segmented proof 已启动，但被真实库 schema 兼容性阻塞。
- 阶段九 replay 继续保持阻塞状态，不得宣称已重新发起。
- 下一轮若继续推进，应先处理真实 `malf_day` / `building` 库的 schema backfill 兼容性，再重新执行阶段十四原顺序：
  - `100 symbol`
  - `500 symbol interrupt/resume`
  - `1000 symbol`
  - full-universe segmented completion
  - stage-nine replay restart
