# 批次 42 记录：阶段十四 MALF day 真实分段证明与阶段九重发

记录编号：`42`
日期：`2026-04-19`

## 1. 做了什么

1. 对真实 source、正式 `malf_day.duckdb`、active building DB 和 abandoned building DB 做了只读 preflight。
2. 确认当前 active 前沿为 `600771.SH`，剩余 `2026` 个 symbol。
3. 按计划执行首轮真实命令：`python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100`。
4. 在命令失败后复核 artifacts、checkpoint、latest run 与 report sidecar 状态。
5. 按阶段十四假设停止后续 `500 / 1000 / full-universe / replay`，只登记 blocker 文档闭环。

## 2. 偏差项

- 首轮真实 proof 失败在 schema 初始化阶段，而不是运行阶段。
- 失败点是 `initialize_malf_schema` 对现有真实 `malf_run` 表补列时执行：
  - `ALTER TABLE malf_run ADD COLUMN symbols_total BIGINT NOT NULL DEFAULT 0`
  - DuckDB 返回：`Parser Error: Adding columns with constraints not yet supported`
- 因为失败发生在 run stub 与 progress 持久化之前，本轮没有 summary JSON，也没有 progress sidecar。
- 阶段十四原计划中的 `500 / 1000 / full-universe / stage-nine replay` 均未启动。

## 3. 备注

- 本批次没有改代码，只执行了真实验证和文档登记。
- 两个历史 building 库继续保持原状：
  - `malf_day.day-d48ab7015ff4.building.duckdb` 继续登记为 active continuation artifact
  - `malf_day.day-d696fdcd4774.building.duckdb` 继续登记为 abandoned artifact
- 下一轮若要恢复阶段十四原计划，前置条件应改为：先处理真实 `malf_day` / `building` 库的 schema 兼容性与 backfill，再重新启动 `100 / 500 / 1000 symbol` proof。
