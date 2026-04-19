# 批次 43 记录：阶段十五 MALF day schema backfill 兼容修复

记录编号：`43`
日期：`2026-04-19`

## 1. 执行记录

阶段十五按 `stage-fifteen-malf-day-schema-backfill-compatibility` 规格执行。

核心实现选择：

- 不直接重建真实库。
- 不修改 MALF 语义。
- 不推断历史 run 的 `symbols_total / symbols_completed`。
- 采用 DuckDB 兼容的分步 schema backfill。
- 同时保留 runner 在线自愈与显式 repair/probe CLI。

## 2. 执行结果

- 全量 `pytest` 通过：`82 passed`
- `python scripts/malf/repair_malf_day_schema.py` 首轮完成：`scanned_database_count = 3`，`repaired_database_count = 3`
- repair CLI 幂等复跑完成：`repaired_database_count = 0`
- 正确 active artifact 上的 `100 symbol` proof 已完成
- `500 symbol` proof 已完成
- 同一 `500 symbol` resume proof 已完成，`symbols_updated = 0`
- `1000 symbol` proof 已完成

## 3. 执行偏差

首轮 repair 实现改变了 building DB mtime，导致一次 `100 symbol` proof 写入原 abandoned artifact `malf_day.day-d696fdcd4774.building.duckdb`。

处理结果：

- 已修复 `repair_malf_day_schema`，确保 repair 后保留既有 mtime。
- 已增加单元测试覆盖 mtime 不变。
- 已恢复真实 active artifact 排序，使 `malf_day.day-d48ab7015ff4.building.duckdb` 重新成为 active。
- 已在正确 active artifact 上重新执行 `100 / 500 / resume / 1000 symbol` proof。

该偏差未触发 target promote，未删除或归档 artifact。

## 4. 当前状态

阶段十五完成后：

- active continuation artifact checkpoint 已推进到 `5075`
- remaining symbols 为 `426`
- 下一 frontier 为 `688618.SH`
- formal `malf_day.duckdb` 未被替换
- full-universe completion 未登记
- 阶段九 replay 未登记为完成
