# 批次 43 结论：阶段十五 MALF day schema backfill 兼容修复

结论编号：`43`
日期：`2026-04-19`
文档标识：`stage-fifteen-malf-day-schema-backfill-compatibility`

## 1. 裁决

`已接受，保留后续 full-universe / replay 门槛`

## 2. 结论

阶段十五已经按规格完成 schema backfill 兼容修复与真实 proof 重启：

- `initialize_malf_schema()` 支持旧版 `malf_run` 分步 backfill
- `repair_malf_day_schema` 显式 repair/probe 入口已建立
- `scripts/malf/repair_malf_day_schema.py` 已在真实 target + building artifacts 上完成 repair/probe
- `100 / 500 / resume / 1000 symbol` proof 已在正确 active building artifact 上完成

## 3. 关键证据

- `pytest`：`82 passed`
- repair CLI 首轮：`status = completed`，`repaired_database_count = 3`
- repair CLI 幂等复跑：`repaired_database_count = 0`
- `100 symbol`：`day-2848b546a00e`，`symbols_completed = 100`
- `500 symbol`：`day-3de72856bbec`，`symbols_completed = 500`
- `500 resume`：`day-880a6067faff`，`symbols_updated = 0`
- `1000 symbol`：`day-51dbd4bf0753`，`symbols_completed = 1000`

## 4. 偏差与处理

首轮 repair 实现曾改变 building DB mtime，导致一次 `100 symbol` proof 写入原 abandoned artifact `malf_day.day-d696fdcd4774.building.duckdb`。

该偏差已处理：

- repair 逻辑已改为保留既有 mtime
- 测试已覆盖 repair 后 mtime 不变
- 真实 active artifact 顺序已恢复
- 正确 active artifact `malf_day.day-d48ab7015ff4.building.duckdb` 已完成 `100 / 500 / resume / 1000 symbol` proof

## 5. 后续门槛

阶段十五不登记 full-universe completion，也不登记阶段九 replay 完成。

当前 active continuation artifact：

- `checkpoint_count = 5075`
- `remaining_symbols = 426`
- `next_start_symbol = 688618.SH`

下一批次应从 `688618.SH` 继续完成 remaining symbols，随后再按阶段十四原合同执行 full-universe run、promote 和阶段九 replay。
