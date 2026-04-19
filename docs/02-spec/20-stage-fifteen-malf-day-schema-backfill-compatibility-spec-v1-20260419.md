# 阶段十五 MALF day schema backfill 兼容修复规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-fifteen-malf-day-schema-backfill-compatibility`

## 1. 定位

阶段十五只处理阶段十四首轮真实 proof 暴露的 schema blocker：

```text
Parser Error: Adding columns with constraints not yet supported
```

该 blocker 来自 DuckDB 1.5.1 在现有真实 `malf_run` 表上不支持直接执行带约束的 `ADD COLUMN ... NOT NULL DEFAULT`。

阶段十五不修改 MALF 语义状态机，不修改 `guard anchor / reborn window / 历史谱系 profile`，不 archive/remove 历史 building artifact，也不宣称 full-universe completion 或阶段九 replay 完成。

## 2. Schema Backfill 合同

`initialize_malf_schema()` 必须能兼容旧版真实 `malf_run` 表，并补齐当前正式进度合同所需列：

- `symbols_total`
- `symbols_completed`
- `current_symbol`
- `elapsed_seconds`
- `estimated_remaining_symbols`

对缺失列必须采用 DuckDB 兼容的分步 backfill：

```text
ADD COLUMN <裸类型>
ALTER COLUMN SET DEFAULT
UPDATE 旧行 NULL 值
ALTER COLUMN SET NOT NULL
```

历史旧行不做推断性反算，固定按保守值回填：

- `symbols_total = 0`
- `symbols_completed = 0`
- `current_symbol = NULL`
- `elapsed_seconds = 0`
- `estimated_remaining_symbols = 0`

该逻辑必须幂等。若列已存在但 default 或 nullability 未达当前正式口径，也必须被补齐。

## 3. Repair / Probe 入口

阶段十五新增显式入口：

```text
python scripts/malf/repair_malf_day_schema.py
```

该入口调用 `repair_malf_day_schema`，自动发现：

- `workspace.databases.malf_day`
- 同目录下全部 `malf_day.*.building.duckdb`

每个库执行 probe + repair，并输出 runner-style JSON summary。summary 至少登记：

- 扫描到的数据库路径
- repair 前缺失列
- 实际执行动作
- repair 后 `malf_run` schema 是否兼容

该入口只修 schema，不触发 `run_malf_day_build`，不写业务 ledger，不归档或删除 artifact。

## 4. Proof 重启门槛

只有当真实 `malf_day.duckdb`、active building DB、abandoned building DB 的 `malf_run` 均通过 schema probe 后，才允许恢复阶段十四真实 proof。

恢复顺序仍固定为：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100
500 symbol
500 symbol resume
1000 symbol
```

`100 / 500 / 1000 symbol` 通过前，不推进 full-universe segmented build，不重新发起阶段九 replay。

## 5. 验收标准

阶段十五完成标准为：

1. `initialize_malf_schema()` 可在旧版 `malf_run` 上完成 backfill 且不丢历史行。
2. `repair_malf_day_schema` 可修复 target + building artifacts，并可重复执行。
3. `python scripts/malf/repair_malf_day_schema.py` 在真实库上完成并输出可登记 summary。
4. `100 symbol` frontier proof 可重新发起并生成有效 `segment_summary / progress_summary / artifact_summary`。
5. 若 `100` 通过，再继续按阶段十四口径推进 `500 / resume / 1000`；未满足前不得登记 full-universe completion 或 replay 完成。
