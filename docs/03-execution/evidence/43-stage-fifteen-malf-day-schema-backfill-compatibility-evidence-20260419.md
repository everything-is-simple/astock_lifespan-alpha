# 批次 43 证据：阶段十五 MALF day schema backfill 兼容修复

证据编号：`43`
日期：`2026-04-19`

## 1. 代码变更

- `src/astock_lifespan_alpha/malf/schema.py`
  - `initialize_malf_schema()` 改为兼容旧版 `malf_run` 的分步 backfill。
  - 新增固定 backfill 列：`symbols_total / symbols_completed / current_symbol / elapsed_seconds / estimated_remaining_symbols`。
- `src/astock_lifespan_alpha/malf/repair.py`
  - 新增 `repair_malf_day_schema`，自动发现 target + building artifacts 并输出 probe/repair summary。
- `scripts/malf/repair_malf_day_schema.py`
  - 新增显式 repair/probe CLI。
- `tests/unit/malf/test_runner.py`
  - 新增旧 schema backfill、legacy building runner、repair 幂等测试。

## 2. 测试

```text
pytest
```

结果：

```text
82 passed in 20.93s
```

## 3. 真实 Repair / Probe

命令：

```text
python scripts/malf/repair_malf_day_schema.py
```

首轮结果：

- `status = completed`
- `scanned_database_count = 3`
- `repaired_database_count = 3`
- repair 对象：
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`
- 三个库 repair 后：
  - `missing_columns = []`
  - `compatible = true`

幂等复跑结果：

- `status = completed`
- `scanned_database_count = 3`
- `repaired_database_count = 0`
- 三个库均保持 `compatible = true`

## 4. Proof 重启

### 4.1 首轮偏差

首轮 repair 实现会改变 building DB 的 mtime，导致 runner 将原 abandoned artifact `malf_day.day-d696fdcd4774.building.duckdb` 识别为 active。

该偏差已修复：

- `repair_malf_day_schema` 现保留既有数据库文件 mtime
- 单元测试已覆盖 repair 后 mtime 不变
- 真实 artifact 顺序已恢复为：
  - active：`malf_day.day-d48ab7015ff4.building.duckdb`
  - abandoned：`malf_day.day-d696fdcd4774.building.duckdb`

偏差期间产生一次额外 segmented run：

- run_id：`day-02686332592b`
- artifact：`malf_day.day-d696fdcd4774.building.duckdb`
- `symbols_total = 100`
- `symbols_completed = 100`
- `symbols_updated = 100`

该 artifact 仍登记为 abandoned，未 promote 到正式 target。

### 4.2 正确 active 上的 100 symbol proof

命令：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100
```

结果：

- run_id：`day-2848b546a00e`
- `status = completed`
- `message = MALF day segmented build completed without target promotion.`
- `symbols_total = 100`
- `symbols_seen = 100`
- `symbols_completed = 100`
- `symbols_updated = 100`
- `progress_path = H:\Lifespan-report\astock_lifespan_alpha\malf\run_malf_day_build-day-2848b546a00e-progress.json`
- `active_build_path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- `promoted_to_target = false`

### 4.3 500 symbol proof

frontier：

- `done_symbols = 3575`
- `remaining_symbols = 1926`
- `next_start_symbol = 600893.SH`

命令：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 600893.SH --symbol-limit 500
```

结果：

- run_id：`day-3de72856bbec`
- `status = completed`
- `symbols_total = 500`
- `symbols_seen = 500`
- `symbols_completed = 500`
- `symbols_updated = 500`
- `progress_path = H:\Lifespan-report\astock_lifespan_alpha\malf\run_malf_day_build-day-3de72856bbec-progress.json`
- `active_build_path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- `promoted_to_target = false`

### 4.4 500 symbol resume proof

命令：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 600893.SH --symbol-limit 500
```

结果：

- run_id：`day-880a6067faff`
- `status = completed`
- `symbols_total = 500`
- `symbols_seen = 500`
- `symbols_completed = 500`
- `symbols_updated = 0`
- `ledger_rows_written` 全部为 `0`
- `progress_path = H:\Lifespan-report\astock_lifespan_alpha\malf\run_malf_day_build-day-880a6067faff-progress.json`
- `active_build_path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- `promoted_to_target = false`

### 4.5 1000 symbol proof

frontier：

- `done_symbols = 4075`
- `remaining_symbols = 1426`
- `next_start_symbol = 603259.SH`

命令：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 603259.SH --symbol-limit 1000
```

结果：

- run_id：`day-51dbd4bf0753`
- `status = completed`
- `symbols_total = 1000`
- `symbols_seen = 1000`
- `symbols_completed = 1000`
- `symbols_updated = 1000`
- `progress_path = H:\Lifespan-report\astock_lifespan_alpha\malf\run_malf_day_build-day-51dbd4bf0753-progress.json`
- `active_build_path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- `promoted_to_target = false`

## 5. Post-Proof 快照

active continuation artifact：

- `path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- `checkpoint_count = 5075`
- `remaining_symbols = 426`
- `next_start_symbol = 688618.SH`
- `run_status_counts = {"completed": 4, "running": 1}`
- latest run：`day-51dbd4bf0753`

formal target：

- `path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
- `checkpoint_count = 6`
- 未被 promote 替换

abandoned artifact：

- `path = H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`
- `checkpoint_count = 2000`
- `run_status_counts = {"completed": 1, "running": 1}`

三类数据库 `malf_run` 均已具备：

- `symbols_total BIGINT NOT NULL DEFAULT 0`
- `symbols_completed BIGINT NOT NULL DEFAULT 0`
- `current_symbol VARCHAR`
- `elapsed_seconds DOUBLE NOT NULL DEFAULT 0`
- `estimated_remaining_symbols BIGINT NOT NULL DEFAULT 0`
