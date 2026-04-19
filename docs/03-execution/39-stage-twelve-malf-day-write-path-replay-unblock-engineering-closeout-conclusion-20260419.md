# 阶段十二批次 39 MALF day 写路径重演 unblock 工程收口结论

结论编号：`39`
日期：`2026-04-19`
状态：`已接收，保留剩余偏差`

## 1. 裁决

- 接受：`write_timing_summary` 已进入 MALF runner 与 diagnostics 输出合同
- 接受：`write_timing` 已拆分为 `delete_old_rows_seconds / insert_ledgers_seconds / checkpoint_seconds / queue_update_seconds`
- 接受：MALF day ledger 插入已从 Python `executemany` 改为 DuckDB registered relation 写入
- 接受：真实采样窗口下，`write_seconds` 已从 `66.626593` 降到 `1.491133`
- 接受：安装 `pyarrow 23.0.1` 后，真实采样窗口 `write_seconds` 进一步降到 `0.911749`
- 保留偏差：真实全量 `run_malf_day_build` 在 60 分钟观察窗内仍未完成，阶段九全量重演仍不得宣称已打通

## 2. 原因

- 新真实诊断报告 `malf-day-diag-996a9a2aa5e1` 显示：
  - `source_load_seconds = 0.654821`
  - `engine_seconds = 0.783883`
  - `write_seconds = 1.491133`
  - `insert_ledgers_seconds = 1.329266`
  - `delete_old_rows_seconds = 0.029203`
  - `checkpoint_seconds = 0.066266`
  - `queue_update_seconds = 0.066398`
- 对比优化前报告，写路径主耗时从 `66.626593` 降至 `1.491133`
- 安装 `pyarrow 23.0.1` 后，真实诊断报告 `malf-day-diag-60cb27b8ff52` 显示：
  - `source_load_seconds = 0.679347`
  - `engine_seconds = 0.773222`
  - `write_seconds = 0.911749`
  - `insert_ledgers_seconds = 0.755103`
  - `delete_old_rows_seconds = 0.03129`
  - `checkpoint_seconds = 0.05889`
  - `queue_update_seconds = 0.066466`
- 第一次真实全量 build 暴露旧 `malf_day.duckdb` 的 DuckDB index delete fatal，错误为 `Failed to delete all rows from index`
- runner 已改为在发现旧库遗留 `running` 状态时写入新 building 库，成功后备份旧库并提升新库
- 第二次真实全量 build 未再出现 index fatal，但 building 库增长到约 `3.14GB` 后仍在 35 分钟观察窗内未完成，本轮手动终止后台进程
- 安装 `pyarrow` 后第三次真实全量 build 未再出现 index fatal，但 building 库增长到约 `5.22GB` 后仍在 60 分钟观察窗内未完成，本轮受控终止进程

## 3. 影响

- 阶段十二工程实现完成，但阶段九真实全量重演仍不能登记为完成
- 当前剩余瓶颈不再是 `delete old rows / checkpoint / queue update`，而是全量 ledger materialization 体量与持续写入窗口
- 下一轮若继续推进阶段九，需要选择超过 60 分钟的真实执行窗口、分段落库、或进一步降低全量 materialization 写入体量
