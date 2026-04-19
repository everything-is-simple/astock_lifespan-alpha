# 阶段十三 MALF day segmented build completion 规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-thirteen-malf-day-segmented-build-completion`

## 1. 定位

阶段十三不再继续深挖 `insert_ledgers_seconds` 的微观优化，而是把真实全量 `run_malf_day_build` 改造成可 `segmented build`、可 `resume`、可 `progress` 观测、最终可完成的正式执行路径。

阶段十三明确继承阶段十二结论：

- `write_timing` 已拆开
- `insert_ledgers_seconds` 已显著下降
- `pyarrow` 路径已生效
- 真实全量 build 已不再 fatal crash
- 但 60 分钟观察窗仍未完成，building 库已增长到 `5.22GB`

因此本轮主问题不是单批写入慢，而是全量 materialization 体量过大，且 runner 还缺少分段、续跑、进度与完成性证明。

阶段十三不修改 MALF 语义，不重开 `guard anchor / reborn window / 历史谱系 profile`。

## 2. 分段 build 合同

`run_malf_day_build` 必须支持以下可选参数：

- `start_symbol`
- `end_symbol`
- `symbol_limit`
- `resume`
- `progress_path`

过滤顺序固定为：

1. 先按 `symbol ASC` 选择 universe
2. 再应用 `start_symbol / end_symbol`
3. 最后应用 `symbol_limit`

阶段十三允许用 `100 / 500 / 1000 symbol` 作为分段完成性证明，但不允许把分段证明误登记为阶段九 replay 已打通。

## 3. resume 与完成性合同

恢复依据固定为 `malf_checkpoint(symbol, timeframe)`，而不是旧 `running` queue 的干净程度。

当 source 最新 `bar_dt` 已被 checkpoint 覆盖时：

- `resume = true` 必须直接跳过该 symbol
- 不得重复 materialize 已完成 symbol

当 `malf_day.day-*.building.duckdb` 已存在可恢复 building 库时：

- day build 必须优先复用 active building 库
- 其余未采用的 building 库必须登记为 `abandoned build artifacts`

分段 run 完成后只更新 building 库，不直接宣称正式 `malf_day.duckdb` 已完成。

只有在 full-universe run 下确认所有 selected symbols 已被 checkpoint 覆盖后，才允许把 building 库 promote 到正式 target。

## 4. progress 合同

runner summary 与 sidecar progress 至少必须输出：

- `symbols_total`
- `symbols_seen`
- `symbols_completed`
- `current_symbol`
- `elapsed_seconds`
- `estimated_remaining_symbols`
- 各 ledger 已写行数

`malf_run` 至少必须持续刷新：

- `symbols_total`
- `symbols_completed`
- `current_symbol`
- `elapsed_seconds`
- `estimated_remaining_symbols`

阶段十三之后，真实 build 运行 60 分钟不得再只剩 “看 DB 文件大小” 这一种观测方式。

## 5. artifact 隔离与阶段九关系

阶段十三必须 preflight 扫描 `malf_day*.building.duckdb` 并输出：

- 当前 active build path
- `abandoned build artifacts`
- 是否已 promote 到 target

当前已知需要登记的 artifacts 至少包括：

- `malf_day.day-d696fdcd4774.building.duckdb`
- `malf_day.day-d48ab7015ff4.building.duckdb`

阶段十三只做登记与隔离，不自动 `archive` 或 `remove`。

阶段九 replay 的顺序固定为：

1. 先完成 `100 / 500 / 1000 symbol` 分段证明
2. 再完成 full-universe segmented build
3. 最后重新发起阶段九 replay

正式结论口径固定为：

> 阶段九 replay 待阶段十三完成后重新发起。
