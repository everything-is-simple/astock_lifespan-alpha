# 阶段十四 MALF day 真实分段证明与阶段九重发规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-fourteen-malf-day-real-segmented-proof-and-stage-nine-replay-restart`

## 1. 定位

阶段十四不再继续修改 `run_malf_day_build` 实现，而是直接在真实 `H:\Lifespan-data` 上验证阶段十三已经落地的 `segmented build / resume / progress / artifact_summary` 合同是否真正成立。

本轮要回答的不是“单次耗时是否漂亮”，而是：

- 分段 run 能否稳定完成
- checkpoint-based resume 是否真能续跑
- progress sidecar 是否真能持续写出
- full-universe segmented build 最终能否 promote 到正式 `malf_day.duckdb`
- 在正式 MALF day 完成后，阶段九 replay 能否重新发起

阶段十四不改 MALF 语义，不重开 `guard anchor / reborn window / 历史谱系 profile`，也不自动处理历史 building 库的 `archive / remove`。

## 2. Preflight 与 schema 准入

阶段十四真实执行前，必须先做只读 preflight，并在证据中固定登记：

- day source path：`H:\Lifespan-data\base\market_base.duckdb`
- day source table：`stock_daily_adjusted`
- `adjust_method = backward` 下的 symbol 总量：`5501`
- 正式 `malf_day.duckdb`、active building DB、abandoned building DB 的：
  - 路径
  - 文件大小
  - 最后修改时间
  - day checkpoint 数
  - 最近 run 状态
- 当前 active building DB 必须固定为：
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d48ab7015ff4.building.duckdb`
- 当前 abandoned building DB 必须固定为：
  - `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.day-d696fdcd4774.building.duckdb`

阶段十四默认复用当前 active building DB，不新建独立 proof artifact。

在任何真实 proof 命令之前，还必须满足一个兼容性准入门槛：

> `initialize_malf_schema(active_target_path)` 必须能在现有真实 `malf_day` / `building` 库上完成 schema 补齐。

如果这个门槛失败，阶段十四本轮立即停止，不继续 500 / 1000 / full-universe / replay，只登记 blocker evidence / record / conclusion，且不改代码。

## 3. 真实分段证明顺序

首轮 proof 固定不是裸 `--symbol-limit 100`，而是未完成前沿：

```text
python scripts/malf/run_malf_day_build.py --start-symbol 600771.SH --symbol-limit 100
```

首轮通过标准固定为：

- 命令完成
- stdout summary 含 `segment_summary / progress_summary / artifact_summary`
- `progress_summary.progress_path` 指向实际 sidecar JSON
- `artifact_summary.active_build_path` 指向当前 active building DB
- `artifact_summary.promoted_to_target = false`

首轮通过后，阶段十四继续执行：

1. `500 symbol` 分段，并在中途人为中断
2. 复跑同一 `500 symbol` 命令验证 resume
3. `1000 symbol` 递进证明

`500 symbol` resume 证明必须确认：

- 复跑继续使用同一 active building DB
- 已 checkpoint symbol 不重跑
- `symbols_updated < symbols_seen`
- progress 数字持续可解释

如果首轮 `100 symbol` 在 summary/progress 写出前失败，本轮不允许继续 500 / 1000。

## 4. Full-Universe Completion 与 artifact 处置

当前 active building DB 的真实前沿定义固定为：

> 在 source `symbol ASC` 序列中，第一个不在 active building DB `malf_checkpoint(timeframe='day')` 中的 symbol。

阶段十四必须按以下节奏推进：

1. 每轮以 `--start-symbol <当前前沿> --symbol-limit 1000` 推进
2. 每轮执行前重新计算前沿
3. 当 remaining symbols 归零后，再执行一次最终 full-universe run：

```text
python scripts/malf/run_malf_day_build.py
```

只有满足以下条件，才允许登记 full-universe completion：

- 最终 full-universe run 返回 `artifact_summary.promoted_to_target = true`
- 正式 `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb` 已被新库替换

两个历史 building 库在阶段十四只允许登记处置结论：

- active continuation artifact：`malf_day.day-d48ab7015ff4.building.duckdb`
- abandoned artifact：`malf_day.day-d696fdcd4774.building.duckdb`

阶段十四不自动 `archive` 或 `remove`。

## 5. 阶段九 replay 重发合同

只有在 full-universe promote 成功后，才允许重新发起阶段九 replay。

固定顺序为：

1. `python scripts/malf/run_malf_day_build.py`
2. `python scripts/malf/run_malf_week_build.py`
3. `python scripts/malf/run_malf_month_build.py`
4. `python scripts/alpha/run_alpha_bof_build.py`
5. `python scripts/alpha/run_alpha_tst_build.py`
6. `python scripts/alpha/run_alpha_pb_build.py`
7. `python scripts/alpha/run_alpha_cpb_build.py`
8. `python scripts/alpha/run_alpha_bpb_build.py`
9. `python scripts/alpha/run_alpha_signal_build.py`
10. `python scripts/position/run_position_from_alpha_signal.py`
11. `python scripts/portfolio_plan/run_portfolio_plan_build.py`
12. `python scripts/trade/run_trade_from_portfolio_plan.py`
13. `python scripts/system/run_system_from_trade.py`
14. `python scripts/pipeline/run_data_to_system_pipeline.py`

如果阶段十四在 MALF day 首轮 proof 前就被 schema 兼容性阻塞，则阶段九 replay 不得宣称已重发。

## 6. 验收标准

阶段十四完整完成的标准固定为：

1. 只读 preflight 已登记 source、frontier、active/abandoned artifact
2. `100 / 500 / 1000 symbol` 分段证明全部成立
3. `500 symbol` 中断后 resume 证明成立
4. full-universe segmented build 完成并 promote 到正式 `malf_day.duckdb`
5. 阶段九 module-by-module build 与 pipeline replay 已重新发起并登记结果

如果首轮真实 proof 失败，则本轮最小收口标准固定为：

1. blocker 命令、异常、现场快照已完整登记
2. 明确说明未生成 summary / progress sidecar
3. 明确说明 500 / 1000 / full-universe / replay 未启动
4. 明确说明下一轮应先处理真实库 schema 兼容性，再恢复阶段十四原计划
