# 批次 44 证据：阶段十四 MALF day full-universe completion 与阶段九 replay 收口

证据编号：`44`
日期：`2026-04-21`

## 1. MALF day full-universe completion

阶段十五结束后的 preflight：

- source symbols：`5501`
- active checkpoint：`5075`
- remaining symbols：`426`
- frontier：`688618.SH`
- formal target checkpoint：`6`

remaining segment：

- 命令：`python scripts/malf/run_malf_day_build.py --start-symbol 688618.SH --symbol-limit 1000`
- 结果：`status = completed`
- `symbols_completed = 426`
- `symbols_updated = 426`
- `promoted_to_target = false`

full-universe promote：

- 命令：`python scripts/malf/run_malf_day_build.py`
- 结果：`promoted_to_target = true`
- formal `malf_day.duckdb` checkpoint：`5501`

## 2. MALF 正式库接受依据

后续 pipeline 曾在 `2026-04-20 22:11` 触发 `day-fc56ff5e5441`，并从 `day-d696...building` 谱系生成正式库。

对比 `malf_day.duckdb` 与 `malf_day.backup-day-fc56ff5e5441.duckdb`：

- `malf_pivot_ledger`：双向 `EXCEPT = 0`
- `malf_wave_ledger`：双向 `EXCEPT = 0`
- `malf_state_snapshot`：双向 `EXCEPT = 0`
- `malf_wave_scale_snapshot`：双向 `EXCEPT = 0`
- `malf_wave_scale_profile`：双向 `EXCEPT = 0`
- `malf_checkpoint(symbol, timeframe, last_bar_dt)`：双向 `EXCEPT = 0`

因此接受当前正式 `malf_day.duckdb`，将差异登记为谱系归因偏差，不恢复、不重建。

## 3. Scalability fix

真实 replay 暴露的瓶颈与处理：

- alpha trigger：原路径加载全量 market rows，改为 DuckDB set-based source join 与 trigger materialization
- alpha_signal：改为 DuckDB set-based 汇总五个 trigger DB
- position：改为 DuckDB set-based materialization，并使用 ASOF join 获取不早于 signal date 的参考价格
- trade：补 checkpoint reused fast path，并将主订单删除/插入包进事务，避免中断后留下空表
- MALF day artifact selection：完整正式 target 优先，stale `.building.duckdb` 只登记为 abandoned
- MALF incomplete work 判定：只将未被 checkpoint 覆盖的 running queue 视为 incomplete

## 4. Orphan running 治理

显式标记为 `interrupted` 的 orphan：

- `trade-3f485b2b81cf`
- `pipeline-537e38c0c12e`
- `trade-3a93ba69841d`
- `trade-f9b520455f05`
- `pipeline-5bcbb03d612f`

治理原则：

- 不删除历史 run 行
- 不删除审计记录
- 对 trade 中断导致的空主表，通过重新 materialize 恢复正式 `trade_order_intent / trade_order_execution`

## 5. 最终真实 replay 结果

最新 pipeline：

- run_id：`pipeline-4a2a2455df18`
- status：`completed`
- step_count：`13`
- started_at：`2026-04-21 10:08:39`
- finished_at：`2026-04-21 10:54:04`

关键步骤：

- step 1 `run_malf_day_build`：`day-27acb098c94d`，completed
- step 10 `run_position_from_alpha_signal`：`position-58147458c7ad`，completed
- step 11 `run_portfolio_plan_build`：`portfolio-plan-336e6174306a`，completed
- step 12 `run_trade_from_portfolio_plan`：`trade-38bf18c8918c`，completed
- step 13 `run_system_from_trade`：`system-1bc072d08b83`，completed

最新 trade：

- run_id：`trade-38bf18c8918c`
- status：`completed`
- input_rows：`5892934`
- work_units_seen：`5497`
- work_units_updated：`0`
- intents_reused：`5892934`
- executions_reused：`5892934`

最新 system：

- run_id：`system-1bc072d08b83`
- status：`completed`
- readout_rows：`5892934`
- summary_rows：`1`

## 6. 测试

定向测试：

```text
pytest tests/unit/malf/test_runner.py
pytest tests/unit/alpha/test_alpha_runner.py
pytest tests/unit/position/test_position_runner.py
pytest tests/unit/trade/test_trade_runner.py
pytest tests/unit/system/test_system_runner.py tests/unit/pipeline/test_pipeline_runner.py
```

结果已通过。

全量测试：

```text
pytest
```

结果：

```text
86 passed in 34.89s
```
