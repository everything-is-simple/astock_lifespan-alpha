# 阶段十批次 35 MALF day 真实库诊断工程收口结论

结论编号：`35`
日期：`2026-04-19`
状态：`已接受`

## 1. 裁决

- 接受：阶段十 MALF day 真实库诊断链路已完成。
- 接受：`run_malf_day_build` 的脚本入口问题已修正，不再依赖手动 `PYTHONPATH`。
- 接受：当前真实库主瓶颈已确认落在 `engine_timing`。
- 接受：阶段九全链路真实演练尚未重开，阶段九重演待重新发起。

## 2. 原因

- 无参执行 `python scripts/malf/profile_malf_day_real_data.py` 已能产出真实库诊断报告。
- 真实诊断报告确认源表为 `stock_daily_adjusted`，规模为 `49,044,339` 行、`5,501` 个 symbol。
- 当前无参诊断默认使用 `symbol_limit = 10`、`bar_limit_per_symbol = 1000` 的真实采样窗口。
- 本轮真实诊断结果为：
  - `source_load_seconds = 2.231766`
  - `engine_seconds = 6.789267`
  - `write_seconds = 0.0`
  - `bottleneck_stage = engine_timing`
- 诊断写回临时 DuckDB 时同时暴露 `snapshot_nk` 与 `pivot_nk` 重复主键异常，说明下一步修复除了 engine 性能外，还需核对真实数据下的主键唯一性假设。

## 3. 影响

- 阶段十完成。
- 当前正式状态切换为：阶段十完成，阶段九重演待重新发起。
- 下一步不应转 Go+DuckDB；应先做 MALF day 最小修复，再重开阶段九真实演练。
