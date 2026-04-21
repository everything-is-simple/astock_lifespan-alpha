# 批次 46 记录：阶段十六正式增量与自动续跑工程收口

记录编号：`46`
日期：`2026-04-21`
文档标识：`stage-sixteen-incremental-resume-engineering-closeout`

## 1. 执行顺序

1. 补齐 `portfolio_plan / system / pipeline` 的 schema、contracts、runner、repair 入口与 CLI。
2. 补齐 unit tests、contracts tests、docs tests。
3. 跑受影响测试面，确认增量契约与 resume 契约站住。
4. 跑 3 个 repair CLI，回填真实库 checkpoint。
5. 跑 `portfolio_plan` 首轮与复跑 proof。
6. 跑 `system` 首轮与复跑 proof。
7. 跑 `pipeline` 首轮 proof。
8. 第二次 `pipeline` 本地中断后，读库定性为无进程 orphan run。
9. 将 `pipeline-cb3824690208` 精确标记为 `interrupted`，不改任何业务物化结果。
10. 跑第 1 次 `pipeline` 恢复 proof。
11. 跑第 2 次 `pipeline` 正常日跑 proof。
12. 跑全量 `pytest`。

## 2. 关键现场与偏差

### orphan run 定性

本批次唯一需要治理的真实库偏差是：

- `pipeline-cb3824690208`
- `status = running`
- `step_count = 0`
- 无任何业务 runner 进程

该偏差被定性为：

- 本地中断造成的 ledger 残留
- 不是仍在执行的后台进程
- 不应删除 `pipeline_step_run`
- 不应删除 `pipeline_step_checkpoint`
- 不应删除任何业务库物化结果

因此本批次采用精确治理更新：

- 仅将这一条 `pipeline_run` 改为 `interrupted`
- 保留 step checkpoints 与全部业务结果

### portfolio_plan 首轮 `work_units_updated = 1`

这不是失败，而是 repair 后首次切换到新 `portfolio_id` 级 checkpoint/fingerprint 契约的对齐重算。
复跑已归零，说明正式 fast path 成立。

## 3. 最终观察

- `portfolio_plan` 增量粒度保持在 `portfolio_id`，未改变组合容量累计裁决语义。
- `system` 已具备 `portfolio_id + symbol` 级 selective rematerialize 与 reused fast path。
- `pipeline` 只在同组合最新 run 为 `interrupted` 时进入 resume。
- `pipeline` 正常 completed run 不误触发 resume。
- 本批次未扩展到“自动扫描无进程 orphan run”的 OS 级治理。
