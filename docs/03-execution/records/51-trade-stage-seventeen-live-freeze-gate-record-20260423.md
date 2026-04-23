# 批次 51 记录：trade stage-seventeen live freeze gate

记录编号：`51`
日期：`2026-04-23`
文档标识：`trade-stage-seventeen-live-freeze-gate`

## 1. 执行顺序

1. 以当前分支上已有的 `trade` 本地修复为基线，不回退既有改动。
2. 在 `src/astock_lifespan_alpha/trade/runner.py` 中把 `trade_source_work_unit_summary` 前的指纹构建拆成两阶段聚合。
3. 在 `tests/unit/trade/test_trade_runner.py` 中新增同一 work unit 多行输入的回归测试。
4. 运行并收口代码侧 gate：
   - `pytest tests/unit/trade -q`
   - `pytest tests/unit/contracts/test_module_boundaries.py -q`
   - `pytest tests/unit/docs/test_trade_specs.py -q`
5. 对正式库执行 preflight，确认：
   - 无活跃 writer
   - `trade-5a83d9f388af` 为孤儿 `running`
   - 上游 `portfolio_plan` live `0.50` 已放行
   - 正式 `trade` 仍停在旧态
6. 将 `trade-5a83d9f388af` 标记为 `interrupted`，恢复正式库状态一致性。
7. 以后台受控方式启动正式 rerun，并把 stdout/stderr 固定写入 `H:\Lifespan-report\astock_lifespan_alpha\trade\`。
8. 连续两个窗口观测 PID、内存、日志和 `trade.duckdb` mtime。
9. 因连续两个窗口都无 CPU / stderr / 数据库写入进展，终止新 live 进程。
10. 回库把 `trade-6f780ccc1005` 标记为 `interrupted`，并登记 Card 51 结论为 `待修`。

## 2. 偏差项

- 计划中的 live pass 线本轮没有达到，原因不是边界或旧 schema 漂移，而是新的 slow path 仍在正式体量下无进展。
- 计划中预设的 `trade_work_queue` 回写、本轮正式下游账表补齐、本轮 checkpoint 切换，全部都没有发生。
- stderr 日志未输出任何新的 progress 信息，因此本轮 blocker 仍需要继续在 `trade` 内部定位。

## 3. 备注

- 本轮没有进入 `system`。
- 本轮没有变更 public runner 名称。
- 本轮没有 bump `trade_contract_version`。
- 本轮正式 live slow path blocker 已经缩小到 `trade` 模块内部，可继续沿 Card 51 在同一模块内迭代。
