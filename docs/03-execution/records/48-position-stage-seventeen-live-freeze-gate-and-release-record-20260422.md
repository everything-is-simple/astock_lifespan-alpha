# 批次 48 记录：position 阶段十七 live freeze gate 与放行

记录编号：`48`
日期：`2026-04-22`
文档标识：`position-stage-seventeen-live-freeze-gate-and-release`

## 1. 执行顺序

1. 复核 `position` 当前 stage-seventeen 合约、正式库 schema 与 live row counts。
2. 确认正式库已处于“代码新、正式库旧”的 contract drift 状态：
   - 缺 `position_exit_plan / position_exit_leg`
   - `planned_entry_trade_date` 未完整回填
3. 为 `position` runner 增加 legacy contract drift 检测与 full refresh 触发。
4. 将 exit 规划从未来自连接改为窗口式“下一个 exit 日期”算法。
5. 补齐回归测试，验证旧 checkpoint 仍会在 contract drift 下触发重算。
6. 跑本地 gate 与 bounded real-data replay。
7. 将第一条正式超时残留 run 标记为 `interrupted`。
8. 以 detached/长时方式重跑正式 `position` repair。
9. 复核正式 DuckDB 并给出本轮唯一判定。

## 2. 关键偏差

### 第一次正式 repair 残留 `running`

第一次 live repair 因交互超时留下：

- `run_id = position-a9f946c50d22`
- `status = running`
- 无对应活跃后台进程

因此本轮将其精确改成：

- `status = interrupted`
- 保留历史 run ledger，不删任何业务物化结果

### exit 规划的正式库性能问题

bounded replay 证明语义成立，但正式库全量数据下，未来自连接 exit 规划代价过高。

因此本轮采取的修补是：

- 不改 stage-seventeen 业务语义
- 只把实现改成窗口式“下一个 exit 日期”算法
- 目标是完成 live cutover，而不是扩展新的 exit contract

## 3. 本轮观察

- `position` 已完成 stage-seventeen 正式 cutover
- `position_exit_plan / position_exit_leg` 已在 live 库正式落表
- `planned_entry_trade_date` 已在 live `position_sizing_snapshot` 大规模回填
- 本轮只证明 `position = 放行`
- 是否进入 `冻结` 必须等待后续 `portfolio_plan` 验证不反向打破
