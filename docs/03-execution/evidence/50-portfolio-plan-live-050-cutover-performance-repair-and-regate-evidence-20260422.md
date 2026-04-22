# 批次 50 证据：portfolio_plan live 0.50 cutover 性能修复与重验收

证据编号：`50`
日期：`2026-04-22`
文档标识：`portfolio-plan-live-050-cutover-performance-repair-and-regate`

## 1. 本轮代码修复

本轮 `portfolio_plan` slow path 已从全表递归累计改为分层正式路径：

- 一次性物化 `portfolio_plan_ordered_source`
- 按 `planned_entry_trade_date` 稳定推进 carry-in active gross
- 同日内使用 DuckDB window SQL 按行序分配容量
- `plan_snapshot_nk / checkpoint / work_queue / public runner` 合同保持不变

同时补齐 live 可观测性：

- `portfolio_plan_run.message` 周期性刷新
- stderr 进度日志不再为空
- 最终 summary 追加 phase timing
- 尾段额外输出：
  - `Building materialized_with_action join`
  - `Starting committed snapshot replace`
  - `old snapshot deleted`
  - `snapshot inserted`
  - `run_snapshot inserted`

## 2. 单测与本地回归

新增/强化的 `portfolio_plan` 单测覆盖：

- 同日 candidate 顺序消耗容量
- `scheduled_exit_trade_date` 边界日释放容量
- `0.50` 不再退化成旧的 `1 admitted + 1 trimmed` 形态

回归结果：

```text
pytest tests/unit/portfolio_plan -q
9 passed
```

## 3. 正式 live rerun 观测

本轮正式 live rerun 采用：

- CLI：`python -X utf8 scripts/portfolio_plan/run_portfolio_plan_build.py`
- stderr：
  `H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-card50-20260422-160527.stderr.log`
- stdout：
  `H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-card50-20260422-160527.stdout.log`
- 最新验证 run：
  `portfolio-plan-0875345c4aa5`

stderr 已连续记录：

- `portfolio_plan slow path prepared: source_rows=5892934, entry_dates=8531`
- `dates=100/8531 ...`
- `dates=8531/8531 ...`
- `Building materialized_with_action join`
- `Starting committed snapshot replace`
- `old snapshot deleted`
- `snapshot inserted`
- `run_snapshot inserted`

这证明：

- live 运行中 progress 已可观测
- 主段按日 materialization 已能跑穿全量 `5892934` source rows
- `materialized_with_action` join 已可完成
- committed replace 已进入正式写入阶段

## 4. 当前未完成项

尽管主段与尾段步骤日志都已跑出，但本轮验证窗口内，正式 rerun 仍未完成最终 commit 收口。

本轮最终处理：

- 手动停止进程
- 将 `portfolio-plan-0875345c4aa5` 显式改写为 `interrupted`

停机后正式库状态仍为旧口径：

- `portfolio_plan_checkpoint.last_run_id = portfolio-plan-bd3a42d2fafe`
- `portfolio_plan_snapshot.plan_status` 仍是：
  - `blocked = 5892932`
  - `admitted = 1`
  - `trimmed = 1`

## 5. 证据裁决

本轮证据表明：

- Card 50 已实质修复前序黑盒：
  - 全表递归物化已移除
  - live progress 日志已恢复
  - `materialized_with_action` 与 committed replace 阶段已可被直接观察
- 但正式 `0.50` gate 仍未在本轮验证窗口内完成最终提交

因此当前不能判 `放行`，`portfolio_plan` 继续维持 `待修`。
