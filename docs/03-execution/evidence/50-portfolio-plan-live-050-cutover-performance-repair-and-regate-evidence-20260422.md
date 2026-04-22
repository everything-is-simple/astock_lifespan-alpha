# 批次 50 证据：portfolio_plan live 0.50 cutover 性能修复与重验收

证据编号：`50`
日期：`2026-04-22`
文档标识：`portfolio-plan-live-050-cutover-performance-repair-and-regate`

## 1. 本轮代码修复

本轮 `portfolio_plan` slow path 已按 Card 50 路线收口为正式两阶段尾段：

- 一次性物化 `portfolio_plan_ordered_source`
- 按 `planned_entry_trade_date` 稳定推进 carry-in active gross
- 同日内使用 DuckDB window SQL 按行序分配容量
- `snapshot_stage -> run_snapshot prewrite -> short cutover -> backup drop/index rebuild`
- `plan_snapshot_nk / checkpoint / work_queue / public runner` 合同保持不变

同时补齐 live 可观测性：

- `portfolio_plan_run.message` 周期性刷新
- stderr 进度日志不再为空
- 最终 summary 追加 phase timing
- 尾段额外输出：
  - `snapshot_stage_loading`
  - `snapshot_stage_loaded`
  - `run_snapshot_prewrite_loaded`
  - `cutover_started`
  - `cutover_committed`
  - `backup_dropped`

## 2. 单测与本地回归

新增/强化的 `portfolio_plan` 单测覆盖：

- 同日 candidate 顺序消耗容量
- `scheduled_exit_trade_date` 边界日释放容量
- `0.50` 不再退化成旧的 `1 admitted + 1 trimmed` 形态
- cutover 失败清理不会污染 live snapshot / checkpoint
- 遗留 `stage / backup / same-run run_snapshot` 可被启动前清理并恢复

回归结果：

```text
pytest tests/unit/portfolio_plan -q
11 passed

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed

pytest tests/unit/docs/test_portfolio_plan_specs.py -q
2 passed

pytest tests/unit/docs/test_position_specs.py -q
4 passed

pytest -q
110 passed
```

## 3. 正式 live rerun 观测

本轮正式 live rerun 采用：

- CLI：`python scripts/portfolio_plan/run_portfolio_plan_build.py`
- stderr：
  `H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-card50-20260422-195527.stderr.log`
- stdout：
  `H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\portfolio-plan-live-card50-20260422-195527.stdout.json`
- 最新验证 run：
  `portfolio-plan-68ab0db998ad`

stderr 已连续记录：

- `portfolio_plan slow path prepared: source_rows=5892934, entry_dates=8531`
- `dates=100/8531 ...`
- `dates=8531/8531 ...`
- `snapshot_stage_loading`
- `snapshot_stage_loaded rows=5892934`
- `run_snapshot_prewrite_loaded`
- `cutover_started`
- `cutover_committed`
- `backup_dropped`

stdout summary 已记录关键 timing：

- `stage_snapshot_seconds = 186.311`
- `stage_run_snapshot_seconds = 223.199`
- `cutover_seconds = 0.115`
- `backup_drop_seconds = 7.822`

这证明：

- live 运行中 progress 已可观测
- 主段按日 materialization 已能跑穿全量 `5892934` source rows
- `materialized_with_action` join 已可完成
- 大体量写入已被前移到 stage / run_snapshot 预写阶段
- 最终 cutover 已被收缩成短事务并完成提交

## 4. 正式库验收状态

正式库验收查询确认：

- `portfolio_plan_run.status = completed`
- `portfolio_plan_checkpoint.last_run_id = portfolio-plan-68ab0db998ad`
- `portfolio_plan_snapshot` 当前仅保留 `portfolio_gross_cap_weight = 0.50`
- `portfolio_plan_snapshot.plan_status` 当前为：
  - `blocked = 5883494`
  - `admitted = 6638`
  - `trimmed = 2802`
- 库内不存在 `portfolio_plan_snapshot_stage`
- 库内不存在 `portfolio_plan_snapshot_backup`
- `idx_portfolio_plan_snapshot_portfolio` 已恢复到 live snapshot

## 5. 证据裁决

本轮证据表明：

- Card 50 已完成从 `0.15` live snapshot 到正式 `0.50` live snapshot 的真实 cutover
- `portfolio_plan` 对外合同未变，但正式尾段已经切换为更短的 snapshot swap 路线
- 正式库、stderr 日志、stdout summary 三侧证据一致证明本轮 gate 已通过

因此本轮可以正式判定：`portfolio_plan = 放行`。
