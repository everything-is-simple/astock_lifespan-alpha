# 批次 50 记录：portfolio_plan live 0.50 cutover 性能修复与重验收

记录编号：`50`
日期：`2026-04-22`
文档标识：`portfolio-plan-live-050-cutover-performance-repair-and-regate`

## 1. 执行顺序

1. 以当前本地未提交 `runner.py / test_portfolio_plan_runner.py` 为实施基线，保持 `portfolio_plan` 对外合同不变。
2. 将 slow path 尾段固定为：
   - `snapshot_stage_loading`
   - `run_snapshot_prewrite_loaded`
   - `cutover_started`
   - `cutover_committed`
   - `backup_dropped`
3. 在 runner 启动前增加 `stage / backup / same-run run_snapshot` 残留清理与恢复。
4. 补强 cutover 失败清理与遗留恢复单测，并确认以下前置验证全部通过：
   - `pytest tests/unit/portfolio_plan -q`
   - `pytest tests/unit/contracts/test_module_boundaries.py -q`
   - `pytest tests/unit/docs/test_portfolio_plan_specs.py -q`
   - `pytest tests/unit/docs/test_position_specs.py -q`
   - `pytest -q`
5. 在正式 `H:\Lifespan-data` 上执行 `python scripts/portfolio_plan/run_portfolio_plan_build.py`，stderr 与 stdout 分别落到 `H:\Lifespan-report\astock_lifespan_alpha\portfolio_plan\`。
6. 观察到正式 run `portfolio-plan-68ab0db998ad` 完整走过：
   - `dates=8531/8531`
   - `snapshot_stage_loaded rows=5892934`
   - `run_snapshot_prewrite_loaded`
   - `cutover_committed`
   - `backup_dropped`
7. 查询正式库，确认 `checkpoint / snapshot / stage-cleanup / live index` 全部满足 Card 50 放行口径。
8. 回写 Card 50 证据、记录、结论，以及治理面板和目录索引。

## 2. 关键结论

- 本轮已经确认：
  - 旧的 live `DELETE + INSERT + INSERT + COMMIT` 尾段已被替换为 `snapshot swap + checkpoint` 短事务
  - `portfolio_plan_checkpoint` 已切到新的 `0.50` run
  - 正式 `portfolio_plan_snapshot` 已从旧 `0.15` 结果切到新的 `0.50`
  - `stage / backup` 残留未污染正式库，live 索引已恢复
- 本轮未做：
  - 不进入 `trade/system/pipeline`
  - 不升级 `portfolio_plan_contract_version`
  - 不把 `position` 直接升级为 `冻结`

## 3. 当前观察

- `position` 继续维持 `放行`
- `portfolio_plan` 已从 `待修` 切到 `放行`
- 当前下一锤模块切换为 `trade`
- `pipeline` 继续只承担 orchestration gate，不反推业务模块健康
