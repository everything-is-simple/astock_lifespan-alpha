# 阶段十 MALF day 真实库诊断规格 v1

日期：`2026-04-19`
状态：`冻结`
文档标识：`stage-ten-malf-day-diagnosis`

## 1. 定位

本规格冻结阶段十 `run_malf_day_build` 真实库诊断边界。

阶段十不是 Go+DuckDB 迁移阶段，也不是 MALF 业务语义重写阶段。阶段十明确 `不修改 MALF 业务语义`，只处理两件事：

1. 修正从仓库根目录直跑脚本时暴露的 `PYTHONPATH` 入口问题。
2. 为 `run_malf_day_build` 建立真实 `stock_daily_adjusted` 读库诊断链路，确认瓶颈主要落在 `source load timing`、`engine timing` 或 `write timing` 之一。

## 2. 已知事实

- 真实日线源表为 `stock_daily_adjusted`。
- 当前真实规模约为 `49,044,339` 行、`5,501` 个 symbol。
- 阶段九 blocker 发生在 `run_malf_day_build` 首步，尚未进入 alpha 之后的真实复跑。
- 当前 `load_source_bars()` 会把整表按 `symbol, bar_dt` 全量读回 Python，再按 symbol 分组。
- 当前 `scripts/malf/run_malf_day_build.py` 直接运行会先暴露 `PYTHONPATH` 问题。

## 3. 正式边界

阶段十固定范围为：

- 脚本 bootstrap 入口修正
- MALF day 真实库只读诊断
- runner 内部最小诊断拆分

阶段十明确不纳入：

- Go+DuckDB
- MALF 业务语义改写
- formal DuckDB schema 变更
- `MalfRunSummary / CheckpointSummary / RunStatus` 契约变更
- 阶段九全链路真实重演

## 4. 入口修正规则

- 新增共享脚本 bootstrap helper，负责脚本直跑时自动注入 `src`。
- 阶段九演练链路涉及脚本全部接入同一 helper：
  - `malf`
  - `alpha`
  - `position`
  - `portfolio_plan`
  - `trade`
  - `system`
  - `pipeline`
- 不改 public runner 名称。
- 不改 CLI 输出 JSON summary 的基本形态。

## 5. 诊断输出规则

阶段十新增只读诊断入口，用于真实库下的 MALF day 阶段耗时定位。

诊断输出不写 formal business DuckDB，只允许写到 `report_root` 或 `temp_root`。

诊断报告至少包含：

- source 表行数
- symbol 数
- 日期范围
- `load_source_bars` 总耗时
- `source load timing`
- `engine timing`
- `write timing`
- top-N 慢 symbol
- 是否卡在整表加载、engine 计算或 DuckDB 写回

## 6. runner 约束

- `run_malf_day_build(*, settings: WorkspaceRoots | None = None) -> MalfRunSummary` 保持不变。
- `_run_malf_build()` 允许为诊断拆成明确阶段，但不改变 checkpoint 语义、表名、`runner_name` 和 `status` 枚举。
- 诊断链路允许复用 runner 内部阶段函数，但不改变既有对外接口。

## 7. 验收标准

阶段十必须满足：

1. 从仓库根目录执行 `python scripts/malf/run_malf_day_build.py` 不再依赖手动设置 `PYTHONPATH`。
2. `python scripts/malf/profile_malf_day_real_data.py` 能产出诊断报告。
3. 报告能明确指出瓶颈主要落在 `source load timing`、`engine timing` 或 `write timing`。
4. docs 测试、MALF 单测、contracts 测试与全量测试通过。
5. 阶段十完成后，正式状态表达为：诊断链路已完成，阶段九重演待重新发起。
