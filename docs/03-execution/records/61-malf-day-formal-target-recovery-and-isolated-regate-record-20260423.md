# 批次 61 记录：MALF day formal target 恢复与 isolated regate

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/61-malf-day-formal-target-recovery-and-isolated-regate-card-20260423.md`

## 1. 本轮执行

本轮严格按 Card 61 范围推进：

- 先只读核实 MALF authority 材料与 polluted target 现场
- 修改 `runner.py`，把 `day + full_universe + --no-resume` 固定为 staging build
- 新增 `recover_malf_day_formal_target` 入口与 CLI
- 补齐单测
- 先 recovery formal target，再执行 live rebuild，最后对新 run 直接 forced audit

## 2. 关键实现

### 2.1 no-resume full-universe 选路

本轮把 Card 60 暴露的根因直接落成新 contract：

- 当 `timeframe = day`、`full_universe = true`、`resume = false` 时，runner 无条件新建 `malf_day.<run_id>.building.duckdb`
- canonical target 在 promotion 前不再写入任何 `malf_run / malf_work_queue / malf_checkpoint / malf_*`
- 即使 target 当前是完整 formal target，也不再因为“无 incomplete work”而回落到 target 直写

### 2.2 formal target recovery

本轮新增恢复面：

- `src/astock_lifespan_alpha/malf/recover.py`
- `scripts/malf/recover_malf_day_formal_target.py`

恢复策略遵守以下顺序：

1. 只从 materialized completed run 中解析 baseline
2. 先重建新的 recovery DB
3. 再把旧 canonical target rename 为 quarantine
4. 最后原子替换 canonical target

这使得 polluted target 从“在线正式库”退回成“保留证物”，不会在恢复过程中再次参与写入。

## 3. 关键偏差

### 3.1 Card 61 工程通过，但 MALF 总 gate 仍未放行

Card 61 的工程目标已经全部完成：

- recovery 完成
- isolated rebuild 完成
- forced audit 命中新 run

但 forced audit 的最终 `verdict` 仍是 `部分通过`，因为 `zone_coverage = 3` 仍触发唯一软 flag。

因此本轮不能把结论写成：

- `malf = 放行`

只能写成：

- engineering blocker 已清除
- live formal ledger 已更新到新 run
- 剩余入口只剩 `zone_coverage` 软观察

### 3.2 recovery baseline 没有前移到 `day-a1c965e1f7a9`

本轮没有因为 `day-a1c965e1f7a9` 时间更晚就把它当成恢复基线。

原因是它虽然：

- `completed`
- `5501 / 5501`

但它在五张 `malf_*` materialized 表中是 0 rows。

本轮正式把这类 run 定义为：

- `completed bookkeeping pass`
- 不是 `materialized formal baseline`

## 4. 本轮正式登记

- Card 61 已把 Card 60 的 formal target 污染与 target 直写 blocker 正式清除
- Stage 20 的工程合同已经从“经验路径”升级为“代码 + 单测 + live run + audit”闭环
- `malf_day.duckdb` 当前已重新回到单一正式 run 口径：`day-e687a8277f61`
- MALF 下一张卡不得再回到 recovery/build 基础设施，只允许围绕 `zone_coverage` 软观察继续
