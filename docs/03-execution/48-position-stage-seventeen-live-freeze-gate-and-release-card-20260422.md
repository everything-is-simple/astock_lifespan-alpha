# 批次 48 卡片：position 阶段十七 live freeze gate 与放行

卡片编号：`48`
日期：`2026-04-22`
文档标识：`position-stage-seventeen-live-freeze-gate-and-release`

## 目标

将当前 `position` 的 stage-seventeen rolling backtest contract 完整切到正式 `H:\Lifespan-data`，并给出本轮唯一正式判定：`放行` 或 `待修`。

## 验收口径

- `pytest tests/unit/position -q` 通过。
- `pytest tests/unit/contracts/test_module_boundaries.py -q` 通过。
- bounded real-data replay 能证明 `position` 在真实样本上产出 entry/exit 链。
- live `position.duckdb` 的正式 gate 满足：
  - `position_exit_plan` 非零
  - `position_exit_leg` 非零
  - `position_sizing_snapshot.planned_entry_trade_date` 大规模回填完成
  - 最新 `position_run` 为 `completed`
  - 最新 `position_run.inserted_exit_plan_rows / inserted_exit_leg_rows` 非零

## 本轮边界

- 只继续 `position`，不切到 `portfolio_plan`
- 不改 public runner 名称
- 不改 stage-seventeen 既有业务语义
- 本轮结论只允许写 `放行`，不写 `冻结`
