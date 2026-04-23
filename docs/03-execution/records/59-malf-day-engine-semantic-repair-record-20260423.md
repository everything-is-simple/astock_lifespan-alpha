# 批次 59 记录：MALF day engine 纯语义修复

- 日期：`2026-04-23`
- 对应卡片：`docs/03-execution/59-malf-day-engine-semantic-repair-card-20260423.md`

## 1. 代码变更

- 更新 `src/astock_lifespan_alpha/malf/engine.py`
  - 新增内部 `pending_guard_price` 状态
  - 将 `reborn` 改为“先形成 guard 候选，再允许首次正式 HH/LL 确认”
  - 将 `guard_price` 改为确认式结构锚点，只在推进 bar 上落正式 `HL / LH` pivot
- 更新 `tests/unit/malf/test_engine.py`
  - 新增 `reborn` 连续窗口测试
  - 新增 `guard` 确认式更新测试
  - 新增四区 `wave_position_zone` 覆盖测试
  - 将 legacy 稳定性测试收敛为 `rank` 计算稳定，而不是旧 zone 输出逐字不变

## 2. 本轮没有做的事

- 未修改 `runner.py`
- 未修改 `schema.py`
- 未修改 `audit.py` 的 7 条硬规则定义
- 未执行 live `day` build
- 未改 `week / month`
- 未进入下游模块

## 3. 本轮正式登记

- Stage 19 的“图版优先” authority stack 已落到正式规格
- `engine` 纯语义修复已完成代码实现与本地单测验证
- live audit 已复跑，但仍在审旧 formal ledger，因此未推动 formal gate 结论前进
