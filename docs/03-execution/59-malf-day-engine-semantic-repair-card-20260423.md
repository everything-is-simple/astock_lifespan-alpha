# 批次 59 卡片：MALF day engine 纯语义修复

- 日期：`2026-04-23`
- 对应规格：`docs/02-spec/24-stage-nineteen-malf-day-engine-semantic-repair-spec-v1-20260423.md`

## 1. 目标

在不改 `runner / build / queue / checkpoint / schema` 的前提下，收紧 `MALF day engine` 的 `reborn / guard / zone` 纯语义表达。

## 2. 本卡边界

本卡只允许：

- 修改 `src/astock_lifespan_alpha/malf/engine.py`
- 补强 `tests/unit/malf/test_engine.py`
- 补齐 `card / evidence / record / conclusion`

本卡明确排除：

- live build
- formal DuckDB cutover
- `audit.py` 7 条硬规则改写
- `week / month`
- 下游模块

## 3. 预期交付

1. `engine.py` 落地新的 `reborn` 与 `guard` 状态机
2. `test_engine.py` 增加最小序列测试
3. MALF 相关 4 组单测通过
4. Stage 18 audit CLI 复跑并留痕

## 4. 验收重点

- `reborn` 不能再被实现压缩成“guard 候选与首次确认同 bar”
- `guard_price` 不再逐 bar 抖动更新
- `wave_position_zone` 保持四区 contract，且单测可覆盖四区
- 若 live audit 仍反映旧 formal ledger，结论文档必须明确写出“账本未重算”
