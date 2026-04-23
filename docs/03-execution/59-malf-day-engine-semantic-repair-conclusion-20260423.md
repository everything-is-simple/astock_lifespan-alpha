# 批次 59 结论：MALF day engine 纯语义修复

- 日期：`2026-04-23`
- 裁决：`已记录，代码落地；live formal gate 待重算`

## 1. 结论

本轮 `Stage 19` 已完成 `MALF day engine` 纯语义修复的代码落地与单测收口，但尚未推动 live formal gate 从 Card 58 的 `部分通过` 前进。

原因不是代码未落，而是本轮严格遵守了“只修 engine、不进入 build”的边界，formal ledger 没有用新 engine 重算。

## 2. 本轮已完成

- `reborn` 从“guard 候选与首次确认同 bar”改为需要先形成 guard 候选，再允许首次正式 `HH / LL` 确认
- `guard_price` 从逐 bar 收紧改为确认式结构锚点
- `wave_position_zone` 四区 contract 已由单测显式覆盖
- MALF 相关单测已通过：`25 passed`

## 3. 为什么 live 结论没变

本轮复跑的命令是：

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id day-a1c965e1f7a9 --sample-count 12
```

该命令只审计现存 formal ledger，不会重建 `malf_day.duckdb`。

因此复跑结果仍然是：

- `effective_run_id = day-fc56ff5e5441`
- `verdict = 部分通过`
- 4 项软观察仍保持 Stage 18 的旧账本结果

## 4. 正式判断

本轮正式接受以下判断：

- Stage 19 的代码与测试闭环已经完成
- Card 58 暴露的 engine 语义问题，已经在源代码层进入正式修复状态
- 但只要 formal ledger 未重算，就不得把当前 live 审计结果误判为“Stage 19 已 live 通过”

## 5. 下一步唯一入口

若要验证 Stage 19 是否真正清掉 Card 58 的软 flag，下一步只能进入：

1. 允许一次受控 `day` formal rebuild
2. 用新 formal run 再跑 `audit_malf_day_semantics`
3. 只根据新 formal ledger 重判 `zone_coverage / reborn / guard` 软观察
