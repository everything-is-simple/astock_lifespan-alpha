# 批次 61 结论：MALF day formal target 恢复与 isolated regate

- 日期：`2026-04-23`
- 裁决：`已接受，恢复与 isolated rebuild 通过；live semantic gate 仍部分通过`

## 1. 结论

Card 61 已完成两件 Card 60 没完成的事：

1. 把被 `day-107059a919fc` 污染的 canonical `malf_day.duckdb` 正式恢复为干净 formal target
2. 把 `day + full_universe + --no-resume` rebuild 重新收敛到 `.building.duckdb -> promote` 的正式路径

因此本轮工程裁决为：`已接受`。

但本轮 forced audit 直接命中新 formal run `day-e687a8277f61` 后，最终 `verdict` 仍是 `部分通过`，因为 `zone_coverage = 3` 仍触发唯一软观察 flag。

所以本轮的正式总判断是：

- `formal target recovery = 通过`
- `isolated rebuild regate = 通过`
- `MALF day live semantic gate = 仍部分通过`

## 2. 本轮已完成

- MALF 单测通过：`28 passed`
- recovery CLI 已成功从 `day-fc56ff5e5441` 重建 canonical target
- polluted target 已隔离为 `malf_day.quarantine-2892d82b7c0d.duckdb`
- 新 formal run `day-e687a8277f61` 已 `completed`
- `artifact_summary.promoted_to_target = true`
- `malf_checkpoint.last_run_id` 已全量切到 `day-e687a8277f61`
- forced audit 已直接命中新 run，未 fallback
- 7 项硬规则全部 `pass`

## 3. 为什么不是“完全通过”

这次已经不再是 Card 60 的工程阻塞。

本轮没有出现：

- target 直写
- stale `running`
- interrupted row 污染
- fallback 到旧 formal run

剩余问题只剩一个语义软观察：

- `zone_coverage = 3`

也就是说，当前 MALF 已经不再卡在“formal target 坏了”或“rebuild 路径坏了”，而是回到真正的 live semantic revalidation 尾差。

## 4. 正式判断

本轮正式接受以下判断：

- Card 60 的 formal target 恢复问题已经关闭
- Stage 20 的“双库隔离 + 原子 cutover”合同已经在代码、单测、live run 中全部成立
- 新 formal ledger 已经完成 Stage 19 语义代码的 live 重算
- 当前 `malf` 不应再登记为“rebuild blocker 未清”
- 但在 `zone_coverage` 软观察处理前，`malf` 仍不放行

## 5. 下一步唯一入口

下一张卡只允许进入：

1. 解释并重判 `zone_coverage = 3` 的剩余语义差异
2. 明确这是 sampling/coverage 观察问题，还是仍需继续收紧 `wave_position_zone` / 样本分类口径
3. 不再回到 recovery、quarantine、target cutover 或 `--no-resume` 选路基础设施
