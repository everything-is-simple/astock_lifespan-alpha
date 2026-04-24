# 批次 64 记录：Alpha(PAS) 核心升级边界与 legacy delta selection

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/64-alpha-pas-upgrade-boundary-and-legacy-delta-selection-card-20260424.md`

## 1. 做了什么

1. 只读回查 Card 57 的 `alpha 放行` 结论。
2. 只读回查 Card 63 的 lineage lessons。
3. 只读检查 live formal `alpha` 六个数据库的表与字段。
4. 只读确认 `position` 仍消费 `alpha_signal.duckdb`。
5. 只读确认当前 `runner.py` 与 `scripts/alpha/` 的命名对应关系。
6. 新增 `docs/02-spec/28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md`。
7. 新增 Card 64 evidence、record、conclusion。
8. 更新 `docs/02-spec/README.md` 与 `tests/unit/docs/test_alpha_specs.py`。
9. 运行文档契约、`alpha` 单测与模块边界回归。

## 2. 偏差项

- 本卡不修改 `alpha` 运行时代码。
- 本卡不修改 schema。
- 本卡不引入 registry、readout、16-cell 实现。
- 本卡只冻结升级边界与下一轮最小目标。

## 3. 备注

- `16-cell` 当前系统没有这个东西。
- 本卡已把 `16-cell` 收口为前代历史素材，而不是当前能力或下一轮治理候选。
- 下一轮默认方向仍是治理升级，而不是评分引擎升级。
- 文档契约结果：`4 passed in 0.05s`。
- `alpha` 单测结果：`5 passed in 5.04s`。
- 模块边界结果：`4 passed in 0.06s`。
