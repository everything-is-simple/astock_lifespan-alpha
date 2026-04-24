# 批次 65 记录：MALF 结构寿命语义证明夹具

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/65-malf-structural-lifespan-proof-harness-card-20260424.md`

## 1. 做了什么

1. 只读回查 Card 61 与 Card 62 的 MALF 放行结论。
2. 只读回查 `day-e687a8277f61` formal MALF run。
3. 新增 `docs/02-spec/29-malf-structural-lifespan-proof-harness-v1-20260424.md`。
4. 新增 Card 65 evidence、record、conclusion。
5. 更新 `docs/02-spec/README.md` 的 MALF 阅读路径。
6. 扩展 `tests/unit/malf/test_engine.py` 的对称语义证明夹具。
7. 扩展 `tests/unit/docs/test_malf_specs.py` 的文档契约。
8. 运行文档契约、MALF 单测与模块边界回归。

## 2. 红灯记录

新增文档契约后，首次运行：

```powershell
D:\miniconda\py310\python.exe -m pytest H:\astock_lifespan-alpha\tests\unit\docs\test_malf_specs.py -q
```

结果：

- `1 failed, 7 passed in 0.20s`
- 失败原因：`29-malf-structural-lifespan-proof-harness-v1-20260424.md` 尚未创建

新增 engine 夹具后，首次运行：

```powershell
D:\miniconda\py310\python.exe -m pytest H:\astock_lifespan-alpha\tests\unit\malf\test_engine.py -q
```

结果：

- `7 passed in 0.79s`

说明：

- engine 对称场景已由现有实现满足
- 本卡对 engine 的作用是补齐证明夹具，而不是修复运行时语义

## 3. 偏差项

- 本卡没有修改 `src/astock_lifespan_alpha/malf/engine.py`。
- 本卡没有修改 schema。
- 本卡没有修改 runner 名称。
- 本卡没有修改 alpha 或任何下游模块。

## 4. 备注

- `break_up / break_down` 继续作为结构破坏 pivot。
- `HH / LL` 继续作为新方向正式确认 pivot。
- `HL / LH` 继续作为 guard 守成 pivot。
- `WavePosition` 继续只表达历史生命位置。
- 文档契约结果：`8 passed`。
- MALF 单测结果：`32 passed`。
- 模块边界结果：`4 passed`。
