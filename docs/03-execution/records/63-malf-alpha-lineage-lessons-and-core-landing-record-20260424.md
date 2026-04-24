# 批次 63 记录：前代经验教训与 MALF+Alpha(PAS) 核心落地收口

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/63-malf-alpha-lineage-lessons-and-core-landing-card-20260424.md`

## 1. 做了什么

1. 只读检查 `lifespan-0.01` 的 alpha 层，确认前代曾同时存在 PAS、family、formal_signal、filter、structure 等多条语义线。
2. 只读检查 `MarketLifespan-Quant` 的 PAS 层，确认 registry、ledger、formal signal、condition matrix、16-cell readout 等规格化素材。
3. 只读检查 `EmotionQuant-gamma` 的 backtest 与 Normandy 代码，确认 ablation、quality/filter、BOF exit 等实验素材。
4. 只读整理 `H:\Lifespan-Validated` 的 MALF 图版与 PDF 来源。
5. 新增 `docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`。
6. 新增 Card 63 evidence、record、conclusion。
7. 更新 `docs/02-spec/README.md` 的阅读路径。
8. 增加文档契约测试，锁定四个证据源与核心治理短语。
9. 使用 `D:\miniconda\py310\python.exe` 运行文档契约与模块边界测试。

## 2. 偏差项

- 本卡不复制前代代码。
- 本卡不修改运行时代码。
- 本卡不修改 schema。
- 本卡不启动 alpha(PAS) 工程修复；若发现差距，拆到下一张 card。

## 3. 备注

- 当前核心事实链收口为 `data -> MALF -> alpha(PAS)`。
- 当前正式路线是完整落地 `MALF+alpha(PAS) 为系统核心`。
- 前代实验素材只能作为 audit/readout/research 的候选来源，不能直接进入 formal core。
- 文档契约结果：`7 passed in 0.05s`。
- 模块边界结果：`4 passed in 0.06s`。
