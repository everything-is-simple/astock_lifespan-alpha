# 批次 63 卡片：前代经验教训与 MALF+Alpha(PAS) 核心落地收口

- 日期：`2026-04-24`
- 对应规格：`docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`

## 1. 目标

把多个前代系统的经验教训纳入当前正式治理文档，明确当前版本不是恢复旧复杂系统，而是完整落地 `MALF+alpha(PAS) 为系统核心`。

## 2. 本卡边界

本卡允许：

- 只读考古 `G:\history-lifespan\lifespan-0.01`
- 只读考古 `G:\history-lifespan\MarketLifespan-Quant`
- 只读考古 `G:\history-lifespan\EmotionQuant-gamma`
- 只读整理 `H:\Lifespan-Validated`
- 新增 lineage lessons 正式规格
- 新增 evidence、record、conclusion
- 更新 `docs/02-spec/README.md`
- 新增文档契约测试

本卡明确排除：

- 复制前代代码
- 修改运行时代码
- 修改 MALF 或 alpha schema
- 修改 runner summary
- 恢复旧 `structure/filter/family/formal_signal` 为上游真值
- 把实验素材直接并入核心

## 3. 任务切片

1. 抽取 `lifespan-0.01` 的复杂度教训。
2. 抽取 `MarketLifespan-Quant` 的 PAS 规格化素材。
3. 抽取 `EmotionQuant-gamma` 的实验与 ablation 方法。
4. 抽取 `H:\Lifespan-Validated` 的 MALF Canon 来源。
5. 写入 `02-spec` 正式规格。
6. 补齐 Card 63 evidence / record / conclusion。
7. 更新规格入口与文档测试。

## 4. 验收

本卡通过条件：

- 新规格明确 `MALF+alpha(PAS) 为系统核心`
- 新规格明确 `data -> MALF -> alpha(PAS)` 是当前核心事实链
- 新规格包含四个证据源路径
- 新规格明确不恢复 `structure/filter/family/formal_signal` 为上游真值
- 新规格明确实验素材不得直接进入核心
- 文档契约测试通过
- 模块边界测试通过

## 5. 执行命令

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_malf_specs.py -q
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```
