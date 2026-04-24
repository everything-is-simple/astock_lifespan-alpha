# 规格区入口

`docs/02-spec/` 是正式契约层。这里回答的是：系统边界、模块输入输出、字段口径、验收规则到底是什么。

## 当前系统地基：`MALF`

当前正式主链路固定为：

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

正式链路短语：`data -> malf -> alpha -> position -> portfolio_plan -> trade -> system`。

其中：

- `data` 提供客观事实输入
- `malf` 是唯一正式市场结构真值层
- `alpha` 以后都是下游消费、汇总、持仓、组合、执行与读出层

Card 62 后，当前 gate 结论为：

- `malf = 放行`
- forced audit `malf-day-semantic-audit-ad35dcbbae62`
- `zone_coverage = ok (4)`
- `verdict = 通过`

## MALF 阅读路径

阅读 MALF 地基语义时，按下面顺序：

1. `docs/02-spec/00-astock-lifespan-alpha-reconstruction-master-plan-v1-20260419.md`
2. `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
3. `docs/02-spec/02-malf-wave-scale-diagram-edition-placeholder-20260419.md`
4. `docs/02-spec/26-malf-foundation-canon-v1-20260424.md`
5. `docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`
6. `docs/02-spec/29-malf-structural-lifespan-proof-harness-v1-20260424.md`
7. `docs/03-execution/62-malf-foundation-canon-import-and-zone-sampling-conclusion-20260424.md`
8. `docs/03-execution/65-malf-structural-lifespan-proof-harness-conclusion-20260424.md`
9. `docs/03-execution/63-malf-alpha-lineage-lessons-and-core-landing-conclusion-20260424.md`

其中 `docs/02-spec/26-malf-foundation-canon-v1-20260424.md` 是 Card 62 后的 MALF 地基 Canon，用来把图版、文本规格与 live audit 结论收束成当前长期口径。

`docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md` 是 Card 63 后的前代经验收口规格，用来明确当前系统不是恢复旧复杂系统，而是以 `MALF+alpha(PAS) 为系统核心`，按 `26 MALF Canon -> 27 MALF+Alpha lineage lessons -> 03/04 Alpha specs` 的顺序继续推进 alpha(PAS)。

`docs/02-spec/29-malf-structural-lifespan-proof-harness-v1-20260424.md` 是 Card 65 后的 MALF 结构寿命语义证明规格，用来把 `break != confirmation`、`reborn -> alive`、`new-count × no-new-span × life-state` 与 `WavePosition` 固定为可回归证明。阅读 MALF 深水区语义时按 `26 MALF Canon -> 29 MALF structural lifespan proof harness -> 62/65 conclusions`。

## Alpha(PAS) 阅读路径

阅读 `alpha(PAS)` 当前合同与下一轮边界时，按下面顺序：

1. `docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`
2. `docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`
3. `docs/02-spec/28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md`
4. `docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`
5. `docs/03-execution/57-alpha-live-freeze-audit-conclusion-20260423.md`
6. `docs/03-execution/64-alpha-pas-upgrade-boundary-and-legacy-delta-selection-conclusion-20260424.md`

其中：

- `03 / 04` 说明当前 trigger 合同与 `alpha_signal` 聚合合同
- `28` 说明哪些历史 PAS 能力暂不吸收，哪些成为下一轮最小升级入口
- `27` 说明更高层的 lineage lessons 与 `MALF+alpha(PAS)` 核心路线

## 历史阶段规格的读法

本目录包含两类文档：

- 长期基准规格：总方案、文档治理规格、MALF 语义规格、MALF Canon
- 阶段规格：围绕某一阶段或某一张 card 的执行准入规格

阶段规格不得反向改写 MALF 地基语义。若阶段规格与 MALF Canon 冲突，应以长期基准规格和最新已接受结论为准，再开新 card 修订历史口径或补记。

## 下游消费边界

下游模块只允许消费 MALF 已正式输出的结构事实：

- `wave_id`
- `direction`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

下游模块不得把交易动作、收益概率、均线解释或组合裁决反写进 MALF。

正式短语：

> 下游只消费 MALF 事实，不反向定义 MALF。
