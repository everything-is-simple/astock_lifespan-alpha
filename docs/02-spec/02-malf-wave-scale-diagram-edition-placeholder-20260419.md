# MALF 波段标尺图版规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档把 `MALF` 文本规格映射成统一图版口径。

图版职责只有两个：

- 帮助审阅文本规则是否一致
- 给后续实现与测试提供可对照的读图入口

权威顺序固定为：

1. `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
2. `docs/02-spec/26-malf-foundation-canon-v1-20260424.md`
3. 本图版规格
4. 仓库根目录或 `H:\Lifespan-Validated` 中已接受的 PDF / PNG 图稿
5. 代码实现

如果以上对象冲突，按以上顺序裁决。

2026-04-24 补记：Card 62 后，图版不再单独承担“地基定义”职责；图版只负责帮助检查 `MALF` 文本规格与 Canon 是否被正确表达。

## 2. 图版统一口径

本仓库根目录已有三份图稿 PDF：

- `MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
- `MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
- `MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`

从 `2026-04-19` 起，统一使用下面的整理口径：

- 18 页稿视为信息最全的主图版来源
- 6 页稿视为摘要图版来源
- 旧“第四战场”图稿只保留作历史参考，不再单独定义规则

## 3. 图版必须表达的对象

图版必须完整表达：

- 波段方向切换
- `HH / HL / LH / LL`
- `new_count`
- `no_new_span`
- `life_state`
- guard anchor
- `break != confirmation`
- `reborn`
- `update_rank / stagnation_rank`
- `wave_position_zone`

## 4. 推荐图层

### 4.1 生命周期图层

必须至少分成：

- `alive`
- `broken`
- `reborn`

其中：

- `broken` 与 `reborn` 不允许画成同一状态
- 新方向确认前必须保留 `reborn` 过渡段

### 4.2 结构点图层

必须明确区分：

- `HH / LL`：推进点
- `HL / LH`：guard 锚点

图中不得把 `HL / LH` 画成更新 `new_count` 的事件。

### 4.3 位置读模图层

必须明确标出：

- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

`wave_position_zone` 只画四个正式区：

- `early_progress`
- `mature_progress`
- `mature_stagnation`
- `weak_stagnation`

## 5. 文本条款 -> 图版位置对照

| 文本条款 | 图版必须出现的表达 |
| --- | --- |
| `new_count` 只统计正式 `HH / LL` | 推进箭头或更新计数标签只挂在 `HH / LL` 上 |
| `HL / LH` 不计入 `new_count` | guard 锚点与计数标签分离 |
| `no_new_span` 在未继续、未破坏时增长 | 停滞区间必须单独标注 span 增长 |
| `break != confirmation` | break 节点后必须保留确认前区段 |
| `reborn` 是正式中间态 | 生命周期图中有单独 `reborn` 状态块 |
| 最近有效 `HL / LH` 为 guard | guard 锚点必须跟随最近有效同波结构点 |
| 三周期独立 | 图版或图注中必须声明 day / week / month 不共享尺度 |
| `wave_position_zone` 面向 `alpha` | 读模层必须标明快照读取口径 |

## 6. 图版审阅规则

审阅图版时固定检查：

1. 是否存在只在图中出现、但文本未定义的规则。
2. 是否把 break 直接画成 confirmation。
3. 是否省略 `reborn`。
4. 是否把 `HL / LH` 混成推进点。
5. 是否混用了不同周期的生命尺度。

只要任一项为真，就必须回修图版，而不是修改文本去迎合图稿。

## 7. 输出要求

后续整理图版时，至少要产出：

- 一份完整图版口径说明
- 一份图版页码与文本条款对照表
- 一份差异清单，记录哪些历史图稿内容已被废止

## 8. 冻结结论

本文冻结以下结论：

1. 文本规格优先于图版。
2. `MALF` Canon 与文本规格共同定义当前长期地基口径。
3. 18 页稿为主图版来源，6 页稿为摘要来源。
4. 图版必须逐条映射文本规则，不允许代码自行猜图。
5. `reborn`、guard、`break != confirmation`、四区读模必须在图版中显式出现。
