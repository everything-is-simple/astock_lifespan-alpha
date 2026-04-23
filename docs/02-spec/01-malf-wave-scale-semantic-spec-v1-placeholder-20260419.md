# MALF 波段标尺正式语义规格 v1

日期：`2026-04-19`
状态：`冻结`

## 1. 文档定位

本文档是 `MALF` 在新系统中的正式中文文本规格。

它回答五个问题：

1. `MALF` 到底输出什么真值。
2. `HH / HL / LH / LL` 与生命状态如何定义。
3. `new_count / no_new_span / life_state` 如何计算。
4. `guard`、`reborn`、`break != confirmation` 的边界是什么。
5. 日 / 周 / 月三周期如何保持独立。

当图版、代码、临时讨论与本文冲突时，以本文为准。

## 2. 真值边界

`MALF` 是新系统唯一正式市场结构真值层。

它只处理：

- 价格结构事实
- 波段方向
- 波段生命
- 波段位置坐标

它不处理：

- 交易动作
- 胜率判断
- 仓位建议
- 下游组合决策

## 3. 基本术语

### 3.1 波段

波段是同一方向价格推进过程的正式语义单位。

- 上升波：以新 `HH` 推进的波。
- 下降波：以新 `LL` 推进的波。

### 3.2 方向

正式方向固定为：

- `up`
- `down`

方向描述结构推进方向，不描述交易动作。

### 3.3 结构点

- `HH`：当前上升波出现严格更高高点。
- `HL`：当前上升波内仍有效的更高低点。
- `LL`：当前下降波出现严格更低低点。
- `LH`：当前下降波内仍有效的更低高点。

约束冻结为：

- 只有严格更高高点才是 `HH`。
- 只有严格更低低点才是 `LL`。
- 相等、近似、失败突破都不是正式新值。
- `HL / LH` 是 guard 候选锚点，不增加 `new_count`。

## 4. 最小生命表达

正式最小生命表达固定为：

```text
Life = (direction, new_count, no_new_span, life_state)
```

### 4.1 `new_count`

`new_count` 只记录当前方向上的严格新值替换次数。

- 上升波只统计新 `HH`。
- 下降波只统计新 `LL`。
- `HL / LH` 不计入。
- break 当根 bar 不自动计入新方向的 `new_count`。

### 4.2 `no_new_span`

`no_new_span` 表示自最近一次新 `HH / LL` 之后，连续未再创正式新值的 bar 数。

增长规则冻结为：

- 当旧波未被破坏，且当前 bar 没有创造新的正式 `HH / LL` 时，`no_new_span += 1`
- 一旦出现新的正式 `HH / LL`，`no_new_span = 0`
- 一旦旧波被 break，旧波不再继续累计 `no_new_span`

### 4.3 `life_state`

正式生命状态固定为：

- `alive`
- `broken`
- `reborn`

含义冻结为：

- `alive`：当前方向仍有效，且已具备正式推进状态。
- `broken`：旧波 guard 已失效，旧波终止。
- `reborn`：旧波已失效，但新方向第一次有效 `new_count` 尚未确认。

补充冻结边界：

- `broken` 是旧波终止态，正式写入 `malf_wave_ledger`。
- `malf_state_snapshot` 只描述当前正在展开的新波生命周期。
- `malf_state_snapshot` 正式 materialize `reborn / alive`，不单独展开 `broken`。

## 5. Guard 与 Break

### 5.1 guard 定义

guard 规则冻结为：

> 使用当前波内最近一个仍然有效的同波结构锚点作为 guard anchor。

- 上升波 guard = 最近有效 `HL`
- 下降波 guard = 最近有效 `LH`

### 5.2 break 定义

当 guard anchor 被破坏时，旧波进入 `broken`。

- 上升波：价格跌破当前有效 `HL`，旧上升波 break
- 下降波：价格上破当前有效 `LH`，旧下降波 break

### 5.3 `break != confirmation`

`break` 只表示旧波失效，不等于新方向已经被正式确认。

正式语义为：

1. 旧波 break
2. 新方向进入 `reborn`
3. 新方向出现第一次有效新值替换
4. 新方向才从 `reborn` 进入 `alive`

## 6. Reborn 规则

`reborn` 是正式保留状态，不允许删去或偷换成“弱确认”。

它出现在：

- 旧波 guard 已经失效
- 新方向已开始组织
- 但新方向第一次有效 `HH / LL` 还未出现

离开 `reborn` 的唯一方式是：

- 上升重生方向出现第一次正式 `HH`
- 下降重生方向出现第一次正式 `LL`

## 7. 波段位置坐标

正式波段位置坐标固定为：

```text
WavePosition = (direction, update_rank, stagnation_rank, life_state)
```

这是描述性坐标，不携带交易动作含义。

### 7.1 `update_rank`

`update_rank` 是当前 `new_count` 在历史同类波样本中的经验百分位。

样本口径固定为：

- 同一标的
- 同一周期
- 同一方向
- 以完整历史波作为样本单位

### 7.2 `stagnation_rank`

`stagnation_rank` 是当前 `no_new_span` 在历史同类波样本中的经验百分位。

样本口径与 `update_rank` 相同。

### 7.3 `wave_position_zone`

正式区域固定为四类：

- `early_progress`
- `mature_progress`
- `mature_stagnation`
- `weak_stagnation`

区域的职责是给 `alpha` 提供稳定读模型，而不是表达交易建议。

## 8. 三周期独立性

`MALF` 必须拆为三个相互独立的正式账本：

- `malf_day`
- `malf_week`
- `malf_month`

每个周期都必须独立拥有：

- runner
- work_queue
- checkpoint
- rebuild flow
- 生命统计
- 样本谱系

明确禁止：

- 用日线波去定义周线生命尺度
- 用周线 guard 去裁决月线 break
- 用一个周期的 checkpoint 污染另一个周期

## 9. 正式输出表

每个周期数据库至少包含：

- `malf_run`
- `malf_work_queue`
- `malf_checkpoint`
- `malf_pivot_ledger`
- `malf_wave_ledger`
- `malf_state_snapshot`
- `malf_wave_scale_snapshot`
- `malf_wave_scale_profile`

其中：

- `malf_state_snapshot` 面向内部状态审计
- `malf_wave_scale_snapshot` 面向 `alpha` 正式读取
- `malf_wave_scale_profile` 面向 rank / zone 画像
- `malf_wave_ledger` 保留旧波终止态，因此 `broken` 在 ledger 层冻结保留

## 10. 面向 Alpha 的最小输出

`malf_wave_scale_snapshot` 的最小字段集冻结为：

- `symbol`
- `timeframe`
- `bar_dt`
- `direction`
- `wave_id`
- `new_count`
- `no_new_span`
- `life_state`
- `update_rank`
- `stagnation_rank`
- `wave_position_zone`

`alpha` 只允许读取这张正式快照，不直接依赖内部 ledger。

## 11. 最小验收样例

### 样例 1：上升波继续创新高

- 给定：旧上升波仍有效
- 当：出现严格更高 `HH`
- 则：`new_count += 1`，`no_new_span = 0`，`life_state` 维持 `alive`

### 样例 2：上升波停滞但未破坏

- 给定：旧上升波 guard 未破坏
- 当：连续 bar 未创造新 `HH`
- 则：`no_new_span` 持续增长，`new_count` 不变

### 样例 3：上升波被 break

- 给定：上升波 guard = 最近有效 `HL`
- 当：价格跌破该 guard
- 则：旧波进入 `broken`

### 样例 4：break 后 reborn

- 给定：旧上升波刚被 break
- 当：新下降方向尚未出现第一次正式 `LL`
- 则：新方向状态为 `reborn`

### 样例 5：reborn 进入 alive

- 给定：下降方向已处于 `reborn`
- 当：出现第一次正式 `LL`
- 则：新方向进入 `alive`

### 样例 6：`HL / LH` 不计数

- 给定：上升波内形成更高低点或下降波内形成更低高点
- 则：允许更新 guard，但不得增加 `new_count`

## 12. 冻结结论

本文冻结以下结论：

1. `MALF` 是唯一正式市场结构真值层。
2. `new_count` 只记录正式 `HH / LL`。
3. `no_new_span` 只在“未继续、未破坏”时增长。
4. `break` 与 `confirmation` 不是同一件事。
5. `reborn` 是正式中间生命态。
6. `day / week / month` 必须完全独立运行。
