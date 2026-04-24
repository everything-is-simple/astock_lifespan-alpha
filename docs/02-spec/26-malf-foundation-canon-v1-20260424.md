# MALF 地基语义 Canon v1

- 状态：`冻结`
- 日期：`2026-04-24`
- 来源：MALF 图版、Card 58-61 live semantic gate、当前 `day` formal target 语义审计

## 1. 定位

MALF 不是交易系统。MALF 不输出买卖动作。

MALF 不是概率系统，不回答收益概率；MALF 不是均线或指标解释器，不用平滑曲线替代结构事实。

MALF 是按 `symbol + timeframe` 独立运行的结构事实账本。它只记录价格条形成的极值、结构、波段、寿命与位置，用这些事实向下游提供可复查的地基。

因此 MALF 的正式边界是：

- 只描述结构事实
- 不解释市场意图
- 不给出买卖建议
- 不把状态描述伪装成预测

## 2. 唯一输入

MALF 的唯一输入是 `price bars`。

正式上位原则：

- 唯一来源：只来自 `price bars`
- 同级别：月、周、日各自独立运行
- 同股：先做单股自我标尺
- 同方向：上升波与下降波不可混织
- 纯位置：输出结构位置，不输出交易动作

工程上，`price bars` 进入 MALF 后，只允许被转换为极值、结构、波段、寿命与标尺位置；不允许在 MALF 内吸收收益标签、概率判断、均线语义或交易动作。

## 3. 唯一原语

MALF 的核心原语是 `HH / HL / LL / LH / break`。

这些原语的正式含义：

- `HH`：更高高点，是上升结构的推进事实
- `HL`：更高低点，是上升结构的守护事实
- `LL`：更低低点，是下降结构的推进事实
- `LH`：更低高点，是下降结构的守护事实
- `break`：结构破坏，是旧结构终止与新结构重组的触发事实

职责分离：

- `HH / LL -> 推进`
- `HL / LH -> 守护`
- `break -> 转移`

状态是描述，不是预测。先识别结构本体，再描述当下状态，最后才允许下游讨论位置。

## 4. 寿命三元

MALF 的最小寿命描述是 `new-count × no-new-span × life-state`。

三者分工：

- `new-count`：记录新值替代次数，给出生长坐标
- `no-new-span`：记录离上次更新过去了多久，给出停滞坐标
- `life-state`：记录波段处在 `reborn / alive / broken` 的寿命边界

这三者共同描述一段波的生长、停顿、边界、终止与重启。它们不含概率，不含收益标签，也不含交易动作。

工程命名可以继续沿用历史字段 `new_count / no_new_span / life_state`，但正式叙事中固定使用 `new-count / no-new-span / life-state` 表达三元关系。

## 5. Break 与确认

`break != confirmation`。

`break` 只是触发，不是确认。旧结构被破坏后，新方向先进入 `reborn` 重组阶段；只有新方向出现正式推进极值，才允许从 `reborn` 进入 `alive`。

正式确认规则：

- 上升波被破坏后进入下降方向 `reborn`，再出现新的 `LL` 才确认下降波
- 下降波被破坏后进入上升方向 `reborn`，再出现新的 `HH` 才确认上升波

`reborn -> alive` 必须由新方向正式 `HH / LL` 确认。

## 6. 波段标尺

MALF 的波段位置必须落在统一坐标系里：

```text
WavePosition = (direction, update-rank, stagnation-rank, life-state)
```

正式短语：`WavePosition = (direction, update-rank, stagnation-rank, life-state)`。

其中：

- `direction`：当前波方向
- `update-rank`：以 `new-count` 在同类历史样本中的排序表示更新位置
- `stagnation-rank`：以 `no-new-span` 在同类历史样本中的排序表示停滞位置
- `life-state`：寿命边界

四区表达只是对该坐标系的可读分区：

- `early_progress`
- `mature_progress`
- `mature_stagnation`
- `weak_stagnation`

四区用于审计与下游读数，不改变原始极值与波段事实。

## 7. 工程映射

当前正式工程映射：

- `malf_wave_ledger`：波段账本，记录 `alive / reborn / broken` 等波段状态事实
- `malf_state_snapshot`：当前状态快照，只描述当前正在展开的新波生命周期
- `new_count`：对应 `new-count`
- `no_new_span`：对应 `no-new-span`
- `life_state`：对应 `life-state`
- `update_rank / stagnation_rank / wave_position_zone`：对应 `WavePosition` 的排序与分区表达

治理口径：

- hard rule 失败表示结构合同破坏
- soft observation 只表示审计观察或采样表达风险
- `zone_coverage` 只解释为 `state_snapshot_sample` 的 sample coverage，不直接等价于全量 formal target 缺区

本 Canon 是 MALF 作为系统地基的最小语义收口。后续模块可以消费 MALF 事实，但不得反向把交易、概率、收益或均线语义写回 MALF。
