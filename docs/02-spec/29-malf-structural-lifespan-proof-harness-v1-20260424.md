# MALF 结构寿命语义证明夹具规格 v1

状态：`冻结`
日期：`2026-04-24`
对应执行卡：`docs/03-execution/65-malf-structural-lifespan-proof-harness-card-20260424.md`

## 1. 文档定位

本文档不是重开 `malf 放行`，也不是为 `alpha(PAS)` 新增评分能力。

本文档只回答一个问题：

> MALF 的结构破坏、重生、确认与寿命坐标，怎样被固定成可回归证明。

正式短语：

> MALF 是结构事实账本。

MALF 只记录结构事实，不解释市场意图。

## 2. 输入边界

唯一输入仍然是 `price bars`。

MALF 不得从以下来源反向取得语义：

- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`
- 收益标签
- 交易结果
- 概率判断

正式短语：

> 状态先由结构决定，再由寿命坐标定位。

## 3. 核心原语

MALF 的最小结构原语仍固定为：

- `HH`
- `HL`
- `LL`
- `LH`
- `break`

其中：

- `HH / LL` 负责推进新方向的结构确认
- `HL / LH` 负责守成，维护 guard 与结构节奏
- `break` 只表示旧结构被破坏

正式短语：

> `break != confirmation`

`break` 可以触发旧波终止与新波 `reborn`，但不能直接确认新波成立。

## 4. reborn 与 alive

`reborn` 是新方向正在组织中的状态。

`reborn` 不等于正式成立。

正式短语：

> `reborn -> alive` 必须由新方向正式 `HH / LL` 确认。

规则固定为：

- 上升结构被破坏后，新下降波进入 `reborn`
- 新下降波只有出现正式 `LL`，才从 `reborn` 转为 `alive`
- 下降结构被破坏后，新上升波进入 `reborn`
- 新上升波只有出现正式 `HH`，才从 `reborn` 转为 `alive`
- `HL / LH` 只能作为守成 guard 事实，不推进 `new-count`

## 5. 寿命三元

MALF 的寿命描述固定为：

> `new-count × no-new-span × life-state`

其中：

- `new-count` 记录正式新极值替代的累计
- `no-new-span` 记录离上一次正式更新经过了多少 bar
- `life-state` 记录当前波段处于 `reborn / alive`，旧波终止态写入 `malf_wave_ledger.broken`

语义约束：

- 正式新 `HH / LL` 出现时，`new-count` 推进，`no-new-span` 归零
- 未出现正式新 `HH / LL` 时，`no-new-span` 递增
- `HL / LH` 不作为新值替代，不推进 `new-count`

## 6. 波段位置

MALF 的统一坐标固定为：

> `WavePosition = (direction, update-rank, stagnation-rank, life-state)`

含义固定为：

- `direction` 表示当前波段方向
- `update-rank` 表示新值更新在历史同类样本中的位置
- `stagnation-rank` 表示停滞跨度在历史同类样本中的位置
- `life-state` 表示当前寿命边界

`WavePosition` 只表达历史生命位置，不表达买卖、胜率、收益或仓位建议。

## 7. 明确不做

MALF 不输出交易动作。

MALF 不输出收益概率。

MALF 不使用均线语义。

MALF 不反向消费 alpha 判断。

MALF 不承担以下职责：

- 买卖建议
- 持仓裁决
- 组合计划
- 收益评分
- 概率预测
- 交易执行

## 8. 证明夹具

Card 65 后，以下语义必须被测试固定：

1. 上升结构 `break_down` 后，新下降波先进入 `reborn`。
2. 新下降波只有出现 `LL`，才确认 `reborn -> alive`。
3. 下降结构 `break_up` 后，新上升波先进入 `reborn`。
4. 新上升波只有出现 `HH`，才确认 `reborn -> alive`。
5. `HL / LH` 守成 guard，不推进 `new-count`。
6. `no-new-span` 在未出现正式新值时递增，在确认新值时归零。

这些夹具不是交易样例，而是 MALF 结构寿命语义的回归证明。

## 9. 冻结结论

本文冻结以下结论：

1. `break` 只触发结构破坏，不等于确认。
2. `reborn` 只表示新方向正在组织，不等于正式成立。
3. `reborn -> alive` 必须由新方向正式 `HH / LL` 确认。
4. `new-count × no-new-span × life-state` 共同定义波段寿命。
5. `WavePosition = (direction, update-rank, stagnation-rank, life-state)` 只表达历史生命位置。
