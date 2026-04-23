# 阶段十八 MALF day 纯语义重验规格 v1

- 日期：`2026-04-23`
- 状态：`冻结`
- 文档 ID：`stage-eighteen-malf-day-semantic-revalidation`

## 1. 目标

本轮正式工作定义为：对 `day` 周期的 MALF 做一次只读、可复现、正式留痕的纯语义重验，不先改 `engine.py`、`schema.py`、`runner.py`。

本轮要回答的唯一问题是：

`最新 MALF day 正式输出，是否已经把当前冻结的 HH / HL / LL / LH / break / reborn / wave-scale 语义，以一致、可审计的方式表达出来。`

## 2. 权威基准栈

本轮审计使用三层 authority stack：

1. 仓库冻结文本规格：
   - `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
2. 视觉权威材料：
   - `H:\Lifespan-Validated\malf-six\001.png` 至 `006.png`
   - `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
   - `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
   - `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
   - `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
3. 历史补充材料：
   - `H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf`
   - 已贴出的旧聊天正文摘录

若 PDF 正文不可直接抽取，本轮仍按视觉基准使用，不伪装成全文可检索文本源。

## 3. 本轮边界

本轮只验 MALF 纯语义核：

- `new_count`
- `no_new_span`
- `life_state`
- `wave_id`
- `guard_price`
- `pivot / break`
- `wave_position_zone`

本轮明确排除：

- `execution_interface`
- `structure / filter`
- `alpha`、`position`、`portfolio_plan`、`trade`、`system` 的消费层解释
- `week / month` 周期扩展
- `runner`、`queue`、`checkpoint` 的工程稳定性评分

## 4. 审计对象与回落规则

默认审计对象为 live `malf_day.duckdb` 最新 `completed` run，初始锁定：

- `day-a1c965e1f7a9`

若最新 `completed` run 已登记完成，但未物化 `malf_state_snapshot / malf_wave_ledger` 等核心账本行，则允许自动回落到：

- 最新一个 `completed` 且已物化核心 MALF 账本的 run

该回落必须：

- 在 summary 中保留 `requested_run_id`
- 明确登记 `effective_run_id`
- 在 evidence / conclusion 中解释回落原因

现存 `running` run 与 work queue 只做 stale bookkeeping 记录，不进入语义评分。

## 5. 必须产出的结果物

本轮必须同时产出：

1. `JSON` summary
2. `Markdown` summary
3. 4 张必需表：
   - `wave_summary`
   - `state_snapshot_sample`
   - `break_events`
   - `reborn_windows`
4. 固定 `12` 段样本图
5. `docs/03-execution/` 下的 `card / evidence / record / conclusion`

## 6. 硬规则检查

硬规则必须自动扫描 violation，不依赖人工目测。

### 6.1 `new_count`

- 只允许在同波 `HH / LL` bar 上加一
- 不允许跳增
- 不允许在 break bar 增加

### 6.2 `no_new_span`

- 只允许在未创新值且未 break 时递增
- 见新值时必须归零

### 6.3 `wave_id`

- 只允许在 `break_up / break_down` 事件处切换

### 6.4 新 wave 首 bar

- 新 wave 首 bar 必须是 `life_state = reborn`
- 同时 `new_count = 0`

### 6.5 `reborn -> alive`

- 只有首个正式 `HH / LL` 出现后，才允许从 `reborn` 进入 `alive`

### 6.6 `guard_price`

- `guard_price` 更新必须能对应同波 `HL / LH` pivot
- break 判定必须相对前一有效 `guard_price`

### 6.7 `wave_position_zone`

- `wave_position_zone` 必须与现代码分类函数一致
- 本轮先验的是数据一致性，不是主观解释

## 7. 软观察项

本轮必须登记，但不直接构成硬失败：

- `zone_coverage`
- `reborn_median_bar_count`
- `single_bar_reborn_share`
- `guard_churn_p90`

同时必须固定抽 `12` 段样本：

- 上升推进 `4` 段
- 下降推进 `4` 段
- break / reborn 过渡 `4` 段

每段样本固定输出 `6` 面板：

1. 价格 + `wave_id / life_state`
2. `new_count`
3. `no_new_span`
4. `guard_price`
5. pivot / break 标记
6. `wave_position_zone`

## 8. 判定规则

### 8.1 通过

满足以下条件：

- 7 项硬规则 violation 全部为 `0`
- 12 段样本中没有实质语义矛盾
- 软观察无持续性异常

### 8.2 部分通过

满足以下条件：

- 7 项硬规则 violation 全部为 `0`
- 但 guard / reborn / zone 存在重复性粗糙
- 样本图能看出“骨架一致，但还不够终态化”

### 8.3 不通过

出现任一情况即判定不通过：

- 任一硬规则 violation 大于 `0`
- `new_count`、`no_new_span`、`wave_id`、`reborn`、`guard_price` 与 pivot / break 脱钩
- 样本图出现明显自相矛盾

## 9. 下一步约束

若本轮结论为 `部分通过` 或 `不通过`，下一张卡只允许进入：

- `MALF engine` 纯语义修复

不得把以下事项混入同一卡：

- `runner / queue / checkpoint / build` 工程稳定性
- 下游 `alpha / position / portfolio_plan / trade / system`
- `week / month` 周期扩展
