# 前代经验教训与 MALF+Alpha(PAS) 核心落地规格 v1

状态：`冻结`
日期：`2026-04-24`
对应执行卡：`docs/03-execution/63-malf-alpha-lineage-lessons-and-core-landing-card-20260424.md`

## 1. 目的

本文档把前代系统经验收束为当前 `astock_lifespan-alpha` 的正式治理口径。

当前系统核心不是恢复旧系统的全部模块，而是完整落地 `MALF+alpha(PAS) 为系统核心`：

```text
data -> MALF -> alpha(PAS) -> position -> portfolio_plan -> trade -> system
```

其中：

- `data` 提供客观 `price bars`
- `MALF` 生成结构事实、寿命坐标与波段位置
- `alpha(PAS)` 只消费 MALF 事实生成五类 trigger 与 `alpha_signal`
- 下游只消费 MALF 与 alpha 的正式账本输出

## 2. 考古证据源

本规格只引用本机可验证的只读证据源：

- `G:\history-lifespan\lifespan-0.01`
- `G:\history-lifespan\MarketLifespan-Quant`
- `G:\history-lifespan\EmotionQuant-gamma`
- `H:\Lifespan-Validated`

当前正式工程根仍为：

- `H:\astock_lifespan-alpha`

## 3. 前代经验教训

### 3.1 `lifespan-0.01`：复杂度教训

`lifespan-0.01` 的 alpha 层曾经同时牵引 PAS、family、formal_signal、filter、structure 等多条上游语义线。

可验证证据：

- `G:\history-lifespan\lifespan-0.01\src\mlq\alpha\__init__.py`
  - 暴露 `run_alpha_pas_five_trigger_build`
  - 暴露 `run_alpha_family_build`
  - 暴露 `run_alpha_trigger_build`
  - 暴露 `run_alpha_formal_signal_build`
- `G:\history-lifespan\lifespan-0.01\src\mlq\alpha\bootstrap_columns.py`
  - 包含 `formal_signal_status`
  - 包含 `filter_gate_code`
  - 包含 `filter_reject_reason_code`
  - 包含 `malf_context_4`
  - 包含 `malf_alignment`
  - 包含 `malf_phase_bucket`
- `G:\history-lifespan\lifespan-0.01\src\mlq\alpha\trigger_shared.py`
  - 记录 `filter_ledger_path`
  - 记录 `structure_ledger_path`

本轮继承的经验：

- runner、ledger、checkpoint、event/profile 分层是有效工程资产
- alpha 需要可追溯来源与可重放 runner summary
- trigger 事件要保留 `formal_signal_status`

本轮拒绝恢复的经验：

- 不恢复 `structure/filter/family/formal_signal` 为上游真值
- 不把 `filter_gate_code` 或 `filter_reject_reason_code` 作为 alpha(PAS) 首版核心
- 不把 `malf_alignment` 或 `malf_phase_bucket` 作为新的 MALF 解释层

### 3.2 `MarketLifespan-Quant`：PAS 规格化素材

`MarketLifespan-Quant` 的 PAS 层已经形成 registry、ledger、formal signal、condition matrix、16-cell readout 等规格化尝试。

可验证证据：

- `G:\history-lifespan\MarketLifespan-Quant\src\mlq\alpha\pas\__init__.py`
  - 暴露 `PAS_TRIGGER_LEDGER_*`
  - 暴露 `PAS_FORMAL_SIGNAL_*`
  - 暴露 `PAS_READOUT_SCHEMA_SQL`
  - 暴露 `run_stock_pas_registry_bootstrap`
  - 暴露 `run_stock_pas_trigger_build`
  - 暴露 `run_stock_pas_trigger_ledger_build`
  - 暴露 `run_pas_formal_signal_build`
  - 暴露 `run_pas_trigger_16cell_readout`
  - 暴露 `run_pas_condition_matrix_readout`
- `G:\history-lifespan\MarketLifespan-Quant\src\mlq\alpha\pas\bof_16cell.py`
  - 以正式 `MALF/PAS/trade` 运行构建 BOF-only 16 格 readout
- `G:\history-lifespan\MarketLifespan-Quant\src\mlq\alpha\pas\trigger_16cell_runtime.py`
  - 以 `malf_path`
  - `malf_context_run_id`
  - `trade_run_id`
  - `min_trade_count`
  构建 PAS trigger 16-cell readout

本轮继承的经验：

- PAS 可以作为 alpha 的正式触发器族
- registry/ledger/readout 可以作为后续治理和诊断素材
- 16-cell 与 condition matrix 适合放在 audit/readout 层，而不是首版核心 runner

本轮降级的经验：

- trade-linked readout 只能作为验证与研究观察，不得反向决定 MALF 或 alpha(PAS) 触发语义
- 16-cell 不能替代 `alpha_trigger_event` 与 `alpha_signal` 的正式账本地位

### 3.3 `EmotionQuant-gamma`：实验素材

`EmotionQuant-gamma` 保留了大量 Normandy、BOF、ablation、quality/filter 实验。

可验证证据：

- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\ablation.py`
  - 包含 ablation scenario
  - 包含 `pas_trigger_trace_exp`
- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\engine.py`
  - 包含 `signal_filter`
  - 包含 `pas_patterns`
- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\normandy_bof_quality.py`
  - 包含 Normandy BOF quality scenario
  - 包含 `pas_quality`
  - 包含 `pas_reference`
  - 包含 branch payload 与 quality/filter 研究字段
- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\normandy_bof_exit.py`
  - 包含 Normandy BOF exit variant

本轮继承的经验：

- ablation 是验证 PAS 是否有增量价值的必要方法
- quality/filter 适合成为后续研究卡或 audit 卡的证据来源
- Normandy 变量适合保留为实验命名，不直接进入正式主链路

本轮拒绝恢复的经验：

- 实验素材不得直接进入核心
- `signal_filter`、`pas_quality`、`pas_reference` 不得成为 alpha(PAS) 首版必经门
- 不把回测收益、质量过滤或退出变体反写为 MALF 语义

### 3.4 `H:\Lifespan-Validated`：MALF Canon 来源

`H:\Lifespan-Validated` 保存 MALF 图版、PDF 与六图材料，是当前 MALF Canon 的外部定义来源。

可验证证据：

- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
- `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
- `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
- `H:\Lifespan-Validated\malf-six\001.png`
- `H:\Lifespan-Validated\malf-six\002.png`
- `H:\Lifespan-Validated\malf-six\003.png`
- `H:\Lifespan-Validated\malf-six\004.png`
- `H:\Lifespan-Validated\malf-six\005.png`
- `H:\Lifespan-Validated\malf-six\006.png`

本轮继承的经验：

- MALF 是纯语义结构账本
- MALF 的唯一输入是 `price bars`
- MALF 不输出交易动作、不输出收益概率、不使用均线解释
- `HH/HL/LL/LH/break` 是核心原语
- `new-count × no-new-span × life-state` 是寿命三元
- `WavePosition = (direction, update-rank, stagnation-rank, life-state)` 是波段坐标表达

## 4. 当前系统收口口径

### 4.1 核心事实链

当前正式核心事实链固定为：

```text
data -> MALF -> alpha(PAS)
```

正式短语：`data -> MALF -> alpha(PAS)`。

MALF 之后的模块可以消费核心事实，但不能反向定义核心事实。

正式短语：

> MALF+alpha(PAS) 为系统核心。

### 4.2 MALF 职责

MALF 只负责：

- 极值推进
- 结构破坏
- 波段边界
- 寿命状态
- 更新排序
- 停滞排序
- 波段位置

MALF 不负责：

- 买卖动作
- 收益判断
- 概率标签
- 均线语义
- PAS 触发器解释
- position / trade / system 决策

### 4.3 Alpha(PAS) 职责

正式短语：alpha(PAS) 只消费 MALF 事实。

alpha(PAS) 只负责消费 MALF 事实并生成：

- `alpha_trigger_event`
- `alpha_trigger_profile`
- `alpha_signal`

五类 trigger 继续固定为：

- `alpha_bof`
- `alpha_tst`
- `alpha_pb`
- `alpha_cpb`
- `alpha_bpb`

alpha(PAS) 不负责：

- 重判 MALF wave boundary
- 重算 MALF life-state
- 引入旧 `structure/filter/family/formal_signal` 作为上游真值
- 用 trade 结果反向筛选 trigger 是否成立

### 4.4 研究层与正式层分离

允许保留前代研究经验，但必须分层：

- `formal core`：MALF 与 alpha(PAS) 正式账本
- `audit/readout`：coverage、matrix、16-cell、quality observation
- `research`：ablation、Normandy、filter、exit、收益观察

研究层进入正式层前，必须另开 card，并写明：

- 输入是否仍只来自正式账本
- 是否改变 schema
- 是否改变 runner summary
- 是否改变下游消费契约
- 是否引入交易或收益反馈

## 5. 决策

本规格作出以下决策：

1. 继承前代 ledger/runner/checkpoint/event/profile 工程经验。
2. 保留 PAS registry/readout/16-cell 作为后续治理素材。
3. 保留 EmotionQuant 的 ablation 与 Normandy 方法作为研究素材。
4. 不恢复 `structure/filter/family/formal_signal` 为上游真值。
5. 不把实验素材直接进入核心。
6. 不让收益、交易、质量过滤反向定义 MALF 或 alpha(PAS)。
7. 当前系统以 `MALF+alpha(PAS)` 完整落地为核心路线。

## 6. 后续步骤

下一张语义卡应聚焦 alpha(PAS) 当前实现与本规格的差距，而不是继续扩大考古范围。

建议下一步只读诊断：

- 当前 `alpha` runner 是否完整覆盖五类 trigger
- `alpha_trigger_event` 是否只消费 MALF 正式字段
- `alpha_signal` 是否保留 MALF 波段坐标
- 是否存在需要从前代 PAS registry/readout 继承的最小治理字段

若需要工程修复，另开新 card，不在本规格中直接修改运行时代码。
