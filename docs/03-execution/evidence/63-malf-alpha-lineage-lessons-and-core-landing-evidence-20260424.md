# 批次 63 证据：前代经验教训与 MALF+Alpha(PAS) 核心落地收口

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/63-malf-alpha-lineage-lessons-and-core-landing-card-20260424.md`

## 1. 只读证据源

本卡考古范围固定为：

- `G:\history-lifespan\lifespan-0.01`
- `G:\history-lifespan\MarketLifespan-Quant`
- `G:\history-lifespan\EmotionQuant-gamma`
- `H:\Lifespan-Validated`

当前正式工程根固定为：

- `H:\astock_lifespan-alpha`

## 2. `lifespan-0.01` 证据

路径：

- `G:\history-lifespan\lifespan-0.01\src\mlq\alpha\__init__.py`

可验证内容：

- `run_alpha_pas_five_trigger_build`
- `run_alpha_family_build`
- `run_alpha_trigger_build`
- `run_alpha_formal_signal_build`

路径：

- `G:\history-lifespan\lifespan-0.01\src\mlq\alpha\bootstrap_columns.py`

可验证字段：

- `formal_signal_status`
- `filter_gate_code`
- `filter_reject_reason_code`
- `malf_context_4`
- `malf_alignment`
- `malf_phase_bucket`

路径：

- `G:\history-lifespan\lifespan-0.01\src\mlq\alpha\trigger_shared.py`

可验证字段：

- `filter_ledger_path`
- `structure_ledger_path`

证据结论：

- 前代 alpha 层曾同时牵引 PAS、family、formal_signal、filter、structure。
- 当前版本应继承 runner/ledger/checkpoint/event/profile 的工程经验，但不恢复这些旧上游真值。

## 3. `MarketLifespan-Quant` 证据

路径：

- `G:\history-lifespan\MarketLifespan-Quant\src\mlq\alpha\pas\__init__.py`

可验证内容：

- `PAS_TRIGGER_LEDGER_*`
- `PAS_FORMAL_SIGNAL_*`
- `PAS_READOUT_SCHEMA_SQL`
- `run_stock_pas_registry_bootstrap`
- `run_stock_pas_trigger_build`
- `run_stock_pas_trigger_ledger_build`
- `run_pas_formal_signal_build`
- `run_pas_trigger_16cell_readout`
- `run_pas_condition_matrix_readout`

路径：

- `G:\history-lifespan\MarketLifespan-Quant\src\mlq\alpha\pas\bof_16cell.py`
- `G:\history-lifespan\MarketLifespan-Quant\src\mlq\alpha\pas\trigger_16cell_runtime.py`

可验证内容：

- `malf_path`
- `malf_context_run_id`
- `trade_run_id`
- `min_trade_count`

证据结论：

- 前代 PAS 已有 registry、ledger、formal signal、condition matrix、16-cell readout 经验。
- 当前版本应把它们作为治理与诊断素材，而不是用 readout 或 trade 结果反向定义核心 trigger。

## 4. `EmotionQuant-gamma` 证据

路径：

- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\ablation.py`

可验证内容：

- ablation scenario
- `pas_trigger_trace_exp`

路径：

- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\engine.py`

可验证内容：

- `signal_filter`
- `pas_patterns`

路径：

- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\normandy_bof_quality.py`
- `G:\history-lifespan\EmotionQuant-gamma\src\backtest\normandy_bof_exit.py`

可验证内容：

- Normandy BOF quality scenario
- `pas_quality`
- `pas_reference`
- BOF exit variant

证据结论：

- 前代实验系统提供 ablation、quality/filter、Normandy 研究方法。
- 当前版本只继承方法，不让实验素材直接进入核心。

## 5. `H:\Lifespan-Validated` 证据

路径：

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

证据结论：

- `H:\Lifespan-Validated` 是当前 MALF Canon 的图版与定义来源。
- 当前系统以 MALF 纯语义结构账本作为 alpha(PAS) 的上游事实层。

## 6. 本卡产物

- `docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`
- `docs/03-execution/63-malf-alpha-lineage-lessons-and-core-landing-card-20260424.md`
- `docs/03-execution/evidence/63-malf-alpha-lineage-lessons-and-core-landing-evidence-20260424.md`
- `docs/03-execution/records/63-malf-alpha-lineage-lessons-and-core-landing-record-20260424.md`
- `docs/03-execution/63-malf-alpha-lineage-lessons-and-core-landing-conclusion-20260424.md`

## 7. 验证结果

文档契约：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_malf_specs.py -q
```

结果：

- `7 passed in 0.05s`

模块边界：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```

结果：

- `4 passed in 0.06s`
