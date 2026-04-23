# 阶段十九 MALF day engine 纯语义修复规格 v1

- 日期：`2026-04-23`
- 状态：`冻结`
- 文档 ID：`stage-nineteen-malf-day-engine-semantic-repair`

## 1. 目标

本轮正式工作定义为：只修改 `day` 周期 `MALF engine` 的纯语义状态机表达，不进入 `runner / build / queue / checkpoint / schema / downstream`。

本轮要回答的唯一问题是：

`在不改 public 账本 shape 的前提下，是否能把 reborn、guard、wave_position_zone 的 engine 表达收紧到更接近图版正式定义。`

## 2. 图版优先 authority stack

本轮 authority stack 固定为：

1. 图版与正式图版 PDF：
   - `H:\Lifespan-Validated\malf-six\001.png` 至 `006.png`
   - `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
   - `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
   - `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
   - `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
2. 仓库冻结文本规格：
   - `docs/02-spec/01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md`
   - `docs/02-spec/23-stage-eighteen-malf-day-semantic-revalidation-spec-v1-20260423.md`
3. 补充解释材料：
   - `H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf`

补充材料只用于术语对照和歧义解释，不得覆盖图版主定义。

## 3. 图版页码 -> engine 语义职责映射

| 图版页 | 正式语义职责 | 本轮对应修复 |
| --- | --- | --- |
| `001` | `new-count / no-new-span / life-state` 是核心三元 | 保留三元，不改 public 字段 |
| `003` | `HH / LL = 推进`，`HL / LH = 守护`，`break = 转移` | 把 guard 改为结构确认锚点 |
| `006` | `WavePosition = (direction, update-rank, stagnation-rank, life-state)` | 保留坐标系，不改字段 shape |

## 4. 本轮边界

本轮只允许修改：

- `src/astock_lifespan_alpha/malf/engine.py`
- `tests/unit/malf/test_engine.py`
- 本批次文档与索引

本轮明确排除：

- `runner.py`
- `schema.py`
- `audit.py` 的 7 条硬规则定义
- `week / month`
- 下游 `alpha / position / portfolio_plan / trade / system`
- 正式 live build / cutover / replay

## 5. 正式修复口径

### 5.1 `reborn`

`reborn` 仍是 break 后到首次正式 `HH / LL` 之前的唯一合法中间生命态。

本轮新增约束：

- 新 wave 不再允许在“同一根 bar 同时形成 guard 候选并完成 alive 确认”
- `reborn -> alive` 之前，必须先出现可被后续推进确认的 guard 候选
- `reborn` 期间允许极值继续探索，但不得提前记作正式 `new_count`

### 5.2 `guard_price`

本轮将 `guard_price` 从逐 bar 收紧，收紧为“结构确认式 guard anchor”。

正式口径冻结为：

- 上升波只在后续正式 `HH` 出现时，确认先前累计的 `HL` 候选
- 下降波只在后续正式 `LL` 出现时，确认先前累计的 `LH` 候选
- `HL / LH` 仍要求在 ledger 中留下正式 pivot，但登记时点是确认 bar，而不是候选 bar
- break 仍相对前一有效 `guard_price` 判定

### 5.3 `wave_position_zone`

本轮不改变正式坐标系：

```text
WavePosition = (direction, update-rank, stagnation-rank, life-state)
```

本轮同时冻结以下实现约束：

- `update_rank / stagnation_rank` 仍是唯一历史坐标轴
- `wave_position_zone` 仍保留四区：
  - `early_progress`
  - `mature_progress`
  - `mature_stagnation`
  - `weak_stagnation`
- 由于 Stage 18 硬规则已把 zone 分类函数写入审计基线，本轮不改 `audit.py` 硬规则，只通过 engine 语义状态变化改善四区覆盖

### 5.4 `broken` / snapshot 分层

- `broken` 仍定义为旧波终止态，正式写入 `malf_wave_ledger`。
- `malf_state_snapshot` 继续只描述当前正在展开的新波生命周期。
- `malf_state_snapshot` 正式 materialize `reborn / alive`，不单独展开 `broken`。
- `zone_coverage` 只解释为 `state_snapshot_sample` 的 sample coverage。

## 6. Public contract 不变项

本轮不得修改以下 public shape：

- `EngineResult`
- `PivotRow`
- `WaveRow`
- `SnapshotRow`
- `ProfileRow`

不得新增或删除正式表字段，不得改 runner summary 名称。

## 7. 验收口径

### 7.1 单测口径

必须通过：

```powershell
pytest tests/unit/malf/test_engine.py tests/unit/malf/test_runner.py tests/unit/malf/test_diagnostics.py tests/unit/malf/test_audit.py -q --basetemp H:\Lifespan-temp\pytest-malf-stage19
```

### 7.2 live 复验口径

必须重跑：

```powershell
python scripts/malf/audit_malf_day_semantics.py --run-id day-a1c965e1f7a9 --sample-count 12
```

但本轮不包含 live build，因此：

- 若 formal ledger 未用新 engine 重算，live audit 结果允许保持旧 run 语义
- evidence / conclusion 必须显式区分“代码语义已落地”与“live formal ledger 尚未重算”

## 8. 冻结结论

本文冻结以下结论：

1. Stage 19 只修 `engine` 纯语义，不修工程壳。
2. authority stack 改为图版优先。
3. `reborn` 不得再压缩成“候选 guard 与首次确认同 bar”。
4. `guard_price` 必须回到“确认式结构锚点”。
5. `wave_position_zone` 继续保留四区与统一坐标系，不在本轮改 `audit.py` 硬规则。
