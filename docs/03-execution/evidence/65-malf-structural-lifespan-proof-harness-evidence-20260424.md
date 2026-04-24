# 批次 65 证据：MALF 结构寿命语义证明夹具

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/65-malf-structural-lifespan-proof-harness-card-20260424.md`

## 1. Authority materials

本卡只读引用以下本机材料：

- `H:\Lifespan-Validated\malf-six\001.png`
- `H:\Lifespan-Validated\malf-six\002.png`
- `H:\Lifespan-Validated\malf-six\003.png`
- `H:\Lifespan-Validated\malf-six\004.png`
- `H:\Lifespan-Validated\malf-six\005.png`
- `H:\Lifespan-Validated\malf-six\006.png`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_6页稿.pdf`
- `H:\Lifespan-Validated\MALF_波段标尺正式语义规格_v1_图版_18页稿.pdf`
- `H:\Lifespan-Validated\MALF_第四战场_波段标尺正式语义规格_v1_图版.pdf`
- `H:\Lifespan-Validated\MALF_终极定义文件_六图合并版.pdf`
- `H:\Lifespan-Validated\MALF_终极定义文件_与chatgpt聊天.pdf`

对应已冻结规格：

- `docs/02-spec/26-malf-foundation-canon-v1-20260424.md`
- `docs/03-execution/61-malf-day-formal-target-recovery-and-isolated-regate-conclusion-20260423.md`
- `docs/03-execution/62-malf-foundation-canon-import-and-zone-sampling-conclusion-20260424.md`

## 2. 当前实现证据

当前 MALF engine 语义入口：

- `src/astock_lifespan_alpha/malf/engine.py`

当前 MALF audit 语义入口：

- `src/astock_lifespan_alpha/malf/audit.py`

当前 MALF 回归测试：

- `tests/unit/malf/test_engine.py`
- `tests/unit/malf/test_audit.py`
- `tests/unit/docs/test_malf_specs.py`

## 3. live formal run 只读核对

只读目标：

- `H:\Lifespan-data\astock_lifespan_alpha\malf\malf_day.duckdb`
- `run_id = day-e687a8277f61`

回查结果：

- `malf_run.status = completed`
- `symbols_total = 5501`
- `symbols_completed = 5501`
- `started_at = 2026-04-23 22:31:33.804158`
- `finished_at = 2026-04-23 23:29:01.594726`
- `malf_state_snapshot` 行数：`16,348,113`
- `malf_wave_ledger` 行数：`2,448,048`

`malf_state_snapshot.life_state`：

- `alive = 10,103,382`
- `reborn = 6,244,731`

`malf_wave_ledger.life_state`：

- `alive = 3,300`
- `broken = 2,442,547`
- `reborn = 2,201`

`malf_pivot_ledger.pivot_type`：

- `HH = 1,689,920`
- `HL = 910,194`
- `LH = 931,310`
- `LL = 1,823,511`
- `break_down = 1,221,572`
- `break_up = 1,220,975`

`malf_state_snapshot.wave_position_zone`：

- `early_progress = 6,246,189`
- `mature_progress = 1,872,674`
- `mature_stagnation = 8,227,525`
- `weak_stagnation = 1,725`

证据结论：

- state snapshot 只 materialize `alive / reborn`
- 旧波终止态主要写入 `malf_wave_ledger.broken`
- live formal run 已同时覆盖四个 `wave_position_zone`
- `break_up / break_down` 与 `HH / LL` 在 pivot 类型上保持分离

## 4. 证明夹具证据

新增 engine 夹具固定以下对称场景：

- 上升结构 `break_down` 后，新下降波进入 `reborn`
- 新下降波在 `LL` 出现前保持 `reborn`
- 新下降波出现 `LL` 后转为 `alive`
- 下降结构 `break_up` 后，新上升波进入 `reborn`
- 新上升波在 `HH` 出现前保持 `reborn`
- 新上升波出现 `HH` 后转为 `alive`

## 5. 验证结果

文档契约：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_malf_specs.py -q
```

结果：

- `8 passed`

MALF 单测：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/malf -q
```

结果：

- `32 passed`

模块边界：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```

结果：

- `4 passed`
