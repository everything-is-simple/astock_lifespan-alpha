# 批次 64 证据：Alpha(PAS) 核心升级边界与 legacy delta selection

- 日期：`2026-04-24`
- 对应卡片：`docs/03-execution/64-alpha-pas-upgrade-boundary-and-legacy-delta-selection-card-20260424.md`

## 1. 设计输入

本卡只读对照以下正式结论：

- `docs/03-execution/57-alpha-live-freeze-audit-conclusion-20260423.md`
- `docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`

## 2. live formal DB 回查

执行只读回查后，当前正式 alpha 数据库存在：

- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_bof.duckdb`
- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_tst.duckdb`
- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_pb.duckdb`
- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_cpb.duckdb`
- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_bpb.duckdb`
- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`

各库可验证表：

- 5 个 trigger 库均为：
  - `alpha_run`
  - `alpha_work_queue`
  - `alpha_checkpoint`
  - `alpha_trigger_event`
  - `alpha_trigger_profile`
- `alpha_signal.duckdb` 为：
  - `alpha_signal`
  - `alpha_signal_run`
  - `alpha_signal_work_queue`
  - `alpha_signal_checkpoint`

回查结果：

- 未发现 `opportunity_score`
- 未发现 `grade`
- 未发现 `risk_reward_ratio`
- 未发现 `quality_flag`
- 未发现 `neutrality`
- 未发现 `cell_id / cell_code`
- 未发现 `readout_id`
- 未发现 `condition_matrix_id`
- 未发现 `trigger_trace_id`

证据结论：

- 当前 live formal `alpha` 合同中不存在评分型 PAS 字段。
- 当前系统不存在 `16-cell` 正式能力。

## 3. 下游消费面回查

只读回查 `position.duckdb` 最新 `position_run.alpha_source_path` 结果为：

- `H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`

证据结论：

- `position` 仍只消费正式 `alpha_signal`
- 未发现下游直接消费 trigger 库或历史 PAS readout

## 4. runner / script 对齐证据

当前 `alpha.runner` 可验证 public runner：

- `run_alpha_bof_build`
- `run_alpha_tst_build`
- `run_alpha_pb_build`
- `run_alpha_cpb_build`
- `run_alpha_bpb_build`
- `run_alpha_signal_build`

当前 `scripts/alpha/` 可验证 CLI：

- `run_alpha_bof_build.py`
- `run_alpha_tst_build.py`
- `run_alpha_pb_build.py`
- `run_alpha_cpb_build.py`
- `run_alpha_bpb_build.py`
- `run_alpha_signal_build.py`

证据结论：

- 当前 runner / script 名称已一一对应
- 下一轮只需要把这种对应关系补成正式治理短语与文档契约

## 5. 本卡裁决依据

结合 Card 57 与 Card 63，可直接成立：

- 当前 `alpha` 已放行
- 当前 `alpha` 是 `trigger ledger producer`
- 当前 `alpha` 不是完整 `PAS scoring engine`
- 历史 PAS 评分、quality filter、trade-linked readout 不进入当前正式合同
- `16-cell` 只作为前代历史素材登记，当前系统不存在

## 6. 验证结果

文档契约：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/docs/test_alpha_specs.py -q
```

结果：

- `4 passed in 0.05s`

Alpha 单测：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/alpha -q
```

结果：

- `5 passed in 5.04s`

模块边界：

```powershell
D:\miniconda\py310\python.exe -m pytest tests/unit/contracts/test_module_boundaries.py -q
```

结果：

- `4 passed in 0.06s`
