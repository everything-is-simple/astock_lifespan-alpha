# 批次 57 证据：alpha live freeze audit

证据编号：`57`
日期：`2026-04-23`
文档标识：`alpha-live-freeze-audit`

## 1. preflight

正式 preflight 确认：

- 工作区：`H:\astock_lifespan-alpha`
- 起始分支：`lifespan0.01/card56-pipeline-live-freeze-gate`
- Card 57 分支：`lifespan0.01/card57-alpha-live-freeze-audit`
- 起始 git 状态只有未跟踪 `.vscode/`
- 无活跃 `run_alpha_*` / `run_alpha_signal_build.py` Python 进程

Card 57 前最新正式 alpha runs：

- `bof-2bfa3f351665`
- `tst-c986697a870d`
- `pb-172c010e4ba2`
- `cpb-abede9a0e185`
- `bpb-aff8057c665c`
- `alpha-signal-a16700405abf`

Card 57 前 live formal DB 口径：

- 5 个 trigger 库均存在 `alpha_run / alpha_work_queue / alpha_checkpoint / alpha_trigger_event / alpha_trigger_profile`
- `alpha_signal.duckdb` 存在 `alpha_signal_run / alpha_signal_work_queue / alpha_signal_checkpoint / alpha_signal`
- `alpha_signal = 5892934`
- `alpha_signal distinct symbol = 5497`
- `alpha_signal max(signal_date) = 2026-04-10`
- `position_candidate_audit = 5892934`
- `position_candidate_audit distinct symbol = 5497`
- `position_candidate_audit max(signal_date) = 2026-04-10`
- 最新 `position_run.alpha_source_path = H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`

## 2. 当前合同审计

现行 `astock` alpha 正式合同只覆盖：

- 5 个 trigger runner：
  - `run_alpha_bof_build`
  - `run_alpha_tst_build`
  - `run_alpha_pb_build`
  - `run_alpha_cpb_build`
  - `run_alpha_bpb_build`
- `run_alpha_signal_build`
- `alpha_trigger_event / alpha_trigger_profile`
- `alpha_signal`
- shared input adapter / checkpoint / replay

当前正式输入边界仍是：

- `market_base`
- `malf_day.malf_wave_scale_snapshot`

未发现当前 `astock alpha` 依赖历史 PAS 体系中的额外 raw/factor 表。

## 3. 局部 gate

本轮先执行 alpha 局部验证：

```text
pytest tests/unit/alpha -q
5 passed in 5.99s

pytest tests/unit/docs/test_alpha_specs.py -q
2 passed in 0.04s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.07s
```

## 4. live audit

正式执行命令组：

- `python scripts/alpha/run_alpha_bof_build.py`
- `python scripts/alpha/run_alpha_tst_build.py`
- `python scripts/alpha/run_alpha_pb_build.py`
- `python scripts/alpha/run_alpha_cpb_build.py`
- `python scripts/alpha/run_alpha_bpb_build.py`
- `python scripts/alpha/run_alpha_signal_build.py`

stdout / stderr：

- `bof`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-bof-20260423-135110.stdout.log`
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-bof-20260423-135110.stderr.log`
- `tst`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-tst-20260423-135110.stdout.log`
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-tst-20260423-135110.stderr.log`
- `pb`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-pb-20260423-135110.stdout.log`
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-pb-20260423-135110.stderr.log`
- `cpb`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-cpb-20260423-135110.stdout.log`
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-cpb-20260423-135110.stderr.log`
- `bpb`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-bpb-20260423-135110.stdout.log`
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-bpb-20260423-135110.stderr.log`
- `alpha_signal`
  - stdout：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-signal-20260423-135110.stdout.log`
  - stderr：`H:\Lifespan-report\astock_lifespan_alpha\alpha\alpha-live-card57-signal-20260423-135110.stderr.log`

stderr 观察：

- 6 个 stderr 文件均为空
- 6 个命令 exit code 均为 `0`

stdout summary 摘要：

- `bof` 最新 run：`bof-7f0155fe8bf0`
- `tst` 最新 run：`tst-6eb9d845971d`
- `pb` 最新 run：`pb-ced2863032cf`
- `cpb` 最新 run：`cpb-d3670031d272`
- `bpb` 最新 run：`bpb-6bb1d9858cf2`
- `alpha_signal` 最新 run：`alpha-signal-755796862970`

所有本轮 live audit runs 均：

- `status = completed`
- `work_units_updated / sources_updated = 0`
- 属于 checkpoint 命中后的正式 skip-path 审计通过，而不是失败或中断

## 5. formal DB 回查

5 个 trigger 库回查结果一致：

| scope | latest run | status | symbols_seen | symbols_updated | checkpoint rows | work_queue |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `bof` | `bof-7f0155fe8bf0` | `completed` | 5501 | 0 | 5501 | `skipped = 5501` |
| `tst` | `tst-6eb9d845971d` | `completed` | 5501 | 0 | 5501 | `skipped = 5501` |
| `pb` | `pb-ced2863032cf` | `completed` | 5501 | 0 | 5501 | `skipped = 5501` |
| `cpb` | `cpb-d3670031d272` | `completed` | 5501 | 0 | 5501 | `skipped = 5501` |
| `bpb` | `bpb-6bb1d9858cf2` | `completed` | 5501 | 0 | 5501 | `skipped = 5501` |

累计 trigger rows：

- `bof alpha_trigger_event = 3395478`
- `tst alpha_trigger_event = 1600877`
- `pb alpha_trigger_event = 444924`
- `cpb alpha_trigger_event = 116085`
- `bpb alpha_trigger_event = 335570`

`alpha_signal` 回查结果：

- latest run：`alpha-signal-755796862970`
- `status = completed`
- `source_trigger_count = 5`
- `sources_updated = 0`
- `inserted_signals = 0`
- `alpha_signal_checkpoint = 5`
- `alpha_signal_work_queue.status = skipped (5)`
- `alpha_signal = 5892934`
- `distinct symbol = 5497`
- `max(signal_date) = 2026-04-10`

`alpha_signal.formal_signal_status` 分布：

- `bof.candidate = 1441843`
- `bof.confirmed = 1953635`
- `tst.candidate = 1531890`
- `tst.confirmed = 68987`
- `pb.confirmed = 444924`
- `cpb.candidate = 14484`
- `cpb.confirmed = 101601`
- `bpb.candidate = 281218`
- `bpb.confirmed = 54352`

`position` 消费面回查：

- `position_candidate_audit = 5892934`
- `distinct symbol = 5497`
- `max(signal_date) = 2026-04-10`
- `candidate = 3269435`
- `confirmed = 2623499`
- 最新 `position_run = position-c2c3b1d40a52`
- 最新 `position_run.alpha_source_path = H:\Lifespan-data\astock_lifespan_alpha\alpha\alpha_signal.duckdb`

结论：

- 当前 `position` 继续消费正式 `alpha_signal`
- 本轮 `alpha` live audit 不需要反向修复 `position`

## 6. legacy delta register

本轮只把历史 PAS/alpha 体系作为审计参照，不吸收进当前正式合同。

| 分类 | 当前审计结论 | 参照来源 |
| --- | --- | --- |
| `已在 astock 吸收` | 5 trigger、shared runner、checkpoint、`alpha_signal` 聚合、run 元数据先落库后收口 | `astock alpha` 当前实现；`Validated` 卡 100 的 bootstrap/run-state 治理 |
| `明确不在本卡吸收` | 完整 PAS 因子评分、`opportunity_score / grade`、`risk_reward_ratio`、`quality_flag`、`neutrality`、16-cell/readout 体系 | `EmotionQuant-alpha` PAS 算法设计；`Validated` PAS/backtest/readout 资料 |
| `后续可单开治理卡` | trigger trace 命名治理、历史 run 清理、更丰富的 alpha formal contract、是否引入评分型 producer | `Validated` 卡 100 后续入口；历史 PAS 治理资料 |

审计口径明确为：

- 当前 `astock alpha` 是 **trigger ledger producer**
- 不是 **完整 PAS scoring engine**
- 这个差距在本轮被正式登记为后续治理入口，而不是当前实现缺陷

## 7. 证据裁决

本地验证结果：

```text
pytest tests/unit/alpha -q
5 passed in 5.99s

pytest tests/unit/docs/test_alpha_specs.py -q
2 passed in 0.04s

pytest tests/unit/contracts/test_module_boundaries.py -q
4 passed in 0.07s

pytest
117 passed in 50.17s
```

Card 57 已完成当前 `alpha` producer 合同的 live freeze audit。

因此：

- `alpha = 放行`
- 本轮只通过冻结审计，不吸收历史 PAS 因子体系
- 下一顺序切到 `malf`
